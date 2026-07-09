//! Follow/rejoin logic: streaming replication configuration, upstream following,
//! rejoining as replica via pg_rewind/pg_basebackup.

use tracing::{error, info, warn};

use crate::cascading::CascadeManager;
use crate::cluster::MemberRole;

use super::{CycleResult, Ha};

impl Ha {
    /// Follow the appropriate upstream node, using cascading replication when configured.
    ///
    /// If this node has the `replicatefrom` tag set, it will attempt to stream from
    /// the tagged member. If that member is unavailable, it falls back to the primary.
    pub(super) async fn follow_upstream(&mut self, lock_owner: &str) -> CycleResult {
        let replicatefrom = self.config.tags.replicatefrom.as_deref();

        let upstream = CascadeManager::select_upstream(
            &self.config.name,
            replicatefrom,
            &self.cluster,
        );

        let upstream_name = match upstream {
            Some(m) => m.name.clone(),
            None => lock_owner.to_string(),
        };

        // If replicatefrom is set but the source is unhealthy, log the fallback
        if let Some(source) = replicatefrom
            && !CascadeManager::is_cascade_source_healthy(source, &self.cluster) {
                warn!(
                    source = source,
                    fallback = %upstream_name,
                    "cascade source failed, redirecting to primary"
                );
            }

        // ─── Key check: is this node running as primary but NOT holding the lock? ───
        // This happens when an old primary restarts after failover.
        // It needs to be converted to a streaming replica via pg_rewind.
        if self.postgresql.is_running() && self.postgresql.is_primary() {
            info!(
                "This node is running as primary but another node ({}) holds the leader lock — initiating rejoin as replica",
                lock_owner
            );
            return self.rejoin_as_replica(lock_owner).await;
        }

        // Also check: PG is running but NOT in recovery and no standby.signal exists
        // This covers the case where role tracking says Replica but PG is actually a standalone primary
        if self.postgresql.is_running() && !self.has_standby_signal() {
            info!(
                "Node has no standby.signal — needs reconfiguration to follow {}",
                upstream_name
            );
            return self.rejoin_as_replica(lock_owner).await;
        }

        // Ensure role is set to Replica when following
        if self.postgresql.is_running() && self.has_standby_signal() {
            self.postgresql.set_role(MemberRole::Replica);

            // ─── Check if primary_conninfo needs updating ───
            // If the upstream changed (e.g., after failover), we need to reconfigure
            // PG to stream from the new primary instead of the old one.
            // BUT: only do this if PG is NOT currently streaming successfully.
            // If it's already streaming (even from a cascade source), don't interrupt it.
            let expected_upstream = upstream_name.clone();
            let current_upstream = self.read_current_upstream();

            // Get the expected host from the upstream member's conn_url
            let expected_host = self.cluster.get_member(&expected_upstream)
                .map(|m| {
                    m.conn_url.split_whitespace()
                        .find(|p| p.starts_with("host="))
                        .and_then(|p| p.strip_prefix("host="))
                        .unwrap_or("")
                        .to_string()
                })
                .unwrap_or_default();

            let needs_reconfig = current_upstream
                .as_ref()
                .is_some_and(|current| !expected_host.is_empty() && current != &expected_host);

            if needs_reconfig && !self.is_recently_reconfigured() {
                let current = current_upstream.unwrap_or_default();
                info!(
                    current_upstream = %current,
                    expected_upstream = %expected_host,
                    "Upstream changed and PG not streaming — reconfiguring replica"
                );
                let conn_url = self.cluster.get_member(&expected_upstream)
                    .map(|m| m.conn_url.clone());
                if let Some(url) = conn_url {
                    self.reconfigure_replica(&url).await;
                }
            }
        }

        // Touch member heartbeat
        let _ = self.touch_member().await;

        CycleResult::Follower(format!(
            "no action. I am ({}), a secondary, following upstream ({})",
            self.config.name, upstream_name
        ))
    }

    /// Rejoin the cluster as a replica after being a former primary.
    ///
    /// Steps:
    /// 1. Stop PostgreSQL (if running as primary)
    /// 2. Run pg_rewind to resync from the current primary
    /// 3. Write standby.signal + primary_conninfo
    /// 4. Restart PostgreSQL in standby mode
    ///
    /// If pg_rewind fails (e.g., WAL diverged too much), falls back to pg_basebackup.
    pub(super) async fn rejoin_as_replica(&mut self, leader_name: &str) -> CycleResult {
        // Get the leader's connection info for pg_rewind
        let leader_member = self.cluster.get_member(leader_name);
        let leader_connstr = match leader_member {
            Some(m) => m.conn_url.clone(),
            None => {
                warn!("Cannot rejoin: leader '{}' not found in cluster members", leader_name);
                return CycleResult::Error(format!(
                    "Cannot rejoin: leader '{}' not found", leader_name
                ));
            }
        };

        // Step 1: Stop PostgreSQL (only if it's running)
        if self.postgresql.is_running() {
            info!("Stopping PostgreSQL for rejoin (pg_rewind)");
            if let Err(e) = self.postgresql.stop("fast").await {
                error!("Failed to stop PostgreSQL for rejoin: {e}");
                return CycleResult::Error(format!("Rejoin stop failed: {e}"));
            }
        }

        // Step 2: Run pg_rewind
        info!(source = %leader_name, "Running pg_rewind to resync from current primary");
        let rewind_result = self.postgresql.rewind(&leader_connstr).await;

        match rewind_result {
            Ok(()) => {
                info!("pg_rewind succeeded");
            }
            Err(e) => {
                warn!("pg_rewind failed: {e} — falling back to pg_basebackup");
                // Fallback: remove data and clone fresh
                if let Err(re) = self.postgresql.remove_data_directory() {
                    error!("Failed to remove data directory for fresh clone: {re}");
                    return CycleResult::Error(format!("Rejoin failed: rewind={e}, cleanup={re}"));
                }
                // Clone from leader
                match self.postgresql.basebackup(&leader_connstr).await {
                    Ok(()) => {
                        info!("pg_basebackup fallback succeeded");
                    }
                    Err(be) => {
                        return CycleResult::Error(format!(
                            "Rejoin failed: rewind={e}, basebackup={be}"
                        ));
                    }
                }
            }
        }

        // Step 3: Write standby.signal + primary_conninfo
        self.write_standby_config_for_rejoin(&leader_connstr);

        // Step 4: Start PostgreSQL in standby mode
        match self.postgresql.start().await {
            Ok(()) => {
                self.postgresql.set_role(MemberRole::Replica);
                let _ = self.touch_member().await;
                info!("Successfully rejoined cluster as replica");
                CycleResult::Follower(format!(
                    "rejoined as replica via pg_rewind, following ({})",
                    leader_name
                ))
            }
            Err(e) => {
                CycleResult::Error(format!("Failed to start PostgreSQL after rejoin: {e}"))
            }
        }
    }

    /// Reconfigure a running replica to follow a new upstream.
    /// Updates primary_conninfo in postgresql.auto.conf and restarts PG.
    pub(super) async fn reconfigure_replica(&mut self, new_upstream_connstr: &str) {
        // Parse host and port from the new upstream's conn_url
        let mut host = "127.0.0.1".to_string();
        let mut port = "5432".to_string();
        for part in new_upstream_connstr.split_whitespace() {
            if let Some(val) = part.strip_prefix("host=") {
                host = val.to_string();
            } else if let Some(val) = part.strip_prefix("port=") {
                port = val.to_string();
            }
        }

        let repl_user = &self.config.postgresql.replication.username;
        let repl_pass = self.config.postgresql.replication.password.as_deref().unwrap_or("");
        let primary_conninfo = format!(
            "host={host} port={port} user={repl_user} password={repl_pass} application_name={}",
            self.config.name
        );

        // Write updated postgresql.auto.conf
        let auto_conf_path = self.config.postgresql.data_dir.join("postgresql.auto.conf");
        let content = format!(
            "# pg-ha managed standby configuration\nprimary_conninfo = '{primary_conninfo}'\nrecovery_target_timeline = 'latest'\n"
        );
        if let Err(e) = std::fs::write(&auto_conf_path, &content) {
            error!("Failed to write postgresql.auto.conf: {e}");
            return;
        }

        info!(
            host = %host, port = %port,
            "Updated primary_conninfo — restarting PostgreSQL to follow new upstream"
        );

        // Restart PG to pick up the new primary_conninfo
        if let Err(e) = self.postgresql.stop("fast").await {
            error!("Failed to stop PG for reconfiguration: {e}");
            return;
        }
        if let Err(e) = self.postgresql.start().await {
            error!("Failed to restart PG after reconfiguration: {e}");
        }
    }

    /// Write standby.signal and primary_conninfo for a node rejoining as replica.
    pub(super) fn write_standby_config_for_rejoin(&self, leader_connstr: &str) {
        let data_dir = &self.config.postgresql.data_dir;

        // Create standby.signal
        let standby_signal = data_dir.join("standby.signal");
        if let Err(e) = std::fs::write(&standby_signal, "") {
            warn!("Failed to create standby.signal: {e}");
        }

        // Parse host/port from leader connection string
        let mut host = "127.0.0.1".to_string();
        let mut port = "5432".to_string();
        for part in leader_connstr.split_whitespace() {
            if let Some(val) = part.strip_prefix("host=") {
                host = val.to_string();
            } else if let Some(val) = part.strip_prefix("port=") {
                port = val.to_string();
            }
        }

        let repl_user = &self.config.postgresql.replication.username;
        let repl_pass = self.config.postgresql.replication.password.as_deref().unwrap_or("");
        let primary_conninfo = format!(
            "host={host} port={port} user={repl_user} password={repl_pass} application_name={}",
            self.config.name
        );

        // Write to postgresql.auto.conf
        let auto_conf_path = data_dir.join("postgresql.auto.conf");
        let auto_conf_content = format!(
            "# pg-ha managed standby configuration (rejoin)\nprimary_conninfo = '{primary_conninfo}'\nrecovery_target_timeline = 'latest'\n"
        );
        if let Err(e) = std::fs::write(&auto_conf_path, auto_conf_content) {
            warn!("Failed to write postgresql.auto.conf: {e}");
        }

        info!("Wrote standby configuration for rejoin (host={host}, port={port})");
    }
}
