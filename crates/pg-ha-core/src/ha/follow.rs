//! Follow/rejoin logic: streaming replication configuration, upstream following,
//! rejoining as replica via pg_rewind/pg_basebackup.

use tracing::{error, info, warn};

use crate::cascading::CascadeManager;
use crate::cluster::MemberRole;

use super::identity::{FollowerAction, follower_action};
use super::{CycleResult, Ha};

impl Ha {
    /// Follow the appropriate upstream node, using cascading replication when configured.
    ///
    /// If this node has the `replicatefrom` tag set, it will attempt to stream from
    /// the tagged member. If that member is unavailable, it falls back to the primary.
    pub(super) async fn follow_upstream(&mut self, lock_owner: &str) -> CycleResult {
        let replicatefrom = self.config.tags.replicatefrom.as_deref();

        let upstream =
            CascadeManager::select_upstream(&self.config.name, replicatefrom, &self.cluster);

        let upstream_name = match upstream {
            Some(m) => m.name.clone(),
            None => lock_owner.to_string(),
        };

        // If replicatefrom is set but the source is unhealthy, log the fallback
        if let Some(source) = replicatefrom
            && !CascadeManager::is_cascade_source_healthy(source, &self.cluster)
        {
            warn!(
                source = source,
                fallback = %upstream_name,
                "cascade source failed, redirecting to primary"
            );
        }

        // ─── What is this node, given that another node holds the lock? ───
        // Decided from this cycle's observation (see `identity`).
        let identity = self.pg_identity();
        match follower_action(identity) {
            FollowerAction::Rejoin => {
                // Old primary that lost the lock (S4): convert it via pg_rewind.
                info!(
                    "PostgreSQL is a primary but another node ({}) holds the leader lock — initiating rejoin as replica",
                    lock_owner
                );
                return self.rejoin_as_replica(lock_owner).await;
            }
            FollowerAction::HandOff => {
                let cause = if self.has_recovery_signal() {
                    "recovery.signal present (archive or point-in-time recovery)"
                } else {
                    "neither standby.signal nor recovery.signal present"
                };
                error!(
                    cause,
                    "PostgreSQL is in recovery pg-ha did not create; not following, rewinding or \
                     restarting it. Operator action required: finish the recovery, or recreate \
                     standby.signal to hand the node back to pg-ha"
                );
                let _ = self.touch_member().await;
                return CycleResult::Error(format!(
                    "PostgreSQL in unmanaged recovery ({cause}); operator action required"
                ));
            }
            FollowerAction::Fence => {
                warn!(
                    "Cannot observe PostgreSQL recovery state and there is no standby.signal, while \
                     {} holds the leader lock — stopping PostgreSQL so it cannot serve writes",
                    lock_owner
                );
                if let Err(e) = self.postgresql.stop("fast").await {
                    error!("Failed to stop PostgreSQL while fencing: {e}");
                    return CycleResult::Error(format!("Fence stop failed: {e}"));
                }
                return CycleResult::Follower(
                    "stopped PostgreSQL: recovery state unobserved and no standby.signal".into(),
                );
            }
            FollowerAction::Wait => {
                let _ = self.touch_member().await;
                return CycleResult::Follower(format!(
                    "PostgreSQL is {}; waiting for the next cycle before following ({})",
                    identity.describe(),
                    upstream_name
                ));
            }
            FollowerAction::Follow => {}
        }

        // A standby pg-ha configured: keep it following the right upstream.
        {
            self.postgresql.set_role(MemberRole::Replica);

            // ─── Check if primary_conninfo needs updating ───
            // If the upstream changed (e.g., after failover), we need to reconfigure
            // PG to stream from the new primary instead of the old one.
            // BUT: only do this if PG is NOT currently streaming successfully.
            // If it's already streaming (even from a cascade source), don't interrupt it.
            let expected_upstream = upstream_name.clone();
            let current_upstream = self.read_current_upstream();

            // Get the expected host from the upstream member's conn_url
            let expected_host = self
                .cluster
                .get_member(&expected_upstream)
                .map(|m| {
                    m.conn_url
                        .split_whitespace()
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
                let conn_url = self
                    .cluster
                    .get_member(&expected_upstream)
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
                warn!(
                    "Cannot rejoin: leader '{}' not found in cluster members",
                    leader_name
                );
                return CycleResult::Error(format!(
                    "Cannot rejoin: leader '{}' not found",
                    leader_name
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
        self.write_standby_config_for_rejoin(&leader_connstr).await;

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
                // pg_rewind appeared to succeed but PG can't start (e.g., timeline
                // history files missing, or checkpoint at exact fork point).
                // Fallback: full clone via pg_basebackup.
                warn!("PG failed to start after pg_rewind: {e} — falling back to pg_basebackup");
                if let Err(re) = self.postgresql.remove_data_directory() {
                    return CycleResult::Error(format!(
                        "Rejoin fallback failed: start={e}, cleanup={re}"
                    ));
                }
                match self.postgresql.basebackup(&leader_connstr).await {
                    Ok(()) => {
                        self.write_standby_config_for_rejoin(&leader_connstr).await;
                        match self.postgresql.start().await {
                            Ok(()) => {
                                self.postgresql.set_role(MemberRole::Replica);
                                let _ = self.touch_member().await;
                                info!("Rejoined cluster via pg_basebackup fallback");
                                CycleResult::Follower(format!(
                                    "rejoined as replica via basebackup fallback, following ({})",
                                    leader_name
                                ))
                            }
                            Err(e2) => CycleResult::Error(format!(
                                "Failed to start after basebackup fallback: {e2}"
                            )),
                        }
                    }
                    Err(be) => {
                        CycleResult::Error(format!("Rejoin fallback basebackup failed: {be}"))
                    }
                }
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
        let repl_pass = self
            .config
            .postgresql
            .replication
            .password
            .as_deref()
            .unwrap_or("");
        let primary_conninfo = format!(
            "host={host} port={port} user={repl_user} password={repl_pass} application_name={}",
            self.config.name
        );

        // Write updated postgresql.auto.conf (offload to blocking thread)
        let auto_conf_path = self.config.postgresql.data_dir.join("postgresql.auto.conf");
        let content = format!(
            "# pg-ha managed standby configuration\nprimary_conninfo = '{primary_conninfo}'\nrecovery_target_timeline = 'latest'\n"
        );
        let write_result =
            tokio::task::spawn_blocking(move || std::fs::write(&auto_conf_path, &content)).await;
        match write_result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                error!("Failed to write postgresql.auto.conf: {e}");
                return;
            }
            Err(e) => {
                error!("reconfigure_replica spawn_blocking failed: {e}");
                return;
            }
        }

        info!(
            host = %host, port = %port,
            "Updated primary_conninfo — restarting PostgreSQL to follow new upstream"
        );
        // Befor restarting, check timeline compatibility.
        // If the upstream is on a newer timeline that we can't follow,
        // a simple restart won't work - we need pg_rewind or basebackup.
        let local_tl = self.postgresql.controldata_timeline().await.unwrap_or(0);
        if local_tl > 0 {
            let upstream_tl = self
                .cluster
                .get_member(&host)
                .and_then(|m| m.timeline)
                .unwrap_or(0);

            if upstream_tl > 0 && local_tl != upstream_tl {
                info!(
                    local_timeline = local_tl,
                    upstream_timeline = upstream_tl,
                    "Timeline mismatch — cannot restart, need rejoin via pg_rewind"
                );
                let _ = self.postgresql.stop("fast").await;
                return;
            }
        }

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
    pub(super) async fn write_standby_config_for_rejoin(&self, leader_connstr: &str) {
        let data_dir = self.config.postgresql.data_dir.clone();

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

        let repl_user = self.config.postgresql.replication.username.clone();
        let repl_pass = self
            .config
            .postgresql
            .replication
            .password
            .clone()
            .unwrap_or_default();
        let app_name = self.config.name.clone();
        let primary_conninfo = format!(
            "host={host} port={port} user={repl_user} password={repl_pass} application_name={app_name}",
        );

        let host_log = host.clone();
        let port_log = port.clone();

        // Offload filesystem writes to blocking thread pool
        let write_result = tokio::task::spawn_blocking(move || {
            // Create standby.signal
            let standby_signal = data_dir.join("standby.signal");
            if let Err(e) = std::fs::write(&standby_signal, "") {
                tracing::warn!("Failed to create standby.signal: {e}");
            }

            // Write to postgresql.auto.conf
            let auto_conf_path = data_dir.join("postgresql.auto.conf");
            let auto_conf_content = format!(
                "# pg-ha managed standby configuration (rejoin)\nprimary_conninfo = '{primary_conninfo}'\nrecovery_target_timeline = 'latest'\n"
            );
            if let Err(e) = std::fs::write(&auto_conf_path, auto_conf_content) {
                tracing::warn!("Failed to write postgresql.auto.conf: {e}");
            }
        })
        .await;

        if let Err(e) = write_result {
            warn!("write_standby_config_for_rejoin spawn_blocking failed: {e}");
        }

        info!(host = %host_log, port = %port_log, "Wrote standby configuration for rejoin");
    }
}
