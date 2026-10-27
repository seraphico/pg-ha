//! Follow/rejoin logic: streaming replication configuration, upstream following,
//! rejoining as replica via pg_rewind/pg_basebackup.

use tracing::{error, info, warn};

use crate::cascading::CascadeManager;
use crate::cluster::MemberRole;

use super::identity::{
    self, AfterRewindStart, FollowerAction, PgIdentity, follower_action, leader_ready_for_rewind,
    rewind_source_not_ready_reason,
};
use super::{CycleResult, Ha};

/// Error text without the "PostgreSQL error: " wrapper, for the hold message.
fn error_text(e: &crate::error::Error) -> String {
    match e {
        crate::error::Error::Postgres(m) => m.clone(),
        other => other.to_string(),
    }
}

impl Ha {
    /// Follow the appropriate upstream node, using cascading replication when configured.
    ///
    /// If this node has the `replicatefrom` tag set, it will attempt to stream from
    /// the tagged member. If that member is unavailable, it falls back to the primary.
    pub(super) async fn follow_upstream(&mut self, lock_owner: &str) -> CycleResult {
        // D6 / 2A first: data of an older history generation is never
        // followed or rewound onto the new history (also not by D5 Rebuild).
        if let Some(r) = self.enforce_generation().await {
            return r;
        }
        // D5: a held node is kept stopped (also when started by hand).
        if let Some(r) = self.enforce_rewind_hold().await {
            return r;
        }

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
        match follower_action(identity, self.stop_pending, self.has_rewind_check_marker()) {
            FollowerAction::Rejoin => {
                // Old primary that lost the lock (S4): convert it via pg_rewind.
                info!(
                    "PostgreSQL is a primary but another node ({}) holds the leader lock — initiating rejoin as replica",
                    lock_owner
                );
                return self.rejoin_as_replica(lock_owner).await;
            }
            FollowerAction::RewindCheck => {
                // D4: this standby may hold WAL beyond a fork point. Check
                // with pg_rewind once the leader runs as a primary; until
                // then keep it running read-only (the live probe runs in
                // rejoin_as_replica, before anything is stopped).
                if !identity::leader_ready_in_dcs(
                    Some(lock_owner),
                    lock_owner,
                    self.cluster.get_member(lock_owner),
                ) {
                    let _ = self.touch_member().await;
                    return CycleResult::Follower(format!(
                        "rewind-check marker present; waiting for {lock_owner} to run as a primary before pg_rewind"
                    ));
                }
                info!(
                    "Rewind-check marker present and {lock_owner} holds the leader lock — checking with pg_rewind before following"
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
                let retry = self.stop_pending;
                if retry {
                    warn!(
                        "A previous stop after writing standby.signal failed and PostgreSQL is \
                         unobserved, while {} holds the leader lock — stopping PostgreSQL again",
                        lock_owner
                    );
                } else {
                    warn!(
                        "Cannot observe PostgreSQL recovery state and there is no standby.signal, while \
                         {} holds the leader lock — stopping PostgreSQL so it cannot serve writes",
                        lock_owner
                    );
                }
                // Write standby.signal first (D2); the lock holder is the
                // known upstream.
                let upstream = self
                    .cluster
                    .get_member(lock_owner)
                    .map(|m| m.conn_url.clone());
                let written = self.prepare_demotion(upstream, None).await;
                if let Err(e) = &written {
                    error!(
                        "Failed to write standby configuration before stopping PostgreSQL: {e}; stopping it anyway"
                    );
                }
                if let Err(e) = self.stop_after_demotion_write("fast").await {
                    return CycleResult::Error(format!("Fence stop failed: {e}"));
                }
                if let Err(e) = written {
                    return CycleResult::Error(format!(
                        "Fence: stopped PostgreSQL, but standby configuration could not be written: {e}"
                    ));
                }
                return CycleResult::Follower(if retry {
                    "stopped PostgreSQL: a demotion stop had failed".into()
                } else {
                    "stopped PostgreSQL: recovery state unobserved and no standby.signal".into()
                });
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

            let needs_reconfig =
                needs_upstream_reconfig(current_upstream.as_deref(), &expected_host);

            if needs_reconfig && !self.is_recently_reconfigured() {
                let current = current_upstream.unwrap_or_else(|| "none".to_string());
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

    /// Rejoin the cluster as a replica: a former primary, or a standby with
    /// the rewind-check marker (D4).
    ///
    /// Steps:
    /// 0. Stop if held after a pg_rewind failure (D5); stop if the source is
    ///    not ready (A12: lock holder running as a primary in the DCS view
    ///    and in a live check), without counting it as a failure
    /// 1. Stop PostgreSQL (a primary through `fence_primary`)
    /// 2. Check the source live again, then run pg_rewind against it
    /// 3. Remove the marker, write standby.signal + primary_conninfo
    /// 4. Start PostgreSQL in standby mode
    ///
    /// When pg_rewind fails, or PostgreSQL does not start after it, the node
    /// is held (stopped, data directory kept) unless
    /// `postgresql.remove_data_directory_on_rewind_failure` is set, in which
    /// case it is rebuilt with pg_basebackup.
    pub(super) async fn rejoin_as_replica(&mut self, leader_name: &str) -> CycleResult {
        let rebuild_enabled = self
            .config
            .postgresql
            .remove_data_directory_on_rewind_failure;

        // Step 0a: a held node does not rewind again on its own.
        if self.refresh_rewind_hold() && !rebuild_enabled {
            return self.hold_stop_and_report().await;
        }

        // Step 0b: only rewind from a ready lock holder. The live probe runs
        // only when the DCS view already says ready, and before any stop.
        let lock_holder = self.cluster.leader.as_ref().map(|l| l.name.clone());
        let member = self.cluster.get_member(leader_name).cloned();
        let dcs_ready =
            identity::leader_ready_in_dcs(lock_holder.as_deref(), leader_name, member.as_ref());
        let probe = match (&member, dcs_ready) {
            (Some(m), true) => self.postgresql.source_answers_as_primary(&m.conn_url).await,
            _ => None,
        };
        if !leader_ready_for_rewind(lock_holder.as_deref(), leader_name, member.as_ref(), probe) {
            // Not a failure: never enters the D5 hold.
            let why = rewind_source_not_ready_reason(dcs_ready, probe);
            if self.pg_identity() == PgIdentity::Primary {
                warn!(
                    "PostgreSQL is a primary without the leader lock and {leader_name} {why} — stopping PostgreSQL; it will start as a standby and be rewound once the leader is ready"
                );
                // D3: point primary_conninfo only at this cycle's lock holder.
                let upstream = if lock_holder.as_deref() == Some(leader_name) {
                    member.as_ref().map(|m| m.conn_url.clone())
                } else {
                    None
                };
                if let Err(e) = self
                    .fence_primary(
                        "fast",
                        "stopped while running as a primary without the leader lock; leader not ready for pg_rewind",
                        upstream,
                    )
                    .await
                {
                    return CycleResult::Error(format!("Rejoin stop failed: {e}"));
                }
                return CycleResult::Follower(format!(
                    "stopped PostgreSQL: {leader_name} {why}; pg_rewind will run once it is ready"
                ));
            }
            info!("Not running pg_rewind: {leader_name} {why}");
            let _ = self.touch_member().await;
            return CycleResult::Follower(format!("not running pg_rewind: {leader_name} {why}"));
        }
        // Ready implies the member record exists.
        let Some(leader_connstr) = member.map(|m| m.conn_url) else {
            return CycleResult::Error(format!("Cannot rejoin: leader '{leader_name}' not found"));
        };

        // Step 1: Stop PostgreSQL (only if it's running). A primary without
        // the lock may be writable: write standby.signal + marker first (D2).
        if self.postgresql.is_running() {
            info!("Stopping PostgreSQL for rejoin (pg_rewind)");
            let stopped = if self.pg_identity() == PgIdentity::Primary {
                self.fence_primary(
                    "fast",
                    "stopped for pg_rewind while running as a primary without the leader lock",
                    Some(leader_connstr.clone()),
                )
                .await
            } else {
                self.postgresql.stop("fast").await
            };
            if let Err(e) = stopped {
                error!("Failed to stop PostgreSQL for rejoin: {e}");
                return CycleResult::Error(format!("Rejoin stop failed: {e}"));
            }
        }

        // Step 2: the local stop can take long on a large database; check the
        // source again right before pg_rewind. Not ready is not a failure:
        // PostgreSQL stays stopped and starts as a standby next cycle (D1).
        let probe = self
            .postgresql
            .source_answers_as_primary(&leader_connstr)
            .await;
        let member_now = self.cluster.get_member(leader_name);
        if !leader_ready_for_rewind(lock_holder.as_deref(), leader_name, member_now, probe) {
            let why = rewind_source_not_ready_reason(true, probe);
            info!(
                "Not running pg_rewind: {leader_name} {why}; PostgreSQL stays stopped and starts as a standby next cycle"
            );
            let _ = self.touch_member().await;
            return CycleResult::Follower(format!(
                "not running pg_rewind: {leader_name} {why}; PostgreSQL stopped, it starts as a standby next cycle"
            ));
        }

        info!(source = %leader_name, "Running pg_rewind to resync from current primary");
        if let Err(e) = self.postgresql.rewind(&leader_connstr).await {
            if rebuild_enabled {
                warn!(
                    "pg_rewind failed: {e} — remove_data_directory_on_rewind_failure is enabled, rebuilding with pg_basebackup"
                );
                return self
                    .rebuild_from_leader(leader_name, &leader_connstr, "rewind", &e)
                    .await;
            }
            return self.enter_rewind_hold("pg_rewind", &error_text(&e)).await;
        }

        // Step 3: the rewind check is done (also for "no rewind required").
        // Files pg_rewind copied from the source are dropped with it (A16).
        let data_dir = self.config.postgresql.data_dir.clone();
        let removed = tokio::task::spawn_blocking(move || {
            crate::standby_config::remove_node_state_files(&data_dir)
        })
        .await
        .map_err(std::io::Error::other)
        .and_then(|r| r);
        match removed {
            Ok(()) => info!("pg_rewind succeeded; removed the rewind-check marker"),
            Err(e) => {
                warn!("pg_rewind succeeded but the rewind-check marker could not be removed: {e}")
            }
        }
        self.clear_rewind_hold();

        // D1: without standby.signal PostgreSQL would start as a primary, so a
        // failed write means no start.
        if let Err(e) = self.write_standby_config(Some(&leader_connstr)).await {
            error!(
                "Failed to write standby configuration for rejoin: {e}; not starting PostgreSQL"
            );
            return CycleResult::Error(format!(
                "Rejoin: not starting PostgreSQL, standby configuration could not be written: {e}"
            ));
        }

        // Step 4: Start PostgreSQL in standby mode
        let start = self.postgresql.start().await;
        let outcome = identity::after_rewind_start(start.is_ok(), self.postgresql.is_running());
        match (outcome, start) {
            (AfterRewindStart::Started, _) | (_, Ok(())) => {
                self.postgresql.set_role(MemberRole::Replica);
                let _ = self.touch_member().await;
                info!("Successfully rejoined cluster as replica");
                CycleResult::Follower(format!(
                    "rejoined as replica via pg_rewind, following ({})",
                    leader_name
                ))
            }
            (AfterRewindStart::StillStarting, Err(e)) => {
                // A28: pg_ctl -w gave up, but the postmaster is still
                // replaying to the consistent point (large databases).
                warn!(
                    "PostgreSQL is still starting after pg_rewind ({e}); not treating this as a failure"
                );
                self.postgresql
                    .set_state(crate::postgresql::PgState::Starting);
                self.postgresql.set_role(MemberRole::Replica);
                let _ = self.touch_member().await;
                CycleResult::Follower("rejoined via pg_rewind; PostgreSQL still starting".into())
            }
            (AfterRewindStart::Failed, Err(e)) => {
                if rebuild_enabled {
                    warn!(
                        "PostgreSQL failed to start after pg_rewind: {e} — remove_data_directory_on_rewind_failure is enabled, rebuilding with pg_basebackup"
                    );
                    return self
                        .rebuild_from_leader(leader_name, &leader_connstr, "start", &e)
                        .await;
                }
                self.enter_rewind_hold("Starting PostgreSQL after pg_rewind", &error_text(&e))
                    .await
            }
        }
    }

    /// D5 with `remove_data_directory_on_rewind_failure: true`: delete the
    /// data directory and clone it again from the leader, then start as a
    /// standby. `failed` names the step that failed ("rewind" or "start").
    async fn rebuild_from_leader(
        &mut self,
        leader_name: &str,
        leader_connstr: &str,
        failed: &str,
        e: &crate::error::Error,
    ) -> CycleResult {
        if let Err(re) = self.postgresql.remove_data_directory() {
            error!("Failed to remove data directory for fresh clone: {re}");
            return CycleResult::Error(format!("Rejoin failed: {failed}={e}, cleanup={re}"));
        }
        if let Err(be) = self.postgresql.basebackup(leader_connstr).await {
            return CycleResult::Error(format!("Rejoin failed: {failed}={e}, basebackup={be}"));
        }
        info!("pg_basebackup fallback succeeded");
        // The clone carries the source's pg-ha state files (A16).
        let data_dir = self.config.postgresql.data_dir.clone();
        let removed = tokio::task::spawn_blocking(move || {
            crate::standby_config::remove_node_state_files(&data_dir)
        })
        .await
        .map_err(std::io::Error::other)
        .and_then(|r| r);
        if let Err(re) = removed {
            warn!("Could not remove pg-ha state files copied by pg_basebackup: {re}");
        }
        self.clear_rewind_hold();

        if let Err(e) = self.write_standby_config(Some(leader_connstr)).await {
            error!(
                "Failed to write standby configuration for rejoin: {e}; not starting PostgreSQL"
            );
            return CycleResult::Error(format!(
                "Rejoin: not starting PostgreSQL, standby configuration could not be written: {e}"
            ));
        }
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
            Err(e2) => {
                CycleResult::Error(format!("Failed to start after basebackup fallback: {e2}"))
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

        // Rewrite primary_conninfo (other postgresql.auto.conf lines are kept).
        // D1: only reached for a ManagedStandby, and the writer always writes
        // standby.signal; a failed write returns before the stop, so the
        // restart below never starts PostgreSQL as a primary.
        if let Err(e) = self.write_standby_config(Some(new_upstream_connstr)).await {
            error!("Failed to write standby configuration: {e}");
            return;
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
}

/// D3: reconfigure when the expected upstream is known and the configured one
/// differs from it or is missing (no primary_conninfo, e.g. removed at
/// demotion because the new leader was unknown).
pub(super) fn needs_upstream_reconfig(current: Option<&str>, expected_host: &str) -> bool {
    !expected_host.is_empty() && current != Some(expected_host)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn needs_upstream_reconfig_table() {
        let table = [
            (None, "", false),
            (None, "node1", true),
            (Some("node1"), "node1", false),
            (Some("node1"), "node2", true),
            (Some("node1"), "", false),
        ];
        for (current, expected_host, expected) in table {
            assert_eq!(
                needs_upstream_reconfig(current, expected_host),
                expected,
                "{current:?} {expected_host:?}"
            );
        }
    }
}
