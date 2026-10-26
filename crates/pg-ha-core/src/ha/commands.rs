//! Command processing: switchover, failover, restart, and reinitialize commands
//! received from the management API.

use tracing::{error, info};

use crate::cluster::MemberRole;
use crate::commands::{CommandResponse, ManagementCommand};

use super::Ha;
use super::election::sync_failover_allowed;
use super::identity::PgIdentity;

/// A21: a candidate with the rewind-check marker may not campaign once the
/// lock is released (D6), while the other nodes defer to it.
fn marked_candidate_message(candidate: &str) -> String {
    format!("Candidate '{candidate}' has a rewind-check marker (not yet verified with pg_rewind)")
}

impl Ha {
    /// Process pending management commands from the API
    pub(super) async fn process_commands(&mut self) {
        while let Ok((cmd, reply_tx)) = self.cmd_rx.try_recv() {
            if self.shutting_down {
                let _ = reply_tx
                    .send(CommandResponse::rejected(
                        "Graceful shutdown in progress; command rejected",
                    ))
                    .await;
                continue;
            }
            let response = match cmd {
                ManagementCommand::Switchover {
                    leader,
                    candidate,
                    scheduled_at,
                } => {
                    self.handle_switchover_command(leader, candidate, scheduled_at)
                        .await
                }
                ManagementCommand::Failover { candidate } => {
                    self.handle_failover_command(candidate).await
                }
                ManagementCommand::CancelSwitchover => {
                    self.pending_switchover = None;
                    CommandResponse::accepted("Scheduled switchover cancelled")
                }
                ManagementCommand::Restart => self.handle_restart_command().await,
                ManagementCommand::Reinitialize => self.handle_reinitialize_command().await,
            };
            let _ = reply_tx.send(response).await;
        }
    }

    fn reject_if_sync_ineligible_candidate(&self, candidate: &str) -> Option<CommandResponse> {
        let sync_mode = self
            .dynamic_config_state
            .last_config
            .synchronous_mode
            .unwrap_or(false);
        if sync_failover_allowed(candidate, sync_mode, self.cluster.sync_state.as_ref()) {
            return None;
        }
        Some(CommandResponse::rejected(format!(
            "Candidate '{candidate}' is not a synchronous standby (sync mode requires /sync membership)"
        )))
    }

    async fn handle_switchover_command(
        &mut self,
        leader: Option<String>,
        candidate: Option<String>,
        scheduled_at: Option<String>,
    ) -> CommandResponse {
        // Validate: must be the current leader to initiate switchover
        if !self.is_leader {
            return CommandResponse::rejected("This node is not the leader");
        }

        // Validate leader name if specified
        if let Some(ref expected_leader) = leader
            && expected_leader != &self.config.name
        {
            return CommandResponse::rejected(format!(
                "Leader mismatch: expected '{}', actual '{}'",
                expected_leader, self.config.name
            ));
        }

        // Validate candidate exists and is eligible
        if let Some(ref cand) = candidate {
            match self.cluster.get_member(cand) {
                None => {
                    return CommandResponse::rejected(format!("Candidate '{}' not found", cand));
                }
                Some(m) if m.is_nofailover() => {
                    return CommandResponse::rejected(format!(
                        "Candidate '{}' has nofailover tag",
                        cand
                    ));
                }
                Some(m) if m.state != crate::cluster::MemberState::Running => {
                    return CommandResponse::rejected(format!(
                        "Candidate '{}' is not running",
                        cand
                    ));
                }
                Some(m) if m.rewind_check => {
                    return CommandResponse::rejected(marked_candidate_message(cand));
                }
                _ => {}
            }
            if let Some(resp) = self.reject_if_sync_ineligible_candidate(cand) {
                return resp;
            }
        }

        // If scheduled, store for later
        if scheduled_at.is_some() {
            self.pending_switchover = Some(ManagementCommand::Switchover {
                leader,
                candidate,
                scheduled_at,
            });
            return CommandResponse::accepted("Switchover scheduled");
        }

        // No candidate: the election excludes this leader. Demoting when no
        // other member may campaign would leave the cluster with no primary
        // and a /failover key that only a successful acquisition clears.
        if candidate.is_none() && !self.has_switchover_successor() {
            return CommandResponse::rejected(
                "No other running member can take over; refusing switchover without a candidate",
            );
        }

        // Execute immediate switchover. The leader lock stays until PostgreSQL
        // has stopped, so another node cannot promote while this one is still
        // writable (L5). standby.signal is written before the stop (D2).
        info!(candidate = ?candidate, "Initiating switchover");
        let failover_value = serde_json::json!({
            "leader": self.config.name,
            "candidate": candidate,
        });
        let _ = self
            .dcs
            .set_failover_value(&failover_value.to_string())
            .await;

        // The HA loop is blocked inside this command, so renew once before the
        // stop. The lease then covers ttl - safety_margin from here.
        if !self.renew_held_lock().await {
            let _ = self.fence_for_lost_lease().await;
            return CommandResponse::error(
                "Switchover aborted: leader lock renewal failed; pg-ha keeps fencing it and holds the leader lock",
            );
        }

        // With a candidate, point primary_conninfo at it; without one the new
        // leader is unknown, so primary_conninfo is removed (D3).
        let candidate_conn_url = candidate
            .as_deref()
            .and_then(|c| self.cluster.get_member(c))
            .map(|m| m.conn_url.clone());
        let write_error = self.prepare_demotion(candidate_conn_url, None).await.err();
        if let Some(we) = &write_error {
            error!(
                "Failed to write standby configuration before stopping PostgreSQL: {we}; stopping it anyway"
            );
        }
        let stop_error = self.stop_after_demotion_write("fast").await.err();
        self.postgresql.set_role(MemberRole::Replica);

        // A failed stop comes first: PostgreSQL may still be writable, so the
        // lock is kept and later cycles keep fencing (stop_pending).
        if let Some(e) = stop_error {
            let write = write_error.map_or(String::new(), |we| {
                format!("; writing standby configuration also failed: {we}")
            });
            return CommandResponse::error(format!(
                "Switchover initiated, but stopping PostgreSQL failed: {e}; pg-ha keeps fencing it and holds the leader lock{write}"
            ));
        }

        if !self.release_leader_lock_if_stopped().await {
            let write = write_error.map_or(String::new(), |we| {
                format!("; writing standby configuration also failed: {we}")
            });
            return CommandResponse::error(format!(
                "Switchover stopped PostgreSQL, but the leader lock was not released{write}"
            ));
        }

        if let Some(we) = write_error {
            return CommandResponse::error(format!(
                "Switchover initiated and PostgreSQL stopped, but writing standby configuration failed: {we}"
            ));
        }

        CommandResponse::accepted("Switchover initiated, leader lock released")
    }

    async fn handle_failover_command(&mut self, candidate: Option<String>) -> CommandResponse {
        // Failover can be initiated from any node — it writes /failover key
        if let Some(ref cand) = candidate {
            match self.cluster.get_member(cand) {
                None => {
                    return CommandResponse::rejected(format!("Candidate '{}' not found", cand));
                }
                Some(m) if m.rewind_check => {
                    return CommandResponse::rejected(marked_candidate_message(cand));
                }
                _ => {}
            }
            if let Some(resp) = self.reject_if_sync_ineligible_candidate(cand) {
                return resp;
            }
        }

        info!(candidate = ?candidate, "Initiating manual failover");
        let failover_value = serde_json::json!({
            "candidate": candidate,
        });
        let _ = self
            .dcs
            .set_failover_value(&failover_value.to_string())
            .await;

        CommandResponse::accepted("Failover request submitted")
    }

    async fn handle_restart_command(&mut self) -> CommandResponse {
        // D5: a held node stays stopped until the operator reinitializes it.
        if self.refresh_rewind_hold() {
            return CommandResponse::rejected(
                "PostgreSQL is held after a pg_rewind failure; use POST /reinitialize (see the ERROR log)",
            );
        }
        info!("Restarting PostgreSQL (requested via API)");
        // D1: pg-ha starts PostgreSQL as a primary only while holding the
        // leader lock. `is_leader` is last cycle's value (commands run before
        // get_cluster), so the lock must also be ours in the current view.
        let holding = self.is_leader && self.is_lock_owner();
        let stopped = if holding {
            self.postgresql.stop("fast").await
        } else {
            // Prepare the standby start before touching PostgreSQL; if that
            // fails, leave it as it is.
            if let Err(e) = self.prepare_standby_start().await {
                error!(
                    "Not restarting PostgreSQL: could not write the standby configuration ({e})"
                );
                return CommandResponse::error(format!(
                    "Not restarting PostgreSQL: standby configuration could not be written: {e}"
                ));
            }
            // It may be a writable primary without the lock: a failed stop
            // keeps it fenced (stop_pending).
            self.stop_after_demotion_write("fast").await
        };
        if let Err(e) = stopped {
            return CommandResponse::error(format!("Stop failed: {e}"));
        }
        if let Err(e) = self.postgresql.start().await {
            return CommandResponse::error(format!("Start failed: {e}"));
        }
        // Clear pending_restart flag after successful restart
        self.dynamic_config_state.clear_pending_restart();
        CommandResponse::accepted("PostgreSQL restarted")
    }

    async fn handle_reinitialize_command(&mut self) -> CommandResponse {
        if self.is_leader {
            return CommandResponse::rejected("Cannot reinitialize the leader node");
        }
        info!("Reinitializing node (requested via API)");
        if self.postgresql.is_running() && self.pg_identity() == PgIdentity::Primary {
            // A primary without the lock may be writable: write
            // standby.signal + marker first, and never delete the data
            // directory under a server that could not be stopped.
            if let Err(e) = self
                .fence_primary(
                    "immediate",
                    "stopped for reinitialize while running as a primary without the leader lock",
                    None,
                )
                .await
            {
                return CommandResponse::error(format!(
                    "Reinitialize: stopping PostgreSQL failed: {e}; data directory not removed (pg-ha keeps fencing it)"
                ));
            }
        } else {
            let _ = self.postgresql.stop("immediate").await;
        }
        if let Err(e) = self.postgresql.remove_data_directory() {
            return CommandResponse::error(format!("Remove data dir failed: {e}"));
        }
        // The hold file went with the data directory (D5).
        self.clear_rewind_hold();
        CommandResponse::accepted("Node reinitialized, will clone on next cycle")
    }
}
