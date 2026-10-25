//! Command processing: switchover, failover, restart, and reinitialize commands
//! received from the management API.

use tracing::{error, info};

use crate::cluster::MemberRole;
use crate::commands::{CommandResponse, ManagementCommand};

use super::Ha;
use super::election::sync_failover_allowed;
use super::identity::PgIdentity;

impl Ha {
    /// Process pending management commands from the API
    pub(super) async fn process_commands(&mut self) {
        while let Ok((cmd, reply_tx)) = self.cmd_rx.try_recv() {
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

        // Execute immediate switchover: write /failover key, then release lock
        info!(candidate = ?candidate, "Initiating switchover");
        let failover_value = serde_json::json!({
            "leader": self.config.name,
            "candidate": candidate,
        });
        let _ = self
            .dcs
            .set_failover_value(&failover_value.to_string())
            .await;

        // Release leader lock — this triggers election
        if let Some(leader) = &self.cluster.leader.clone() {
            let _ = self.dcs.delete_leader(leader).await;
        }
        self.is_leader = false;
        self.reset_sync_cache();

        // Write standby.signal before stopping PostgreSQL (D2), so this node
        // restarts as a standby. With a candidate, point primary_conninfo at
        // it; without one the new leader is unknown, so primary_conninfo is
        // removed (D3).
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

        // A failed stop comes first: PostgreSQL may still be writable.
        match (write_error, stop_error) {
            (None, None) => {}
            (None, Some(e)) => {
                return CommandResponse::error(format!(
                    "Switchover initiated, but stopping PostgreSQL failed: {e}; pg-ha keeps fencing it"
                ));
            }
            (Some(we), Some(e)) => {
                return CommandResponse::error(format!(
                    "Switchover initiated, but stopping PostgreSQL failed: {e}; pg-ha keeps fencing it; writing standby configuration also failed: {we}"
                ));
            }
            (Some(we), None) => {
                return CommandResponse::error(format!(
                    "Switchover initiated and PostgreSQL stopped, but writing standby configuration failed: {we}"
                ));
            }
        }

        CommandResponse::accepted("Switchover initiated, leader lock released")
    }

    async fn handle_failover_command(&mut self, candidate: Option<String>) -> CommandResponse {
        // Failover can be initiated from any node — it writes /failover key
        if let Some(ref cand) = candidate {
            if self.cluster.get_member(cand).is_none() {
                return CommandResponse::rejected(format!("Candidate '{}' not found", cand));
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
        info!("Restarting PostgreSQL (requested via API)");
        if let Err(e) = self.postgresql.stop("fast").await {
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
        CommandResponse::accepted("Node reinitialized, will clone on next cycle")
    }
}
