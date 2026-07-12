//! Command processing: switchover, failover, restart, and reinitialize commands
//! received from the management API.

use tracing::info;

use crate::cluster::MemberRole;
use crate::commands::{CommandResponse, ManagementCommand};

use super::Ha;

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

        // Stop PostgreSQL to ensure clean demotion
        let _ = self.postgresql.stop("fast").await;
        self.postgresql.set_role(MemberRole::Replica);

        // Write standby.signal so this node is recognized as a replica on next cycle
        // (prevents it from re-acquiring the lock as a stale primary)
        let standby_signal = self.config.postgresql.data_dir.join("standby.signal");
        let _ = std::fs::write(&standby_signal, "");

        // Write primary_conninfo pointing to the candidate (new primary)
        // so when PG restarts it streams from the correct source
        if let Some(ref cand_name) = candidate
            && let Some(cand_member) = self.cluster.get_member(cand_name)
        {
            let mut host = "127.0.0.1".to_string();
            let mut port = "5432".to_string();
            for part in cand_member.conn_url.split_whitespace() {
                if let Some(val) = part.strip_prefix("host=") {
                    host = val.to_string();
                }
                if let Some(val) = part.strip_prefix("port=") {
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
            let conninfo = format!(
                "host={host} port={port} user={repl_user} password={repl_pass} application_name={}",
                self.config.name
            );
            let auto_conf = self.config.postgresql.data_dir.join("postgresql.auto.conf");
            let content = format!(
                "# pg-ha managed (switchover demotion)\nprimary_conninfo = '{conninfo}'\nrecovery_target_timeline = 'latest'\n"
            );
            let _ = std::fs::write(&auto_conf, content);
        }

        CommandResponse::accepted("Switchover initiated, leader lock released")
    }

    async fn handle_failover_command(&mut self, candidate: Option<String>) -> CommandResponse {
        // Failover can be initiated from any node — it writes /failover key
        if let Some(ref cand) = candidate
            && self.cluster.get_member(cand).is_none()
        {
            return CommandResponse::rejected(format!("Candidate '{}' not found", cand));
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
        let _ = self.postgresql.stop("immediate").await;
        if let Err(e) = self.postgresql.remove_data_directory() {
            return CommandResponse::error(format!("Remove data dir failed: {e}"));
        }
        CommandResponse::accepted("Node reinitialized, will clone on next cycle")
    }
}
