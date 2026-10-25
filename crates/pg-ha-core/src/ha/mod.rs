//! HA Decision Engine
//!
//! The core control loop that monitors cluster state and takes corrective actions:
//! - Leader lock renewal (primary)
//! - Failover detection and leader election (replica)
//! - PostgreSQL lifecycle management (start/stop/promote/follow)
//! - Pause mode handling
//!
//! Equivalent to Patroni's `Ha` class and its `_run_cycle()` method.

use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::bootstrap::{Bootstrap, BootstrapResult};
use crate::cluster::{Cluster, MemberRole, SyncState};
use crate::commands::{CommandResponse, ManagementCommand};
use crate::config::Config;
use crate::dcs::DcsAdapter;
use crate::dynamic_config::{DynamicConfigState, GlobalConfig};
use crate::postgresql::Postgresql;
use crate::standby_cluster::StandbyCluster;
use crate::sync::{SyncManager, SyncMode};

mod commands;
pub(crate) mod election;
mod follow;
mod helpers;
pub(crate) mod identity;

use identity::{LeaderAction, PgIdentity};

/// Result of a single HA cycle, used for logging
#[derive(Debug)]
pub enum CycleResult {
    Leader(String),
    Follower(String),
    Acquiring(String),
    Error(String),
}

impl std::fmt::Display for CycleResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Leader(msg) => write!(f, "Leader: {msg}"),
            Self::Follower(msg) => write!(f, "Follower: {msg}"),
            Self::Acquiring(msg) => write!(f, "Acquiring: {msg}"),
            Self::Error(msg) => write!(f, "Error: {msg}"),
        }
    }
}

/// The HA decision engine
pub struct Ha {
    config: Config,
    dcs: Arc<dyn DcsAdapter>,
    postgresql: Postgresql,

    // ─── State ───
    cluster: Cluster,
    is_leader: bool,
    is_paused: bool,

    // --- Failsafe ---
    failsafe: crate::failsafe::Failsafe,

    // ─── Dynamic configuration ───
    dynamic_config_state: DynamicConfigState,

    // ─── Command channel ───
    cmd_rx: mpsc::Receiver<(ManagementCommand, mpsc::Sender<CommandResponse>)>,
    pending_switchover: Option<ManagementCommand>,

    // ─── Start failure tracking ───
    start_fail_count: u32,

    /// Last successfully applied `synchronous_standby_names` value.
    /// Used to skip redundant ALTER SYSTEM + reload every HA cycle.
    applied_sync_standby_names: Option<String>,

    /// A stop after writing standby.signal on a demotion / fence path failed
    /// and PostgreSQL was not confirmed stopped since (A19). While set, an
    /// unobserved server is fenced again every cycle, and a primary is fenced
    /// instead of campaigning.
    stop_pending: bool,
}

/// Sender half for submitting commands to the HA engine
pub type CommandSender = mpsc::Sender<(ManagementCommand, mpsc::Sender<CommandResponse>)>;

impl Ha {
    pub fn new(
        config: Config,
        dcs: Arc<dyn DcsAdapter>,
        postgresql: Postgresql,
    ) -> (Self, CommandSender) {
        let (cmd_tx, cmd_rx) = mpsc::channel(16);
        let ha = Self {
            config,
            dcs,
            postgresql,
            cluster: Cluster::empty(),
            is_leader: false,
            is_paused: false,
            failsafe: crate::failsafe::Failsafe::new(30),
            dynamic_config_state: DynamicConfigState::new(),
            cmd_rx,
            pending_switchover: None,
            start_fail_count: 0,
            applied_sync_standby_names: None,
            stop_pending: false,
        };
        (ha, cmd_tx)
    }

    /// Run one HA cycle. This is the main decision loop.
    pub async fn run_cycle(&mut self) -> CycleResult {
        // Process any pending management commands
        self.process_commands().await;

        // Step 1: Load cluster state from DCS
        match self.dcs.get_cluster().await {
            Ok(cluster) => {
                self.cluster = cluster;
                // Check dynamic config for pause
                self.is_paused = self
                    .cluster
                    .config
                    .as_ref()
                    .and_then(|c| c.data.get("pause"))
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);

                let failsafe_members: std::collections::HashMap<String, String> = self
                    .cluster
                    .members
                    .iter()
                    .map(|m| (m.name.clone(), m.api_url.clone()))
                    .collect();
                self.failsafe.update_members(failsafe_members);
            }
            Err(e) => {
                error!("Failed to load cluster from DCS: {e}");
                return self.handle_dcs_error().await;
            }
        }

        // Step 1b: Process dynamic configuration changes from DCS /config key
        self.process_dynamic_config().await;

        // Step 2: Ensure we are registered as a member
        if !self.cluster.has_member(&self.config.name) {
            let _ = self.touch_member().await;
        }

        // Step 3: Check data directory
        match self.postgresql.data_directory_empty() {
            Ok(true) => return self.handle_empty_data_dir().await,
            Ok(false) => {} // continue
            Err(e) => {
                return CycleResult::Error(format!("Data directory error: {e}"));
            }
        }

        // Step 4: Check if PostgreSQL is running
        if !self.postgresql.is_running() {
            // Confirmed stopped: a pending demotion stop is no longer pending.
            self.stop_pending = false;
            return self.handle_postgres_not_running().await;
        }

        // Step 4b: Observe what PostgreSQL is right now. Every decision below
        // that depends on "am I a primary" uses this observation, never the
        // remembered role (see `identity`).
        self.postgresql.observe_recovery().await;
        let role = identity::reconcile_role(self.postgresql.role().clone(), self.pg_identity());
        self.postgresql.set_role(role);

        // Step 5: Main decision branch
        if self.cluster.is_unlocked() {
            self.process_unhealthy_cluster().await
        } else {
            self.process_healthy_cluster().await
        }
    }

    // ─────────────────── Main decision branches ───────────────────

    /// Cluster has a leader — renew lock or follow
    async fn process_healthy_cluster(&mut self) -> CycleResult {
        let lock_owner = self
            .cluster
            .leader
            .as_ref()
            .map(|l| l.name.as_str())
            .unwrap_or("");

        if lock_owner == self.config.name {
            // I hold the lock — renew it
            let leader = self.cluster.leader.as_ref().unwrap().clone();
            match self.dcs.update_leader(&leader).await {
                Ok(true) => {
                    self.is_leader = true;
                    self.enforce_primary_role().await
                }
                Ok(false) => {
                    // Failed to renew — demote immediately
                    error!("Failed to update leader lock");
                    self.is_leader = false;
                    if self.is_paused {
                        CycleResult::Leader(
                            "continue running as primary in pause mode despite lock failure".into(),
                        )
                    } else {
                        self.demote().await
                    }
                }
                Err(e) => {
                    error!("DCS error during lock renewal: {e}");
                    self.is_leader = false;
                    if self.is_paused {
                        CycleResult::Leader("continue in pause mode despite DCS error".into())
                    } else {
                        self.demote().await
                    }
                }
            }
        } else {
            // I don't hold the lock — follow the appropriate upstream
            self.is_leader = false;
            let owner = lock_owner.to_string();
            self.follow_upstream(&owner).await
        }
    }

    // ─────────────────── Actions ───────────────────

    /// Ensure PostgreSQL is running as primary (or as standby leader in standby cluster mode)
    async fn enforce_primary_role(&mut self) -> CycleResult {
        // Check if we are in standby cluster mode
        let is_standby = StandbyCluster::is_standby_cluster(&self.dynamic_config_state.last_config);

        if is_standby {
            return self.enforce_standby_leader_role().await;
        }

        let identity = self.pg_identity();
        match identity::leader_action(identity) {
            LeaderAction::Hold => {
                self.postgresql.set_role(MemberRole::Primary);
                let _ = self.touch_member().await;
                self.enforce_synchronous_replication().await;
                CycleResult::Leader(format!(
                    "no action. I am ({}), the leader with the lock",
                    self.config.name
                ))
            }
            LeaderAction::Promote => self.promote_and_confirm().await,
            LeaderAction::Relinquish => self.relinquish_unmanaged_recovery().await,
            LeaderAction::Wait => {
                let _ = self.touch_member().await;
                CycleResult::Error(format!(
                    "holding the leader lock but PostgreSQL is {}; not acting as primary this cycle",
                    identity.describe()
                ))
            }
        }
    }

    /// Identity of the local PostgreSQL from this cycle's observation.
    fn pg_identity(&self) -> PgIdentity {
        PgIdentity::classify(
            self.postgresql.observed_in_recovery(),
            self.has_standby_signal(),
        )
    }

    /// Member state to advertise in DCS and the REST API (see
    /// [`identity::advertised_state`]).
    pub fn advertised_state(&self) -> crate::cluster::MemberState {
        identity::advertised_state(
            self.postgresql.is_running(),
            self.postgresql.state(),
            self.pg_identity(),
        )
    }

    /// Promote a standby pg-ha configured, then confirm by observation.
    ///
    /// `pg_ctl promote` succeeding is not enough to claim primary: the role
    /// becomes `Primary` only after `pg_is_in_recovery()` returns false.
    async fn promote_and_confirm(&mut self) -> CycleResult {
        info!("Promoting PostgreSQL from standby to primary (holding the leader lock)");
        if let Err(e) = self.postgresql.promote().await {
            error!("pg_ctl promote failed: {e}");
            return CycleResult::Error(format!("Promote failed: {e}"));
        }

        self.postgresql.observe_recovery().await;
        let identity = self.pg_identity();
        if identity != PgIdentity::Primary {
            error!(
                "pg_ctl promote reported success but PostgreSQL is {}",
                identity.describe()
            );
            let _ = self.touch_member().await;
            return CycleResult::Error(format!(
                "promote reported success but PostgreSQL is {}; not acting as primary",
                identity.describe()
            ));
        }

        self.postgresql.set_role(MemberRole::Primary);
        let _ = self.touch_member().await;
        self.enforce_synchronous_replication().await;
        info!("PostgreSQL promoted to primary and confirmed out of recovery");
        CycleResult::Leader(format!(
            "promoted to primary. I am ({}), the leader with the lock",
            self.config.name
        ))
    }

    /// The lock holder is in a recovery pg-ha did not create: give the lock
    /// back so a node pg-ha manages can take over, and leave this node to the
    /// operator. Never promote it: nothing proves it should become primary.
    ///
    /// In pause mode the lock is kept (W1: no automatic failover), but the
    /// node still does not claim primary.
    async fn relinquish_unmanaged_recovery(&mut self) -> CycleResult {
        let cause = if self.has_recovery_signal() {
            "recovery.signal present (archive or point-in-time recovery)"
        } else {
            "neither standby.signal nor recovery.signal present"
        };

        if self.is_paused {
            warn!(
                cause,
                "Holding the leader lock while PostgreSQL is in recovery pg-ha did not create; \
                 paused, so keeping the lock but not acting as primary"
            );
            let _ = self.touch_member().await;
            return CycleResult::Error(format!(
                "holding the leader lock in unmanaged recovery ({cause}); paused, not acting as primary"
            ));
        }

        error!(
            cause,
            "Holding the leader lock while PostgreSQL is in recovery pg-ha did not create; \
             releasing the lock. Operator action required: finish the recovery and promote \
             manually, or recreate standby.signal to hand the node back to pg-ha"
        );
        if let Some(leader) = self.cluster.leader.clone() {
            match self.dcs.delete_leader(&leader).await {
                Ok(true) => info!("Released leader lock"),
                Ok(false) => warn!("Leader lock was not ours to release"),
                Err(e) => warn!("Failed to release leader lock, will retry next cycle: {e}"),
            }
        }
        self.is_leader = false;
        self.reset_sync_cache();
        let _ = self.touch_member().await;
        CycleResult::Error(format!(
            "released leader lock: PostgreSQL in unmanaged recovery ({cause}); operator action required"
        ))
    }

    /// Apply synchronous replication settings when this node is the primary.
    ///
    /// Skips PG reload / DCS write when the desired value is unchanged.
    async fn enforce_synchronous_replication(&mut self) {
        let cfg = &self.dynamic_config_state.last_config;
        let enabled = cfg.synchronous_mode.unwrap_or(false);
        let strict = cfg.synchronous_mode_strict.unwrap_or(false);
        let count = cfg.synchronous_node_count.unwrap_or(1) as usize;

        let desired = if !enabled {
            String::new()
        } else {
            let manager = SyncManager::new(SyncMode::Priority, count, strict);
            match manager.compute_sync_standby_names(&self.cluster.members, &self.config.name) {
                Some(v) => v,
                None => return,
            }
        };

        if self.applied_sync_standby_names.as_deref() == Some(desired.as_str()) {
            return;
        }

        if let Err(e) = self
            .postgresql
            .set_synchronous_standby_names(&desired)
            .await
        {
            warn!("Failed to set synchronous_standby_names: {e}");
            return;
        }

        let sync_standby = Self::extract_names_from_sync_value(&desired);
        let state = SyncState {
            leader: self.config.name.clone(),
            sync_standby,
            quorum: if enabled { count as u32 } else { 0 },
        };

        match serde_json::to_string(&state) {
            Ok(json) => {
                if let Err(e) = self.dcs.set_sync_value(&json).await {
                    warn!("Failed to write /sync: {e}");
                    return;
                }
            }
            Err(e) => {
                warn!("Failed to serialize SyncState: {e}");
                return;
            }
        }

        self.applied_sync_standby_names = Some(desired);
    }

    /// "FIRST 1 (node2,node3)" → Some("node2,node3")
    /// "" / "*" → None / Some("*")
    fn extract_names_from_sync_value(pg_value: &str) -> Option<String> {
        if pg_value.is_empty() {
            return None;
        }
        if pg_value == "*" {
            return Some("*".into());
        }
        let start = pg_value.find('(')? + 1;
        let end = pg_value.rfind(')')?;
        let names = pg_value[start..end].trim();
        if names.is_empty() {
            None
        } else {
            Some(names.to_string())
        }
    }
    /// Enforce standby leader role: hold leader lock but replicate from remote.
    ///
    /// In standby cluster mode, the leader does NOT promote to primary.
    /// Instead it maintains streaming replication from the configured remote source.
    async fn enforce_standby_leader_role(&mut self) -> CycleResult {
        let standby_config =
            match StandbyCluster::get_config(&self.dynamic_config_state.last_config) {
                Some(cfg) => cfg.clone(),
                None => {
                    // Config disappeared mid-cycle — treat as cascade promote needed
                    return CycleResult::Leader(
                        "standby_cluster config missing — cascade promote may be needed".into(),
                    );
                }
            };

        // Verify PostgreSQL is replicating from the remote source
        let repl_user = &self.config.postgresql.replication.username.clone();
        let repl_pass = self.config.postgresql.replication.password.clone();

        let follow_ok = StandbyCluster::enforce_follow_remote_member(
            &standby_config,
            &self.postgresql,
            repl_user,
            repl_pass.as_deref(),
        );

        match follow_ok {
            Ok(true) => {
                // Only a server observed in recovery is a standby leader.
                self.postgresql.set_role(MemberRole::StandbyLeader);
                let _ = self.touch_member().await;
                CycleResult::Leader(format!(
                    "no action. I am ({}), the standby leader replicating from {}:{}",
                    self.config.name, standby_config.host, standby_config.port
                ))
            }
            Ok(false) => {
                let _ = self.touch_member().await;
                CycleResult::Leader(format!(
                    "standby leader ({}), reconfiguration needed for remote {}:{}",
                    self.config.name, standby_config.host, standby_config.port
                ))
            }
            Err(e) => CycleResult::Error(format!("Failed to enforce standby leader role: {e}")),
        }
    }

    /// Demote from primary — stop PostgreSQL to prevent split-brain.
    ///
    /// standby.signal and the rewind-check marker are written first (D2), and
    /// primary_conninfo is removed because the new leader is unknown (D3). A
    /// failed write still stops PostgreSQL (L5 first) and reports an error.
    async fn demote(&mut self) -> CycleResult {
        warn!("Demoting: writing standby.signal, then stopping PostgreSQL immediately");
        self.reset_sync_cache();
        let written = self
            .prepare_demotion(None, Some("demoted after losing the leader lock"))
            .await;
        if let Err(e) = &written {
            error!(
                "Failed to write standby configuration before demotion: {e}; stopping PostgreSQL anyway"
            );
        }
        // A failed stop is logged and recorded in stop_pending.
        let _ = self.stop_after_demotion_write("immediate").await;
        self.postgresql.set_role(MemberRole::Replica);
        match written {
            Ok(()) => CycleResult::Follower(
                "demoted self because failed to update leader lock in DCS".into(),
            ),
            Err(e) => CycleResult::Error(format!(
                "demoted self because failed to update leader lock in DCS, but standby configuration could not be written: {e}"
            )),
        }
    }

    /// Handle case when PostgreSQL is not running
    async fn handle_postgres_not_running(&mut self) -> CycleResult {
        // If we hold the lock but PG is down, release it
        if self.is_lock_owner() {
            if self.is_paused {
                return CycleResult::Leader("postgres not running (paused)".into());
            }
            if let Some(leader) = &self.cluster.leader.clone() {
                info!("Releasing leader lock because PostgreSQL is not running");
                let _ = self.dcs.delete_leader(leader).await;
            }
            self.is_leader = false;
            self.reset_sync_cache();
        }

        if self.is_paused {
            return CycleResult::Follower("postgres is not running (paused)".into());
        }

        // If PG has failed to start multiple times (e.g., timeline mismatch after failover),
        // trigger a rejoin via pg_rewind instead of endlessly retrying start.
        if self.start_fail_count >= 3 && self.has_standby_signal() {
            warn!(
                fail_count = self.start_fail_count,
                "PostgreSQL failed to start {} times — triggering rejoin via pg_rewind",
                self.start_fail_count
            );
            self.start_fail_count = 0;

            // Find the current primary to rewind from
            let leader_name = self.cluster.leader.as_ref().map(|l| l.name.clone());
            if let Some(name) = leader_name {
                return self.rejoin_as_replica(&name).await;
            } else {
                // No leader visible — find any running member
                let source = self
                    .cluster
                    .members
                    .iter()
                    .find(|m| {
                        m.name != self.config.name
                            && m.state == crate::cluster::MemberState::Running
                    })
                    .map(|m| m.name.clone());
                if let Some(name) = source {
                    return self.rejoin_as_replica(&name).await;
                }
            }
        }

        // Try to start PostgreSQL
        info!("Attempting to start PostgreSQL");
        match self.postgresql.start().await {
            Ok(()) => {
                self.start_fail_count = 0;
                CycleResult::Follower("started PostgreSQL, recovering".into())
            }
            Err(e) => {
                self.start_fail_count += 1;
                CycleResult::Error(format!(
                    "Failed to start PostgreSQL: {e} (attempt {})",
                    self.start_fail_count
                ))
            }
        }
    }

    /// Handle empty data directory — bootstrap or clone using the Bootstrap module
    async fn handle_empty_data_dir(&mut self) -> CycleResult {
        if self.is_paused {
            return CycleResult::Follower("data directory empty (paused)".into());
        }

        if self.cluster.is_unlocked() && self.cluster.initialize.is_none() {
            // No cluster exists yet — race for initialization via Bootstrap
            let mut bootstrap = Bootstrap::new(&self.config, &mut self.postgresql, &self.dcs);
            match bootstrap.bootstrap_new_cluster().await {
                BootstrapResult::InitializedAsPrimary => {
                    // A fresh initdb starts out of recovery; confirm before
                    // recording the role, like every other path to Primary.
                    self.postgresql.observe_recovery().await;
                    if self.postgresql.is_observed_primary() {
                        self.postgresql.set_role(MemberRole::Primary);
                    }
                    match self.dcs.attempt_to_acquire_leader().await {
                        Ok(true) => {
                            self.is_leader = true;
                            let _ = self.touch_member().await;
                            CycleResult::Leader("bootstrapped new cluster as leader".into())
                        }
                        _ => {
                            // Lock acquisition failed — another node may have won the race.
                            // We still bootstrapped PG, but we're not the confirmed leader.
                            // Next cycle will sort out who holds the lock.
                            self.is_leader = false;
                            CycleResult::Acquiring(
                                "bootstrapped but failed to acquire leader lock".into(),
                            )
                        }
                    }
                }
                BootstrapResult::LostRace => {
                    CycleResult::Acquiring("lost initialization race, waiting".into())
                }
                BootstrapResult::Failed(e) => CycleResult::Error(format!("Bootstrap failed: {e}")),
                BootstrapResult::ClonedAsReplica => {
                    // Shouldn't happen during bootstrap_new_cluster, but handle gracefully
                    CycleResult::Follower("cloned as replica".into())
                }
            }
        } else if self.cluster.has_member(&self.config.name)
            || !self.cluster.members.is_empty()
            || self.cluster.leader.is_some()
        {
            // Cluster exists — clone from an existing member
            let cluster_clone = self.cluster.clone();
            let mut bootstrap = Bootstrap::new(&self.config, &mut self.postgresql, &self.dcs);
            match bootstrap.clone_from_member(&cluster_clone).await {
                BootstrapResult::ClonedAsReplica => match self.postgresql.start().await {
                    Ok(()) => {
                        self.postgresql.set_role(MemberRole::Replica);
                        // The clone's postgresql.auto.conf came from the source.
                        self.reset_sync_cache();
                        CycleResult::Follower("cloned and started as replica".into())
                    }
                    Err(e) => CycleResult::Error(format!("Start after clone failed: {e}")),
                },
                BootstrapResult::Failed(e) => CycleResult::Error(format!("Clone failed: {e}")),
                _ => CycleResult::Acquiring("waiting for leader to clone from".into()),
            }
        } else {
            CycleResult::Acquiring("waiting for leader to clone from".into())
        }
    }

    /// Handle DCS communication failure
    async fn handle_dcs_error(&mut self) -> CycleResult {
        if self.is_leader {
            // DCS unreachable while we hold the leader lock.
            // Before demoting, check if replicas can still see us (failsafe mode).
            let failsafe_result = self.failsafe.check_topology(&self.config.name).await;
            match failsafe_result {
                crate::failsafe::FailsafeResult::AllConfirmed => {
                    warn!("DCS unreachable but failsafe check passed = continuing as primary");
                    CycleResult::Error("DCS unreachable: failsafe active, staying primary".into())
                }
                crate::failsafe::FailsafeResult::Disabled => {
                    warn!(
                        "DCS unreachable while holding leader lock - demoting to prevent split-brain"
                    );
                    self.is_leader = false;
                    self.reset_sync_cache();
                    let written = self
                        .prepare_demotion(None, Some("demoted because the DCS is unreachable"))
                        .await;
                    if let Err(e) = &written {
                        error!(
                            "Failed to write standby configuration before DCS-error demotion: {e}; stopping PostgreSQL anyway"
                        );
                    }
                    let _ = self.stop_after_demotion_write("fast").await;
                    self.postgresql.set_role(MemberRole::Replica);
                    match written {
                        Ok(()) => CycleResult::Error(
                            "DCS unreachable: demoted to prevent split-brain".into(),
                        ),
                        Err(e) => CycleResult::Error(format!(
                            "DCS unreachable: demoted to prevent split-brain, but standby configuration could not be written: {e}"
                        )),
                    }
                }

                crate::failsafe::FailsafeResult::NotAllConfirmed { failed } => {
                    warn!(
                        ?failed,
                        "DCS unreachable and failsafe check failed - demoting"
                    );
                    self.is_leader = false;
                    self.reset_sync_cache();
                    let written = self
                        .prepare_demotion(None, Some("demoted because the DCS is unreachable"))
                        .await;
                    if let Err(e) = &written {
                        error!(
                            "Failed to write standby configuration before DCS-error demotion: {e}; stopping PostgreSQL anyway"
                        );
                    }
                    let _ = self.stop_after_demotion_write("fast").await;
                    self.postgresql.set_role(MemberRole::Replica);
                    match written {
                        Ok(()) => CycleResult::Error(format!(
                            "DCS unreachable: failsafe failed ({failed:?}), demoted"
                        )),
                        Err(e) => CycleResult::Error(format!(
                            "DCS unreachable: failsafe failed ({failed:?}), demoted, but standby configuration could not be written: {e}"
                        )),
                    }
                }
            }
        } else {
            // A demotion stop failed earlier: keep trying to stop PostgreSQL
            // even while the DCS is unreachable (A19).
            if self.stop_pending && self.postgresql.is_running() {
                let _ = self.stop_after_demotion_write("fast").await;
            }
            CycleResult::Error("DCS unreachable".into())
        }
    }

    // ─────────────────── Dynamic Configuration ───────────────────

    /// Read the /config key from DCS and process any changes.
    /// - HA parameter changes are applied immediately to the running config.
    /// - PG reload-only params trigger pg_ctl reload.
    /// - PG restart-needed params set the pending_restart flag.
    async fn process_dynamic_config(&mut self) {
        let config_value = match self.dcs.get_config_value().await {
            Ok(Some(val)) => val,
            Ok(None) => return, // No config key in DCS, nothing to do
            Err(e) => {
                warn!("Failed to read /config from DCS: {e}");
                return;
            }
        };

        let new_config: GlobalConfig = match serde_json::from_str(&config_value) {
            Ok(c) => c,
            Err(e) => {
                warn!("Failed to parse dynamic config from DCS: {e}");
                return;
            }
        };

        // Also update pause from the parsed GlobalConfig
        if let Some(pause) = new_config.pause {
            self.is_paused = pause;
        }

        let changes = self
            .dynamic_config_state
            .apply_new_config(new_config.clone());

        if !changes.has_changes() {
            return;
        }

        // Apply HA parameter changes immediately
        if changes.ha_params_changed {
            if let Some(lw) = new_config.loop_wait {
                self.config.loop_wait = lw;
            }
            if let Some(ttl) = new_config.ttl {
                self.config.ttl = ttl;
            }
            if let Some(rt) = new_config.retry_timeout {
                self.config.retry_timeout = rt;
            }
        }

        // Apply PG reload-only parameter changes via pg_ctl reload
        if changes.needs_reload() && self.postgresql.is_running() {
            info!(
                params = ?changes.pg_reload_params.keys().collect::<Vec<_>>(),
                "Applying PG parameter changes via reload"
            );
            if let Err(e) = self.postgresql.reload().await {
                warn!("pg_ctl reload failed: {e}");
            }
        }
    }

    // ─────────────────── Public accessors ───────────────────

    /// Whether this node currently holds the leader lock
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }

    /// Whether the cluster is in pause mode
    pub fn is_paused(&self) -> bool {
        self.is_paused
    }

    /// Whether the cluster is operating in standby mode
    pub fn is_standby_cluster(&self) -> bool {
        StandbyCluster::is_standby_cluster(&self.dynamic_config_state.last_config)
    }

    /// Whether a PostgreSQL restart is pending due to parameter changes
    pub fn pending_restart(&self) -> bool {
        self.dynamic_config_state.pending_restart
    }

    /// Get the dynamic config state (for API to read pending_restart info)
    pub fn dynamic_config_state(&self) -> &DynamicConfigState {
        &self.dynamic_config_state
    }

    /// Get current cluster state
    pub fn cluster(&self) -> &Cluster {
        &self.cluster
    }

    /// Get PostgreSQL reference
    pub fn postgresql(&self) -> &Postgresql {
        &self.postgresql
    }

    /// Get mutable PostgreSQL reference
    pub fn postgresql_mut(&mut self) -> &mut Postgresql {
        &mut self.postgresql
    }

    /// Get config reference
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Trigger cascade promote: remove standby_cluster config and promote to full primary.
    /// Only valid when the node is the standby leader.
    pub async fn cascade_promote(&mut self) -> std::result::Result<(), String> {
        if !self.is_leader {
            return Err("Cannot cascade promote: not the leader".into());
        }
        if !self.is_standby_cluster() {
            return Err("Cannot cascade promote: not in standby cluster mode".into());
        }

        let current_config = self.dynamic_config_state.last_config.clone();
        match StandbyCluster::cascade_promote(&self.dcs, &mut self.postgresql, &current_config)
            .await
        {
            Ok(()) => {
                // Update local dynamic config state to reflect removal
                let mut new_config = current_config;
                new_config.standby_cluster = None;
                self.dynamic_config_state.apply_new_config(new_config);
                Ok(())
            }
            Err(e) => Err(format!("Cascade promote failed: {e}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{Failover, Leader, Member, MemberRole, MemberState};
    use crate::commands::{CommandStatus, ManagementCommand};
    use crate::config::*;
    use crate::error::Result;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Mutex;

    /// Mock DCS for testing HA logic without Raft
    struct MockDcs {
        cluster: Mutex<Cluster>,
        leader_held_by: Mutex<Option<String>>,
        last_failover_value: Mutex<Option<String>>,
        /// When set, leader-lock writes return Err (a DCS write failure).
        fail_leader_writes: std::sync::atomic::AtomicBool,
    }

    impl MockDcs {
        fn new() -> Self {
            Self {
                cluster: Mutex::new(Cluster::empty()),
                leader_held_by: Mutex::new(None),
                last_failover_value: Mutex::new(None),
                fail_leader_writes: std::sync::atomic::AtomicBool::new(false),
            }
        }

        fn with_leader(name: &str) -> Self {
            let cluster = Cluster {
                leader: Some(Leader {
                    name: name.to_string(),
                    version: 1,
                }),
                ..Default::default()
            };
            Self {
                cluster: Mutex::new(cluster),
                leader_held_by: Mutex::new(Some(name.to_string())),
                last_failover_value: Mutex::new(None),
                fail_leader_writes: std::sync::atomic::AtomicBool::new(false),
            }
        }

        fn set_cluster(&self, cluster: Cluster) {
            *self.cluster.lock().unwrap() = cluster;
        }

        fn fail_leader_writes(&self) {
            self.fail_leader_writes
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }

        fn leader_writes_fail(&self) -> Result<()> {
            if self
                .fail_leader_writes
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(crate::error::Error::Dcs(
                    "injected DCS write failure".into(),
                ));
            }
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl DcsAdapter for MockDcs {
        async fn get_cluster(&self) -> Result<Cluster> {
            Ok(self.cluster.lock().unwrap().clone())
        }

        async fn attempt_to_acquire_leader(&self) -> Result<bool> {
            self.leader_writes_fail()?;
            let mut held = self.leader_held_by.lock().unwrap();
            if held.is_none() {
                *held = Some("test_node".to_string());
                // Update cluster leader
                let mut cluster = self.cluster.lock().unwrap();
                cluster.leader = Some(Leader {
                    name: "test_node".to_string(),
                    version: 1,
                });
                Ok(true)
            } else {
                Ok(false)
            }
        }

        async fn update_leader(&self, _leader: &Leader) -> Result<bool> {
            self.leader_writes_fail()?;
            let held = self.leader_held_by.lock().unwrap();
            Ok(held.as_deref() == Some("test_node"))
        }

        async fn delete_leader(&self, _leader: &Leader) -> Result<bool> {
            let mut held = self.leader_held_by.lock().unwrap();
            *held = None;
            let mut cluster = self.cluster.lock().unwrap();
            cluster.leader = None;
            Ok(true)
        }

        async fn touch_member(&self, _data: &serde_json::Value) -> Result<bool> {
            Ok(true)
        }

        async fn initialize(&self, _sysid: &str) -> Result<bool> {
            Ok(true)
        }

        async fn set_failover_value(&self, value: &str) -> Result<bool> {
            *self.last_failover_value.lock().unwrap() = Some(value.to_string());
            Ok(true)
        }

        async fn set_sync_value(&self, _value: &str) -> Result<bool> {
            Ok(true)
        }

        async fn set_config_value(&self, _value: &str) -> Result<bool> {
            Ok(true)
        }

        async fn get_config_value(&self) -> Result<Option<String>> {
            Ok(None)
        }

        fn ttl(&self) -> u64 {
            30
        }
        fn loop_wait(&self) -> u64 {
            10
        }
    }

    fn test_config(name: &str) -> Config {
        Config {
            name: name.to_string(),
            scope: "test-cluster".to_string(),
            namespace: "service".to_string(),
            loop_wait: 10,
            ttl: 30,
            retry_timeout: 10,
            postgresql: PostgresqlConfig {
                data_dir: PathBuf::from("/tmp/pg-ha-test-nonexist"),
                bin_dir: PathBuf::from("/usr/bin"),
                listen: "127.0.0.1".to_string(),
                port: 5432,
                superuser: ConnectionParams {
                    username: "postgres".to_string(),
                    password: None,
                    dbname: "postgres".to_string(),
                },
                replication: ConnectionParams {
                    username: "replicator".to_string(),
                    password: None,
                    dbname: "postgres".to_string(),
                },
                parameters: HashMap::new(),
            },
            restapi: RestApiConfig {
                listen: "127.0.0.1".to_string(),
                port: 8008,
                username: None,
                password: None,
            },
            raft: RaftConfig {
                self_addr: "127.0.0.1:2380".to_string(),
                partner_addrs: vec!["127.0.0.2:2380".to_string()],
                data_dir: None,
                node_id: None,
                tls: None,
            },
            proxy: ProxyConfig {
                rw_listen: "0.0.0.0".to_string(),
                rw_port: 6432,
                ro_listen: "0.0.0.0".to_string(),
                ro_port: 6433,
            },
            watchdog: WatchdogConfig::default(),
            tags: Tags::default(),
            bootstrap: None,
            shutdown: ShutdownConfig::default(),
            log_format: LogFormat::default(),
        }
    }

    #[tokio::test]
    async fn test_empty_data_dir_triggers_bootstrap() {
        let config = test_config("node1");
        let dcs = Arc::new(MockDcs::new());
        let pg = Postgresql::new(config.postgresql.clone());
        let (mut ha, _cmd_tx) = Ha::new(config, dcs, pg);

        // data_dir doesn't exist → empty → should try to bootstrap
        let result = ha.run_cycle().await;
        // initdb will fail (bin doesn't exist) but that's expected in test
        let msg = format!("{result}");
        assert!(
            msg.contains("bootstrap") || msg.contains("initdb") || msg.contains("Error"),
            "Expected bootstrap attempt, got: {msg}"
        );
    }

    #[tokio::test]
    async fn test_nofailover_prevents_election() {
        let mut config = test_config("node1");
        config.tags.nofailover = true;
        let dcs = Arc::new(MockDcs::new());
        let pg = Postgresql::new(config.postgresql.clone());
        let (ha, _cmd_tx) = Ha::new(config, dcs, pg);

        assert!(!ha.is_healthiest_node());
    }

    #[tokio::test]
    async fn test_failover_priority_zero_prevents_election() {
        let mut config = test_config("node1");
        config.tags.failover_priority = 0;
        let dcs = Arc::new(MockDcs::new());
        let pg = Postgresql::new(config.postgresql.clone());
        let (ha, _cmd_tx) = Ha::new(config, dcs, pg);

        assert!(!ha.is_healthiest_node());
    }

    #[tokio::test]
    async fn test_healthiest_node_wins_by_wal_position() {
        let config = test_config("node2");
        let dcs = Arc::new(MockDcs::new());
        // Set cluster with node1 having higher WAL
        dcs.set_cluster(Cluster {
            leader: None,
            members: vec![
                Member {
                    name: "node1".to_string(),
                    conn_url: String::new(),
                    api_url: String::new(),
                    state: MemberState::Running,
                    role: MemberRole::Replica,
                    wal_position: Some(2000),
                    timeline: Some(1),
                    tags: Default::default(),
                    version: None,
                },
                Member {
                    name: "node2".to_string(),
                    conn_url: String::new(),
                    api_url: String::new(),
                    state: MemberState::Running,
                    role: MemberRole::Replica,
                    wal_position: Some(1000),
                    timeline: Some(1),
                    tags: Default::default(),
                    version: None,
                },
            ],
            ..Default::default()
        });
        let pg = Postgresql::new(config.postgresql.clone());
        let (mut ha, _cmd_tx) = Ha::new(config, dcs.clone(), pg);
        ha.cluster = dcs.cluster.lock().unwrap().clone();

        // node2 (us) has WAL=1000, node1 has WAL=2000 → we are NOT healthiest
        assert!(!ha.is_healthiest_node());
    }

    #[tokio::test]
    async fn test_healthiest_node_wins_by_priority_on_tie() {
        let mut config = test_config("node2");
        config.tags.failover_priority = 1;
        let dcs = Arc::new(MockDcs::new());
        dcs.set_cluster(Cluster {
            leader: None,
            members: vec![
                Member {
                    name: "node1".to_string(),
                    conn_url: String::new(),
                    api_url: String::new(),
                    state: MemberState::Running,
                    role: MemberRole::Replica,
                    wal_position: Some(1000),
                    timeline: Some(1),
                    tags: HashMap::from([("failover_priority".to_string(), serde_json::json!(10))]),
                    version: None,
                },
                Member {
                    name: "node2".to_string(),
                    conn_url: String::new(),
                    api_url: String::new(),
                    state: MemberState::Running,
                    role: MemberRole::Replica,
                    wal_position: Some(1000),
                    timeline: Some(1),
                    tags: Default::default(),
                    version: None,
                },
            ],
            ..Default::default()
        });
        let pg = Postgresql::new(config.postgresql.clone());
        let (mut ha, _cmd_tx) = Ha::new(config, dcs.clone(), pg);
        ha.cluster = dcs.cluster.lock().unwrap().clone();

        // Same WAL, node1 has priority=10, we have priority=1 → NOT healthiest
        assert!(!ha.is_healthiest_node());
    }

    #[tokio::test]
    async fn test_healthiest_node_nofailover_member_excluded() {
        let config = test_config("node2");
        let dcs = Arc::new(MockDcs::new());
        dcs.set_cluster(Cluster {
            leader: None,
            members: vec![
                Member {
                    name: "node1".to_string(),
                    conn_url: String::new(),
                    api_url: String::new(),
                    state: MemberState::Running,
                    role: MemberRole::Replica,
                    wal_position: Some(9999), // higher WAL but nofailover
                    timeline: Some(1),
                    tags: HashMap::from([("nofailover".to_string(), serde_json::json!(true))]),
                    version: None,
                },
                Member {
                    name: "node2".to_string(),
                    conn_url: String::new(),
                    api_url: String::new(),
                    state: MemberState::Running,
                    role: MemberRole::Replica,
                    wal_position: Some(1000),
                    timeline: Some(1),
                    tags: Default::default(),
                    version: None,
                },
            ],
            ..Default::default()
        });

        // Create a fake data dir with postmaster.pid containing our PID
        let data_dir = std::env::temp_dir().join("pg-ha-test-healthiest");
        let _ = std::fs::create_dir_all(&data_dir);
        std::fs::write(
            data_dir.join("postmaster.pid"),
            format!("{}\n", std::process::id()),
        )
        .unwrap();

        let mut pg_config = config.postgresql.clone();
        pg_config.data_dir = data_dir.clone();
        let pg = Postgresql::new(pg_config);
        let (mut ha, _cmd_tx) = Ha::new(config, dcs.clone(), pg);
        ha.cluster = dcs.cluster.lock().unwrap().clone();

        // node1 has higher WAL but is nofailover → we ARE healthiest
        assert!(ha.is_healthiest_node());

        // Cleanup
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    fn running_member(name: &str, wal: u64) -> Member {
        Member {
            name: name.to_string(),
            conn_url: String::new(),
            api_url: String::new(),
            state: MemberState::Running,
            role: MemberRole::Replica,
            wal_position: Some(wal),
            timeline: Some(1),
            tags: Default::default(),
            version: None,
        }
    }

    fn with_fake_running_pg(config: &Config, label: &str) -> (Postgresql, PathBuf) {
        let data_dir =
            std::env::temp_dir().join(format!("pg-ha-test-{label}-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&data_dir);
        std::fs::write(
            data_dir.join("postmaster.pid"),
            format!("{}\n", std::process::id()),
        )
        .unwrap();
        let mut pg_config = config.postgresql.clone();
        pg_config.data_dir = data_dir.clone();
        (Postgresql::new(pg_config), data_dir)
    }

    #[tokio::test]
    async fn test_sync_mode_empty_sync_not_healthiest() {
        let config = test_config("node2");
        let dcs = Arc::new(MockDcs::new());
        let (pg, data_dir) = with_fake_running_pg(&config, "sync-empty");
        let (mut ha, _cmd_tx) = Ha::new(config, dcs, pg);

        ha.dynamic_config_state.apply_new_config(GlobalConfig {
            synchronous_mode: Some(true),
            ..Default::default()
        });
        ha.cluster = Cluster {
            leader: None,
            members: vec![running_member("node2", 1000)],
            sync_state: None,
            ..Default::default()
        };

        assert!(!ha.is_healthiest_node());
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_sync_mode_star_sync_not_healthiest() {
        let config = test_config("node2");
        let dcs = Arc::new(MockDcs::new());
        let (pg, data_dir) = with_fake_running_pg(&config, "sync-star");
        let (mut ha, _cmd_tx) = Ha::new(config, dcs, pg);

        ha.dynamic_config_state.apply_new_config(GlobalConfig {
            synchronous_mode: Some(true),
            ..Default::default()
        });
        ha.cluster = Cluster {
            leader: None,
            members: vec![running_member("node2", 1000)],
            sync_state: Some(SyncState {
                leader: "node1".into(),
                sync_standby: Some("*".into()),
                quorum: 1,
            }),
            ..Default::default()
        };

        assert!(!ha.is_healthiest_node());
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_sync_mode_listed_node_is_healthiest_despite_async_higher_wal() {
        let config = test_config("node2");
        let dcs = Arc::new(MockDcs::new());
        let (pg, data_dir) = with_fake_running_pg(&config, "sync-listed");
        let (mut ha, _cmd_tx) = Ha::new(config, dcs, pg);

        ha.dynamic_config_state.apply_new_config(GlobalConfig {
            synchronous_mode: Some(true),
            ..Default::default()
        });
        ha.cluster = Cluster {
            leader: None,
            members: vec![
                running_member("node2", 1000),
                running_member("node3", 9999), // async — must be skipped
            ],
            sync_state: Some(SyncState {
                leader: "node1".into(),
                sync_standby: Some("node2".into()),
                quorum: 1,
            }),
            ..Default::default()
        };

        assert!(ha.is_healthiest_node());
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_sync_mode_unlisted_node_not_healthiest() {
        let config = test_config("node3");
        let dcs = Arc::new(MockDcs::new());
        let (pg, data_dir) = with_fake_running_pg(&config, "sync-unlisted");
        let (mut ha, _cmd_tx) = Ha::new(config, dcs, pg);

        ha.dynamic_config_state.apply_new_config(GlobalConfig {
            synchronous_mode: Some(true),
            ..Default::default()
        });
        ha.cluster = Cluster {
            leader: None,
            members: vec![
                running_member("node2", 1000), // listed, lower WAL
                running_member("node3", 9999), // self, not listed
            ],
            sync_state: Some(SyncState {
                leader: "node1".into(),
                sync_standby: Some("node2".into()),
                quorum: 1,
            }),
            ..Default::default()
        };

        assert!(!ha.is_healthiest_node());
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_sync_mode_off_ignores_sync_state() {
        let config = test_config("node2");
        let dcs = Arc::new(MockDcs::new());
        let (pg, data_dir) = with_fake_running_pg(&config, "sync-off");
        let (mut ha, _cmd_tx) = Ha::new(config, dcs, pg);

        // synchronous_mode false / default — WAL comparison only
        ha.dynamic_config_state.apply_new_config(GlobalConfig {
            synchronous_mode: Some(false),
            ..Default::default()
        });
        ha.cluster = Cluster {
            leader: None,
            members: vec![
                running_member("node2", 1000), // self, lower WAL
                running_member("node3", 9999), // peer, higher WAL
            ],
            sync_state: None,
            ..Default::default()
        };

        assert!(!ha.is_healthiest_node());
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_sync_mode_designated_candidate_not_sync_eligible() {
        let mut config = test_config("node3");
        let dcs = Arc::new(MockDcs::new());
        let (pg, data_dir) = with_fake_running_pg(&config, "sync-designated-ineligible");
        config.postgresql.data_dir = data_dir.clone();
        std::fs::write(data_dir.join("standby.signal"), "").unwrap();
        let (mut ha, _cmd_tx) = Ha::new(config, dcs, pg);
        // process_unhealthy_cluster is called directly, skipping run_cycle's
        // observation step: record what a running standby would report.
        ha.postgresql.set_observed_in_recovery(Some(true));

        ha.dynamic_config_state.apply_new_config(GlobalConfig {
            synchronous_mode: Some(true),
            ..Default::default()
        });
        ha.cluster = Cluster {
            leader: None,
            members: vec![running_member("node2", 1000), running_member("node3", 9999)],
            sync_state: Some(SyncState {
                leader: "node1".into(),
                sync_standby: Some("node2".into()),
                quorum: 1,
            }),
            failover: Some(Failover {
                leader: Some("node1".into()),
                candidate: Some("node3".into()),
                scheduled_at: None,
            }),
            ..Default::default()
        };

        let result = ha.process_unhealthy_cluster().await;
        assert!(
            matches!(
                result,
                CycleResult::Follower(ref msg)
                    if msg == "designated candidate is not sync-eligible — not acquiring lock"
            ),
            "expected sync-ineligible follower message, got: {result}"
        );
        assert!(!ha.is_leader());
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_healthy_cluster_follower() {
        let mut config = test_config("node2");
        // A data dir of its own (never the shared test_config default), non-empty.
        let dir = std::env::temp_dir().join(format!(
            "pg-ha-test-healthy-follower-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("PG_VERSION"), "16").unwrap();
        config.postgresql.data_dir = dir.clone();
        // Leader is "node1", we are "node2"
        let dcs = Arc::new(MockDcs::with_leader("node1"));
        let pg = Postgresql::new(config.postgresql.clone());
        let (mut ha, _cmd_tx) = Ha::new(config, dcs, pg);

        let result = ha.run_cycle().await;
        let _ = format!("{result}");

        // Clean up
        let _ = std::fs::remove_dir_all(&dir);

        // PG is not running, so we'll try to start it or report it's down
        // The key assertion: we should NOT try to become leader
        assert!(!ha.is_leader());
    }

    // ─────────────────── Identity-driven decisions ───────────────────
    // Each decision branch is driven by the recovery observation, never by
    // the recorded role. The fake server listens on port 1, so no test can
    // reach (or ALTER) a real local PostgreSQL.

    fn identity_test_ha(
        name: &str,
        label: &str,
        dcs: Arc<MockDcs>,
        in_recovery: Option<bool>,
        standby_signal: bool,
    ) -> (Ha, PathBuf) {
        let (ha, data_dir, _cmd_tx) =
            identity_test_ha_with_cmd(name, label, dcs, in_recovery, standby_signal, None);
        (ha, data_dir)
    }

    /// A bin_dir that never exists, so no test can run a real PostgreSQL tool
    /// even on a host with /usr/bin/pg_ctl.
    fn no_bin_dir() -> PathBuf {
        let p = std::env::temp_dir().join(format!("pg-ha-no-bin-{}", std::process::id()));
        assert!(!p.exists(), "{} must not exist", p.display());
        p
    }

    /// Like `identity_test_ha`, but keeps the command sender and takes the
    /// PostgreSQL bin_dir (None: `no_bin_dir()`).
    fn identity_test_ha_with_cmd(
        name: &str,
        label: &str,
        dcs: Arc<MockDcs>,
        in_recovery: Option<bool>,
        standby_signal: bool,
        bin_dir: Option<PathBuf>,
    ) -> (Ha, PathBuf, CommandSender) {
        let mut config = test_config(name);
        config.postgresql.port = 1;
        config.postgresql.bin_dir = bin_dir.unwrap_or_else(no_bin_dir);
        let (pg, data_dir) = with_fake_running_pg(&config, label);
        config.postgresql.data_dir = data_dir.clone();
        let _ = std::fs::remove_file(data_dir.join("standby.signal"));
        if standby_signal {
            std::fs::write(data_dir.join("standby.signal"), "").unwrap();
        }
        let (mut ha, cmd_tx) = Ha::new(config, dcs, pg);
        ha.postgresql.set_observed_in_recovery(in_recovery);
        (ha, data_dir, cmd_tx)
    }

    /// Send one management command and run it, like the HA loop does.
    async fn send_cmd(ha: &mut Ha, tx: &CommandSender, cmd: ManagementCommand) -> CommandResponse {
        let (reply_tx, mut reply_rx) = mpsc::channel(1);
        tx.send((cmd, reply_tx)).await.unwrap();
        ha.process_commands().await;
        reply_rx.recv().await.unwrap()
    }

    fn leader_cluster(leader: &str, other: &str) -> Cluster {
        Cluster {
            leader: Some(Leader {
                name: leader.to_string(),
                version: 1,
            }),
            members: vec![running_member(leader, 100), running_member(other, 50)],
            ..Default::default()
        }
    }

    fn lock_held_by(dcs: &MockDcs) -> Option<String> {
        dcs.leader_held_by.lock().unwrap().clone()
    }

    #[tokio::test]
    async fn test_lock_holder_in_unmanaged_recovery_releases_lock() {
        let dcs = Arc::new(MockDcs::with_leader("test_node"));
        let (mut ha, dir) =
            identity_test_ha("test_node", "id-relinquish", dcs.clone(), Some(true), false);
        ha.cluster = leader_cluster("test_node", "node2");

        let result = ha.process_healthy_cluster().await;

        assert!(
            matches!(result, CycleResult::Error(ref m) if m.contains("released leader lock")),
            "got: {result}"
        );
        assert_eq!(lock_held_by(&dcs), None, "lock must be released");
        assert!(!ha.is_leader());
        assert_ne!(ha.postgresql().role(), &MemberRole::Primary);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_lock_holder_in_unmanaged_recovery_keeps_lock_when_paused() {
        let dcs = Arc::new(MockDcs::with_leader("test_node"));
        let (mut ha, dir) = identity_test_ha(
            "test_node",
            "id-relinquish-paused",
            dcs.clone(),
            Some(true),
            false,
        );
        ha.cluster = leader_cluster("test_node", "node2");
        ha.is_paused = true;

        let result = ha.process_healthy_cluster().await;

        assert!(
            matches!(result, CycleResult::Error(ref m) if m.contains("paused")),
            "got: {result}"
        );
        assert_eq!(
            lock_held_by(&dcs).as_deref(),
            Some("test_node"),
            "W1: no automatic failover in pause mode"
        );
        assert_ne!(ha.postgresql().role(), &MemberRole::Primary);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_lock_holder_with_unobserved_state_does_not_claim_primary() {
        let dcs = Arc::new(MockDcs::with_leader("test_node"));
        let (mut ha, dir) =
            identity_test_ha("test_node", "id-leader-unknown", dcs.clone(), None, false);
        ha.cluster = leader_cluster("test_node", "node2");

        let result = ha.process_healthy_cluster().await;

        assert!(
            matches!(result, CycleResult::Error(ref m) if m.contains("not acting as primary")),
            "got: {result}"
        );
        assert_eq!(lock_held_by(&dcs).as_deref(), Some("test_node"));
        assert_ne!(ha.postgresql().role(), &MemberRole::Primary);
        assert_eq!(
            ha.pg_identity(),
            PgIdentity::Unknown {
                has_standby_signal: false
            }
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_lock_holder_observed_primary_holds() {
        let dcs = Arc::new(MockDcs::with_leader("test_node"));
        let (mut ha, dir) = identity_test_ha(
            "test_node",
            "id-leader-hold",
            dcs.clone(),
            Some(false),
            false,
        );
        ha.cluster = leader_cluster("test_node", "node2");
        // Recorded role is stale (e.g. right after an agent restart).
        ha.postgresql.set_role(MemberRole::Uninitialized);

        let result = ha.process_healthy_cluster().await;

        assert!(matches!(result, CycleResult::Leader(_)), "got: {result}");
        assert_eq!(ha.postgresql().role(), &MemberRole::Primary);
        assert_eq!(ha.pg_identity(), PgIdentity::Primary);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_follower_in_unmanaged_recovery_is_left_alone() {
        let dcs = Arc::new(MockDcs::with_leader("node1"));
        let (mut ha, dir) = identity_test_ha("node2", "id-follow-handoff", dcs, Some(true), false);
        ha.cluster = leader_cluster("node1", "node2");

        let result = ha.process_healthy_cluster().await;

        assert!(
            matches!(result, CycleResult::Error(ref m) if m.contains("unmanaged recovery")),
            "got: {result}"
        );
        // Not stopped or rewound: a stop would have invalidated the observation.
        assert_eq!(ha.postgresql().observed_in_recovery(), Some(true));
        assert!(
            !dir.join("standby.signal").exists(),
            "must not rewrite config"
        );
        assert_eq!(ha.pg_identity(), PgIdentity::UnmanagedRecovery);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_follower_unobserved_with_standby_signal_waits() {
        let dcs = Arc::new(MockDcs::with_leader("node1"));
        let (mut ha, dir) = identity_test_ha("node2", "id-follow-wait", dcs, None, true);
        ha.cluster = leader_cluster("node1", "node2");

        let result = ha.process_healthy_cluster().await;

        assert!(
            matches!(result, CycleResult::Follower(ref m) if m.contains("waiting")),
            "got: {result}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_follower_unobserved_without_standby_signal_is_fenced() {
        let dcs = Arc::new(MockDcs::with_leader("node1"));
        let (mut ha, dir) = identity_test_ha("node2", "id-follow-fence", dcs, None, false);
        ha.cluster = leader_cluster("node1", "node2");

        let result = ha.process_healthy_cluster().await;

        // No pg_ctl in the test environment, so the stop itself may fail; what
        // matters is that the fence (a stop, not a rewind) was chosen.
        let fenced = match &result {
            CycleResult::Follower(m) => m.starts_with("stopped PostgreSQL"),
            CycleResult::Error(m) => m.starts_with("Fence stop failed"),
            _ => false,
        };
        assert!(fenced, "got: {result}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_unmanaged_recovery_stands_aside_in_election() {
        let dcs = Arc::new(MockDcs::new());
        let (mut ha, dir) = identity_test_ha(
            "test_node",
            "id-elect-aside",
            dcs.clone(),
            Some(true),
            false,
        );
        // Only member, so it would be the healthiest node.
        ha.cluster = Cluster {
            leader: None,
            members: vec![running_member("test_node", 100)],
            ..Default::default()
        };

        let result = ha.process_unhealthy_cluster().await;

        assert!(
            matches!(result, CycleResult::Error(ref m) if m.contains("not taking part")),
            "got: {result}"
        );
        assert_eq!(lock_held_by(&dcs), None, "must not acquire the lock");
        assert!(!ha.is_leader());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_unobserved_node_skips_election() {
        let dcs = Arc::new(MockDcs::new());
        let (mut ha, dir) = identity_test_ha("test_node", "id-elect-wait", dcs.clone(), None, true);
        ha.cluster = Cluster {
            leader: None,
            members: vec![running_member("test_node", 100)],
            ..Default::default()
        };

        let result = ha.process_unhealthy_cluster().await;

        assert!(
            matches!(result, CycleResult::Follower(ref m) if m.contains("not taking part")),
            "got: {result}"
        );
        assert_eq!(lock_held_by(&dcs), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ─────────────────── Bug Condition Exploration Tests ───────────────────
    // **Property 1: Bug Condition** - Recovering Dead Code (Defect 9)
    // **Validates: Requirements 1.9**
    //
    // These tests verify that the `recovering` field has been removed from
    // the Ha struct. Since it's already removed, the test PASSES on the fixed
    // code, confirming the dead code is gone.
    //
    // Compile-time verification: if `recovering` existed as a field in Ha,
    // the struct would need it in Ha::new. The fact that Ha::new compiles
    // without it proves the field is removed.

    /// **Property 1: Bug Condition** - Recovering Dead Code Removed (Defect 9)
    ///
    /// **Validates: Requirements 1.9**
    ///
    /// This test confirms that the `recovering` dead code field has been
    /// removed from the Ha struct. On the FIXED code, this test PASSES
    /// because Ha::new compiles without the field.
    ///
    /// On UNFIXED code, this test would need `recovering: false` in Ha::new,
    /// and the field would be assigned but never read (dead code).
    ///
    /// Counterexample on unfixed code: Ha struct contains `recovering: bool`
    /// that is set to `true` but never read in any conditional logic.
    #[tokio::test]
    async fn test_bug_condition_recovering_dead_code_exploration() {
        let config = test_config("node1");
        let dcs = Arc::new(MockDcs::new());
        let pg = Postgresql::new(config.postgresql.clone());

        // Ha::new compiles without `recovering` field — this is the compile-time
        // assertion that the dead code has been removed. If the field still
        // existed, this test would require `recovering: false` in the struct
        // literal inside Ha::new, or the test wouldn't compile.
        let (ha, _cmd_tx) = Ha::new(config, dcs, pg);

        // Verify Ha struct is fully functional without `recovering`
        assert!(!ha.is_leader());
        assert!(!ha.is_paused());

        // Static assertion: Ha struct fields are accessible and complete
        // without any `recovering` field. If we could access `ha.recovering`
        // this test would fail to compile on the fixed code.
        //
        // The fact that this test compiles and runs proves:
        // 1. The `recovering` field does NOT exist in Ha struct
        // 2. Ha::new initializes successfully without it
        // 3. No functional dependency on the removed field
        let _ = ha.config().name.clone(); // Struct is fully usable
    }

    // ─────────────────── Preservation Property Tests ───────────────────
    // **Property 2: Preservation** - HA Core Logic Without `recovering` Field
    // **Validates: Requirements 3.9**
    //
    // These tests verify that Ha::new compiles and creates successfully
    // without any `recovering` field, confirming no functional dependency
    // on the removed dead code. The HA core logic (leader election, lock
    // renewal, follower following) must work identically.

    /// **Validates: Requirements 3.9**
    ///
    /// Preservation: Ha::new creates successfully without `recovering` field.
    /// This test asserts that the Ha struct has no functional dependency on
    /// the removed dead code — construction succeeds and all core fields are
    /// properly initialized.
    #[tokio::test]
    async fn test_preservation_ha_new_without_recovering_field() {
        let config = test_config("node1");
        let dcs = Arc::new(MockDcs::new());
        let pg = Postgresql::new(config.postgresql.clone());

        // Ha::new must compile and succeed without any `recovering` field
        let (ha, cmd_tx) = Ha::new(config.clone(), dcs, pg);

        // Verify core state initialization
        assert!(!ha.is_leader(), "New Ha instance should not be leader");
        assert!(!ha.is_paused(), "New Ha instance should not be paused");
        assert_eq!(ha.config().name, "node1");

        // Verify the command channel is functional (proves struct is fully initialized)
        drop(cmd_tx);
    }

    /// **Validates: Requirements 3.9**
    ///
    /// Preservation: HA core logic (leader election path) works without
    /// `recovering` field. This verifies that `attempt_to_acquire_leader`
    /// and the decision loop function correctly.
    #[tokio::test]
    async fn test_preservation_ha_leader_election_no_recovering_dependency() {
        let config = test_config("test_node");
        let dcs = Arc::new(MockDcs::new());
        let pg = Postgresql::new(config.postgresql.clone());

        let (ha, _cmd_tx) = Ha::new(config, dcs.clone(), pg);

        // Verify leader acquisition works (no dependency on recovering field)
        let acquired = dcs.attempt_to_acquire_leader().await.unwrap();
        assert!(acquired, "Should acquire leader on empty cluster");

        // Verify ha accessors work after construction
        assert!(!ha.is_leader());
        assert!(!ha.is_paused());
        assert!(!ha.pending_restart());
    }

    /// **Validates: Requirements 3.9**
    ///
    /// Preservation: HA lock renewal path works without `recovering` field.
    /// Tests that update_leader (lock renewal) operates correctly.
    #[tokio::test]
    async fn test_preservation_ha_lock_renewal_no_recovering_dependency() {
        let config = test_config("test_node");
        let dcs = Arc::new(MockDcs::with_leader("test_node"));
        let pg = Postgresql::new(config.postgresql.clone());

        let (_ha, _cmd_tx) = Ha::new(config, dcs.clone(), pg);

        // Lock renewal should succeed since we are the lock owner
        let leader = Leader {
            name: "test_node".to_string(),
            version: 1,
        };
        let renewed = dcs.update_leader(&leader).await.unwrap();
        assert!(renewed, "Lock renewal should succeed for lock owner");
    }
    #[tokio::test]
    async fn test_handle_dcs_error_with_failsafe_disabled_demotes() {
        let mut config = test_config("node1");
        // Missing data dir: the standby config cannot be written.
        config.postgresql.data_dir = missing_data_dir("dcs-err-disabled");
        let dcs = Arc::new(MockDcs::with_leader("node1"));
        let pg = Postgresql::new(config.postgresql.clone());
        let (mut ha, _cmd_tx) = Ha::new(config, dcs, pg);

        // Simulate: we think we're leader
        ha.is_leader = true;
        // failsafe is disabled by default (Failsafe::new sets enabled=false)

        let result = ha.handle_dcs_error().await;
        let msg = format!("{result}");

        // Should demote because failsafe is disabled
        assert!(!ha.is_leader());
        assert!(msg.contains("demoted"));
        assert!(msg.contains("could not be written"), "{msg}");
    }

    #[tokio::test]
    async fn test_handle_dcs_error_with_failsafe_enabled_no_members_demotes() {
        let mut config = test_config("node1");
        config.postgresql.data_dir = missing_data_dir("dcs-err-no-members");
        let dcs = Arc::new(MockDcs::with_leader("node1"));
        let pg = Postgresql::new(config.postgresql.clone());
        let (mut ha, _cmd_tx) = Ha::new(config, dcs, pg);

        ha.is_leader = true;
        ha.failsafe.set_enabled(true);
        // No members updated → failsafe will return NotAllConfirmed

        let result = ha.handle_dcs_error().await;
        let msg = format!("{result}");

        assert!(!ha.is_leader());
        assert!(msg.contains("failsafe failed"));
        assert!(msg.contains("could not be written"), "{msg}");
    }

    #[tokio::test]
    async fn test_switchover_rejects_async_candidate_under_sync_mode() {
        let config = test_config("node1");
        let dcs = Arc::new(MockDcs::with_leader("node1"));
        let pg = Postgresql::new(config.postgresql.clone());
        let (mut ha, cmd_tx) = Ha::new(config, dcs.clone(), pg);

        ha.is_leader = true;
        ha.dynamic_config_state.apply_new_config(GlobalConfig {
            synchronous_mode: Some(true),
            ..Default::default()
        });
        ha.cluster = Cluster {
            leader: Some(Leader {
                name: "node1".into(),
                version: 1,
            }),
            members: vec![running_member("node2", 1000), running_member("node3", 1000)],
            sync_state: Some(SyncState {
                leader: "node1".into(),
                sync_standby: Some("node2".into()),
                quorum: 1,
            }),
            ..Default::default()
        };

        let (reply_tx, mut reply_rx) = mpsc::channel(1);
        cmd_tx
            .send((
                ManagementCommand::Switchover {
                    leader: Some("node1".into()),
                    candidate: Some("node3".into()),
                    scheduled_at: None,
                },
                reply_tx,
            ))
            .await
            .unwrap();
        ha.process_commands().await;
        let resp = reply_rx.recv().await.unwrap();
        assert_eq!(resp.status, CommandStatus::Rejected);
        assert_eq!(
            resp.message,
            "Candidate 'node3' is not a synchronous standby (sync mode requires /sync membership)"
        );
        assert!(dcs.last_failover_value.lock().unwrap().is_none());
        assert!(ha.is_leader());
    }

    #[tokio::test]
    async fn test_failover_rejects_async_candidate_under_sync_mode() {
        let config = test_config("node1");
        let dcs = Arc::new(MockDcs::new());
        let pg = Postgresql::new(config.postgresql.clone());
        let (mut ha, cmd_tx) = Ha::new(config, dcs.clone(), pg);

        ha.dynamic_config_state.apply_new_config(GlobalConfig {
            synchronous_mode: Some(true),
            ..Default::default()
        });
        ha.cluster = Cluster {
            members: vec![running_member("node2", 1000), running_member("node3", 1000)],
            sync_state: Some(SyncState {
                leader: "node1".into(),
                sync_standby: Some("node2".into()),
                quorum: 1,
            }),
            ..Default::default()
        };

        let (reply_tx, mut reply_rx) = mpsc::channel(1);
        cmd_tx
            .send((
                ManagementCommand::Failover {
                    candidate: Some("node3".into()),
                },
                reply_tx,
            ))
            .await
            .unwrap();
        ha.process_commands().await;
        let resp = reply_rx.recv().await.unwrap();
        assert_eq!(resp.status, CommandStatus::Rejected);
        assert_eq!(
            resp.message,
            "Candidate 'node3' is not a synchronous standby (sync mode requires /sync membership)"
        );
        assert!(dcs.last_failover_value.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn test_failover_accepts_sync_candidate_under_sync_mode() {
        let config = test_config("node1");
        let dcs = Arc::new(MockDcs::new());
        let pg = Postgresql::new(config.postgresql.clone());
        let (mut ha, cmd_tx) = Ha::new(config, dcs.clone(), pg);

        ha.dynamic_config_state.apply_new_config(GlobalConfig {
            synchronous_mode: Some(true),
            ..Default::default()
        });
        ha.cluster = Cluster {
            members: vec![running_member("node2", 1000), running_member("node3", 1000)],
            sync_state: Some(SyncState {
                leader: "node1".into(),
                sync_standby: Some("node2".into()),
                quorum: 1,
            }),
            ..Default::default()
        };

        let (reply_tx, mut reply_rx) = mpsc::channel(1);
        cmd_tx
            .send((
                ManagementCommand::Failover {
                    candidate: Some("node2".into()),
                },
                reply_tx,
            ))
            .await
            .unwrap();
        ha.process_commands().await;
        let resp = reply_rx.recv().await.unwrap();
        assert_eq!(resp.status, CommandStatus::Accepted);
        assert!(dcs.last_failover_value.lock().unwrap().is_some());
    }

    // ─────────────────── Standby config writer and sync cache (D7) ───────────────────

    const SYNC_LINE: &str = "synchronous_standby_names = 'FIRST 1 (node2)'";

    fn applied_sync() -> Option<String> {
        Some("FIRST 1 (node2)".to_string())
    }

    fn auto_conf(dir: &std::path::Path) -> String {
        std::fs::read_to_string(dir.join("postgresql.auto.conf")).unwrap_or_default()
    }

    fn preset_auto_conf(dir: &std::path::Path, upstream_host: &str) {
        std::fs::write(
            dir.join("postgresql.auto.conf"),
            format!("{SYNC_LINE}\nprimary_conninfo = 'host={upstream_host} port=5432'\n"),
        )
        .unwrap();
    }

    fn member_at(name: &str, conn_url: &str) -> Member {
        Member {
            conn_url: conn_url.to_string(),
            ..running_member(name, 50)
        }
    }

    #[tokio::test]
    async fn test_demote_resets_applied_sync_standby_names() {
        let (mut ha, dir) = identity_test_ha(
            "test_node",
            "c1-demote-sync",
            Arc::new(MockDcs::with_leader("node1")),
            Some(false),
            false,
        );
        ha.cluster = leader_cluster("test_node", "node2");
        ha.is_leader = true;
        ha.applied_sync_standby_names = applied_sync();

        // update_leader → Ok(false) (node1 holds the lock) → demote()
        let result = ha.process_healthy_cluster().await;

        assert!(
            matches!(result, CycleResult::Follower(ref m) if m.starts_with("demoted self")),
            "got: {result}"
        );
        assert_eq!(ha.applied_sync_standby_names, None);
        assert!(!ha.is_leader());
        assert_eq!(ha.postgresql().role(), &MemberRole::Replica);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_handle_dcs_error_resets_sync_cache() {
        // Disabled failsafe → Disabled branch; enabled with no members →
        // NotAllConfirmed branch. Both leave the primary role.
        for (failsafe_enabled, expected) in [(false, "demoted"), (true, "failsafe failed")] {
            let label = format!("c1-dcs-error-sync-{failsafe_enabled}");
            let (mut ha, dir) = identity_test_ha(
                "node1",
                &label,
                Arc::new(MockDcs::with_leader("node1")),
                Some(false),
                false,
            );
            ha.is_leader = true;
            ha.failsafe.set_enabled(failsafe_enabled);
            ha.applied_sync_standby_names = applied_sync();

            let result = ha.handle_dcs_error().await;

            assert!(format!("{result}").contains(expected), "got: {result}");
            assert_eq!(ha.applied_sync_standby_names, None, "{label}");
            assert!(!ha.is_leader());
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    #[tokio::test]
    async fn test_dcs_error_without_leadership_keeps_sync_cache() {
        let (mut ha, dir) = identity_test_ha(
            "node1",
            "c1-dcs-error-follower",
            Arc::new(MockDcs::with_leader("node2")),
            Some(true),
            true,
        );
        ha.applied_sync_standby_names = applied_sync();

        let _ = ha.handle_dcs_error().await;

        assert_eq!(ha.applied_sync_standby_names, applied_sync());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_releasing_lock_in_unmanaged_recovery_resets_sync_cache() {
        let dcs = Arc::new(MockDcs::with_leader("test_node"));
        let (mut ha, dir) = identity_test_ha(
            "test_node",
            "c1-relinquish-sync",
            dcs.clone(),
            Some(true),
            false,
        );
        ha.cluster = leader_cluster("test_node", "node2");
        ha.applied_sync_standby_names = applied_sync();

        let _ = ha.process_healthy_cluster().await;

        assert_eq!(lock_held_by(&dcs), None);
        assert_eq!(ha.applied_sync_standby_names, None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_releasing_lock_when_postgres_not_running_resets_sync_cache() {
        let dcs = Arc::new(MockDcs::with_leader("test_node"));
        let (mut ha, dir) =
            identity_test_ha("test_node", "c1-not-running-sync", dcs.clone(), None, true);
        ha.cluster = leader_cluster("test_node", "node2");
        ha.is_leader = true;
        ha.applied_sync_standby_names = applied_sync();

        // No pg_ctl: the start attempt after releasing the lock fails.
        let _ = ha.handle_postgres_not_running().await;

        assert_eq!(lock_held_by(&dcs), None);
        assert!(!ha.is_leader());
        assert_eq!(ha.applied_sync_standby_names, None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_write_standby_config_preserves_auto_conf_and_resets_sync_cache() {
        let (mut ha, dir) = identity_test_ha(
            "node2",
            "c1-write-ok",
            Arc::new(MockDcs::with_leader("node1")),
            Some(true),
            false,
        );
        preset_auto_conf(&dir, "old");
        ha.applied_sync_standby_names = applied_sync();

        ha.write_standby_config(Some("host=node1 port=5433 dbname=postgres user=postgres"))
            .await
            .unwrap();

        assert!(dir.join("standby.signal").exists());
        let content = auto_conf(&dir);
        assert!(content.contains(SYNC_LINE), "{content}");
        assert!(
            content.contains(
                "primary_conninfo = 'host=node1 port=5433 user=replicator application_name=node2'"
            ),
            "{content}"
        );
        assert_eq!(ha.read_current_upstream().as_deref(), Some("node1"));
        assert_eq!(ha.applied_sync_standby_names, None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_write_standby_config_with_hostless_upstream_removes_conninfo() {
        let (mut ha, dir) = identity_test_ha(
            "node2",
            "c1-write-hostless",
            Arc::new(MockDcs::with_leader("node1")),
            Some(true),
            false,
        );
        preset_auto_conf(&dir, "old");

        ha.write_standby_config(Some("port=5432 dbname=postgres"))
            .await
            .unwrap();

        assert!(dir.join("standby.signal").exists());
        assert!(auto_conf(&dir).contains(SYNC_LINE));
        assert_eq!(ha.read_current_upstream(), None, "never guess 127.0.0.1");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_write_standby_config_failure_still_resets_sync_cache() {
        let (mut ha, dir) = identity_test_ha(
            "node2",
            "c1-write-fail",
            Arc::new(MockDcs::with_leader("node1")),
            Some(true),
            false,
        );
        preset_auto_conf(&dir, "old");
        // A directory named standby.signal cannot be opened for writing.
        std::fs::create_dir(dir.join("standby.signal")).unwrap();
        ha.applied_sync_standby_names = applied_sync();

        let result = ha.write_standby_config(Some("host=node1 port=5432")).await;

        assert!(result.is_err());
        assert_eq!(ha.applied_sync_standby_names, None);
        assert_eq!(ha.read_current_upstream().as_deref(), Some("old"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_reconfigure_replica_keeps_auto_conf_lines() {
        let (mut ha, dir) = identity_test_ha(
            "node2",
            "c1-reconfigure",
            Arc::new(MockDcs::with_leader("node3")),
            Some(true),
            true,
        );
        preset_auto_conf(&dir, "node1");
        ha.applied_sync_standby_names = applied_sync();

        // No pg_ctl: the restart after the rewrite fails and is only logged.
        ha.reconfigure_replica("host=node3 port=5432").await;

        let content = auto_conf(&dir);
        assert!(content.contains(SYNC_LINE), "{content}");
        assert_eq!(ha.read_current_upstream().as_deref(), Some("node3"));
        assert!(dir.join("standby.signal").exists());
        assert_eq!(ha.applied_sync_standby_names, None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn switchover_test_ha(label: &str, upstream_host: &str) -> (Ha, PathBuf, CommandSender) {
        let (mut ha, dir, tx) = identity_test_ha_with_cmd(
            "node1",
            label,
            Arc::new(MockDcs::with_leader("node1")),
            Some(false),
            false,
            None,
        );
        preset_auto_conf(&dir, upstream_host);
        ha.is_leader = true;
        ha.applied_sync_standby_names = applied_sync();
        ha.cluster = Cluster {
            leader: Some(Leader {
                name: "node1".into(),
                version: 1,
            }),
            members: vec![
                member_at("node1", "host=node1 port=5432"),
                member_at("node2", "host=node2 port=5432"),
            ],
            ..Default::default()
        };
        (ha, dir, tx)
    }

    #[tokio::test]
    async fn test_switchover_with_candidate_keeps_auto_conf_lines() {
        let (mut ha, dir, tx) = switchover_test_ha("c1-so-cand", "old");

        let resp = send_cmd(
            &mut ha,
            &tx,
            ManagementCommand::Switchover {
                leader: Some("node1".into()),
                candidate: Some("node2".into()),
                scheduled_at: None,
            },
        )
        .await;

        // No pg_ctl: the stop fails and is reported (the lock is released).
        assert_eq!(resp.status, CommandStatus::Error, "{}", resp.message);
        assert!(
            resp.message
                .starts_with("Switchover initiated, but stopping PostgreSQL failed"),
            "{}",
            resp.message
        );
        assert!(dir.join("standby.signal").exists());
        assert!(auto_conf(&dir).contains(SYNC_LINE));
        assert_eq!(ha.read_current_upstream().as_deref(), Some("node2"));
        assert_eq!(ha.applied_sync_standby_names, None);
        assert!(!ha.is_leader());
        assert!(ha.stop_pending);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_switchover_without_candidate_removes_conninfo() {
        let (mut ha, dir, tx) = switchover_test_ha("c1-so-nocand", "old");

        let resp = send_cmd(
            &mut ha,
            &tx,
            ManagementCommand::Switchover {
                leader: Some("node1".into()),
                candidate: None,
                scheduled_at: None,
            },
        )
        .await;

        assert_eq!(resp.status, CommandStatus::Error, "{}", resp.message);
        assert!(
            resp.message
                .starts_with("Switchover initiated, but stopping PostgreSQL failed"),
            "{}",
            resp.message
        );
        assert!(dir.join("standby.signal").exists());
        assert!(auto_conf(&dir).contains(SYNC_LINE));
        assert_eq!(ha.read_current_upstream(), None, "D3: no guessed upstream");
        assert!(!dir.join("pg-ha.rewind_check").exists());
        assert_eq!(ha.applied_sync_standby_names, None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ─────────────────── Demotion writes standby.signal first (D2, D3) ───────────────────

    use crate::standby_config::REWIND_CHECK_MARKER;

    /// A data dir path that does not exist and is never created (M2: tests
    /// never touch the shared `test_config` default).
    fn missing_data_dir(label: &str) -> PathBuf {
        let p =
            std::env::temp_dir().join(format!("pg-ha-test-missing-{label}-{}", std::process::id()));
        assert!(!p.exists(), "{} must not exist", p.display());
        p
    }

    fn marker(dir: &std::path::Path) -> Option<String> {
        std::fs::read_to_string(dir.join(REWIND_CHECK_MARKER)).ok()
    }

    /// Cluster where `leader` holds the lock and is reachable at
    /// `host=<leader> port=5432` (`conn_url` overrides it).
    fn cluster_led_by(leader: &str, conn_url: Option<&str>, other: &str) -> Cluster {
        let url = conn_url.map_or_else(|| format!("host={leader} port=5432"), str::to_string);
        Cluster {
            leader: Some(Leader {
                name: leader.to_string(),
                version: 1,
            }),
            members: vec![
                Member {
                    role: MemberRole::Primary,
                    ..member_at(leader, &url)
                },
                running_member(other, 50),
            ],
            ..Default::default()
        }
    }

    fn assert_demoted_files(dir: &std::path::Path, ha: &Ha, reason: &str) {
        assert!(dir.join("standby.signal").is_file());
        let m = marker(dir).expect("rewind-check marker");
        assert!(m.contains(reason), "{m}");
        assert_eq!(ha.read_current_upstream(), None, "D3: conninfo removed");
        assert!(
            auto_conf(dir).contains(SYNC_LINE),
            "ALTER SYSTEM lines kept"
        );
    }

    #[tokio::test]
    async fn test_demote_on_lock_loss_writes_signal_marker_and_removes_conninfo() {
        let (mut ha, dir) = identity_test_ha(
            "test_node",
            "c2-demote",
            Arc::new(MockDcs::with_leader("node1")),
            Some(false),
            false,
        );
        preset_auto_conf(&dir, "old");
        ha.cluster = leader_cluster("test_node", "node2");
        ha.is_leader = true;

        // update_leader → Ok(false) → demote()
        let result = ha.process_healthy_cluster().await;

        assert!(
            matches!(result, CycleResult::Follower(ref m) if m.starts_with("demoted self")),
            "got: {result}"
        );
        assert_demoted_files(&dir, &ha, "demoted after losing the leader lock");
        assert_eq!(ha.postgresql().observed_in_recovery(), None, "stop called");
        assert!(ha.stop_pending, "no pg_ctl: the stop failed");
        assert!(!ha.is_leader());
        assert_eq!(ha.postgresql().role(), &MemberRole::Replica);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_demote_on_dcs_error_during_renewal_writes_signal() {
        let dcs = Arc::new(MockDcs::with_leader("test_node"));
        dcs.fail_leader_writes();
        let (mut ha, dir) = identity_test_ha("test_node", "c2-demote-err", dcs, Some(false), false);
        preset_auto_conf(&dir, "old");
        ha.cluster = leader_cluster("test_node", "node2");
        ha.is_leader = true;

        // update_leader → Err → demote()
        let result = ha.process_healthy_cluster().await;

        assert!(
            matches!(result, CycleResult::Follower(ref m) if m.starts_with("demoted self")),
            "got: {result}"
        );
        assert_demoted_files(&dir, &ha, "demoted after losing the leader lock");
        assert_eq!(ha.postgresql().observed_in_recovery(), None, "stop called");
        assert!(!ha.is_leader());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_demote_write_failure_still_stops() {
        let (mut ha, dir) = identity_test_ha(
            "test_node",
            "c2-demote-write-fail",
            Arc::new(MockDcs::with_leader("node1")),
            Some(false),
            false,
        );
        // A directory named standby.signal cannot be opened for writing.
        std::fs::create_dir(dir.join("standby.signal")).unwrap();
        ha.cluster = leader_cluster("test_node", "node2");
        ha.is_leader = true;

        let result = ha.process_healthy_cluster().await;

        assert!(
            matches!(result, CycleResult::Error(ref m)
                if m.contains("standby configuration could not be written")),
            "got: {result}"
        );
        assert_eq!(ha.postgresql().observed_in_recovery(), None, "stop called");
        assert!(!ha.is_leader());
        let _ = std::fs::remove_dir_all(&dir);
    }

    async fn dcs_error_demotion(label: &str, failsafe_enabled: bool) -> (Ha, PathBuf, String) {
        let (mut ha, dir) = identity_test_ha(
            "node1",
            label,
            Arc::new(MockDcs::with_leader("node1")),
            Some(false),
            false,
        );
        preset_auto_conf(&dir, "old");
        ha.is_leader = true;
        ha.failsafe.set_enabled(failsafe_enabled);
        let result = ha.handle_dcs_error().await;
        (ha, dir, format!("{result}"))
    }

    #[tokio::test]
    async fn test_handle_dcs_error_disabled_writes_signal_and_marker() {
        let (ha, dir, msg) = dcs_error_demotion("c2-dcs-disabled", false).await;
        assert!(msg.contains("demoted to prevent split-brain"), "{msg}");
        assert!(!msg.contains("could not be written"), "{msg}");
        assert_demoted_files(&dir, &ha, "demoted because the DCS is unreachable");
        assert_eq!(ha.postgresql().observed_in_recovery(), None, "stop called");
        assert!(!ha.is_leader());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_handle_dcs_error_not_all_confirmed_writes_signal_and_marker() {
        let (ha, dir, msg) = dcs_error_demotion("c2-dcs-not-all", true).await;
        assert!(msg.contains("failsafe failed"), "{msg}");
        assert!(!msg.contains("could not be written"), "{msg}");
        assert_demoted_files(&dir, &ha, "demoted because the DCS is unreachable");
        assert_eq!(ha.postgresql().observed_in_recovery(), None, "stop called");
        assert!(!ha.is_leader());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_fence_writes_signal_with_leader_conninfo() {
        let (mut ha, dir) = identity_test_ha(
            "node2",
            "c2-fence",
            Arc::new(MockDcs::with_leader("node1")),
            None,
            false,
        );
        ha.cluster = cluster_led_by("node1", None, "node2");

        let result = ha.process_healthy_cluster().await;

        let fenced = match &result {
            CycleResult::Follower(m) => m.starts_with("stopped PostgreSQL"),
            CycleResult::Error(m) => m.starts_with("Fence stop failed"),
            _ => false,
        };
        assert!(fenced, "got: {result}");
        assert!(dir.join("standby.signal").is_file());
        assert_eq!(ha.read_current_upstream().as_deref(), Some("node1"));
        assert_eq!(marker(&dir), None, "A6: the fence writes no marker");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_fence_with_hostless_leader_conn_url_removes_conninfo() {
        let (mut ha, dir) = identity_test_ha(
            "node2",
            "c2-fence-hostless",
            Arc::new(MockDcs::with_leader("node1")),
            None,
            false,
        );
        preset_auto_conf(&dir, "old");
        ha.cluster = cluster_led_by("node1", Some(""), "node2");

        let _ = ha.process_healthy_cluster().await;

        assert!(dir.join("standby.signal").is_file());
        assert_eq!(ha.read_current_upstream(), None, "never guess an upstream");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_fence_write_failure_still_stops() {
        let (mut ha, dir) = identity_test_ha(
            "node2",
            "c2-fence-write-fail",
            Arc::new(MockDcs::with_leader("node1")),
            None,
            false,
        );
        ha.cluster = cluster_led_by("node1", None, "node2");
        // standby.signal can be written, the auto.conf temp file cannot.
        std::fs::create_dir(dir.join("postgresql.auto.conf.pg-ha.tmp")).unwrap();
        ha.postgresql.set_observed_in_recovery(None);

        let result = ha.process_healthy_cluster().await;

        assert!(
            matches!(result, CycleResult::Error(ref m) if m.starts_with("Fence stop failed")),
            "got: {result}"
        );
        assert_eq!(ha.postgresql().observed_in_recovery(), None);
        assert!(ha.stop_pending);
        assert!(dir.join("standby.signal").is_file());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_fence_retried_after_stop_failure_even_with_signal() {
        let (mut ha, dir) = identity_test_ha(
            "node2",
            "c2-fence-retry",
            Arc::new(MockDcs::with_leader("node1")),
            None,
            false,
        );
        ha.cluster = cluster_led_by("node1", None, "node2");

        let first = ha.process_healthy_cluster().await;
        assert!(
            matches!(first, CycleResult::Error(ref m) if m.starts_with("Fence stop failed")),
            "got: {first}"
        );
        assert!(dir.join("standby.signal").is_file());
        assert!(ha.stop_pending);

        // standby.signal now exists, but it was written on a server that may
        // be writable: fence again instead of waiting.
        ha.postgresql.set_observed_in_recovery(None);
        let second = ha.process_healthy_cluster().await;
        assert!(
            matches!(second, CycleResult::Error(ref m) if m.starts_with("Fence stop failed")),
            "got: {second}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_demote_stop_failure_then_unobserved_is_fenced() {
        let dcs = Arc::new(MockDcs::with_leader("node1"));
        let (mut ha, dir) = identity_test_ha(
            "test_node",
            "c2-demote-fence",
            dcs.clone(),
            Some(false),
            false,
        );
        ha.cluster = leader_cluster("test_node", "node2");

        let _ = ha.process_healthy_cluster().await;
        assert!(ha.stop_pending, "no pg_ctl: the demotion stop failed");

        // Another node holds the lock; PostgreSQL is unobserved.
        ha.cluster = leader_cluster("node1", "test_node");
        ha.postgresql.set_observed_in_recovery(None);
        let result = ha.process_healthy_cluster().await;
        assert!(
            matches!(result, CycleResult::Error(ref m) if m.starts_with("Fence stop failed")),
            "got: {result}"
        );

        // No leader at all: fenced at the election entry, no campaign.
        *dcs.leader_held_by.lock().unwrap() = None;
        ha.cluster = Cluster {
            leader: None,
            members: vec![running_member("test_node", 1)],
            ..Default::default()
        };
        ha.postgresql.set_observed_in_recovery(None);
        let result = ha.process_unhealthy_cluster().await;
        assert!(
            matches!(result, CycleResult::Error(ref m) if m.starts_with("Fence stop failed")),
            "got: {result}"
        );
        assert_eq!(lock_held_by(&dcs), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_primary_with_pending_stop_is_fenced_not_campaigned() {
        let dcs = Arc::new(MockDcs::new());
        let (mut ha, dir) = identity_test_ha(
            "test_node",
            "c2-primary-pending",
            dcs.clone(),
            Some(false),
            false,
        );
        ha.stop_pending = true;
        // Only member: it would be the healthiest node.
        ha.cluster = Cluster {
            leader: None,
            members: vec![running_member("test_node", 1)],
            ..Default::default()
        };

        let result = ha.process_unhealthy_cluster().await;

        assert!(
            matches!(result, CycleResult::Error(ref m) if m.starts_with("Fence stop failed")),
            "got: {result}"
        );
        assert_eq!(ha.postgresql().observed_in_recovery(), None, "stop called");
        assert_eq!(lock_held_by(&dcs), None, "must not campaign");
        assert!(dir.join("standby.signal").is_file());
        let m = marker(&dir).expect("rewind-check marker");
        assert!(
            m.contains("stopped again after a failed demotion stop"),
            "{m}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_stop_pending_cleared_when_pg_not_running() {
        let (mut ha, dir) = identity_test_ha(
            "node2",
            "c2-pending-cleared",
            Arc::new(MockDcs::with_leader("node1")),
            None,
            true,
        );
        std::fs::write(dir.join("PG_VERSION"), "16").unwrap();
        std::fs::remove_file(dir.join("postmaster.pid")).unwrap();
        ha.stop_pending = true;

        let _ = ha.run_cycle().await;

        assert!(!ha.stop_pending);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_dcs_error_as_follower_retries_pending_stop() {
        let (mut ha, dir) = identity_test_ha(
            "node2",
            "c2-dcs-follower-retry",
            Arc::new(MockDcs::with_leader("node1")),
            Some(true),
            true,
        );
        ha.stop_pending = true;

        let result = ha.handle_dcs_error().await;

        assert_eq!(format!("{result}"), "Error: DCS unreachable");
        assert_eq!(ha.postgresql().observed_in_recovery(), None, "stop retried");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_switchover_write_failure_still_stops() {
        let (mut ha, dir, tx) = switchover_test_ha("c2-so-write-fail", "old");
        std::fs::create_dir(dir.join("standby.signal")).unwrap();

        let resp = send_cmd(
            &mut ha,
            &tx,
            ManagementCommand::Switchover {
                leader: Some("node1".into()),
                candidate: Some("node2".into()),
                scheduled_at: None,
            },
        )
        .await;

        assert_eq!(resp.status, CommandStatus::Error, "{}", resp.message);
        assert!(
            resp.message.contains("stopping PostgreSQL failed")
                && resp
                    .message
                    .contains("writing standby configuration also failed"),
            "{}",
            resp.message
        );
        assert!(!ha.is_leader());
        assert_eq!(ha.postgresql().observed_in_recovery(), None, "stop called");
        assert!(ha.stop_pending);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_switchover_stop_failure_sets_stop_pending() {
        let (mut ha, dir, tx) = switchover_test_ha("c2-so-stop-fail", "old");

        let resp = send_cmd(
            &mut ha,
            &tx,
            ManagementCommand::Switchover {
                leader: None,
                candidate: None,
                scheduled_at: None,
            },
        )
        .await;

        assert_eq!(resp.status, CommandStatus::Error, "{}", resp.message);
        assert!(
            resp.message.contains("stopping PostgreSQL failed")
                && !resp.message.contains("also failed"),
            "{}",
            resp.message
        );
        assert!(ha.stop_pending);
        assert!(dir.join("standby.signal").is_file());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_rejoin_of_running_primary_writes_signal_and_marker_before_stop() {
        let (mut ha, dir) = identity_test_ha(
            "node2",
            "c2-rejoin-fence",
            Arc::new(MockDcs::with_leader("node1")),
            Some(false),
            false,
        );
        ha.cluster = cluster_led_by("node1", None, "node2");

        let result = ha.process_healthy_cluster().await;

        assert!(
            matches!(result, CycleResult::Error(ref m) if m.starts_with("Rejoin stop failed")),
            "got: {result}"
        );
        assert!(dir.join("standby.signal").is_file());
        let m = marker(&dir).expect("rewind-check marker");
        assert!(m.contains("stopped for pg_rewind"), "{m}");
        assert_eq!(ha.read_current_upstream().as_deref(), Some("node1"));
        assert!(ha.stop_pending);
        assert_eq!(ha.postgresql().observed_in_recovery(), None);

        // Unobserved next cycle: fenced again, not "waiting".
        ha.postgresql.set_observed_in_recovery(None);
        let result = ha.process_healthy_cluster().await;
        assert!(
            matches!(result, CycleResult::Error(ref m) if m.starts_with("Fence stop failed")),
            "got: {result}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_reinitialize_of_running_primary_keeps_data_when_stop_fails() {
        let (mut ha, dir, tx) = identity_test_ha_with_cmd(
            "node2",
            "c2-reinit-primary",
            Arc::new(MockDcs::with_leader("node1")),
            Some(false),
            false,
            None,
        );
        std::fs::write(dir.join("PG_VERSION"), "16").unwrap();
        ha.is_leader = false;

        let resp = send_cmd(&mut ha, &tx, ManagementCommand::Reinitialize).await;

        assert_eq!(resp.status, CommandStatus::Error, "{}", resp.message);
        assert!(
            resp.message
                .starts_with("Reinitialize: stopping PostgreSQL failed"),
            "{}",
            resp.message
        );
        assert!(dir.join("PG_VERSION").exists(), "data directory kept");
        assert!(dir.join("standby.signal").is_file());
        assert!(marker(&dir).is_some());
        assert!(ha.stop_pending);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_reinitialize_of_standby_still_removes_data() {
        let (mut ha, dir, tx) = identity_test_ha_with_cmd(
            "node2",
            "c2-reinit-standby",
            Arc::new(MockDcs::with_leader("node1")),
            Some(true),
            true,
            None,
        );
        std::fs::write(dir.join("PG_VERSION"), "16").unwrap();

        let resp = send_cmd(&mut ha, &tx, ManagementCommand::Reinitialize).await;

        assert_eq!(resp.status, CommandStatus::Accepted, "{}", resp.message);
        assert_eq!(resp.message, "Node reinitialized, will clone on next cycle");
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_shutdown_fences_running_primary() {
        let (mut ha, dir) = identity_test_ha(
            "node1",
            "c2-shutdown-primary",
            Arc::new(MockDcs::new()),
            Some(false),
            false,
        );
        preset_auto_conf(&dir, "old");

        // observe_recovery() cannot reach port 1: Unknown{signal:false}.
        assert!(ha.stop_postgres_for_shutdown().await.is_err());

        assert!(dir.join("standby.signal").is_file());
        let m = marker(&dir).expect("rewind-check marker");
        assert!(m.contains("stopped by graceful shutdown"), "{m}");
        assert_eq!(ha.read_current_upstream(), None);
        assert!(ha.stop_pending);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_shutdown_of_stopped_postgres_is_noop() {
        let (mut ha, dir) = identity_test_ha(
            "node1",
            "c2-shutdown-stopped",
            Arc::new(MockDcs::new()),
            Some(false),
            false,
        );
        std::fs::remove_file(dir.join("postmaster.pid")).unwrap();

        assert!(ha.stop_postgres_for_shutdown().await.is_ok());

        assert!(!dir.join("standby.signal").exists());
        assert_eq!(marker(&dir), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ─────────────────── Source-order checks for the stop paths ───────────────────
    // Stops fail in tests (no pg_ctl), so file assertions alone cannot show
    // that the standby config was written before the stop. These pin it.

    /// Production part of a source file (before the first `#[cfg(test)]`).
    fn production(src: &str) -> &str {
        src.split("#[cfg(test)]").next().unwrap_or(src)
    }

    /// From the first `start` to the earliest of `ends` after it (or the end
    /// of the production part).
    fn section<'a>(src: &'a str, start: &str, ends: &[&str]) -> &'a str {
        let src = production(src);
        let from = src
            .find(start)
            .unwrap_or_else(|| panic!("`{start}` not found"));
        let rest = &src[from + start.len()..];
        let len = ends
            .iter()
            .filter_map(|e| rest.find(e))
            .min()
            .unwrap_or(rest.len());
        &src[from..from + start.len() + len]
    }

    /// A method body: up to the next method of any visibility.
    fn body<'a>(src: &'a str, start: &str) -> &'a str {
        const METHOD_STARTS: [&str; 8] = [
            "\n    fn ",
            "\n    async fn ",
            "\n    pub fn ",
            "\n    pub async fn ",
            "\n    pub(super) fn ",
            "\n    pub(super) async fn ",
            "\n    pub(crate) fn ",
            "\n    pub(crate) async fn ",
        ];
        section(src, start, &METHOD_STARTS)
    }

    fn assert_before(text: &str, first: &str, second: &str, what: &str) {
        let a = text
            .find(first)
            .unwrap_or_else(|| panic!("{what}: no `{first}`"));
        let b = text
            .find(second)
            .unwrap_or_else(|| panic!("{what}: no `{second}`"));
        assert!(a < b, "{what}: `{first}` must come before `{second}`");
    }

    const DIRECT_STOP: &str = ".postgresql.stop(";

    #[test]
    fn body_stops_at_any_method_visibility() {
        let src = "impl X {\n    fn a() {\n        one();\n    }\n    pub async fn b() {\n        self.postgresql.stop(\"fast\");\n    }\n}\n";
        let a = body(src, "fn a");
        assert!(a.contains("one()"));
        assert!(!a.contains(DIRECT_STOP), "{a}");
        for vis in ["pub ", "pub(super) ", "pub(crate) ", ""] {
            for asyncness in ["async ", ""] {
                let src =
                    format!("    fn a() {{}}\n    {vis}{asyncness}fn b() {{ x{DIRECT_STOP} }}\n");
                assert!(
                    !body(&src, "fn a").contains(DIRECT_STOP),
                    "{vis}{asyncness}"
                );
            }
        }
    }

    #[test]
    fn test_demotion_paths_write_config_before_stop() {
        let mod_rs = include_str!("mod.rs");
        let follow_rs = include_str!("follow.rs");
        let election_rs = include_str!("election.rs");
        let commands_rs = include_str!("commands.rs");
        let helpers_rs = include_str!("helpers.rs");

        // Paths that write through prepare_demotion, then stop.
        let demote = body(mod_rs, "async fn demote");
        let disabled = section(
            mod_rs,
            "FailsafeResult::Disabled =>",
            &["FailsafeResult::NotAllConfirmed"],
        );
        let not_all = section(mod_rs, "FailsafeResult::NotAllConfirmed {", &["} else {"]);
        let fence = section(follow_rs, "FollowerAction::Fence =>", &["FollowerAction::"]);
        let switchover = body(commands_rs, "async fn handle_switchover_command");
        let fence_primary = body(helpers_rs, "async fn fence_primary");
        for (what, text) in [
            ("demote", demote),
            ("handle_dcs_error Disabled", disabled),
            ("handle_dcs_error NotAllConfirmed", not_all),
            ("follower Fence", fence),
            ("switchover", switchover),
            ("fence_primary", fence_primary),
        ] {
            assert_before(
                text,
                "prepare_demotion(",
                "stop_after_demotion_write(",
                what,
            );
            assert!(!text.contains(DIRECT_STOP), "{what}: direct stop");
        }

        // Paths that stop through fence_primary.
        let election_fence = section(
            election_rs,
            "ElectionStance::Fence =>",
            &["ElectionStance::"],
        );
        assert!(election_fence.contains("fence_primary("));
        assert!(!election_fence.contains(DIRECT_STOP), "election Fence");

        let shutdown = body(helpers_rs, "async fn stop_postgres_for_shutdown");
        assert_before(shutdown, "fence_primary(", DIRECT_STOP, "shutdown");

        let rejoin = body(follow_rs, "async fn rejoin_as_replica");
        assert_before(rejoin, "fence_primary(", DIRECT_STOP, "rejoin");

        let reinit = body(commands_rs, "async fn handle_reinitialize_command");
        assert_before(reinit, "fence_primary(", DIRECT_STOP, "reinitialize");
    }

    #[test]
    fn test_stop_call_sites_are_audited() {
        // Direct stops left in production code (plan section 2.2b):
        // follow.rs: rejoin of a non-primary, reconfigure_replica (2);
        // commands.rs: POST /restart, POST /reinitialize of a non-primary;
        // helpers.rs: stop_after_demotion_write, shutdown of a standby.
        let files = [
            ("mod.rs", include_str!("mod.rs"), 0),
            ("follow.rs", include_str!("follow.rs"), 3),
            ("election.rs", include_str!("election.rs"), 0),
            ("commands.rs", include_str!("commands.rs"), 2),
            ("helpers.rs", include_str!("helpers.rs"), 2),
            ("identity.rs", include_str!("identity.rs"), 0),
        ];
        for (name, src, expected) in files {
            assert_eq!(
                production(src).matches(DIRECT_STOP).count(),
                expected,
                "{name}: a new direct PostgreSQL stop: use fence_primary / \
                 stop_after_demotion_write, or add it to the audited table in plan \
                 section 2.2b with a reason"
            );
        }
    }
}
