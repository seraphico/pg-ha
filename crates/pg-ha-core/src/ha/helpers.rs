//! Helper methods: member heartbeat, standby signal detection, reconfiguration guard,
//! upstream reading, and lock ownership verification.

use tracing::{error, info, warn};

use crate::error::Result;
use crate::standby_config;
use crate::timeline_files;

use crate::cluster::MemberRole;

use super::lease::{LeaderLease, LeaseAnchor};
use super::{CycleResult, Ha, LEASE_EXPIRED_REASON, identity, lineage};

/// This node's I3 verdict (D6), including the operator override.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum OwnEligibility {
    Eligible,
    Ineligible(lineage::Ineligible),
    /// `pg-ha.accept_timeline_loss` is present and nobody else may campaign:
    /// campaign anyway, accepting the loss of the newer history.
    Override(lineage::Ineligible),
    /// The override file is present but `eligible` may campaign.
    OverrideIgnored {
        why: lineage::Ineligible,
        eligible: String,
    },
}

/// Cycle result while a node is held after a pg_rewind failure (D5).
const REWIND_HOLD_RESULT: &str =
    "PostgreSQL stopped after a pg_rewind failure; operator action required (see the ERROR log)";

impl Ha {
    /// Writes standby.signal + primary_conninfo (None removes it) and forgets
    /// the applied synchronous_standby_names (D7). Runs in spawn_blocking.
    ///
    /// An upstream conn_url without `host=` is treated as an unknown leader:
    /// primary_conninfo is removed rather than pointed at a guessed address.
    pub(super) async fn write_standby_config(
        &mut self,
        upstream_conn_url: Option<&str>,
    ) -> std::io::Result<()> {
        self.reset_sync_cache();

        let upstream = upstream_conn_url.and_then(|url| {
            let built = standby_config::build_primary_conninfo(
                url,
                &self.config.postgresql.replication,
                &self.config.name,
            )
            .zip(standby_config::conn_url_host_port(url));
            if built.is_none() {
                warn!(
                    "Upstream conn_url has no host=; treating the leader as unknown and removing primary_conninfo"
                );
            }
            built
        });
        let (conninfo, host_port) = match upstream {
            Some((conninfo, host_port)) => (Some(conninfo), Some(host_port)),
            None => (None, None),
        };

        let data_dir = self.config.postgresql.data_dir.clone();
        tokio::task::spawn_blocking(move || {
            standby_config::write_standby_config(&data_dir, conninfo.as_deref())
        })
        .await
        .map_err(std::io::Error::other)??;

        match host_port {
            Some((host, port)) => info!(host = %host, port = %port, "Wrote standby configuration"),
            None => info!("Wrote standby configuration without primary_conninfo"),
        }
        Ok(())
    }

    /// standby.signal only (postgresql.auto.conf untouched, an existing
    /// primary_conninfo is kept, A8); also resets the sync cache. Runs in
    /// spawn_blocking.
    pub(super) async fn ensure_standby_signal(&mut self) -> std::io::Result<()> {
        self.reset_sync_cache();
        let data_dir = self.config.postgresql.data_dir.clone();
        tokio::task::spawn_blocking(move || standby_config::ensure_standby_signal(&data_dir))
            .await
            .map_err(std::io::Error::other)??;
        info!("Wrote standby.signal (primary_conninfo left as it is)");
        Ok(())
    }

    /// D1: everything that must be on disk before pg-ha starts PostgreSQL
    /// without holding the leader lock. Creates the rewind-check marker when
    /// pg_controldata says the directory last ran as a primary or cannot be
    /// read (A7), then writes standby.signal and, when the leader is known and
    /// is not this node, primary_conninfo pointing at it (A8). Any error means
    /// PostgreSQL must not be started.
    pub(super) async fn prepare_standby_start(&mut self) -> std::io::Result<()> {
        let state = match self.postgresql.controldata_state().await {
            Ok(state) => Some(state),
            Err(e) => {
                warn!(
                    "Could not read the cluster state from pg_controldata ({e}); creating the rewind-check marker to be safe"
                );
                None
            }
        };
        if identity::last_ran_as_primary(state.as_deref()) {
            let reason = match &state {
                Some(s) => format!("pg_controldata reports '{s}' (last ran as a primary)"),
                None => "pg_controldata could not be read".to_string(),
            };
            let data_dir = self.config.postgresql.data_dir.clone();
            let marker_reason = reason.clone();
            tokio::task::spawn_blocking(move || {
                standby_config::create_rewind_check_marker(&data_dir, &marker_reason)
            })
            .await
            .map_err(std::io::Error::other)??;
            info!("Created rewind-check marker: {reason}");
        }

        let leader = self.cluster.leader.as_ref().map(|l| l.name.clone());
        let leader_conn_url = leader
            .as_deref()
            .and_then(|n| self.cluster.get_member(n))
            .map(|m| m.conn_url.clone());
        let upstream = identity::standby_start_upstream(
            leader.as_deref(),
            &self.config.name,
            leader_conn_url.as_deref(),
        )
        .map(str::to_owned);
        match upstream {
            Some(url) => self.write_standby_config(Some(&url)).await,
            None => self.ensure_standby_signal().await,
        }
    }

    /// Before stopping PostgreSQL on a demotion path (D2): create the
    /// rewind-check marker when `rewind_check_reason` is set, then write
    /// standby.signal and set primary_conninfo to `upstream_conn_url`, or
    /// remove it when None (D3: never keep a guess). Both steps are attempted
    /// (standby.signal is what keeps L5); the first error is returned. Never
    /// stops PostgreSQL.
    pub(super) async fn prepare_demotion(
        &mut self,
        upstream_conn_url: Option<String>,
        rewind_check_reason: Option<&'static str>,
    ) -> std::io::Result<()> {
        let mut first_error = None;
        if let Some(reason) = rewind_check_reason {
            let data_dir = self.config.postgresql.data_dir.clone();
            let created = tokio::task::spawn_blocking(move || {
                standby_config::create_rewind_check_marker(&data_dir, reason)
            })
            .await
            .map_err(std::io::Error::other)
            .and_then(|r| r);
            match created {
                Ok(()) => info!("Created rewind-check marker: {reason}"),
                Err(e) => first_error = Some(e),
            }
        }
        if let Err(e) = self
            .write_standby_config(upstream_conn_url.as_deref())
            .await
        {
            first_error.get_or_insert(e);
        }
        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Publish the REST API state of a node that is being demoted or fenced:
    /// not the leader, not Primary, recovery state unobserved. `/primary` is
    /// then 503 whatever the HA state still says until the cycle ends.
    pub(super) async fn publish_not_primary(&self) {
        let Some(publish) = self.state_publisher.as_ref() else {
            return;
        };
        let mut snapshot = self.api_snapshot();
        snapshot.is_leader = false;
        if snapshot.role == MemberRole::Primary {
            snapshot.role = MemberRole::Replica;
        }
        snapshot.pg_in_recovery = None;
        publish(snapshot).await;
    }

    /// Stop PostgreSQL on a demotion / fence path after standby.signal was
    /// written. A failed stop sets `stop_pending`, so later cycles keep fencing
    /// an unobserved server even though standby.signal now exists (A19). A
    /// successful stop is published at once (bounded by `loop_wait`, so an
    /// unreachable DCS cannot stall the HA loop or graceful shutdown).
    pub(super) async fn stop_after_demotion_write(
        &mut self,
        mode: &str,
    ) -> crate::error::Result<()> {
        // H1: the REST API stops advertising primary before PostgreSQL stops
        // being writable, and again after the stop, before the post-stop DCS
        // write (which may wait up to retry_timeout on a minority).
        self.publish_not_primary().await;
        let stopped = self.postgresql.stop(mode).await;
        self.publish_not_primary().await;
        if let Err(e) = stopped {
            self.stop_pending = true;
            error!(
                "Failed to stop PostgreSQL after writing standby.signal: {e}; pg-ha keeps fencing it until it is confirmed stopped"
            );
            return Err(e);
        }
        self.stop_pending = false;
        let limit = std::time::Duration::from_secs(self.config.loop_wait.max(1));
        match tokio::time::timeout(limit, self.touch_member()).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => warn!("Failed to publish member state after stopping PostgreSQL: {e}"),
            Err(_) => warn!("Failed to publish member state after stopping PostgreSQL: timed out"),
        }
        Ok(())
    }

    /// Stop a PostgreSQL that may be a writable primary without the leader
    /// lock (D2): write standby.signal (+ rewind-check marker) first, then
    /// stop; a failed stop sets `stop_pending` so later cycles keep fencing it.
    pub(super) async fn fence_primary(
        &mut self,
        mode: &'static str,
        reason: &'static str,
        upstream_conn_url: Option<String>,
    ) -> crate::error::Result<()> {
        self.reset_sync_cache();
        if let Err(e) = self.prepare_demotion(upstream_conn_url, Some(reason)).await {
            error!(
                "Failed to write standby configuration before stopping PostgreSQL: {e}; stopping it anyway"
            );
        }
        self.stop_after_demotion_write(mode).await
    }

    // ─── Leader lease (L5 time bound, see `lease`) ───

    /// A leader-lock write that started at `anchor` returned `Ok(true)`. The
    /// lease uses the TTL written into the lock (`dcs.ttl()`), not the
    /// dynamic `ttl`.
    pub(super) fn extend_leader_lease(&mut self, anchor: LeaseAnchor) {
        let lease = LeaderLease::new(anchor, self.dcs.ttl(), self.config.watchdog.safety_margin);
        self.set_leader_lease(Some(lease));
    }

    pub(super) fn set_leader_lease(&mut self, lease: Option<LeaderLease>) {
        self.leader_lease = lease;
        self.publish_fence_deadline();
    }

    /// Publish the deadline `run_cycle_bounded` races against: none while
    /// paused (W1).
    pub(super) fn publish_fence_deadline(&self) {
        let deadline = if self.is_paused {
            None
        } else {
            self.leader_lease
        };
        self.fence_deadline_tx.send_replace(deadline);
    }

    /// The lease no longer covers acting as the lock holder (expired, or no
    /// successful lock write). Never in pause mode (W1).
    pub(super) fn lease_lost(&self) -> bool {
        !self.is_paused && self.leader_lease.is_none_or(|l| l.is_expired())
    }

    /// The lease-expiry fence: handled as loss of the lock. standby.signal +
    /// rewind-check marker first, then an immediate stop (a failed stop sets
    /// `stop_pending`).
    pub(super) async fn fence_for_lost_lease(&mut self) -> crate::error::Result<()> {
        self.is_leader = false;
        self.set_leader_lease(None);
        let stopped = self
            .fence_primary("immediate", LEASE_EXPIRED_REASON, None)
            .await;
        self.postgresql.set_role(MemberRole::Replica);
        stopped
    }

    /// Lease checkpoint inside a cycle (before `pg_ctl promote`, before
    /// reporting the leader role).
    pub(super) async fn fence_at_lease_check(&mut self, before: &str) -> CycleResult {
        error!("Leader lease expired before {before}; fencing PostgreSQL (immediate stop)");
        let _ = self.fence_for_lost_lease().await;
        CycleResult::Error(format!(
            "leader lease expired before {before}; PostgreSQL fenced"
        ))
    }

    /// Graceful shutdown, checked when the shutdown signal arrives (before
    /// the drain, `graceful_shutdown`). When this node still believes it holds
    /// the lock but the leader lease has already expired, the lock may belong
    /// to another node by now: fence PostgreSQL at once (standby.signal +
    /// marker, then an immediate stop) and return true, so the caller skips
    /// the drain, CHECKPOINT and lock release. Otherwise (no lock, valid
    /// lease, or pause mode, W1) returns false and the normal shutdown
    /// sequence runs.
    pub(super) async fn shutdown_fence_if_lease_expired(&mut self) -> bool {
        if !self.is_leader || !self.lease_lost() {
            return false;
        }
        error!(
            "Leader lease already expired at shutdown; skipping drain, CHECKPOINT and lock release, fencing PostgreSQL (immediate stop)"
        );
        if let Err(e) = self.fence_for_lost_lease().await {
            error!("Failed to stop PostgreSQL during shutdown: {e}");
        }
        true
    }

    /// Renew the leader lock while this node still holds it.
    ///
    /// False when there is no lock to renew or the renewal did not land. The
    /// caller keeps PostgreSQL fenced and does not delete the lock.
    pub(super) async fn renew_held_lock(&mut self) -> bool {
        let Some(leader) = self.cluster.leader.clone() else {
            return false;
        };
        if leader.name != self.config.name {
            return false;
        }
        let anchor = LeaseAnchor::now();
        match self.dcs.update_leader(&leader).await {
            Ok(true) => {
                self.is_leader = true;
                self.extend_leader_lease(anchor);
                true
            }
            Ok(false) => {
                error!("Failed to renew leader lock during shutdown");
                self.set_leader_lease(None);
                false
            }
            Err(e) => {
                error!("DCS error renewing the leader lock during shutdown: {e}");
                self.set_leader_lease(None);
                false
            }
        }
    }

    /// Sleep for `drain`, renewing the leader lock every `loop_wait` so it
    /// cannot expire while PostgreSQL is still up. The HA loop is already
    /// stopped (`graceful_shutdown`). The last renewal happens right before
    /// it returns, so its lease bounds what follows. Returns true when a
    /// renewal failed and PostgreSQL was fenced; the caller must not
    /// checkpoint or release the lock. In pause mode a failed renewal does
    /// not fence (W1): the drain goes on and the next slice retries.
    pub(super) async fn drain_renewing_leader_lock(&mut self, drain: std::time::Duration) -> bool {
        if !self.is_leader {
            tokio::time::sleep(drain).await;
            return false;
        }
        let deadline = tokio::time::Instant::now() + drain;
        loop {
            if !self.renew_held_lock().await {
                if self.is_paused {
                    warn!(
                        "Leader lock renewal failed during shutdown drain in pause mode; not fencing (pause mode), continuing the shutdown"
                    );
                } else {
                    error!(
                        "Leader lock renewal failed during shutdown drain; fencing PostgreSQL (immediate stop) and not releasing the lock"
                    );
                    let _ = self.fence_for_lost_lease().await;
                    return true;
                }
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return false;
            }
            let slice =
                (deadline - now).min(std::time::Duration::from_secs(self.config.loop_wait.max(1)));
            tokio::time::sleep(slice).await;
        }
    }

    /// Delete the leader lock only after PostgreSQL has stopped. False when it
    /// is still running or the delete did not succeed; the lock stays so
    /// another node cannot promote over a writable primary.
    pub(super) async fn release_leader_lock_if_stopped(&mut self) -> bool {
        if self.postgresql.is_running() {
            error!("Not releasing the leader lock: PostgreSQL is still running");
            return false;
        }
        if !self.is_leader {
            return true;
        }
        let Some(leader) = self.cluster.leader.clone() else {
            self.is_leader = false;
            self.reset_sync_cache();
            return true;
        };
        match self.dcs.delete_leader(&leader).await {
            Ok(true) => {
                info!("Leader lock released after PostgreSQL stopped");
                self.is_leader = false;
                self.reset_sync_cache();
                true
            }
            Ok(false) => {
                error!("Leader lock delete was rejected; the lock is still held");
                false
            }
            Err(e) => {
                error!("Failed to release leader lock after PostgreSQL stopped: {e}");
                false
            }
        }
    }

    /// Graceful shutdown, after the drain (`graceful_shutdown`, raced against
    /// the leader lease): stop PostgreSQL while this node still holds the
    /// leader lock. A primary gets standby.signal first (D2). `-m fast`
    /// first; when it fails and PostgreSQL still runs, once more with
    /// `-m immediate`. The caller releases the lock only if PostgreSQL is
    /// stopped. A failed stop sets `stop_pending` (so pg-ha keeps running and
    /// keeps stopping it) except for a standby pg-ha may leave running
    /// (`identity::shutdown_may_leave_running`).
    pub(super) async fn stop_postgres_for_shutdown(&mut self) -> crate::error::Result<()> {
        if !self.postgresql.is_running() {
            return Ok(());
        }
        self.postgresql.observe_recovery().await;
        self.stop_observed_postgres_for_shutdown().await
    }

    /// `stop_postgres_for_shutdown` after the recovery observation.
    pub(super) async fn stop_observed_postgres_for_shutdown(&mut self) -> crate::error::Result<()> {
        let observed = self.pg_identity();
        let fence = identity::shutdown_needs_fence(observed);
        let fast = if fence {
            self.fence_primary("fast", "stopped by graceful shutdown", None)
                .await
        } else {
            self.postgresql.stop("fast").await
        };
        let Err(e) = fast else {
            return Ok(());
        };
        if !self.postgresql.is_running() {
            return Ok(());
        }
        warn!("pg_ctl stop -m fast failed during shutdown ({e}); retrying once with -m immediate");
        if fence {
            // standby.signal is already written; keeps stop_pending semantics.
            return self.stop_after_demotion_write("immediate").await;
        }
        let immediate = self.postgresql.stop("immediate").await;
        if immediate.is_err()
            && self.postgresql.is_running()
            && !identity::shutdown_may_leave_running(observed, self.is_leader)
        {
            // A lock holder in recovery may be promoted, and unmanaged
            // recovery ends in a promotion of its own: pg-ha keeps running
            // and keeps stopping it instead of promoting it.
            self.stop_pending = true;
            error!(
                "Failed to stop PostgreSQL ({}) during shutdown; pg-ha keeps stopping it until it is confirmed stopped",
                observed.describe()
            );
        }
        immediate
    }

    // ─── D5: rewind hold ───

    /// Is the node held after a pg_rewind failure (A11)? The hold file is the
    /// source of truth; the in-memory flag only covers a failed file write.
    /// Notices an operator deleting the file and clears the in-memory state.
    pub(super) fn refresh_rewind_hold(&mut self) -> bool {
        let data_dir = &self.config.postgresql.data_dir;
        let (held, operator_removed) = identity::rewind_hold_state(
            standby_config::has_rewind_hold(data_dir),
            self.rewind_hold,
            self.rewind_hold_persisted,
        );
        if operator_removed {
            info!(
                "Rewind hold file {}/{} was removed; pg-ha will retry pg_rewind",
                data_dir.display(),
                standby_config::REWIND_HOLD_FILE
            );
            self.clear_rewind_hold();
        }
        held
    }

    pub(super) fn clear_rewind_hold(&mut self) {
        self.rewind_hold = false;
        self.rewind_hold_persisted = false;
        self.rewind_hold_logged_at = None;
    }

    /// D5: pg_rewind (or the start after it) failed and rebuilding is
    /// disabled. Persist the hold, make sure PostgreSQL is not running, and
    /// log the operator message only once it is really stopped.
    pub(super) async fn enter_rewind_hold(&mut self, what: &str, cause: &str) -> CycleResult {
        let cause = standby_config::failure_cause(cause);
        self.rewind_hold = true;
        let data_dir = self.config.postgresql.data_dir.clone();
        let (dir, file_what, file_cause) = (data_dir.clone(), what.to_string(), cause.clone());
        let written = tokio::task::spawn_blocking(move || {
            standby_config::write_rewind_hold(&dir, &file_what, &file_cause)
        })
        .await
        .map_err(std::io::Error::other)
        .and_then(|r| r);
        match written {
            Ok(()) => self.rewind_hold_persisted = true,
            Err(e) => {
                error!(
                    "Could not write {}/{} ({e}); the hold lasts until pg-ha restarts",
                    data_dir.display(),
                    standby_config::REWIND_HOLD_FILE
                );
                self.rewind_hold_persisted = false;
            }
        }
        // The message says "PostgreSQL is stopped": make it true first. If the
        // stop fails, the next cycle stops it again and then logs the message
        // (rewind_hold_logged_at is still None).
        if let Err(e) = self
            .stop_held_postgres("stopped while held after a pg_rewind failure")
            .await
        {
            return CycleResult::Error(format!("rewind hold: stop failed: {e}"));
        }
        self.log_rewind_hold(what, &cause).await;
        let _ = self.touch_member().await;
        CycleResult::Error(REWIND_HOLD_RESULT.into())
    }

    /// Re-log the hold message at most every REWIND_HOLD_LOG_INTERVAL, with
    /// the cause from the hold file; touch_member.
    pub(super) async fn report_rewind_hold(&mut self) -> CycleResult {
        let due = self
            .rewind_hold_logged_at
            .is_none_or(|at| at.elapsed() >= super::REWIND_HOLD_LOG_INTERVAL);
        if due {
            let (what, cause) = standby_config::read_rewind_hold(&self.config.postgresql.data_dir)
                .unwrap_or_else(|| {
                    (
                        "pg_rewind".to_string(),
                        "see the earlier ERROR log".to_string(),
                    )
                });
            self.log_rewind_hold(&what, &cause).await;
        }
        let _ = self.touch_member().await;
        CycleResult::Error(REWIND_HOLD_RESULT.into())
    }

    /// The full D5 ERROR (the data directory size is computed with a 2 s
    /// budget, only when the message is logged).
    async fn log_rewind_hold(&mut self, what: &str, cause: &str) {
        let data_dir = self.config.postgresql.data_dir.clone();
        let dir = data_dir.clone();
        let size = tokio::task::spawn_blocking(move || {
            standby_config::data_dir_size(&dir, std::time::Duration::from_secs(2))
        })
        .await
        .ok()
        .flatten();
        error!(
            "{}",
            standby_config::rewind_hold_message(
                what,
                cause,
                &data_dir,
                &standby_config::format_size(size)
            )
        );
        self.rewind_hold_logged_at = Some(std::time::Instant::now());
    }

    /// One check for every place a held node could start, follow or
    /// campaign. None = not held; Some(result) = the cycle ends here.
    pub(super) async fn enforce_rewind_hold(&mut self) -> Option<CycleResult> {
        if !self.refresh_rewind_hold() {
            return None;
        }
        let leader = self
            .cluster
            .leader
            .as_ref()
            .map(|l| l.name.clone())
            .filter(|n| n != &self.config.name);
        // DCS part only: Rebuild goes through rejoin_as_replica, which runs
        // the live probe before touching anything.
        let ready = leader
            .as_deref()
            .is_some_and(|n| identity::leader_ready_in_dcs(Some(n), n, self.cluster.get_member(n)));
        let rebuild_enabled = self
            .config
            .postgresql
            .remove_data_directory_on_rewind_failure;
        match (
            identity::held_node_action(true, rebuild_enabled, ready),
            leader,
        ) {
            (identity::HeldNodeAction::NotHeld, _) => None,
            (identity::HeldNodeAction::Hold, _) => Some(self.hold_stop_and_report().await),
            (identity::HeldNodeAction::Rebuild, Some(leader)) => {
                Some(self.rejoin_as_replica(&leader).await)
            }
            (identity::HeldNodeAction::Rebuild | identity::HeldNodeAction::WaitForLeader, _) => {
                if let Err(e) = self
                    .stop_held_postgres("stopped while held after a pg_rewind failure")
                    .await
                {
                    return Some(CycleResult::Error(format!("rewind hold: stop failed: {e}")));
                }
                let _ = self.touch_member().await;
                Some(CycleResult::Error(
                    "rewind hold: waiting for a leader running as a primary to rebuild from".into(),
                ))
            }
        }
    }

    /// Stop a running PostgreSQL on a held node. A writable primary goes
    /// through fence_primary (signal + marker first, stop_pending on
    /// failure); a read-only standby is stopped directly. Ok when not running.
    async fn stop_held_postgres(&mut self, fence_reason: &'static str) -> crate::error::Result<()> {
        if !self.postgresql.is_running() {
            return Ok(());
        }
        if self.pg_identity() == identity::PgIdentity::Primary {
            self.fence_primary("fast", fence_reason, None).await
        } else {
            self.postgresql.stop("fast").await
        }
    }

    /// The Hold branch, shared by rejoin step 0a and enforce_rewind_hold.
    pub(super) async fn hold_stop_and_report(&mut self) -> CycleResult {
        if let Err(e) = self
            .stop_held_postgres("stopped while held after a pg_rewind failure")
            .await
        {
            return CycleResult::Error(format!("rewind hold: stop failed: {e}"));
        }
        self.report_rewind_hold().await
    }

    /// Forget what was applied to synchronous_standby_names so the next
    /// primary term re-applies it (ALTER SYSTEM content may have been replaced
    /// by a clone or pg_rewind, and the node may have been a standby since).
    pub(super) fn reset_sync_cache(&mut self) {
        self.applied_sync_standby_names = None;
    }

    /// Update member info in DCS (heartbeat)
    pub(super) async fn touch_member(&self) -> Result<()> {
        // Use the raft self_addr host for conn_url (it's the reachable address)
        let reachable_host = self
            .config
            .raft
            .self_addr
            .split(':')
            .next()
            .unwrap_or("127.0.0.1");
        let mut conn_url = format!(
            "host={} port={} dbname={} user={}",
            reachable_host,
            self.config.postgresql.port,
            self.config.postgresql.superuser.dbname,
            self.config.postgresql.superuser.username,
        );
        if let Some(ref pw) = self.config.postgresql.superuser.password {
            conn_url.push_str(&format!(" password={pw}"));
        }

        // Query real-time WAL position and timeline from PostgreSQL
        let (wal_position, timeline) = if self.postgresql.is_running() {
            match self.postgresql.wal_status().await {
                Ok(status) => (Some(status.wal_position), status.timeline),
                Err(e) => {
                    warn!("Failed to query WAL position: {e}");
                    (None, None)
                }
            }
        } else {
            (None, None)
        };
        let lineage = self.local_lineage(timeline);

        let mut data = serde_json::json!({
            "name": self.config.name,
            "conn_url": conn_url,
            "api_url": format!("http://{}:{}", reachable_host, self.config.restapi.port),
            "state": self.advertised_state(),
            "role": format!("{}", self.postgresql.role()),
            // A18: other nodes apply the marked-node election rule to us.
            "rewind_check": self.has_rewind_check_marker(),
        });

        if let Some(pos) = wal_position {
            data["wal_position"] = serde_json::json!(pos);
        }
        if let Some(tl) = timeline {
            data["timeline"] = serde_json::json!(tl);
        }
        if let Some(lineage) = lineage {
            data["lineage"] = serde_json::to_value(lineage)
                .map_err(|e| crate::error::Error::Dcs(format!("Serialize lineage: {e}")))?;
        }

        self.dcs.touch_member(&data).await?;
        Ok(())
    }

    /// D6 evidence read from the data directory. None (nothing published,
    /// so no node counts this one as on the latest timeline) when any part
    /// cannot be read: a guessed value could admit a node to the election.
    fn local_lineage(&self, timeline: Option<u64>) -> Option<crate::cluster::Lineage> {
        let data_dir = &self.config.postgresql.data_dir;
        let read = || -> std::result::Result<crate::cluster::Lineage, String> {
            let history = timeline_files::newest_history(data_dir)?;
            let timeline_begin = match timeline {
                Some(t) => timeline_files::timeline_begin(data_dir, t)?,
                None => None,
            };
            Ok(crate::cluster::Lineage {
                timeline_begin,
                history: Some(history),
                postmaster_start: timeline_files::server_run(data_dir)?,
                generation: timeline_files::read_generation(data_dir)?,
            })
        };
        match read() {
            Ok(l) => Some(l),
            Err(e) => {
                warn!("Not publishing timeline evidence: {e}");
                None
            }
        }
    }

    /// D6: the latest timeline from this cycle's DCS view.
    pub(super) fn latest_timeline(&self) -> lineage::Latest {
        lineage::resolve_latest(
            &self.cluster.timeline,
            &self.cluster.members,
            self.cluster.dcs_voters,
        )
    }

    /// D6 / I3: may `member` campaign? Judged from what it published.
    pub(super) fn member_eligibility(
        &self,
        member: &crate::cluster::Member,
    ) -> std::result::Result<(), lineage::Ineligible> {
        lineage::eligibility(member, &self.latest_timeline())
    }

    /// I3 for this node, from its own published record (the value every
    /// node uses when judging it, A9), plus the operator override.
    pub(super) fn own_eligibility(&self) -> OwnEligibility {
        let verdict = match self.cluster.get_member(&self.config.name) {
            Some(m) => self.member_eligibility(m),
            None => Err(lineage::Ineligible::NoEvidence),
        };
        match verdict {
            Ok(()) => OwnEligibility::Eligible,
            Err(why) => {
                if !timeline_files::has_accept_timeline_loss(&self.config.postgresql.data_dir) {
                    return OwnEligibility::Ineligible(why);
                }
                // The override only applies when nobody else may campaign.
                let other = self
                    .cluster
                    .members
                    .iter()
                    .find(|m| m.name != self.config.name && self.member_eligibility(m).is_ok());
                match other {
                    Some(m) => OwnEligibility::OverrideIgnored {
                        why,
                        eligible: m.name.clone(),
                    },
                    None => OwnEligibility::Override(why),
                }
            }
        }
    }

    /// Generation of the local data directory.
    pub(super) fn local_generation(&self) -> std::result::Result<u64, String> {
        timeline_files::read_generation(&self.config.postgresql.data_dir)
    }

    /// Check if standby.signal file exists in the data directory.
    pub(super) fn has_standby_signal(&self) -> bool {
        self.config
            .postgresql
            .data_dir
            .join("standby.signal")
            .exists()
    }

    /// Whether the rewind-check marker (PGDATA/pg-ha.rewind_check) exists (D4).
    pub(super) fn has_rewind_check_marker(&self) -> bool {
        standby_config::has_rewind_check_marker(&self.config.postgresql.data_dir)
    }

    /// Log a D6 problem at ERROR once per distinct message (it repeats every
    /// cycle until someone acts).
    pub(super) fn log_timeline_problem(&mut self, message: String) {
        if self.timeline_problem_logged.as_deref() != Some(message.as_str()) {
            error!("{message}");
            self.timeline_problem_logged = Some(message);
        }
    }

    /// The identity of the timeline this primary writes WAL on right now.
    pub(super) async fn primary_timeline_id(
        &self,
    ) -> std::result::Result<crate::cluster::TimelineId, String> {
        let timeline = self
            .postgresql
            .primary_timeline()
            .await
            .map_err(|e| format!("could not read this primary's timeline: {e}"))?;
        let begin = timeline_files::timeline_begin(&self.config.postgresql.data_dir, timeline)?
            .ok_or_else(|| format!("no history file for this primary's timeline {timeline}"))?;
        Ok(crate::cluster::TimelineId { timeline, begin })
    }

    /// D6 Hold: keep `/timeline` equal to this lock-holding primary's
    /// timeline. Never lowers it, never overwrites another history or an
    /// unresolved promote by another node (those are logged as errors).
    pub(super) async fn record_cluster_timeline(&mut self) {
        match self.primary_timeline_id().await {
            Ok(own) => self.record_own_timeline(own).await,
            Err(e) => warn!("Not recording the cluster timeline this cycle: {e}"),
        }
    }

    /// D6 Hold for a standby leader (standby cluster): it never promotes, so
    /// the remote timeline it follows is this cluster's latest. Its newest
    /// history with WAL on it is that timeline.
    pub(super) async fn record_standby_leader_timeline(&mut self) {
        match timeline_files::newest_history(&self.config.postgresql.data_dir) {
            Ok(h) if h.wal => self.record_own_timeline(h.id()).await,
            Ok(h) => info!(
                "Not recording the cluster timeline yet: no WAL received on {} yet",
                h.id()
            ),
            Err(e) => warn!("Not recording the cluster timeline this cycle: {e}"),
        }
    }

    async fn record_own_timeline(&mut self, own: crate::cluster::TimelineId) {
        let own_generation = match self.local_generation() {
            Ok(g) => g,
            Err(e) => {
                warn!("Not recording the cluster timeline this cycle: {e}");
                return;
            }
        };
        match lineage::hold_action(
            &self.config.name,
            &self.cluster.timeline,
            &self.cluster.members,
            self.cluster.dcs_voters,
            own,
            own_generation,
        ) {
            lineage::HoldAction::UpToDate => {
                self.timeline_problem_logged = None;
                self.finish_override();
            }
            lineage::HoldAction::Write(record) => {
                match self
                    .dcs
                    .write_cluster_timeline(&record, self.cluster.timeline.version())
                    .await
                {
                    Ok(true) => {
                        self.timeline_problem_logged = None;
                        self.finish_override();
                    }
                    Ok(false) => {
                        info!("/timeline changed since it was read; checking again next cycle")
                    }
                    Err(e) => warn!("Failed to record cluster timeline {own} in DCS: {e}"),
                }
            }
            lineage::HoldAction::Conflict(msg) => {
                self.log_timeline_problem(format!("Not recording the cluster timeline: {msg}"))
            }
        }
    }

    /// D6 (I2): write `pending` into `/timeline` before `pg_ctl promote`.
    /// Written once per lock term; retries after a failed promote reuse it.
    pub(super) async fn record_pending_promote(
        &mut self,
    ) -> std::result::Result<crate::cluster::PendingPromote, String> {
        if let Some(p) = &self.promote_pending {
            return Ok(p.clone());
        }
        let local = timeline_files::newest_history(&self.config.postgresql.data_dir)?;
        // The server run that will execute the promote, read now: a later
        // run is what makes this entry void for other nodes.
        let postmaster_start = timeline_files::server_run(&self.config.postgresql.data_dir)?
            .ok_or("postmaster.pid is missing: cannot identify the server run to promote")?;
        let own = self.own_eligibility();
        let accept_loss = matches!(own, OwnEligibility::Override(_));
        let generation = self.cluster.timeline.generation();
        let pending = lineage::plan_promote(
            &self.config.name,
            &self.latest_timeline(),
            generation,
            local,
            accept_loss,
            Some(postmaster_start),
        )?;
        if let OwnEligibility::Override(why) = &own {
            warn!(
                "Promoting on the operator's {} although {why}: the newer history is abandoned \
                 and the cluster moves to history generation {}; every other node must be \
                 reinitialized",
                timeline_files::ACCEPT_TIMELINE_LOSS_FILE,
                pending.generation
            );
        }
        let record = match self.cluster.timeline.record() {
            Some(r) => crate::cluster::TimelineRecord {
                pending: Some(pending.clone()),
                ..r.clone()
            },
            None => crate::cluster::TimelineRecord {
                timeline: pending.base_timeline,
                begin: pending.base_begin,
                primary: String::new(),
                generation,
                pending: Some(pending.clone()),
            },
        };
        match self
            .dcs
            .write_cluster_timeline(&record, self.cluster.timeline.version())
            .await
        {
            Ok(true) => {
                self.promote_pending = Some(pending.clone());
                Ok(pending)
            }
            Ok(false) => Err("/timeline changed since it was read; trying again next cycle".into()),
            Err(e) => Err(format!("could not record the pending promote in DCS: {e}")),
        }
    }

    /// D6: after a confirmed promote, replace this node's pending entry with
    /// the timeline PostgreSQL created. Failures leave the pending entry,
    /// which keeps other nodes out; the Hold path retries.
    pub(super) async fn confirm_promote(&mut self, pending: &crate::cluster::PendingPromote) {
        let new = match self.primary_timeline_id().await {
            Ok(id) => id,
            Err(e) => {
                warn!("Promoted, but {e}; recording the timeline next cycle");
                return;
            }
        };
        let record = match lineage::confirmed_record(&self.config.name, pending, new) {
            Ok(r) => r,
            Err(e) => {
                self.log_timeline_problem(format!("Promoted, but not recording the timeline: {e}"));
                return;
            }
        };
        if pending.generation > self.cluster.timeline.generation()
            && let Err(e) = timeline_files::write_generation(
                &self.config.postgresql.data_dir,
                pending.generation,
            )
        {
            self.log_timeline_problem(format!(
                "Promoted on the operator override, but could not write history generation {} \
                 to {}: {e}; not recording the timeline",
                pending.generation,
                timeline_files::GENERATION_FILE
            ));
            return;
        }
        // CAS against the entry this node wrote, read again: the cycle's view
        // predates the pending write.
        let current = match self.dcs.get_cluster().await {
            Ok(c) => c.timeline,
            Err(e) => {
                warn!("Promoted, but could not read /timeline: {e}; recording it next cycle");
                return;
            }
        };
        if current.record().and_then(|r| r.pending.as_ref()) != Some(pending) {
            self.log_timeline_problem(
                "Promoted, but /timeline no longer carries this node's pending promote; not \
                 overwriting it"
                    .into(),
            );
            return;
        }
        match self
            .dcs
            .write_cluster_timeline(&record, current.version())
            .await
        {
            Ok(true) => {
                info!("Recorded {new} as the latest timeline");
                self.promote_pending = None;
                self.timeline_problem_logged = None;
                self.finish_override();
            }
            Ok(false) => warn!("/timeline changed while recording {new}; retrying next cycle"),
            Err(e) => warn!("Failed to record {new} in DCS: {e}; retrying next cycle"),
        }
    }

    /// The override was recorded: the operator's file has done its job.
    fn finish_override(&mut self) {
        let data_dir = &self.config.postgresql.data_dir;
        if !timeline_files::has_accept_timeline_loss(data_dir) {
            return;
        }
        match timeline_files::remove_accept_timeline_loss(data_dir) {
            Ok(()) => info!(
                "Removed {}: this primary's timeline is recorded as the latest",
                timeline_files::ACCEPT_TIMELINE_LOSS_FILE
            ),
            Err(e) => warn!(
                "Could not remove {}: {e}",
                timeline_files::ACCEPT_TIMELINE_LOSS_FILE
            ),
        }
    }

    /// D6 / 2A: is the local data in an older generation than the cluster's
    /// (or is its generation unreadable)? None = not stale; otherwise the
    /// cluster's generation, the local one, and whether an override promote
    /// is still in progress.
    fn stale_generation(&self) -> Option<(u64, std::result::Result<u64, String>, bool)> {
        let recorded = self.cluster.timeline.generation();
        let (generation, in_progress) = match self.latest_timeline() {
            lineage::Latest::Known { generation, .. } => (generation, false),
            // An override promote may be creating the new generation: do not
            // follow or rewind onto its node until it is resolved.
            lineage::Latest::PendingUnresolved { pending } if pending.node == self.config.name => {
                return None;
            }
            lineage::Latest::PendingUnresolved { pending } => (
                recorded.max(pending.generation),
                pending.generation > recorded,
            ),
            lineage::Latest::Unknown(_) => (recorded, false),
        };
        match self.local_generation() {
            Ok(local) if local >= generation => None,
            local => Some((generation, local, in_progress)),
        }
    }

    pub(super) fn data_generation_is_stale(&self) -> bool {
        self.stale_generation().is_some()
    }

    /// D6 / 2A: data of an older history generation (an operator override
    /// accepted losing it) must never be rewound onto, followed from, or
    /// elected over the new history. Keep PostgreSQL stopped until the node
    /// is reinitialized. None = not stale.
    pub(super) async fn enforce_generation(&mut self) -> Option<CycleResult> {
        let (generation, local, in_progress) = self.stale_generation()?;
        let reason = match local {
            Ok(local) if in_progress => format!(
                "An operator override is moving the cluster to history generation {generation}; \
                 this node's data is in generation {local}. pg-ha keeps PostgreSQL stopped until \
                 the override is recorded or abandoned"
            ),
            Ok(local) => format!(
                "This node's data belongs to history generation {local}; the cluster is in \
                 generation {generation} after an operator override accepted losing the older \
                 history. pg-ha keeps PostgreSQL stopped and will not rewind or follow it. \
                 Operator action required: POST /reinitialize on this node to clone it again"
            ),
            Err(e) => format!(
                "Cannot read this node's history generation ({e}); pg-ha keeps PostgreSQL \
                 stopped. Operator action required: fix or remove the file, or POST \
                 /reinitialize on this node"
            ),
        };
        self.log_timeline_problem(reason);
        if let Err(e) = self
            .stop_held_postgres("stopped: data of an older history generation")
            .await
        {
            return Some(CycleResult::Error(format!(
                "older history generation: stop failed: {e}"
            )));
        }
        let _ = self.touch_member().await;
        Some(CycleResult::Error(
            "data of an older history generation; operator action required (see the ERROR log)"
                .into(),
        ))
    }

    /// Check if recovery.signal exists (targeted archive / point-in-time
    /// recovery). pg-ha never writes it; used only to explain why a server is
    /// in recovery pg-ha did not create.
    pub(super) fn has_recovery_signal(&self) -> bool {
        self.config
            .postgresql
            .data_dir
            .join("recovery.signal")
            .exists()
    }

    /// Check if this node recently rejoined/reconfigured and should not be disturbed.
    /// Returns true for `retry_timeout` seconds after the last standby config was written.
    ///
    /// This replaces the old `is_pg_streaming()` heuristic which used an unreliable
    /// 30-second file modification time window. Instead, we use the configured
    /// `retry_timeout` (default 10s) as the grace period after writing
    /// postgresql.auto.conf, preventing unnecessary reconfiguration churn.
    pub(super) fn is_recently_reconfigured(&self) -> bool {
        let auto_conf_path = self.config.postgresql.data_dir.join("postgresql.auto.conf");
        if let Ok(metadata) = std::fs::metadata(&auto_conf_path)
            && let Ok(modified) = metadata.modified()
            && let Ok(elapsed) = modified.elapsed()
        {
            // Don't reconfigure within retry_timeout of the last config write
            return elapsed.as_secs() < self.config.retry_timeout;
        }
        false
    }

    /// Read the current upstream host from postgresql.auto.conf (primary_conninfo).
    /// Returns the host name/IP the replica is currently streaming from.
    pub(super) fn read_current_upstream(&self) -> Option<String> {
        let auto_conf_path = self.config.postgresql.data_dir.join("postgresql.auto.conf");
        let content = std::fs::read_to_string(&auto_conf_path).ok()?;

        // Parse primary_conninfo line: primary_conninfo = 'host=XXX port=...'
        for line in content.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with("primary_conninfo") {
                // Extract host= value from the connection string
                if let Some(start) = trimmed.find("host=") {
                    let after_host = &trimmed[start + 5..];
                    let host = after_host
                        .split_whitespace()
                        .next()
                        .unwrap_or("")
                        .trim_matches('\'')
                        .trim_matches('"');
                    if !host.is_empty() {
                        return Some(host.to_string());
                    }
                }
            }
        }
        None
    }

    /// Check if this node is the lock owner
    pub(super) fn is_lock_owner(&self) -> bool {
        self.cluster
            .leader
            .as_ref()
            .is_some_and(|l| l.name == self.config.name)
    }
}
