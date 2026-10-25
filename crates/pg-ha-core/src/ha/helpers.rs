//! Helper methods: member heartbeat, standby signal detection, reconfiguration guard,
//! upstream reading, and lock ownership verification.

use tracing::{error, info, warn};

use crate::error::Result;
use crate::standby_config;

use crate::cluster::MemberRole;

use super::lease::{LeaderLease, LeaseAnchor};
use super::{CycleResult, Ha, LEASE_EXPIRED_REASON, identity};

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

    /// Stop PostgreSQL on a demotion / fence path after standby.signal was
    /// written. A failed stop sets `stop_pending`, so later cycles keep fencing
    /// an unobserved server even though standby.signal now exists (A19). A
    /// successful stop is published at once (bounded by `loop_wait`, so an
    /// unreachable DCS cannot stall the HA loop or graceful shutdown).
    pub(super) async fn stop_after_demotion_write(
        &mut self,
        mode: &str,
    ) -> crate::error::Result<()> {
        if let Err(e) = self.postgresql.stop(mode).await {
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
    /// the drain, src/main.rs). When this node still believes it holds the
    /// lock but the leader lease has already expired, the lock may belong to
    /// another node by now: fence PostgreSQL at once (standby.signal + marker,
    /// then an immediate stop) and return true, so the caller skips the drain,
    /// CHECKPOINT and lock release. Otherwise (no lock, valid lease, or pause
    /// mode, W1) returns false and the normal shutdown sequence runs.
    pub async fn shutdown_fence_if_lease_expired(&mut self) -> bool {
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
    /// stopped (src/main.rs). Returns true when a renewal failed and
    /// PostgreSQL was fenced; the caller must not checkpoint or release the
    /// lock.
    pub async fn drain_renewing_leader_lock(&mut self, drain: std::time::Duration) -> bool {
        if !self.is_leader {
            tokio::time::sleep(drain).await;
            return false;
        }
        let deadline = tokio::time::Instant::now() + drain;
        loop {
            if !self.renew_held_lock().await {
                error!(
                    "Leader lock renewal failed during shutdown drain; fencing PostgreSQL (immediate stop) and not releasing the lock"
                );
                let _ = self.fence_for_lost_lease().await;
                return true;
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
    pub async fn release_leader_lock_if_stopped(&mut self) -> bool {
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

    /// Graceful shutdown, after the drain (src/main.rs): stop PostgreSQL while
    /// this node still holds the leader lock. A primary gets standby.signal
    /// first (D2), then the caller releases the lock only if the stop
    /// succeeded.
    pub async fn stop_postgres_for_shutdown(&mut self) -> crate::error::Result<()> {
        if !self.postgresql.is_running() {
            return Ok(());
        }
        self.postgresql.observe_recovery().await;
        if identity::shutdown_needs_fence(self.pg_identity()) {
            self.fence_primary("fast", "stopped by graceful shutdown", None)
                .await
        } else {
            self.postgresql.stop("fast").await
        }
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
        if let Err(e) = self.stop_held_postgres().await {
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
                if let Err(e) = self.stop_held_postgres().await {
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
    async fn stop_held_postgres(&mut self) -> crate::error::Result<()> {
        if !self.postgresql.is_running() {
            return Ok(());
        }
        if self.pg_identity() == identity::PgIdentity::Primary {
            self.fence_primary("fast", "stopped while held after a pg_rewind failure", None)
                .await
        } else {
            self.postgresql.stop("fast").await
        }
    }

    /// The Hold branch, shared by rejoin step 0a and enforce_rewind_hold.
    pub(super) async fn hold_stop_and_report(&mut self) -> CycleResult {
        if let Err(e) = self.stop_held_postgres().await {
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

        self.dcs.touch_member(&data).await?;
        Ok(())
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

    /// The timeline this node published in its member record this cycle:
    /// the value every node uses when judging it (A9).
    pub(super) fn own_published_timeline(&self) -> Option<u64> {
        self.cluster
            .get_member(&self.config.name)
            .and_then(|m| m.timeline)
    }

    /// Highest timeline other members published this cycle.
    pub(super) fn max_peer_timeline(&self) -> Option<u64> {
        identity::max_timeline_excluding(&self.cluster.members, &self.config.name)
    }

    /// D6: make /timeline >= the timeline this primary writes WAL on. Failures
    /// only warn; a key above the primary's timeline is never lowered.
    pub(super) async fn record_cluster_timeline(&mut self) {
        let tl = match self.postgresql.primary_timeline().await {
            Ok(tl) => tl,
            Err(e) => {
                warn!("Could not read this primary's timeline to record it in DCS: {e}");
                return;
            }
        };
        let recorded = self.cluster.timeline;
        match identity::timeline_record_action(tl, recorded) {
            identity::TimelineRecord::Write => {
                if let Err(e) = self.dcs.set_cluster_timeline(tl).await {
                    warn!("Failed to record cluster timeline {tl} in DCS: {e}");
                }
            }
            identity::TimelineRecord::UpToDate => {}
            identity::TimelineRecord::RecordedAhead => {
                let k = recorded.unwrap_or(tl);
                if identity::should_warn_timeline_ahead(self.timeline_ahead_warned, k, tl) {
                    warn!(
                        "DCS /timeline ({k}) is higher than this primary's timeline ({tl}); a promote attempt recorded it in advance"
                    );
                    self.timeline_ahead_warned = Some((k, tl));
                }
            }
        }
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
