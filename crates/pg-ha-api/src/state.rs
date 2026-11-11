//! Shared application state for REST API handlers.
//!
//! Updated by the HA loop each cycle, read by API handlers.

use pg_ha_core::cluster::{MemberRole, MemberState};
use pg_ha_core::config::LeaseTiming;
use pg_ha_core::dcs::DcsAdapter;
use pg_ha_core::{ApiSnapshot, LiveWalProbe};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

/// Shared state between HA loop and REST API
#[derive(Clone)]
pub struct AppState {
    inner: Arc<RwLock<NodeState>>,
    dcs: Option<Arc<dyn DcsAdapter>>,
    /// This node's static lease timing; PATCH / PUT /config checks dynamic
    /// `ttl` / `loop_wait` / `retry_timeout` changes against it
    lease_timing: Option<LeaseTiming>,
    /// Live WAL reader behind the open `GET /wal` endpoint (peer vote probes)
    wal_probe: Option<Arc<dyn LiveWalProbe>>,
    /// Caps concurrent `/wal` probes: each one opens a superuser connection
    wal_permits: Arc<tokio::sync::Semaphore>,
    /// Outer bound for one `/wal` probe
    wal_timeout: std::time::Duration,
}
/// Outer bound for one `/wal` probe; shorter than the 2 s a vote caller waits.
pub const WAL_ENDPOINT_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(1800);
/// Most `/wal` probes served at once; the excess gets 503 "busy".
pub const WAL_MAX_CONCURRENT: usize = 8;
impl std::fmt::Debug for AppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppState")
            .field("inner", &"<RwLock<NodeState>>")
            .field("dcs", &self.dcs.as_ref().map(|_| "<DcsAdapter>"))
            .finish()
    }
}

/// Current node state (written by HA loop, read by API)
#[derive(Debug, Clone)]
pub struct NodeState {
    pub name: String,
    pub scope: String,
    pub role: MemberRole,
    /// State pg-ha vouches for, identical to what it publishes in DCS:
    /// `Running` only when the process runs and its role is confirmed.
    pub state: MemberState,
    /// Whether the PostgreSQL process is alive, regardless of role. Backs
    /// `/health`, which reports the process, not the HA role.
    pub pg_running: bool,
    pub is_leader: bool,
    pub is_paused: bool,
    pub pending_restart: bool,
    pub timeline: Option<u64>,
    pub wal_position: Option<u64>,
    pub replication_lag: Option<u64>,
    pub pg_version: Option<String>,
    pub tags: std::collections::HashMap<String, serde_json::Value>,
    /// Timestamp of last successful HA loop execution
    pub last_loop_at: Option<std::time::Instant>,
    /// TTL for liveness check
    pub ttl_seconds: u64,
    /// Timestamp of last successful DCS query
    pub dcs_last_seen: Option<Instant>,
    /// Last `pg_is_in_recovery()` observation from the HA loop. `None` when
    /// not observed (PostgreSQL stopped, query failed, or a start / stop /
    /// promote ran since). At most one HA cycle old.
    pub pg_in_recovery: Option<bool>,
    /// Graceful shutdown in progress: `/primary` is 503 from the signal on,
    /// while the other endpoints keep reporting the real state.
    pub shutting_down: bool,
    /// `pg-ha.rewind_check` present: may hold forked history; `/replica` is 503.
    pub rewind_check: bool,
}

impl AppState {
    pub fn new(name: String, scope: String, ttl: u64) -> Self {
        Self {
            inner: Arc::new(RwLock::new(NodeState {
                name,
                scope,
                role: MemberRole::Uninitialized,
                state: MemberState::Unknown,
                pg_running: false,
                is_leader: false,
                is_paused: false,
                pending_restart: false,
                timeline: None,
                wal_position: None,
                replication_lag: None,
                pg_version: None,
                tags: Default::default(),
                last_loop_at: None,
                ttl_seconds: ttl,
                dcs_last_seen: None,
                pg_in_recovery: None,
                shutting_down: false,
                rewind_check: false,
            })),
            dcs: None,
            lease_timing: None,
            wal_probe: None,
            wal_permits: Arc::new(tokio::sync::Semaphore::new(WAL_MAX_CONCURRENT)),
            wal_timeout: WAL_ENDPOINT_TIMEOUT,
        }
    }

    /// Create AppState with a DCS adapter reference (for /config endpoints)
    /// and this node's static lease timing (checked by PATCH / PUT /config)
    pub fn with_dcs(
        name: String,
        scope: String,
        ttl: u64,
        dcs: Arc<dyn DcsAdapter>,
        lease_timing: LeaseTiming,
    ) -> Self {
        Self {
            inner: Arc::new(RwLock::new(NodeState {
                name,
                scope,
                role: MemberRole::Uninitialized,
                state: MemberState::Unknown,
                pg_running: false,
                is_leader: false,
                is_paused: false,
                pending_restart: false,
                timeline: None,
                wal_position: None,
                replication_lag: None,
                pg_version: None,
                tags: Default::default(),
                last_loop_at: None,
                ttl_seconds: ttl,
                dcs_last_seen: None,
                pg_in_recovery: None,
                shutting_down: false,
                rewind_check: false,
            })),
            dcs: Some(dcs),
            lease_timing: Some(lease_timing),
            wal_probe: None,
            wal_permits: Arc::new(tokio::sync::Semaphore::new(WAL_MAX_CONCURRENT)),
            wal_timeout: WAL_ENDPOINT_TIMEOUT,
        }
    }

    /// Install the live WAL reader that serves `GET /wal`
    pub fn with_wal_probe(mut self, probe: Arc<dyn LiveWalProbe>) -> Self {
        self.wal_probe = Some(probe);
        self
    }
    /// Test seam: shrink the outer `/wal` timeout (the crate has no paused-clock support)
    #[cfg(test)]
    pub(crate) fn with_wal_timeout(mut self, d: std::time::Duration) -> Self {
        self.wal_timeout = d;
        self
    }
    pub(crate) fn wal_timeout(&self) -> std::time::Duration {
        self.wal_timeout
    }
    /// The live WAL reader, if installed
    pub fn wal_probe(&self) -> Option<&Arc<dyn LiveWalProbe>> {
        self.wal_probe.as_ref()
    }
    /// Take a `/wal` concurrency permit without waiting; `None` means busy
    pub fn try_wal_permit(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        Arc::clone(&self.wal_permits).try_acquire_owned().ok()
    }
    pub async fn read(&self) -> tokio::sync::RwLockReadGuard<'_, NodeState> {
        self.inner.read().await
    }

    pub async fn update<F>(&self, f: F)
    where
        F: FnOnce(&mut NodeState),
    {
        let mut state = self.inner.write().await;
        f(&mut state);
    }

    /// Get a reference to the DCS adapter (for /config endpoints)
    pub fn dcs(&self) -> Option<&Arc<dyn DcsAdapter>> {
        self.dcs.as_ref()
    }

    /// This node's static lease timing (set together with the DCS adapter)
    pub fn lease_timing(&self) -> Option<LeaseTiming> {
        self.lease_timing
    }
}

impl NodeState {
    /// Copy the HA-derived fields (role, state, PostgreSQL status, lock,
    /// pause, pending restart) from the HA engine.
    pub fn apply_ha_snapshot(&mut self, snapshot: &ApiSnapshot) {
        self.role = snapshot.role.clone();
        self.state = snapshot.state.clone();
        self.pg_running = snapshot.pg_running;
        self.pg_in_recovery = snapshot.pg_in_recovery;
        self.is_leader = snapshot.is_leader;
        self.is_paused = snapshot.is_paused;
        self.pending_restart = snapshot.pending_restart;
        self.shutting_down = snapshot.shutting_down;
        self.rewind_check = snapshot.rewind_check;
        self.timeline = snapshot.timeline;
        self.wal_position = snapshot.wal_position;
        self.replication_lag = snapshot.replication_lag;
    }

    /// H1: `/primary` is 200 exactly when this node holds the leader lock,
    /// PostgreSQL is a healthy primary, and no graceful shutdown is in
    /// progress. Each clause is required:
    /// - `is_leader`: holds the leader lock
    /// - `state == Running`: PostgreSQL is running and pg-ha vouches for it
    /// - `role == Primary`: the HA loop confirmed and recorded the primary role
    /// - `pg_in_recovery == Some(false)`: observed out of recovery (writable)
    /// - `!shutting_down`: not draining / stopping for a graceful shutdown
    pub fn is_primary_with_lock(&self) -> bool {
        self.is_leader
            && self.state == MemberState::Running
            && self.role == MemberRole::Primary
            && self.pg_in_recovery == Some(false)
            && !self.shutting_down
    }

    /// H2: a healthy replica is running, recorded as a replica, observed in
    /// recovery, and not waiting for a rewind check (forked history must not
    /// be served on the RO path). A server observed out of recovery is never
    /// served as a replica, whatever its recorded role.
    pub fn is_healthy_replica(&self) -> bool {
        self.state == MemberState::Running
            && self.role == MemberRole::Replica
            && self.pg_in_recovery == Some(true)
            && !self.rewind_check
    }

    /// True if this node is a standby leader
    pub fn is_standby_leader(&self) -> bool {
        self.role == MemberRole::StandbyLeader
    }

    /// True if the HA loop ran within the TTL
    pub fn is_live(&self) -> bool {
        self.last_loop_at
            .is_some_and(|t| t.elapsed().as_secs() < self.ttl_seconds)
    }

    /// Set this node's own configured tags (`/patroni` reports them, and
    /// `/replica` honours `noloadbalance`). Called once at startup: the tags
    /// come from the static config and do not change at runtime.
    pub fn set_own_tags(&mut self, tags: &pg_ha_core::config::Tags) {
        self.tags = tags.to_member_tags();
    }

    /// Check if noloadbalance tag is set
    pub fn is_noloadbalance(&self) -> bool {
        self.tags
            .get("noloadbalance")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot() -> ApiSnapshot {
        ApiSnapshot {
            role: MemberRole::Replica,
            state: MemberState::Running,
            pg_running: true,
            pg_in_recovery: Some(true),
            is_leader: false,
            is_paused: false,
            pending_restart: false,
            shutting_down: false,
            rewind_check: false,
            timeline: Some(4),
            wal_position: Some(2000),
            replication_lag: Some(500),
        }
    }

    #[tokio::test]
    async fn apply_ha_snapshot_copies_wal_fields_and_clears_unknown() {
        let state = AppState::new("n".into(), "s".into(), 30);
        state.update(|s| s.apply_ha_snapshot(&snapshot())).await;
        {
            let s = state.read().await;
            assert_eq!(
                (s.timeline, s.wal_position, s.replication_lag),
                (Some(4), Some(2000), Some(500))
            );
        }
        let mut unknown = snapshot();
        unknown.timeline = None;
        unknown.wal_position = None;
        unknown.replication_lag = None;
        state.update(|s| s.apply_ha_snapshot(&unknown)).await;
        let s = state.read().await;
        assert_eq!(
            (s.timeline, s.wal_position, s.replication_lag),
            (None, None, None)
        );
    }
}
