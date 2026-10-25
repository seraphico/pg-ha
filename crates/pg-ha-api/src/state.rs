//! Shared application state for REST API handlers.
//!
//! Updated by the HA loop each cycle, read by API handlers.

use pg_ha_core::cluster::{MemberRole, MemberState};
use pg_ha_core::config::LeaseTiming;
use pg_ha_core::dcs::DcsAdapter;
use pg_ha_core::history::History;
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
    history: Arc<RwLock<History>>,
}

impl std::fmt::Debug for AppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppState")
            .field("inner", &"<RwLock<NodeState>>")
            .field("dcs", &self.dcs.as_ref().map(|_| "<DcsAdapter>"))
            .field("history", &"<RwLock<History>>")
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
    /// Whether failsafe mode is currently active
    pub failsafe_active: bool,
    /// Last `pg_is_in_recovery()` observation from the HA loop. `None` when
    /// not observed (PostgreSQL stopped, query failed, or a start / stop /
    /// promote ran since). At most one HA cycle old.
    pub pg_in_recovery: Option<bool>,
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
                failsafe_active: false,
                pg_in_recovery: None,
            })),
            dcs: None,
            lease_timing: None,
            history: Arc::new(RwLock::new(History::new())),
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
                failsafe_active: false,
                pg_in_recovery: None,
            })),
            dcs: Some(dcs),
            lease_timing: Some(lease_timing),
            history: Arc::new(RwLock::new(History::new())),
        }
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

    /// Get a reference to the shared history
    pub fn history(&self) -> &Arc<RwLock<History>> {
        &self.history
    }
}

impl NodeState {
    /// H1: `/primary` is 200 exactly when this node holds the leader lock and
    /// PostgreSQL is a healthy primary. Each clause is required:
    /// - `is_leader`: holds the leader lock
    /// - `state == Running`: PostgreSQL is running and pg-ha vouches for it
    /// - `role == Primary`: the HA loop confirmed and recorded the primary role
    /// - `pg_in_recovery == Some(false)`: observed out of recovery (writable)
    pub fn is_primary_with_lock(&self) -> bool {
        self.is_leader
            && self.state == MemberState::Running
            && self.role == MemberRole::Primary
            && self.pg_in_recovery == Some(false)
    }

    /// H2: a healthy replica is running, recorded as a replica, and observed
    /// in recovery. A server observed out of recovery is never served as a
    /// replica, whatever its recorded role.
    pub fn is_healthy_replica(&self) -> bool {
        self.state == MemberState::Running
            && self.role == MemberRole::Replica
            && self.pg_in_recovery == Some(true)
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

    /// Check if noloadbalance tag is set
    pub fn is_noloadbalance(&self) -> bool {
        self.tags
            .get("noloadbalance")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }
}
