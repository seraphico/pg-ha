//! Shared application state for REST API handlers.
//!
//! Updated by the HA loop each cycle, read by API handlers.

use pg_ha_core::cluster::{MemberRole, MemberState};
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
    pub state: MemberState,
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
}

impl AppState {
    pub fn new(name: String, scope: String, ttl: u64) -> Self {
        Self {
            inner: Arc::new(RwLock::new(NodeState {
                name,
                scope,
                role: MemberRole::Uninitialized,
                state: MemberState::Unknown,
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
            })),
            dcs: None,
            history: Arc::new(RwLock::new(History::new())),
        }
    }

    /// Create AppState with a DCS adapter reference (for /config endpoints)
    pub fn with_dcs(name: String, scope: String, ttl: u64, dcs: Arc<dyn DcsAdapter>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(NodeState {
                name,
                scope,
                role: MemberRole::Uninitialized,
                state: MemberState::Unknown,
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
            })),
            dcs: Some(dcs),
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

    /// Get a reference to the shared history
    pub fn history(&self) -> &Arc<RwLock<History>> {
        &self.history
    }
}

impl NodeState {
    /// True if this node is the primary with the leader lock
    pub fn is_primary_with_lock(&self) -> bool {
        self.role == MemberRole::Primary && self.is_leader
    }

    /// True if this node is a healthy running replica
    pub fn is_healthy_replica(&self) -> bool {
        self.state == MemberState::Running && self.role == MemberRole::Replica
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
