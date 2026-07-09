//! RaftDcs: implements DcsAdapter trait using the embedded openraft cluster.
//!
//! This is the bridge between the abstract DCS interface (used by the HA engine)
//! and the concrete Raft consensus implementation.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use openraft::{BasicNode, Config as RaftConfig, Raft};
use openraft::storage::Adaptor;
use tokio::sync::Notify;
use tracing::{debug, error, info};

use pg_ha_core::cluster::{Cluster, ClusterConfig, Failover, Leader, Member, MemberRole, MemberState};
use pg_ha_core::dcs::DcsAdapter;
use pg_ha_core::error::{Error, Result};

use crate::network::NetworkFactory;
use crate::state_machine::{Request, Response};
use crate::store::{MemStore, NodeId, TypeConfig};

/// DCS adapter built on top of embedded Raft
pub struct RaftDcs {
    /// The openraft Raft instance
    raft: Arc<Raft<TypeConfig>>,

    /// Direct access to the store for local reads
    store: Arc<MemStore>,

    /// This node's name (used as the value in the leader key)
    node_name: String,

    /// Base path for all keys in this cluster: /namespace/scope/
    base_path: String,

    /// TTL for leader lock in seconds
    ttl: u64,

    /// HA loop wait interval
    loop_wait: u64,

    /// Notification channel for waking up watchers
    notify: Arc<Notify>,
}

impl RaftDcs {
    /// Create a new RaftDcs instance and start the Raft node.
    ///
    /// `node_id`: unique numeric ID for this node in the Raft cluster
    /// `node_name`: human-readable name (used for leader key value)
    /// `scope`: cluster scope name
    /// `namespace`: DCS namespace (default "service")
    /// `ttl`: leader lock TTL in seconds
    /// `loop_wait`: HA loop interval in seconds
    /// `data_dir`: optional directory for persisting Raft state to disk
    pub async fn new(
        node_id: NodeId,
        node_name: String,
        scope: String,
        namespace: String,
        ttl: u64,
        loop_wait: u64,
        data_dir: Option<PathBuf>,
    ) -> std::result::Result<Self, Box<dyn std::error::Error>> {
        let config = RaftConfig {
            heartbeat_interval: 500,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            ..Default::default()
        };
        let config = Arc::new(config.validate()?);

        let store = match data_dir {
            Some(dir) => {
                info!(?dir, "Raft storage: persistent mode");
                Arc::new(MemStore::new_persistent(dir))
            }
            None => {
                info!("Raft storage: in-memory mode (no data_dir configured)");
                Arc::new(MemStore::new())
            }
        };
        let network = NetworkFactory;

        let (log_store, state_machine) = Adaptor::new(store.clone());
        let raft = Raft::new(node_id, config, network, log_store, state_machine).await?;
        let raft = Arc::new(raft);

        let base_path = format!("/{namespace}/{scope}/");

        Ok(Self {
            raft,
            store,
            node_name,
            base_path,
            ttl,
            loop_wait,
            notify: Arc::new(Notify::new()),
        })
    }

    /// Get the Raft instance (for RPC server use)
    pub fn raft(&self) -> &Arc<Raft<TypeConfig>> {
        &self.raft
    }

    /// Bootstrap the Raft cluster with all known members.
    ///
    /// This should be called by exactly ONE node (the first to start).
    /// All members are initialized as voters from the start.
    /// `members`: list of (node_id, raft_rpc_address) pairs for all cluster nodes
    pub async fn bootstrap_cluster(&self, members: &[(NodeId, String)]) -> Result<()> {
        let mut member_map = BTreeMap::new();
        for (id, addr) in members {
            member_map.insert(*id, BasicNode::new(addr));
        }
        info!(
            members = ?members.iter().map(|(id, addr)| format!("{id}@{addr}")).collect::<Vec<_>>(),
            "Bootstrapping Raft cluster"
        );
        self.raft
            .initialize(member_map)
            .await
            .map_err(|e| Error::Dcs(format!("Raft cluster bootstrap failed: {e}")))?;
        Ok(())
    }

    /// Check if this Raft node has been initialized (has a current leader or committed logs)
    pub async fn is_initialized(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader.is_some() || metrics.last_log_index.is_some()
    }

    /// Wait until the Raft cluster has a leader (blocking with timeout)
    pub async fn wait_for_leader(&self, timeout_secs: u64) -> Result<()> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
        loop {
            // Check if Raft reports a leader in metrics
            let has_leader = {
                let metrics = self.raft.metrics().borrow().clone();
                metrics.current_leader.is_some()
            };

            if has_leader {
                // Verify the leader is actually functional by attempting a lightweight write.
                // This ensures Raft quorum is established and not just stale persisted state.
                use crate::state_machine::Request;
                let test_req = Request::Set {
                    key: format!("{}__health_check", self.base_path),
                    value: "ok".to_string(),
                    ttl: Some(5), // 5 second TTL, auto-expires
                    prev_exist: None,
                    prev_value: None,
                    prev_version: None,
                };
                match tokio::time::timeout(
                    std::time::Duration::from_secs(3),
                    self.raft.client_write(test_req),
                ).await {
                    Ok(Ok(_)) => return Ok(()),
                    _ => {
                        // Leader reported but can't write — election not complete
                    }
                }
            }

            if tokio::time::Instant::now() >= deadline {
                return Err(Error::Timeout("Raft cluster has no leader".into()));
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    }

    /// Wake up any watchers
    pub fn wake(&self) {
        self.notify.notify_one();
    }

    // ─────────── Key path helpers ───────────

    fn leader_path(&self) -> String {
        format!("{}leader", self.base_path)
    }

    fn members_path(&self) -> String {
        format!("{}members/", self.base_path)
    }

    fn member_path(&self) -> String {
        format!("{}members/{}", self.base_path, self.node_name)
    }

    fn initialize_path(&self) -> String {
        format!("{}initialize", self.base_path)
    }

    fn config_path(&self) -> String {
        format!("{}config", self.base_path)
    }

    #[allow(dead_code)]
    fn status_path(&self) -> String {
        format!("{}status", self.base_path)
    }

    #[allow(dead_code)]
    fn sync_path(&self) -> String {
        format!("{}sync", self.base_path)
    }

    fn failover_path(&self) -> String {
        format!("{}failover", self.base_path)
    }

    #[allow(dead_code)]
    fn failsafe_path(&self) -> String {
        format!("{}failsafe", self.base_path)
    }

    // ─────────── Internal helpers ───────────

    /// Propose a write to the Raft cluster and wait for commit
    async fn propose(&self, request: Request) -> Result<Response> {
        let result = self.raft.client_write(request.clone()).await;
        match result {
            Ok(resp) => Ok(resp.data),
            Err(e) => {
                // Check if we need to forward to the Raft leader
                let err_str = format!("{e}");
                if err_str.contains("forward") || err_str.contains("ForwardToLeader") {
                    // Extract leader info and forward via HTTP
                    let current_leader = {
                        let metrics = self.raft.metrics().borrow().clone();
                        metrics.current_leader
                    };
                    if let Some(leader_id) = current_leader {
                        return self.forward_to_leader(leader_id, &request).await;
                    }
                }
                Err(Error::Dcs(format!("Raft write failed: {e}")))
            }
        }
    }

    /// Forward a write request to the current Raft leader via HTTP
    async fn forward_to_leader(&self, leader_id: NodeId, request: &Request) -> Result<Response> {
        // Get leader's address from Raft membership
        let leader_node = {
            let metrics = self.raft.metrics().borrow().clone();
            metrics.membership_config
                .membership()
                .nodes()
                .find(|(id, _)| **id == leader_id)
                .map(|(_, node)| node.clone())
        };

        let leader_addr = match leader_node {
            Some(node) => {
                if node.addr.starts_with("http") {
                    node.addr
                } else {
                    format!("http://{}", node.addr)
                }
            }
            None => return Err(Error::Dcs("Cannot find Raft leader address".into())),
        };

        // POST the request to leader's /raft/client-write endpoint
        let client = reqwest::Client::new();
        let url = format!("{leader_addr}/raft/client-write");
        let resp = client
            .post(&url)
            .json(request)
            .send()
            .await
            .map_err(|e| Error::Dcs(format!("Forward to leader failed: {e}")))?;

        if resp.status().is_success() {
            let response: Response = resp
                .json()
                .await
                .map_err(|e| Error::Dcs(format!("Parse leader response: {e}")))?;
            Ok(response)
        } else {
            let text = resp.text().await.unwrap_or_default();
            Err(Error::Dcs(format!("Leader rejected write: {text}")))
        }
    }
}

#[async_trait::async_trait]
impl DcsAdapter for RaftDcs {
    async fn get_cluster(&self) -> Result<Cluster> {
        // Read leader key
        let leader = match self.store.get(&self.leader_path()).await {
            Some(entry) => Some(Leader {
                name: entry.value.clone(),
                version: entry.version,
            }),
            None => None,
        };

        // Read all member keys
        let member_entries = self.store.get_prefix(&self.members_path()).await;
        let mut members = Vec::new();
        for (key, entry) in &member_entries {
            let name = key.strip_prefix(&self.members_path()).unwrap_or(key);
            if let Ok(member) = serde_json::from_str::<Member>(&entry.value) {
                members.push(member);
            } else {
                // Fallback: treat value as minimal member data
                members.push(Member {
                    name: name.to_string(),
                    conn_url: String::new(),
                    api_url: String::new(),
                    state: MemberState::Unknown,
                    role: MemberRole::Uninitialized,
                    wal_position: None,
                    timeline: None,
                    tags: Default::default(),
                    version: None,
                });
            }
        }

        // Read initialize key
        let initialize = self
            .store
            .get(&self.initialize_path())
            .await
            .map(|e| e.value.clone());

        // Read failover key
        let failover = self.store.get(&self.failover_path()).await
            .and_then(|e| serde_json::from_str::<Failover>(&e.value).ok());

        // Read config key
        let config = self.store.get(&self.config_path()).await
            .map(|e| ClusterConfig { version: e.version, data: serde_json::from_str(&e.value).unwrap_or_default() });

        Ok(Cluster {
            leader,
            members,
            initialize,
            config,
            sync_state: None, // TODO: read /sync key
            failover,
            failsafe: None,  // TODO: read /failsafe key
            history: Vec::new(),
        })
    }

    async fn attempt_to_acquire_leader(&self) -> Result<bool> {
        let request = Request::Set {
            key: self.leader_path(),
            value: self.node_name.clone(),
            ttl: Some(self.ttl),
            prev_exist: Some(false), // Only succeed if key does NOT exist
            prev_value: None,
            prev_version: None,
        };

        match self.propose(request).await? {
            Response::Ok { .. } => {
                info!(node = %self.node_name, "Acquired leader lock");
                Ok(true)
            }
            Response::NotChanged => {
                debug!(node = %self.node_name, "Leader lock already held by another node");
                Ok(false)
            }
        }
    }

    async fn update_leader(&self, _leader: &Leader) -> Result<bool> {
        // Renew: set with prevValue=self.node_name (CAS — only current leader can renew)
        let request = Request::Set {
            key: self.leader_path(),
            value: self.node_name.clone(),
            ttl: Some(self.ttl),
            prev_exist: None,
            prev_value: Some(self.node_name.clone()),
            prev_version: None,
        };

        match self.propose(request).await? {
            Response::Ok { .. } => Ok(true),
            Response::NotChanged => {
                error!(node = %self.node_name, "Failed to renew leader lock — lost leadership");
                Ok(false)
            }
        }
    }

    async fn delete_leader(&self, _leader: &Leader) -> Result<bool> {
        let request = Request::Delete {
            key: self.leader_path(),
            prev_value: Some(self.node_name.clone()), // CAS — only delete if we own it
            recursive: false,
        };

        match self.propose(request).await? {
            Response::Ok { .. } => {
                info!(node = %self.node_name, "Released leader lock");
                self.wake();
                Ok(true)
            }
            Response::NotChanged => Ok(false),
        }
    }

    async fn touch_member(&self, data: &serde_json::Value) -> Result<bool> {
        let value = serde_json::to_string(data)
            .map_err(|e| Error::Dcs(format!("Serialize member data: {e}")))?;

        let request = Request::Set {
            key: self.member_path(),
            value,
            ttl: Some(self.ttl),
            prev_exist: None,
            prev_value: None,
            prev_version: None,
        };

        match self.propose(request).await? {
            Response::Ok { .. } => Ok(true),
            Response::NotChanged => Ok(false),
        }
    }

    async fn initialize(&self, sysid: &str) -> Result<bool> {
        // Atomic create: only succeed if /initialize does not exist
        let request = Request::Set {
            key: self.initialize_path(),
            value: sysid.to_string(),
            ttl: None, // No expiry for initialize key
            prev_exist: Some(false),
            prev_value: None,
            prev_version: None,
        };

        match self.propose(request).await? {
            Response::Ok { .. } => {
                info!("Won cluster initialization race");
                Ok(true)
            }
            Response::NotChanged => Ok(false),
        }
    }

    async fn set_failover_value(&self, value: &str) -> Result<bool> {
        let request = Request::Set {
            key: self.failover_path(),
            value: value.to_string(),
            ttl: None,
            prev_exist: None,
            prev_value: None,
            prev_version: None,
        };
        match self.propose(request).await? {
            Response::Ok { .. } => Ok(true),
            Response::NotChanged => Ok(false),
        }
    }

    async fn set_config_value(&self, value: &str) -> Result<bool> {
        let request = Request::Set {
            key: self.config_path(),
            value: value.to_string(),
            ttl: None, // No TTL for config key
            prev_exist: None,
            prev_value: None,
            prev_version: None,
        };
        match self.propose(request).await? {
            Response::Ok { .. } => Ok(true),
            Response::NotChanged => Ok(false),
        }
    }

    async fn get_config_value(&self) -> Result<Option<String>> {
        Ok(self.store.get(&self.config_path()).await.map(|e| e.value.clone()))
    }

    fn ttl(&self) -> u64 {
        self.ttl
    }

    fn loop_wait(&self) -> u64 {
        self.loop_wait
    }
}
