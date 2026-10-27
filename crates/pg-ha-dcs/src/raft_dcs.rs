//! RaftDcs: implements DcsAdapter trait using the embedded openraft cluster.
//!
//! This is the bridge between the abstract DCS interface (used by the HA engine)
//! and the concrete Raft consensus implementation.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use openraft::storage::Adaptor;
use openraft::{BasicNode, Config as RaftConfig, Raft};
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};

use pg_ha_core::cluster::{
    Cluster, ClusterConfig, ClusterTimeline, Failover, Leader, Member, MemberRole, MemberState,
    SyncState, TimelineRecord,
};
use pg_ha_core::dcs::DcsAdapter;
use pg_ha_core::error::{Error, Result};

use crate::network::NetworkFactory;
use crate::state_machine::{Request, Response, current_millis, stamp_leader_time};
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

    /// Upper bound for one DCS write (`retry_timeout`), covering both the
    /// local Raft commit and the forward to the Raft leader
    write_timeout: std::time::Duration,

    /// Notification channel for waking up watchers
    notify: Arc<Notify>,

    /// Shared HTTP client for forwarding requests to the Raft leader
    http_client: reqwest::Client,

    /// Whether TLS is enabled for Raft RPC
    tls_enabled: bool,
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
    /// `retry_timeout`: upper bound in seconds for every DCS write (at least 1)
    /// `data_dir`: optional directory for persisting Raft state to disk
    /// `tls`: optional TLS configuration for Raft RPC connections
    pub async fn new(
        node_id: NodeId,
        node_name: String,
        scope: String,
        namespace: String,
        ttl: u64,
        loop_wait: u64,
        retry_timeout: u64,
        data_dir: Option<PathBuf>,
        tls: Option<&pg_ha_core::config::RaftTlsConfig>,
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
        let network = match tls {
            Some(tls_cfg) => NetworkFactory::with_tls(tls_cfg)?,
            None => NetworkFactory::default(),
        };

        let (log_store, state_machine) = Adaptor::new(store.clone());
        let raft = Raft::new(node_id, config, network, log_store, state_machine).await?;
        let raft = Arc::new(raft);

        let base_path = format!("/{namespace}/{scope}/");

        // Build HTTP client for forward_to_leader — must use same TLS config as NetworkFactory
        let (http_client, tls_enabled) = match tls {
            Some(tls_cfg) => {
                let mut builder = reqwest::Client::builder()
                    .pool_max_idle_per_host(3)
                    .timeout(std::time::Duration::from_secs(10));
                if let Some(ref ca_path) = tls_cfg.ca_cert {
                    if let Ok(ca_pem) = std::fs::read(ca_path) {
                        if let Ok(cert) = reqwest::tls::Certificate::from_pem(&ca_pem) {
                            builder = builder.add_root_certificate(cert);
                        }
                    }
                }
                if let (Some(cert_path), Some(key_path)) =
                    (&tls_cfg.client_cert, &tls_cfg.client_key)
                {
                    if let (Ok(cert_pem), Ok(key_pem)) =
                        (std::fs::read(cert_path), std::fs::read(key_path))
                    {
                        let mut identity_pem = cert_pem;
                        identity_pem.extend_from_slice(&key_pem);
                        if let Ok(identity) = reqwest::tls::Identity::from_pem(&identity_pem) {
                            builder = builder.identity(identity);
                        }
                    }
                }
                (builder.build().unwrap_or_default(), true)
            }
            None => {
                let client = reqwest::Client::builder()
                    .pool_max_idle_per_host(3)
                    .timeout(std::time::Duration::from_secs(10))
                    .build()
                    .unwrap_or_default();
                (client, false)
            }
        };

        Ok(Self {
            raft,
            store,
            node_name,
            base_path,
            ttl,
            loop_wait,
            write_timeout: std::time::Duration::from_secs(retry_timeout.max(1)),
            notify: Arc::new(Notify::new()),
            http_client,
            tls_enabled,
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

    /// Check if a Raft client_write error is a ForwardToLeader redirect.
    /// This indicates the cluster has a known leader - the node is just not it.
    fn is_forward_to_leader(
        err: &openraft::error::RaftError<
            NodeId,
            openraft::error::ClientWriteError<NodeId, BasicNode>,
        >,
    ) -> bool {
        matches!(
            err,
            openraft::error::RaftError::APIError(
                openraft::error::ClientWriteError::ForwardToLeader(_)
            )
        )
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
                    now: current_millis(),
                };
                match tokio::time::timeout(
                    std::time::Duration::from_secs(3),
                    self.raft
                        .client_write(stamp_leader_time(test_req, current_millis())),
                )
                .await
                {
                    Ok(Ok(_)) => return Ok(()),
                    Ok(Err(ref e)) if Self::is_forward_to_leader(e) => {
                        return Ok(());
                    }
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

    fn sync_path(&self) -> String {
        format!("{}sync", self.base_path)
    }

    fn failover_path(&self) -> String {
        format!("{}failover", self.base_path)
    }

    /// Persistent cluster timeline (D6): no TTL, never lowered.
    fn timeline_path(&self) -> String {
        format!("{}timeline", self.base_path)
    }

    #[allow(dead_code)]
    fn failsafe_path(&self) -> String {
        format!("{}failsafe", self.base_path)
    }

    // ─────────── Internal helpers ───────────

    /// Propose a write to the Raft cluster and wait for commit, at most
    /// `retry_timeout`. openraft 0.9 has no check-quorum: a Raft leader cut
    /// off from the majority never steps down, and its `client_write` only
    /// returns after the partition heals. Without this bound a lock renewal
    /// on such a node would hang and the HA loop would never demote.
    ///
    /// A write that times out may still commit later. That is safe: every
    /// lock write is a CAS ordered in the Raft log, so a late renewal either
    /// fails against a newer holder or extends a lock nobody else took.
    async fn propose(&self, request: Request) -> Result<Response> {
        bounded_write(self.write_timeout, self.propose_unbounded(request)).await
    }

    async fn propose_unbounded(&self, request: Request) -> Result<Response> {
        // Stamp only the copy this node appends. A forwarded request is stamped
        // again by the leader in `handle_client_write`, so a follower's clock
        // never becomes the log entry's `now`.
        let result = self
            .raft
            .client_write(stamp_leader_time(request.clone(), current_millis()))
            .await;
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
            metrics
                .membership_config
                .membership()
                .nodes()
                .find(|(id, _)| **id == leader_id)
                .map(|(_, node)| node.clone())
        };

        let leader_addr = match leader_node {
            Some(node) => {
                if node.addr.starts_with("http") {
                    node.addr
                } else if self.tls_enabled {
                    format!("https://{}", node.addr)
                } else {
                    format!("http://{}", node.addr)
                }
            }
            None => return Err(Error::Dcs("Cannot find Raft leader address".into())),
        };

        // POST the request to leader's /raft/client-write endpoint
        let url = format!("{leader_addr}/raft/client-write");
        let resp = self
            .http_client
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
                    rewind_check: false,
                    lineage: None,
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
        let failover = self
            .store
            .get(&self.failover_path())
            .await
            .and_then(|e| serde_json::from_str::<Failover>(&e.value).ok());

        // Read sync key
        let sync_state = self
            .store
            .get(&self.sync_path())
            .await
            .and_then(|e| serde_json::from_str::<SyncState>(&e.value).ok());

        // Read config key
        let config = self
            .store
            .get(&self.config_path())
            .await
            .map(|e| ClusterConfig {
                version: e.version,
                data: serde_json::from_str(&e.value).unwrap_or_default(),
            });

        // Read the persistent cluster timeline key (D6)
        let timeline = self
            .store
            .get(&self.timeline_path())
            .await
            .map_or(ClusterTimeline::Absent, |e| {
                ClusterTimeline::parse(&e.value, e.version)
            });

        // Raft voters = pg-ha nodes (all members are voters, see
        // bootstrap_cluster).
        let dcs_voters = Some(
            self.raft
                .metrics()
                .borrow()
                .membership_config
                .membership()
                .voter_ids()
                .count(),
        );

        Ok(Cluster {
            leader,
            members,
            initialize,
            config,
            sync_state,
            failover,
            failsafe: None, // TODO: read /failsafe key
            history: Vec::new(),
            timeline,
            dcs_voters,
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
            now: current_millis(),
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
            now: current_millis(),
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
            now: current_millis(),
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
            now: current_millis(),
        };

        match self.propose(request).await? {
            Response::Ok { .. } => {
                info!("Won cluster initialization race");
                Ok(true)
            }
            Response::NotChanged => Ok(false),
        }
    }

    async fn set_sync_value(&self, value: &str) -> Result<bool> {
        let request = Request::Set {
            key: self.sync_path(),
            value: value.to_string(),
            ttl: None,
            prev_exist: None,
            prev_value: None,
            prev_version: None,
            now: current_millis(),
        };
        match self.propose(request).await? {
            Response::Ok { .. } => Ok(true),
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
            now: current_millis(),
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
            now: current_millis(),
        };
        match self.propose(request).await? {
            Response::Ok { .. } => Ok(true),
            Response::NotChanged => Ok(false),
        }
    }

    async fn get_config_value(&self) -> Result<Option<String>> {
        Ok(self
            .store
            .get(&self.config_path())
            .await
            .map(|e| e.value.clone()))
    }

    async fn write_cluster_timeline(
        &self,
        record: &TimelineRecord,
        expected: Option<u64>,
    ) -> Result<bool> {
        let request =
            timeline_write_request(self.timeline_path(), record, expected, current_millis())?;
        match self.propose(request).await? {
            Response::Ok { .. } => {
                info!(
                    timeline = record.timeline,
                    begin = record.begin,
                    generation = record.generation,
                    pending = ?record.pending.as_ref().map(|p| &p.node),
                    "Recorded cluster timeline in DCS"
                );
                Ok(true)
            }
            Response::NotChanged => {
                debug!(?expected, "/timeline CAS not applied: changed since read");
                Ok(false)
            }
        }
    }

    fn ttl(&self) -> u64 {
        self.ttl
    }

    fn loop_wait(&self) -> u64 {
        self.loop_wait
    }
}

/// Run one DCS write with an upper bound. On expiry the write is abandoned
/// and reported as `Error::Timeout`; callers treat it like any failed write.
async fn bounded_write<F>(limit: std::time::Duration, write: F) -> Result<Response>
where
    F: std::future::Future<Output = Result<Response>>,
{
    match tokio::time::timeout(limit, write).await {
        Ok(result) => result,
        Err(_) => {
            warn!(
                "DCS write timed out after {}s (retry_timeout); treating it as failed",
                limit.as_secs()
            );
            Err(Error::Timeout(format!(
                "DCS write did not complete within {}s (retry_timeout)",
                limit.as_secs()
            )))
        }
    }
}

/// CAS request that writes the `/timeline` record (no TTL) only if the key
/// still has version `expected`, or does not exist when `expected` is None.
/// A concurrent write makes it fail (NotChanged) instead of being overwritten.
fn timeline_write_request(
    key: String,
    record: &TimelineRecord,
    expected: Option<u64>,
    now: u64,
) -> Result<Request> {
    let value = serde_json::to_string(record)
        .map_err(|e| Error::Dcs(format!("Serialize /timeline record: {e}")))?;
    Ok(Request::Set {
        key,
        value,
        ttl: None,
        prev_exist: expected.is_none().then_some(false),
        prev_value: None,
        prev_version: expected,
        now,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state_machine::KvStateMachine;

    const KEY: &str = "/service/test/timeline";

    fn record(timeline: u64) -> TimelineRecord {
        TimelineRecord {
            timeline,
            begin: 0x100 * timeline,
            primary: "node1".into(),
            generation: 0,
            pending: None,
        }
    }

    fn write(
        sm: &mut KvStateMachine,
        rec: &TimelineRecord,
        expected: Option<u64>,
        index: u64,
    ) -> Response {
        let req = timeline_write_request(KEY.to_string(), rec, expected, 1_000).unwrap();
        sm.apply(&req, index)
    }

    fn stored(sm: &KvStateMachine) -> ClusterTimeline {
        sm.get(KEY).map_or(ClusterTimeline::Absent, |e| {
            ClusterTimeline::parse(&e.value, e.version)
        })
    }

    #[test]
    fn write_request_is_a_cas_without_ttl() {
        let req = timeline_write_request(KEY.to_string(), &record(3), None, 7).unwrap();
        match req {
            Request::Set {
                key,
                value,
                ttl,
                prev_exist,
                prev_value,
                prev_version,
                now,
            } => {
                assert_eq!(key, KEY);
                assert_eq!(
                    serde_json::from_str::<TimelineRecord>(&value).unwrap(),
                    record(3)
                );
                assert_eq!(ttl, None, "the key has no TTL");
                assert_eq!(prev_exist, Some(false), "absent expected: create only");
                assert_eq!(prev_value, None);
                assert_eq!(prev_version, None);
                assert_eq!(now, 7);
            }
            other => panic!("unexpected request {other:?}"),
        }
        let req = timeline_write_request(KEY.to_string(), &record(3), Some(5), 7).unwrap();
        assert!(matches!(
            req,
            Request::Set {
                prev_exist: None,
                prev_version: Some(5),
                ..
            }
        ));
    }

    #[test]
    fn stale_version_write_is_not_applied() {
        let mut sm = KvStateMachine::new();
        assert!(matches!(
            write(&mut sm, &record(2), None, 10),
            Response::Ok { .. }
        ));
        let v = stored(&sm).version();
        // Two writers read the same version: the second loses.
        assert!(matches!(
            write(&mut sm, &record(3), v, 11),
            Response::Ok { .. }
        ));
        assert_eq!(write(&mut sm, &record(4), v, 12), Response::NotChanged);
        assert_eq!(stored(&sm).record(), Some(&record(3)));
        // Create-only fails once the key exists.
        assert_eq!(write(&mut sm, &record(9), None, 13), Response::NotChanged);
    }

    #[test]
    fn legacy_number_is_replaced_by_version() {
        let mut sm = KvStateMachine::new();
        sm.apply(
            &Request::Set {
                key: KEY.into(),
                value: "17".into(),
                ttl: None,
                prev_exist: None,
                prev_value: None,
                prev_version: None,
                now: 1,
            },
            10,
        );
        let legacy = stored(&sm);
        assert!(matches!(legacy, ClusterTimeline::Legacy { value: 17, .. }));
        assert!(matches!(
            write(&mut sm, &record(14), legacy.version(), 11),
            Response::Ok { .. }
        ));
        assert_eq!(stored(&sm).record(), Some(&record(14)));
    }

    /// R19: a DCS write that never completes is abandoned after the bound
    /// and reported as `Error::Timeout`.
    #[tokio::test(start_paused = true)]
    async fn bounded_write_times_out_after_retry_timeout() {
        let limit = std::time::Duration::from_secs(5);
        let start = tokio::time::Instant::now();
        let result = bounded_write(limit, std::future::pending::<Result<Response>>()).await;
        let elapsed = start.elapsed();
        assert!(
            matches!(result, Err(Error::Timeout(ref m)) if m.contains("DCS write")),
            "{result:?}"
        );
        assert!(elapsed >= limit, "returned early after {elapsed:?}");
        assert!(
            elapsed < limit + std::time::Duration::from_millis(10),
            "returned late after {elapsed:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn bounded_write_passes_through_a_completed_write() {
        let limit = std::time::Duration::from_secs(5);
        let result = bounded_write(limit, async { Ok(Response::NotChanged) }).await;
        assert_eq!(result.unwrap(), Response::NotChanged);
        let failed = bounded_write(limit, async { Err(Error::Dcs("rejected".into())) }).await;
        assert!(matches!(failed, Err(Error::Dcs(_))));
    }

    /// R19 reproduction with three in-process Raft nodes: the lock holder is
    /// the Raft leader and loses both peers. openraft 0.9 keeps it leader, so
    /// an unbounded `client_write` never returns; the bounded renewal returns
    /// `Error::Timeout` after `retry_timeout`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn renewal_on_a_minority_raft_leader_returns_within_retry_timeout() {
        use std::time::Duration;
        const RETRY_TIMEOUT: u64 = 2;

        let mut listeners = Vec::new();
        let mut members = Vec::new();
        for id in 1..=3u64 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            members.push((id, listener.local_addr().unwrap().to_string()));
            listeners.push(listener);
        }
        let mut nodes = Vec::new();
        let mut servers = Vec::new();
        for (listener, (id, _)) in listeners.into_iter().zip(&members) {
            let dcs = RaftDcs::new(
                *id,
                format!("node{id}"),
                "r19".into(),
                "service".into(),
                30,
                10,
                RETRY_TIMEOUT,
                None,
                None,
            )
            .await
            .unwrap();
            let router = crate::raft_router(dcs.raft().clone());
            servers.push(tokio::spawn(async move {
                let _ = axum::serve(listener, router).await;
            }));
            nodes.push(Arc::new(dcs));
        }
        nodes[0].bootstrap_cluster(&members).await.unwrap();
        nodes[0].wait_for_leader(20).await.unwrap();

        // Find the Raft leader and let it take the HA leader lock while the
        // cluster is healthy.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        let leader_id = loop {
            let leader = nodes[0].raft().metrics().borrow().current_leader;
            if let Some(id) = leader
                && nodes[(id - 1) as usize]
                    .attempt_to_acquire_leader()
                    .await
                    .ok()
                    == Some(true)
            {
                break id;
            }
            assert!(tokio::time::Instant::now() < deadline, "no Raft leader");
            tokio::time::sleep(Duration::from_millis(200)).await;
        };
        let leader = nodes[(leader_id - 1) as usize].clone();
        assert_eq!(
            leader.raft().metrics().borrow().current_leader,
            Some(leader_id)
        );

        // Partition: stop the peers' RPC servers and their Raft cores. The
        // core shutdown also covers keep-alive connections that outlive the
        // aborted accept loop.
        for (i, node) in nodes.iter().enumerate() {
            if i as u64 + 1 != leader_id {
                servers[i].abort();
                node.raft().shutdown().await.unwrap();
            }
        }

        // The hang: an unbounded write on the minority leader does not return.
        let renewal = Request::Set {
            key: leader.leader_path(),
            value: leader.node_name.clone(),
            ttl: Some(30),
            prev_exist: None,
            prev_value: Some(leader.node_name.clone()),
            prev_version: None,
            now: current_millis(),
        };
        let unbounded = tokio::time::timeout(
            Duration::from_secs(2 * RETRY_TIMEOUT),
            leader.propose_unbounded(renewal),
        )
        .await;
        assert!(
            unbounded.is_err(),
            "unbounded write returned on a minority leader: {unbounded:?}"
        );
        assert_eq!(
            leader.raft().metrics().borrow().current_leader,
            Some(leader_id),
            "openraft 0.9 keeps the minority leader"
        );

        // The fix: the renewal returns Error::Timeout after retry_timeout.
        let start = std::time::Instant::now();
        let lock = Leader {
            name: leader.node_name.clone(),
            version: 0,
        };
        let renewed = tokio::time::timeout(
            Duration::from_secs(RETRY_TIMEOUT + 3),
            leader.update_leader(&lock),
        )
        .await
        .expect("the bounded renewal must return");
        let elapsed = start.elapsed();
        assert!(
            matches!(renewed, Err(Error::Timeout(_))),
            "expected Error::Timeout, got {renewed:?}"
        );
        assert!(
            elapsed >= Duration::from_secs(RETRY_TIMEOUT),
            "returned after {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_secs(RETRY_TIMEOUT + 1),
            "returned after {elapsed:?}"
        );
    }

    #[test]
    fn timeline_key_survives_expiry_and_reload() {
        let mut sm = KvStateMachine::new();
        write(&mut sm, &record(3), None, 10);
        assert_eq!(sm.get(KEY).unwrap().expire_at, None, "no TTL");
        // The Raft leader's periodic expiry, far in the future, keeps it.
        sm.apply(&Request::ExpireKeys { now: u64::MAX / 2 }, 11);
        assert_eq!(stored(&sm).record(), Some(&record(3)));
        // Persisted with the state machine (P1) and read back unchanged.
        let path = std::env::temp_dir().join(format!(
            "pg-ha-dcs-timeline-{}/state_machine.json",
            std::process::id()
        ));
        sm.save_to_disk(&path);
        let loaded = KvStateMachine::load_from_disk(&path);
        assert_eq!(stored(&loaded).record(), Some(&record(3)));
        assert_eq!(loaded.get(KEY).unwrap().expire_at, None);
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }
}
