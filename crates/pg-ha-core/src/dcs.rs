//! DCS adapter trait — the abstract interface to the distributed configuration store.
//!
//! Implemented by pg-ha-dcs (Raft) but could also be implemented for etcd/consul/zk.

use crate::Result;
use crate::cluster::{Cluster, Leader};

/// Abstract DCS interface.
///
/// All mutating methods use CAS (Compare-And-Swap) semantics internally
/// to guarantee consistency across the distributed cluster.
#[async_trait::async_trait]
pub trait DcsAdapter: Send + Sync {
    /// Load the current cluster state from DCS
    async fn get_cluster(&self) -> Result<Cluster>;

    /// Attempt to acquire the leader lock atomically (prevExist=false).
    async fn attempt_to_acquire_leader(&self) -> Result<bool>;

    /// Renew the leader lock TTL using CAS (prevValue=self.name).
    async fn update_leader(&self, leader: &Leader) -> Result<bool>;

    /// Voluntarily release the leader lock using CAS.
    async fn delete_leader(&self, leader: &Leader) -> Result<bool>;

    /// Update this node's member info in DCS (heartbeat).
    async fn touch_member(&self, data: &serde_json::Value) -> Result<bool>;

    /// Race for cluster initialization (atomic create).
    async fn initialize(&self, sysid: &str) -> Result<bool>;

    /// Set failover/switchover request in DCS
    async fn set_failover_value(&self, value: &str) -> Result<bool>;

    /// Write the dynamic configuration value to the DCS /config key.
    async fn set_config_value(&self, value: &str) -> Result<bool>;

    /// Read the dynamic configuration value from the DCS /config key.
    /// Returns None if the key does not exist.
    async fn get_config_value(&self) -> Result<Option<String>>;

    /// Get the configured TTL
    fn ttl(&self) -> u64;

    /// Get the configured loop_wait
    fn loop_wait(&self) -> u64;
}
