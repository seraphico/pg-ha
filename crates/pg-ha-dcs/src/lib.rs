//! pg-ha-dcs: Embedded Raft consensus via openraft 0.9
//!
//! Implements the DcsAdapter trait using an embedded Raft cluster,
//! eliminating the need for external etcd/ZooKeeper/Consul.
//!
//! Modules:
//! - `state_machine`: KV store with TTL and CAS (the replicated data)
//! - `store`: openraft storage (log + state machine + snapshot)
//! - `network`: HTTP client for Raft RPCs to other nodes
//! - `raft_server`: HTTP server endpoints receiving Raft RPCs
//! - `raft_dcs`: DcsAdapter implementation bridging HA engine to Raft

pub mod network;
pub mod raft_dcs;
pub mod raft_server;
pub mod state_machine;
pub mod store;

pub use network::NetworkFactory;
pub use raft_dcs::RaftDcs;
pub use raft_server::raft_router;
pub use state_machine::{KvEntry, KvStateMachine, Request, Response};
pub use store::{MemStore, NodeId, Raft, TypeConfig};
