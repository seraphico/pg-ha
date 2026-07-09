//! Network transport for Raft RPC communication between nodes.
//!
//! Uses HTTP/JSON via reqwest (client) and axum (server) for inter-node Raft RPCs.
//! Endpoints: POST /raft/vote, POST /raft/append, POST /raft/snapshot

use std::future::Future;

use openraft::error::{RPCError, RaftError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::storage::Snapshot;
use openraft::{BasicNode, RaftNetwork, RaftNetworkFactory};
use openraft::error::Fatal;

use crate::store::{NodeId, TypeConfig};

// ─────────────────── Network Factory ───────────────────

/// Creates network connections to other Raft nodes
#[derive(Debug, Clone, Default)]
pub struct NetworkFactory;

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = NetworkConnection;

    async fn new_client(&mut self, _target: NodeId, node: &BasicNode) -> Self::Network {
        let addr = if node.addr.starts_with("http") {
            node.addr.clone()
        } else {
            format!("http://{}", node.addr)
        };
        NetworkConnection {
            target_addr: addr,
            client: reqwest::Client::new(),
        }
    }
}

// ─────────────────── Network Connection ───────────────────

/// A connection to a single remote Raft node
#[derive(Debug, Clone)]
pub struct NetworkConnection {
    target_addr: String,
    client: reqwest::Client,
}

impl RaftNetwork<TypeConfig> for NetworkConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId>>,
    > {
        let url = format!("{}/raft/append", self.target_addr);
        let resp = self
            .client
            .post(&url)
            .json(&rpc)
            .send()
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        let result: AppendEntriesResponse<NodeId> = resp
            .json()
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        Ok(result)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let url = format!("{}/raft/vote", self.target_addr);
        let resp = self
            .client
            .post(&url)
            .json(&rpc)
            .send()
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        let result: VoteResponse<NodeId> = resp
            .json()
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        Ok(result)
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, openraft::error::InstallSnapshotError>>,
    > {
        let url = format!("{}/raft/install-snapshot", self.target_addr);
        let resp = self
            .client
            .post(&url)
            .json(&rpc)
            .send()
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        let result: InstallSnapshotResponse<NodeId> = resp
            .json()
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        Ok(result)
    }

    async fn full_snapshot(
        &mut self,
        _vote: openraft::Vote<NodeId>,
        snapshot: Snapshot<TypeConfig>,
        _cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<NodeId>, StreamingError<TypeConfig, Fatal<NodeId>>> {
        // Serialize snapshot data and send in one shot
        let data = {
            let mut buf = Vec::new();
            let mut cursor = snapshot.snapshot;
            std::io::Read::read_to_end(&mut cursor, &mut buf)
                .map_err(|e| StreamingError::Unreachable(Unreachable::new(&e)))?;
            buf
        };

        let url = format!("{}/raft/snapshot", self.target_addr);

        #[derive(serde::Serialize)]
        struct SnapshotRequest {
            vote: openraft::Vote<NodeId>,
            meta: openraft::SnapshotMeta<NodeId, BasicNode>,
            data: Vec<u8>,
        }

        let req = SnapshotRequest {
            vote: _vote,
            meta: snapshot.meta,
            data,
        };

        let resp = self
            .client
            .post(&url)
            .json(&req)
            .send()
            .await
            .map_err(|e| StreamingError::Unreachable(Unreachable::new(&e)))?;

        let result: SnapshotResponse<NodeId> = resp
            .json()
            .await
            .map_err(|e| StreamingError::Unreachable(Unreachable::new(&e)))?;
        Ok(result)
    }
}
