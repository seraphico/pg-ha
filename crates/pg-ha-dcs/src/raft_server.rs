//! Raft RPC server endpoints (axum router)
//!
//! These endpoints handle incoming Raft RPCs from other cluster nodes:
//! - POST /raft/vote — RequestVote RPC
//! - POST /raft/append — AppendEntries RPC
//! - POST /raft/snapshot — InstallSnapshot (full snapshot transfer)

use std::io::Cursor;
use std::sync::Arc;

use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::post, Json, Router};
use openraft::BasicNode;

use crate::store::{NodeId, Raft, TypeConfig};

/// Shared state for the Raft RPC server
pub type RaftState = Arc<Raft>;

/// Build the axum router for Raft RPC endpoints
pub fn raft_router(raft: Arc<Raft>) -> Router {
    Router::new()
        .route("/raft/vote", post(handle_vote))
        .route("/raft/append", post(handle_append))
        .route("/raft/snapshot", post(handle_snapshot))
        .route("/raft/client-write", post(handle_client_write))
        .with_state(raft)
}

async fn handle_vote(
    State(raft): State<Arc<Raft>>,
    Json(req): Json<openraft::raft::VoteRequest<NodeId>>,
) -> impl IntoResponse {
    match raft.vote(req).await {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("vote error: {e}"),
        )
            .into_response(),
    }
}

async fn handle_append(
    State(raft): State<Arc<Raft>>,
    Json(req): Json<openraft::raft::AppendEntriesRequest<TypeConfig>>,
) -> impl IntoResponse {
    match raft.append_entries(req).await {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("append error: {e}"),
        )
            .into_response(),
    }
}

/// Snapshot request payload (full snapshot in one message)
#[derive(serde::Deserialize)]
struct SnapshotRequest {
    vote: openraft::Vote<NodeId>,
    meta: openraft::SnapshotMeta<NodeId, BasicNode>,
    data: Vec<u8>,
}

async fn handle_snapshot(
    State(raft): State<Arc<Raft>>,
    Json(req): Json<SnapshotRequest>,
) -> impl IntoResponse {
    let snapshot = openraft::storage::Snapshot {
        meta: req.meta,
        snapshot: Box::new(Cursor::new(req.data)),
    };

    match raft.install_full_snapshot(req.vote, snapshot).await {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("snapshot error: {e}"),
        )
            .into_response(),
    }
}

/// Handle forwarded client write requests from non-leader nodes
async fn handle_client_write(
    State(raft): State<Arc<Raft>>,
    Json(req): Json<crate::state_machine::Request>,
) -> impl IntoResponse {
    match raft.client_write(req).await {
        Ok(resp) => (StatusCode::OK, Json(resp.data)).into_response(),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("client write error: {e}"),
        )
            .into_response(),
    }
}
