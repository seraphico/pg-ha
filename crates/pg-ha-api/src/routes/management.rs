//! Management/command handlers for the router.

use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::mpsc;

use super::router_state::RouterState;
use pg_ha_core::commands::{CommandStatus, ManagementCommand};

// ─────────────────── Management Endpoints ───────────────────

#[derive(Debug, Deserialize)]
pub(crate) struct SwitchoverRequest {
    leader: Option<String>,
    candidate: Option<String>,
    scheduled_at: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct FailoverRequest {
    candidate: Option<String>,
}

/// Helper to send a command to the HA loop and await response
async fn send_command(state: RouterState, cmd: ManagementCommand) -> impl IntoResponse {
    let cmd_tx = match &state.cmd_tx {
        Some(tx) => tx.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"status": "error", "message": "Command channel not available"})),
            )
                .into_response();
        }
    };

    let (reply_tx, mut reply_rx) = mpsc::channel(1);
    if cmd_tx.send((cmd, reply_tx)).await.is_err() {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"status": "error", "message": "Failed to send command to HA loop"})),
        )
            .into_response();
    }

    match reply_rx.recv().await {
        Some(resp) => {
            let status_code = match resp.status {
                CommandStatus::Accepted => StatusCode::OK,
                CommandStatus::Rejected => StatusCode::CONFLICT,
                CommandStatus::Error => StatusCode::INTERNAL_SERVER_ERROR,
            };
            let mut body = json!({"status": resp.status, "message": resp.message});
            if let Some(data) = resp.data {
                body["assessment"] = data;
            }
            (status_code, Json(body)).into_response()
        }
        None => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"status": "error", "message": "No response from HA loop"})),
        )
            .into_response(),
    }
}

/// POST /switchover
pub(crate) async fn post_switchover(
    State(state): State<RouterState>,
    Json(body): Json<SwitchoverRequest>,
) -> impl IntoResponse {
    let cmd = ManagementCommand::Switchover {
        leader: body.leader,
        candidate: body.candidate,
        scheduled_at: body.scheduled_at,
    };
    send_command(state, cmd).await
}

/// DELETE /switchover — cancel scheduled switchover
pub(crate) async fn delete_switchover(State(state): State<RouterState>) -> impl IntoResponse {
    send_command(state, ManagementCommand::CancelSwitchover).await
}

/// POST /failover
pub(crate) async fn post_failover(
    State(state): State<RouterState>,
    Json(body): Json<FailoverRequest>,
) -> impl IntoResponse {
    let cmd = ManagementCommand::Failover {
        candidate: body.candidate,
    };
    send_command(state, cmd).await
}

/// POST /restart
pub(crate) async fn post_restart(State(state): State<RouterState>) -> impl IntoResponse {
    send_command(state, ManagementCommand::Restart).await
}

/// POST /reinitialize
pub(crate) async fn post_reinitialize(State(state): State<RouterState>) -> impl IntoResponse {
    send_command(state, ManagementCommand::Reinitialize).await
}

/// `POST /recovery` body (SB-37). Unknown fields are refused.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecoveryRequest {
    lost: Vec<String>,
    digest: String,
}

fn bad_request(message: impl Into<String>) -> axum::response::Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({"status": "rejected", "message": message.into()})),
    )
        .into_response()
}

/// Validate a `POST /recovery` body: 1 or more distinct non-empty names
/// (compared case-insensitively, passed on lower-cased) and a 64-hex digest.
fn validate_recovery(body: RecoveryRequest) -> Result<(Vec<String>, String), String> {
    let mut lost: Vec<String> = Vec::with_capacity(body.lost.len());
    for name in &body.lost {
        let n = name.trim().to_ascii_lowercase();
        if n.is_empty() {
            return Err("lost: empty name".into());
        }
        if lost.contains(&n) {
            return Err(format!("lost: {n} named twice"));
        }
        lost.push(n);
    }
    if lost.is_empty() {
        return Err("lost: name at least one voter".into());
    }
    if !pg_ha_core::quorum::recovery::is_digest(&body.digest) {
        return Err("digest: must be exactly 64 hexadecimal characters".into());
    }
    Ok((lost, body.digest.to_ascii_lowercase()))
}

/// GET /recovery — SB-37 read-only quorum recovery assessment. It reads every
/// member's `GET /wal`, so it is behind the management authentication.
pub(crate) async fn get_recovery(State(state): State<RouterState>) -> impl IntoResponse {
    send_command(state, ManagementCommand::RecoveryExplain).await
}

/// POST /recovery — SB-37 authorize the recovery the explain proposed.
pub(crate) async fn post_recovery(
    State(state): State<RouterState>,
    body: Result<Json<RecoveryRequest>, axum::extract::rejection::JsonRejection>,
) -> axum::response::Response {
    let body = match body {
        Ok(Json(b)) => b,
        Err(e) => return bad_request(format!("invalid body: {}", e.body_text())),
    };
    match validate_recovery(body) {
        Ok((lost, digest)) => {
            send_command(state, ManagementCommand::RecoveryGrant { lost, digest })
                .await
                .into_response()
        }
        Err(e) => bad_request(e),
    }
}

/// DELETE /recovery — SB-37 delete an unused grant.
pub(crate) async fn delete_recovery(State(state): State<RouterState>) -> impl IntoResponse {
    send_command(state, ManagementCommand::RecoveryCancel).await
}
