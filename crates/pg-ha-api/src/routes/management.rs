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
            (
                status_code,
                Json(json!({"status": resp.status, "message": resp.message})),
            )
                .into_response()
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
