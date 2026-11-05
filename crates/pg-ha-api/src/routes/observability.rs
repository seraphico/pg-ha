//! Observability handlers for the router.

use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use serde_json::json;

use super::router_state::RouterState;

// ─────────────────── Observability Endpoints ───────────────────

/// GET /metrics — Prometheus text format metrics
pub(crate) async fn get_metrics(State(state): State<RouterState>) -> impl IntoResponse {
    let s = state.app.read().await;

    let role_str = format!("{:?}", s.role).to_lowercase();
    let state_str = format!("{:?}", s.state).to_lowercase();
    // SB-10: unknown is not 0. HELP/TYPE stay, the sample line is omitted.
    let timeline_sample = s
        .timeline
        .map(|v| format!("pg_ha_timeline {v}\n"))
        .unwrap_or_default();
    let lag_sample = s
        .replication_lag
        .map(|v| format!("pg_ha_replication_lag_bytes {v}\n"))
        .unwrap_or_default();
    let pending_restart: u8 = if s.pending_restart { 1 } else { 0 };
    let is_paused: u8 = if s.is_paused { 1 } else { 0 };

    let dcs_last_seen_seconds = s
        .dcs_last_seen
        .map(|t| t.elapsed().as_secs_f64())
        .unwrap_or(f64::NAN);

    let body = format!(
        r#"# HELP pg_ha_node_role Current role of this node (1 = active for the labeled role)
# TYPE pg_ha_node_role gauge
pg_ha_node_role{{role="{role_str}"}} 1
# HELP pg_ha_pg_state Current PostgreSQL state (1 = active for the labeled state)
# TYPE pg_ha_pg_state gauge
pg_ha_pg_state{{state="{state_str}"}} 1
# HELP pg_ha_replication_lag_bytes Replication lag in bytes
# TYPE pg_ha_replication_lag_bytes gauge
{lag_sample}# HELP pg_ha_timeline Current PostgreSQL timeline
# TYPE pg_ha_timeline gauge
{timeline_sample}# HELP pg_ha_dcs_last_seen_seconds Seconds since last successful DCS communication
# TYPE pg_ha_dcs_last_seen_seconds gauge
pg_ha_dcs_last_seen_seconds {dcs_last_seen_seconds}
# HELP pg_ha_pending_restart Whether a PostgreSQL restart is pending (1 = pending)
# TYPE pg_ha_pending_restart gauge
pg_ha_pending_restart {pending_restart}
# HELP pg_ha_is_paused Whether the cluster is in pause mode (1 = paused)
# TYPE pg_ha_is_paused gauge
pg_ha_is_paused {is_paused}
"#
    );

    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
}

/// GET /history — JSON array of cluster history events (oldest first), read
/// from the DCS `/history` key. `[]` without a DCS handle.
pub(crate) async fn get_history(State(state): State<RouterState>) -> impl IntoResponse {
    let Some(dcs) = state.app.dcs() else {
        return Json(json!([])).into_response();
    };
    match dcs.read_history().await {
        Ok((entries, _version)) => Json(json!(entries)).into_response(),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": format!("failed to read /history from the DCS: {e}")})),
        )
            .into_response(),
    }
}
