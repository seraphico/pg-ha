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
    let timeline = s.timeline.unwrap_or(0);
    let replication_lag = s.replication_lag.unwrap_or(0);
    let pending_restart: u8 = if s.pending_restart { 1 } else { 0 };
    let is_paused: u8 = if s.is_paused { 1 } else { 0 };
    let failsafe_active: u8 = if s.failsafe_active { 1 } else { 0 };

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
pg_ha_replication_lag_bytes {replication_lag}
# HELP pg_ha_timeline Current PostgreSQL timeline
# TYPE pg_ha_timeline gauge
pg_ha_timeline {timeline}
# HELP pg_ha_dcs_last_seen_seconds Seconds since last successful DCS communication
# TYPE pg_ha_dcs_last_seen_seconds gauge
pg_ha_dcs_last_seen_seconds {dcs_last_seen_seconds}
# HELP pg_ha_failsafe_active Whether failsafe mode is currently active (1 = active)
# TYPE pg_ha_failsafe_active gauge
pg_ha_failsafe_active {failsafe_active}
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

/// GET /history — JSON array of cluster history events
pub(crate) async fn get_history(State(state): State<RouterState>) -> impl IntoResponse {
    let history = state.app.history().read().await;
    Json(json!(history.entries()))
}
