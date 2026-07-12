//! REST API routes for health checks and management.

use axum::{
    Json, Router,
    extract::{Query, Request, State},
    http::{StatusCode, header},
    middleware::Next,
    response::IntoResponse,
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::mpsc;

use crate::state::AppState;
use pg_ha_core::commands::{CommandResponse, CommandStatus, ManagementCommand};
use pg_ha_core::dynamic_config::{GlobalConfig, patch_config};

/// Sender for management commands to the HA loop
pub type CommandSender = mpsc::Sender<(ManagementCommand, mpsc::Sender<CommandResponse>)>;

/// Auth configuration for the router
#[derive(Clone, Debug)]
pub struct AuthConfig {
    pub username: Option<String>,
    pub password: Option<String>,
}

impl AuthConfig {
    /// Returns true if authentication is enabled (both username and password are set)
    pub fn is_enabled(&self) -> bool {
        self.username.is_some() && self.password.is_some()
    }
}

/// Combined state for the router
#[derive(Clone)]
pub struct RouterState {
    pub app: AppState,
    pub cmd_tx: Option<CommandSender>,
    pub auth: AuthConfig,
}

/// Build the full API router (without command channel, for tests)
pub fn build_router(state: AppState) -> Router {
    build_router_with_commands(
        state,
        None,
        AuthConfig {
            username: None,
            password: None,
        },
    )
}

/// Build the full API router with command channel and auth config
pub fn build_router_with_commands(
    state: AppState,
    cmd_tx: Option<CommandSender>,
    auth: AuthConfig,
) -> Router {
    let router_state = RouterState {
        app: state,
        cmd_tx,
        auth,
    };

    // Health check endpoints (open, no auth) — used by load balancers
    let health_routes = Router::new()
        .route("/primary", get(health_primary))
        .route("/", get(health_primary))
        .route("/read-write", get(health_primary))
        .route("/replica", get(health_replica))
        .route("/health", get(health_check))
        .route("/liveness", get(liveness))
        .route("/standby-leader", get(health_standby_leader))
        .route("/synchronous", get(health_sync))
        .route("/sync", get(health_sync))
        .route("/asynchronous", get(health_async))
        .route("/async", get(health_async))
        .route("/patroni", get(node_status))
        .route("/cluster", get(get_cluster))
        .route("/metrics", get(get_metrics))
        .route("/history", get(get_history));

    // Management endpoints (protected by Basic Auth if configured)
    let mgmt_routes = Router::new()
        .route(
            "/switchover",
            post(post_switchover).delete(delete_switchover),
        )
        .route("/failover", post(post_failover))
        .route("/restart", post(post_restart))
        .route("/reinitialize", post(post_reinitialize))
        .route(
            "/config",
            get(get_config).put(put_config).patch(patch_config_endpoint),
        )
        .layer(axum::middleware::from_fn_with_state(
            router_state.clone(),
            basic_auth_middleware,
        ));

    health_routes.merge(mgmt_routes).with_state(router_state)
}

/// Basic Auth middleware — checks credentials on protected management endpoints.
/// If auth is not configured, all requests pass through.
async fn basic_auth_middleware(
    State(state): State<RouterState>,
    request: Request,
    next: Next,
) -> impl IntoResponse {
    if !state.auth.is_enabled() {
        // Auth not configured — pass through
        return next.run(request).await.into_response();
    }

    let expected_user = state.auth.username.as_deref().unwrap_or("");
    let expected_pass = state.auth.password.as_deref().unwrap_or("");

    // Extract Authorization header
    let auth_header = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());

    let authorized = if let Some(auth_value) = auth_header {
        if let Some(encoded) = auth_value.strip_prefix("Basic ") {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD
                .decode(encoded.trim())
                .ok()
                .and_then(|bytes| String::from_utf8(bytes).ok())
                .and_then(|decoded| {
                    decoded
                        .split_once(':')
                        .map(|(u, p)| u == expected_user && p == expected_pass)
                })
                .unwrap_or(false)
        } else if let Some(token) = auth_value.strip_prefix("Bearer ") {
            use base64::Engine;
            let expected_token = base64::engine::general_purpose::STANDARD
                .encode(format!("{expected_user}:{expected_pass}"));
            token.trim() == expected_token
        } else {
            false
        }
    } else {
        false
    };

    if authorized {
        next.run(request).await.into_response()
    } else {
        (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Basic realm=\"pg-ha\"")],
            Json(json!({"error": "Unauthorized"})),
        )
            .into_response()
    }
}

/// Query parameters for /replica endpoint
#[derive(Debug, Deserialize, Default)]
struct ReplicaQuery {
    lag: Option<u64>,
}

/// GET /primary, GET /, GET /read-write
/// Returns 200 if this node is the primary with the leader lock.
async fn health_primary(State(state): State<RouterState>) -> impl IntoResponse {
    let s: tokio::sync::RwLockReadGuard<'_, crate::state::NodeState> = state.app.read().await;
    if s.is_primary_with_lock() {
        (
            StatusCode::OK,
            Json(json!({
                "state": "running",
                "role": "primary",
                "timeline": s.timeline,
                "wal_position": s.wal_position,
            })),
        )
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "state": format!("{:?}", s.state),
                "role": format!("{:?}", s.role),
            })),
        )
    }
}

/// GET /replica?lag=<max_lag_bytes>
/// Returns 200 if this node is a healthy replica (optionally with lag check).
async fn health_replica(
    State(state): State<RouterState>,
    Query(params): Query<ReplicaQuery>,
) -> impl IntoResponse {
    let s = state.app.read().await;

    if !s.is_healthy_replica() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "state": format!("{:?}", s.state),
                "role": format!("{:?}", s.role),
            })),
        );
    }

    if s.is_noloadbalance() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "role": "replica",
                "reason": "noloadbalance",
            })),
        );
    }

    // Check lag threshold if specified
    if let Some(max_lag) = params.lag
        && let Some(lag) = s.replication_lag
        && lag > max_lag
    {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "role": "replica",
                "lag": lag,
                "max_lag": max_lag,
                "reason": "lag exceeds threshold",
            })),
        );
    }

    (
        StatusCode::OK,
        Json(json!({
            "state": "running",
            "role": "replica",
            "timeline": s.timeline,
            "wal_position": s.wal_position,
            "lag": s.replication_lag,
        })),
    )
}

/// GET /health
/// Returns 200 if PostgreSQL is running.
async fn health_check(State(state): State<RouterState>) -> impl IntoResponse {
    let s = state.app.read().await;
    if s.state == pg_ha_core::cluster::MemberState::Running {
        (
            StatusCode::OK,
            Json(json!({
                "state": "running",
                "role": format!("{:?}", s.role),
            })),
        )
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "state": format!("{:?}", s.state),
                "role": format!("{:?}", s.role),
            })),
        )
    }
}

/// GET /liveness
/// Returns 200 if the HA loop has executed within the TTL period.
async fn liveness(State(state): State<RouterState>) -> impl IntoResponse {
    let s = state.app.read().await;
    if s.is_live() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// GET /standby-leader
/// Returns 200 only if this node is a standby leader.
async fn health_standby_leader(State(state): State<RouterState>) -> impl IntoResponse {
    let s = state.app.read().await;
    if s.is_standby_leader() {
        (StatusCode::OK, Json(json!({"role": "standby_leader"}))).into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"role": format!("{:?}", s.role)})),
        )
            .into_response()
    }
}

/// GET /synchronous, GET /sync
/// Returns 200 if this node is a synchronous standby.
async fn health_sync(State(state): State<RouterState>) -> impl IntoResponse {
    let s = state.app.read().await;
    // TODO: check actual sync state from cluster sync_state
    let is_sync = s
        .tags
        .get("sync_state")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if s.is_healthy_replica() && is_sync {
        (StatusCode::OK, Json(json!({"role": "sync_standby"}))).into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"role": format!("{:?}", s.role)})),
        )
            .into_response()
    }
}

/// GET /asynchronous, GET /async
/// Returns 200 if this node is an asynchronous standby.
async fn health_async(State(state): State<RouterState>) -> impl IntoResponse {
    let s = state.app.read().await;
    let is_sync = s
        .tags
        .get("sync_state")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if s.is_healthy_replica() && !is_sync {
        (StatusCode::OK, Json(json!({"role": "async_standby"}))).into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"role": format!("{:?}", s.role)})),
        )
            .into_response()
    }
}

/// GET /patroni — Full node status (compatible with Patroni response format)
async fn node_status(State(state): State<RouterState>) -> impl IntoResponse {
    let s = state.app.read().await;
    Json(json!({
        "state": format!("{:?}", s.state),
        "role": format!("{:?}", s.role),
        "scope": s.scope,
        "name": s.name,
        "timeline": s.timeline,
        "wal_position": s.wal_position,
        "replication_lag": s.replication_lag,
        "paused": s.is_paused,
        "pending_restart": s.pending_restart,
        "tags": s.tags,
    }))
}

/// GET /cluster — Cluster members summary with cascade topology
async fn get_cluster(State(state): State<RouterState>) -> impl IntoResponse {
    let s = state.app.read().await;

    // If DCS is available, fetch the full cluster state for topology
    if let Some(dcs) = state.app.dcs() {
        match dcs.get_cluster().await {
            Ok(cluster) => {
                let topology = pg_ha_core::CascadeManager::build_cascade_topology(&cluster);
                return Json(json!({
                    "scope": s.scope,
                    "members": topology,
                }))
                .into_response();
            }
            Err(_) => {
                // Fall through to empty response
            }
        }
    }

    Json(json!({
        "scope": s.scope,
        "members": [],
    }))
    .into_response()
}

// ─────────────────── Management Endpoints ───────────────────

#[derive(Debug, Deserialize)]
struct SwitchoverRequest {
    leader: Option<String>,
    candidate: Option<String>,
    scheduled_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FailoverRequest {
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
async fn post_switchover(
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
async fn delete_switchover(State(state): State<RouterState>) -> impl IntoResponse {
    send_command(state, ManagementCommand::CancelSwitchover).await
}

/// POST /failover
async fn post_failover(
    State(state): State<RouterState>,
    Json(body): Json<FailoverRequest>,
) -> impl IntoResponse {
    let cmd = ManagementCommand::Failover {
        candidate: body.candidate,
    };
    send_command(state, cmd).await
}

/// POST /restart
async fn post_restart(State(state): State<RouterState>) -> impl IntoResponse {
    send_command(state, ManagementCommand::Restart).await
}

/// POST /reinitialize
async fn post_reinitialize(State(state): State<RouterState>) -> impl IntoResponse {
    send_command(state, ManagementCommand::Reinitialize).await
}

// ─────────────────── Config Endpoints ───────────────────

/// GET /config — Read dynamic configuration from DCS
async fn get_config(State(state): State<RouterState>) -> impl IntoResponse {
    let dcs = match state.app.dcs() {
        Some(dcs) => dcs.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "DCS not available"})),
            )
                .into_response();
        }
    };

    match dcs.get_config_value().await {
        Ok(Some(value)) => {
            match serde_json::from_str::<GlobalConfig>(&value) {
                Ok(config) => {
                    let json_val = serde_json::to_value(&config).unwrap_or(json!({}));
                    (StatusCode::OK, Json(json_val)).into_response()
                }
                Err(_) => {
                    // Return raw value if it doesn't parse as GlobalConfig
                    match serde_json::from_str::<serde_json::Value>(&value) {
                        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
                        Err(e) => (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(json!({"error": format!("Invalid config in DCS: {e}")})),
                        )
                            .into_response(),
                    }
                }
            }
        }
        Ok(None) => (StatusCode::OK, Json(json!({}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("Failed to read config from DCS: {e}")})),
        )
            .into_response(),
    }
}

/// PUT /config — Full replacement of dynamic configuration
async fn put_config(
    State(state): State<RouterState>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let dcs = match state.app.dcs() {
        Some(dcs) => dcs.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "DCS not available"})),
            )
                .into_response();
        }
    };

    // Validate that the body can be parsed as a GlobalConfig
    let config: GlobalConfig = match serde_json::from_value(body.clone()) {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("Invalid config: {e}")})),
            )
                .into_response();
        }
    };

    // Serialize and write to DCS
    let value = serde_json::to_string(&config).unwrap_or_default();
    match dcs.set_config_value(&value).await {
        Ok(true) => (
            StatusCode::OK,
            Json(serde_json::to_value(&config).unwrap_or(json!({}))),
        )
            .into_response(),
        Ok(false) => (
            StatusCode::CONFLICT,
            Json(json!({"error": "Failed to write config to DCS"})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("DCS write failed: {e}")})),
        )
            .into_response(),
    }
}

/// PATCH /config — Partial update of dynamic configuration.
/// Keys set to null are removed from the configuration.
async fn patch_config_endpoint(
    State(state): State<RouterState>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let dcs = match state.app.dcs() {
        Some(dcs) => dcs.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "DCS not available"})),
            )
                .into_response();
        }
    };

    // Read current config from DCS
    let current_config: GlobalConfig = match dcs.get_config_value().await {
        Ok(Some(value)) => serde_json::from_str(&value).unwrap_or_default(),
        Ok(None) => GlobalConfig::default(),
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("Failed to read current config: {e}")})),
            )
                .into_response();
        }
    };

    // Apply patch
    let patched = match patch_config(&current_config, &body) {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("Invalid patch: {e}")})),
            )
                .into_response();
        }
    };

    // Write back to DCS
    let value = serde_json::to_string(&patched).unwrap_or_default();
    match dcs.set_config_value(&value).await {
        Ok(true) => (
            StatusCode::OK,
            Json(serde_json::to_value(&patched).unwrap_or(json!({}))),
        )
            .into_response(),
        Ok(false) => (
            StatusCode::CONFLICT,
            Json(json!({"error": "Failed to write config to DCS"})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("DCS write failed: {e}")})),
        )
            .into_response(),
    }
}

// ─────────────────── Observability Endpoints ───────────────────

/// GET /metrics — Prometheus text format metrics
async fn get_metrics(State(state): State<RouterState>) -> impl IntoResponse {
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
async fn get_history(State(state): State<RouterState>) -> impl IntoResponse {
    let history = state.app.history().read().await;
    Json(json!(history.entries()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use pg_ha_core::cluster::{MemberRole, MemberState};
    use tower::ServiceExt;

    fn test_state() -> AppState {
        AppState::new("node1".into(), "test-cluster".into(), 30)
    }

    async fn set_primary(state: &AppState) {
        state
            .update(|s| {
                s.role = MemberRole::Primary;
                s.state = MemberState::Running;
                s.is_leader = true;
                s.timeline = Some(1);
                s.wal_position = Some(12345);
                s.last_loop_at = Some(std::time::Instant::now());
            })
            .await;
    }

    async fn set_replica(state: &AppState) {
        state
            .update(|s| {
                s.role = MemberRole::Replica;
                s.state = MemberState::Running;
                s.is_leader = false;
                s.timeline = Some(1);
                s.wal_position = Some(12300);
                s.replication_lag = Some(45);
                s.last_loop_at = Some(std::time::Instant::now());
            })
            .await;
    }

    #[tokio::test]
    async fn test_primary_returns_200_when_leader() {
        let state = test_state();
        set_primary(&state).await;
        let app = build_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/primary")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_primary_returns_503_when_replica() {
        let state = test_state();
        set_replica(&state).await;
        let app = build_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/primary")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_replica_returns_200_when_healthy() {
        let state = test_state();
        set_replica(&state).await;
        let app = build_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/replica")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_replica_returns_503_when_primary() {
        let state = test_state();
        set_primary(&state).await;
        let app = build_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/replica")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_replica_lag_check_pass() {
        let state = test_state();
        set_replica(&state).await;
        let app = build_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/replica?lag=100")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK); // lag=45 < 100
    }

    #[tokio::test]
    async fn test_replica_lag_check_fail() {
        let state = test_state();
        set_replica(&state).await;
        let app = build_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/replica?lag=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE); // lag=45 > 10
    }

    #[tokio::test]
    async fn test_health_returns_200_when_running() {
        let state = test_state();
        set_replica(&state).await;
        let app = build_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_health_returns_503_when_stopped() {
        let state = test_state();
        state.update(|s| s.state = MemberState::Stopped).await;
        let app = build_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_liveness_returns_200_when_recent() {
        let state = test_state();
        state
            .update(|s| s.last_loop_at = Some(std::time::Instant::now()))
            .await;
        let app = build_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/liveness")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_liveness_returns_503_when_stale() {
        let state = test_state();
        // No last_loop_at set → stale
        let app = build_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/liveness")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_noloadbalance_excludes_from_replica() {
        let state = test_state();
        state
            .update(|s| {
                s.role = MemberRole::Replica;
                s.state = MemberState::Running;
                s.tags
                    .insert("noloadbalance".into(), serde_json::json!(true));
                s.last_loop_at = Some(std::time::Instant::now());
            })
            .await;
        let app = build_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/replica")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_metrics_returns_prometheus_format() {
        let state = test_state();
        state
            .update(|s| {
                s.role = MemberRole::Primary;
                s.state = MemberState::Running;
                s.timeline = Some(3);
                s.replication_lag = Some(0);
                s.pending_restart = false;
                s.is_paused = false;
                s.failsafe_active = false;
                s.dcs_last_seen = Some(std::time::Instant::now());
            })
            .await;
        let app = build_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let ct = resp
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(
            ct.contains("text/plain"),
            "Expected text/plain content type"
        );

        let body = axum::body::to_bytes(resp.into_body(), 16384).await.unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains("pg_ha_node_role{role=\"primary\"} 1"));
        assert!(text.contains("pg_ha_pg_state{state=\"running\"} 1"));
        assert!(text.contains("pg_ha_replication_lag_bytes 0"));
        assert!(text.contains("pg_ha_timeline 3"));
        assert!(text.contains("pg_ha_failsafe_active 0"));
        assert!(text.contains("pg_ha_pending_restart 0"));
        assert!(text.contains("pg_ha_is_paused 0"));
        assert!(text.contains("pg_ha_dcs_last_seen_seconds"));
    }

    #[tokio::test]
    async fn test_metrics_with_replica_state() {
        let state = test_state();
        state
            .update(|s| {
                s.role = MemberRole::Replica;
                s.state = MemberState::Running;
                s.timeline = Some(1);
                s.replication_lag = Some(1024);
                s.pending_restart = true;
                s.is_paused = true;
                s.failsafe_active = true;
            })
            .await;
        let app = build_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), 16384).await.unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains("pg_ha_node_role{role=\"replica\"} 1"));
        assert!(text.contains("pg_ha_replication_lag_bytes 1024"));
        assert!(text.contains("pg_ha_failsafe_active 1"));
        assert!(text.contains("pg_ha_pending_restart 1"));
        assert!(text.contains("pg_ha_is_paused 1"));
    }

    #[tokio::test]
    async fn test_history_returns_empty_array() {
        let state = test_state();
        let app = build_router(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/history")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), 16384).await.unwrap();
        let entries: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
        assert!(entries.is_empty());
    }

    #[tokio::test]
    async fn test_history_returns_recorded_events() {
        use pg_ha_core::history::HistoryEventType;

        let state = test_state();
        // Record an event via the shared history
        {
            let mut history = state.history().write().await;
            history.record_event(
                HistoryEventType::Failover,
                Some("node1".into()),
                Some("node2".into()),
                "leader lock expired".into(),
            );
        }

        let app = build_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/history")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), 16384).await.unwrap();
        let entries: Vec<serde_json::Value> = serde_json::from_slice(&body).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0]["event_type"], "failover");
        assert_eq!(entries[0]["old_leader"], "node1");
        assert_eq!(entries[0]["new_leader"], "node2");
        assert_eq!(entries[0]["reason"], "leader lock expired");
    }
}
