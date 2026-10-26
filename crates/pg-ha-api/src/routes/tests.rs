use super::*;
use axum::body::Body;
use axum::http::{Request, StatusCode};
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
            s.pg_running = true;
            s.is_leader = true;
            s.pg_in_recovery = Some(false);
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
            s.pg_running = true;
            s.is_leader = false;
            s.pg_in_recovery = Some(true);
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

async fn status_of(state: AppState, uri: &str) -> StatusCode {
    build_router(state)
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap()
        .status()
}

/// H1: every clause of "holds the lock AND PostgreSQL is a healthy primary"
/// is necessary. Dropping any one of them must turn /primary into 503.
#[tokio::test]
async fn test_primary_requires_every_h1_clause() {
    type Breaker = fn(&mut crate::state::NodeState);
    let cases: [(&str, Breaker); 7] = [
        ("lock not held", |s| s.is_leader = false),
        ("process stopped", |s| s.state = MemberState::Stopped),
        ("not vouched (unmanaged / unknown)", |s| {
            s.state = MemberState::Unknown
        }),
        ("role not confirmed primary", |s| {
            s.role = MemberRole::Replica
        }),
        ("observed in recovery", |s| s.pg_in_recovery = Some(true)),
        ("recovery state not observed", |s| s.pg_in_recovery = None),
        ("graceful shutdown in progress", |s| s.shutting_down = true),
    ];
    for (name, breaker) in cases {
        let state = test_state();
        set_primary(&state).await;
        state.update(breaker).await;
        assert_eq!(
            status_of(state, "/primary").await,
            StatusCode::SERVICE_UNAVAILABLE,
            "/primary must be 503 when {name}"
        );
    }
}

/// H2: a server observed out of recovery is never served as a replica, even
/// if its recorded role says replica (e.g. a demoted primary that restarted).
#[tokio::test]
async fn test_replica_requires_observed_recovery() {
    for (name, obs) in [("observed primary", Some(false)), ("not observed", None)] {
        let state = test_state();
        set_replica(&state).await;
        state.update(|s| s.pg_in_recovery = obs).await;
        assert_eq!(
            status_of(state, "/replica").await,
            StatusCode::SERVICE_UNAVAILABLE,
            "/replica must be 503 when {name}"
        );
    }
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

/// /health reports the PostgreSQL process, not the HA role: a running server
/// pg-ha cannot vouch for (unmanaged recovery, unobserved) is still "up",
/// while /primary and /replica refuse it.
#[tokio::test]
async fn test_health_reports_process_even_when_role_not_vouched() {
    let state = test_state();
    state
        .update(|s| {
            s.pg_running = true;
            s.state = MemberState::Unknown;
            s.role = MemberRole::Replica;
            s.pg_in_recovery = Some(true);
        })
        .await;
    assert_eq!(status_of(state.clone(), "/health").await, StatusCode::OK);
    assert_eq!(
        status_of(state.clone(), "/replica").await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        status_of(state, "/primary").await,
        StatusCode::SERVICE_UNAVAILABLE
    );
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
            s.pg_running = true;
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
            s.pg_running = true;
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
            s.pg_running = true;
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

// ─── /config: lease timing constraint (R19) ───

/// DCS stub that only stores the /config value.
struct ConfigDcs {
    config: std::sync::Mutex<Option<String>>,
}

#[async_trait::async_trait]
impl pg_ha_core::dcs::DcsAdapter for ConfigDcs {
    async fn get_cluster(&self) -> pg_ha_core::Result<pg_ha_core::cluster::Cluster> {
        Ok(Default::default())
    }
    async fn attempt_to_acquire_leader(&self) -> pg_ha_core::Result<bool> {
        Ok(false)
    }
    async fn update_leader(&self, _: &pg_ha_core::cluster::Leader) -> pg_ha_core::Result<bool> {
        Ok(false)
    }
    async fn delete_leader(&self, _: &pg_ha_core::cluster::Leader) -> pg_ha_core::Result<bool> {
        Ok(false)
    }
    async fn touch_member(&self, _: &serde_json::Value) -> pg_ha_core::Result<bool> {
        Ok(true)
    }
    async fn initialize(&self, _: &str) -> pg_ha_core::Result<bool> {
        Ok(false)
    }
    async fn set_failover_value(&self, _: &str) -> pg_ha_core::Result<bool> {
        Ok(true)
    }
    async fn set_sync_value(&self, _: &str) -> pg_ha_core::Result<bool> {
        Ok(true)
    }
    async fn set_config_value(&self, value: &str) -> pg_ha_core::Result<bool> {
        *self.config.lock().unwrap() = Some(value.to_string());
        Ok(true)
    }
    async fn get_config_value(&self) -> pg_ha_core::Result<Option<String>> {
        Ok(self.config.lock().unwrap().clone())
    }
    async fn set_cluster_timeline(&self, _: u64) -> pg_ha_core::Result<bool> {
        Ok(true)
    }
    fn ttl(&self) -> u64 {
        15
    }
    fn loop_wait(&self) -> u64 {
        5
    }
}

/// Docker cluster values: ttl 15, loop_wait 5, retry_timeout 5, margin 5.
fn config_state(stored: &str) -> (AppState, std::sync::Arc<ConfigDcs>) {
    let dcs = std::sync::Arc::new(ConfigDcs {
        config: std::sync::Mutex::new(Some(stored.to_string())),
    });
    let state = AppState::with_dcs(
        "node1".into(),
        "test-cluster".into(),
        15,
        dcs.clone(),
        pg_ha_core::config::LeaseTiming {
            ttl: 15,
            loop_wait: 5,
            retry_timeout: 5,
            safety_margin: 5,
        },
    );
    (state, dcs)
}

async fn send_config(state: AppState, method: &str, body: &str) -> (StatusCode, String) {
    let resp = build_router(state)
        .oneshot(
            Request::builder()
                .method(method)
                .uri("/config")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let body = axum::body::to_bytes(resp.into_body(), 16384).await.unwrap();
    (status, String::from_utf8(body.to_vec()).unwrap())
}

fn stored_config(dcs: &ConfigDcs) -> serde_json::Value {
    serde_json::from_str(dcs.config.lock().unwrap().as_deref().unwrap()).unwrap()
}

#[tokio::test]
async fn test_patch_config_rejects_timing_below_lease_requirement() {
    let stored = r#"{"loop_wait":5,"ttl":15,"pause":false}"#;
    let (state, dcs) = config_state(stored);
    // 15 < 6 + 5 + 5
    let (status, body) = send_config(state, "PATCH", r#"{"loop_wait":6}"#).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    for part in [
        "not changed",
        "ttl (15)",
        "loop_wait (6)",
        "retry_timeout (5)",
        "watchdog.safety_margin (5)",
        "= 16",
    ] {
        assert!(body.contains(part), "missing {part:?} in {body}");
    }
    let before: serde_json::Value = serde_json::from_str(stored).unwrap();
    assert_eq!(
        stored_config(&dcs),
        before,
        "the change must not be applied"
    );
}

#[tokio::test]
async fn test_put_config_rejects_timing_below_lease_requirement() {
    let stored = r#"{"loop_wait":5,"ttl":15}"#;
    let (state, dcs) = config_state(stored);
    // 14 < 5 + 5 + 5
    let (status, body) = send_config(state, "PUT", r#"{"loop_wait":5,"ttl":14}"#).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(body.contains("ttl (14)"), "{body}");
    let before: serde_json::Value = serde_json::from_str(stored).unwrap();
    assert_eq!(
        stored_config(&dcs),
        before,
        "the change must not be applied"
    );
}

#[tokio::test]
async fn test_config_accepts_timing_that_meets_lease_requirement() {
    let (state, dcs) = config_state(r#"{"loop_wait":5,"ttl":15}"#);
    // 30 >= 10 + 5 + 5
    let (status, body) = send_config(state.clone(), "PATCH", r#"{"loop_wait":10,"ttl":30}"#).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(stored_config(&dcs)["ttl"], 30);
    // Updates that leave the three values alone are not checked.
    let (status, body) = send_config(state, "PATCH", r#"{"pause":true}"#).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(stored_config(&dcs)["pause"], true);
}
