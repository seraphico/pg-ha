//! Health check handlers for the router.

use super::router_state::{ReplicaQuery, RouterState};
use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde_json::json;

/// GET /primary, GET /, GET /read-write
/// Returns 200 if this node is the primary with the leader lock.
pub(crate) async fn health_primary(State(state): State<RouterState>) -> impl IntoResponse {
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
pub(crate) async fn health_replica(
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

    // Check lag threshold if specified. Fail-open (SB-10): an unknown lag (no
    // leader record, missing position) never rejects; only a known lag above
    // the limit does.
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
/// Returns 200 if the PostgreSQL process is running, whatever its HA role.
pub(crate) async fn health_check(State(state): State<RouterState>) -> impl IntoResponse {
    let s = state.app.read().await;
    if s.pg_running {
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
pub(crate) async fn liveness(State(state): State<RouterState>) -> impl IntoResponse {
    let s = state.app.read().await;
    if s.is_live() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// GET /standby-leader
/// Returns 200 only if this node is a standby leader.
pub(crate) async fn health_standby_leader(State(state): State<RouterState>) -> impl IntoResponse {
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

/// 从 DCS /sync 判断该节点是否是同步备库
async fn is_sync_standby(state: &RouterState, node_name: &str) -> bool {
    let Some(dcs) = state.app.dcs() else {
        return false;
    };
    match dcs.get_cluster().await {
        Ok(cluster) => cluster
            .sync_state
            .as_ref()
            .is_some_and(|sync| sync.matches(node_name)),
        Err(_) => false,
    }
}

/// GET /synchronous, GET /sync
/// Returns 200 if this node is a synchronous standby.
pub(crate) async fn health_sync(State(state): State<RouterState>) -> impl IntoResponse {
    let s = state.app.read().await;
    let name = s.name.clone();
    let healthy = s.is_healthy_replica();
    let role = format!("{:?}", s.role);
    drop(s);
    let is_sync = is_sync_standby(&state, &name).await;
    if healthy && is_sync {
        (StatusCode::OK, Json(json!({"role": "sync_standby"}))).into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"role": role}))).into_response()
    }
}

/// GET /asynchronous, GET /async
/// Returns 200 if this node is an asynchronous standby.
pub(crate) async fn health_async(State(state): State<RouterState>) -> impl IntoResponse {
    let s = state.app.read().await;
    let name = s.name.clone();
    let healthy = s.is_healthy_replica();
    let role = format!("{:?}", s.role);
    drop(s);
    let is_sync = is_sync_standby(&state, &name).await;
    if healthy && !is_sync {
        (StatusCode::OK, Json(json!({"role": "async_standby"}))).into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"role": role}))).into_response()
    }
}

/// GET /patroni — Full node status (compatible with Patroni response format)
pub(crate) async fn node_status(State(state): State<RouterState>) -> impl IntoResponse {
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
        "pg_in_recovery": s.pg_in_recovery,
        "tags": s.tags,
    }))
}

/// The `sync` object of `GET /cluster`, from a fresh `/sync` read (SB-13).
///
/// `kind` is `priority` or `quorum`; `voters` are the names in `/sync`;
/// `quorum` is `q` and is null for `priority` (the field is legacy there).
/// `n` (as `SHOW synchronous_standby_names` reported it) and `settled`
/// (`q == |voters| - n`) are known only to the leader's executor, so they are
/// null on other nodes, on a priority `/sync`, and before the first read.
fn sync_json(
    sync: Option<&pg_ha_core::cluster::SyncState>,
    observed_n: Option<usize>,
) -> serde_json::Value {
    use pg_ha_core::cluster::SyncKind;
    let Some(sync) = sync.filter(|s| !s.is_empty()) else {
        return serde_json::Value::Null;
    };
    let quorum = sync.kind == SyncKind::Quorum;
    json!({
        "kind": if quorum { "quorum" } else { "priority" },
        "voters": sync.sync_standby_names(),
        "quorum": quorum.then_some(sync.quorum),
        "n": if quorum { observed_n } else { None },
        "settled": sync.quorum_settled(observed_n),
    })
}

/// GET /cluster — Cluster members summary with cascade topology
pub(crate) async fn get_cluster(State(state): State<RouterState>) -> impl IntoResponse {
    let s = state.app.read().await;

    // If DCS is available, fetch the full cluster state for topology
    if let Some(dcs) = state.app.dcs() {
        match dcs.get_cluster().await {
            Ok(cluster) => {
                let topology = pg_ha_core::CascadeManager::build_cascade_topology(&cluster);
                return Json(json!({
                    "scope": s.scope,
                    "members": topology,
                    "sync": sync_json(cluster.sync_state.as_ref(), s.quorum_n),
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
        "sync": null,
    }))
    .into_response()
}

/// GET /wal
/// Live WAL position and timeline of this node, read from PostgreSQL now (not
/// the published copy). Open like the other health routes; peers call it
/// during a quorum election. 503 when unavailable, busy, slow or failing.
pub(crate) async fn wal_endpoint(State(state): State<RouterState>) -> impl IntoResponse {
    let unavailable = |msg: String| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": msg })),
        )
    };
    let Some(probe) = state.app.wal_probe().cloned() else {
        return unavailable("live WAL probe not available".into());
    };
    let Some(_permit) = state.app.try_wal_permit() else {
        return unavailable("busy".into());
    };
    let wal = match tokio::time::timeout(state.app.wal_timeout(), probe.probe()).await {
        Ok(Ok(wal)) => wal,
        Ok(Err(err)) => return unavailable(err.to_string()),
        Err(_) => return unavailable("live WAL read timed out".into()),
    };
    if wal.timeline.is_none() {
        return unavailable("timeline unknown".into());
    }
    let (name, scope) = {
        let s = state.app.read().await;
        (s.name.clone(), s.scope.clone())
    };
    (
        StatusCode::OK,
        Json(json!({
            "name": name,
            "scope": scope,
            "timeline": wal.timeline,
            "wal_position": wal.wal_position,
            "pg_in_recovery": wal.in_recovery,
        })),
    )
}

/// POST /vote
/// SB-40: a quorum candidate asks for this node's vote. Before answering, the
/// node stops streaming from its upstream and confirms it (see
/// [`pg_ha_core::VoteFencer`]); the answer has the same body as `GET /wal`.
/// Any failure is a 503, which the candidate counts as "no vote".
///
/// It changes configuration, so it is only served while the leader lock is
/// free in the DCS (a candidate asks only then). It is a mutating endpoint, so
/// it sits behind the same Basic Auth as `/switchover` (open when no REST
/// credentials are configured); `GET /wal` stays open.
pub(crate) async fn vote_endpoint(State(state): State<RouterState>) -> impl IntoResponse {
    let refuse = |status: StatusCode, msg: String| (status, Json(json!({ "error": msg })));
    let Some(fencer) = state.app.vote_fencer().cloned() else {
        return refuse(
            StatusCode::SERVICE_UNAVAILABLE,
            "vote fence not available".into(),
        );
    };
    if let Some(dcs) = state.app.dcs() {
        match dcs.get_cluster().await {
            Ok(cluster) if cluster.leader.is_some() => {
                return refuse(StatusCode::CONFLICT, "leader lock is held".into());
            }
            Ok(_) => {}
            Err(err) => {
                return refuse(
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("DCS unavailable: {err}"),
                );
            }
        }
    }
    let Some(_permit) = state.app.try_wal_permit() else {
        return refuse(StatusCode::SERVICE_UNAVAILABLE, "busy".into());
    };
    let wal = match tokio::time::timeout(state.app.wal_timeout(), fencer.fence_and_read()).await {
        Ok(Ok(wal)) => wal,
        Ok(Err(err)) => return refuse(StatusCode::SERVICE_UNAVAILABLE, err.to_string()),
        Err(_) => {
            return refuse(
                StatusCode::SERVICE_UNAVAILABLE,
                "vote fence timed out".into(),
            );
        }
    };
    if wal.timeline.is_none() {
        return refuse(StatusCode::SERVICE_UNAVAILABLE, "timeline unknown".into());
    }
    let (name, scope) = {
        let s = state.app.read().await;
        (s.name.clone(), s.scope.clone())
    };
    (
        StatusCode::OK,
        Json(json!({
            "name": name,
            "scope": scope,
            "timeline": wal.timeline,
            "wal_position": wal.wal_position,
            "pg_in_recovery": wal.in_recovery,
            "fenced": true,
        })),
    )
}
