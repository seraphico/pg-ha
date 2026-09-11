//! REST API routes for health checks and management.

use axum::{
    Router,
    routing::{get, post},
};

use crate::state::AppState;

mod auth;
mod config;
mod health;
mod management;
mod observability;
mod router_state;

use auth::basic_auth_middleware;
use config::{get_config, patch_config_endpoint, put_config};
use health::{
    get_cluster, health_async, health_check, health_primary, health_replica, health_standby_leader,
    health_sync, liveness, node_status,
};
use management::{
    delete_switchover, post_failover, post_reinitialize, post_restart, post_switchover,
};
use observability::{get_history, get_metrics};
pub use router_state::{AuthConfig, CommandSender};
use router_state::RouterState;

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

#[cfg(test)]
mod tests;
