//! pg-ha-api: REST API for health checks and cluster management
//!
//! Compatible with HAProxy health check endpoints and Patroni tooling.

pub mod routes;
pub mod state;

pub use routes::{AuthConfig, CommandSender, build_router, build_router_with_commands};
pub use state::AppState;
