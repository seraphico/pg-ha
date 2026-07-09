//! pg-ha-api: REST API for health checks and cluster management
//!
//! Compatible with HAProxy health check endpoints and Patroni tooling.

pub mod state;
pub mod routes;

pub use state::AppState;
pub use routes::{build_router, build_router_with_commands, AuthConfig, CommandSender};
