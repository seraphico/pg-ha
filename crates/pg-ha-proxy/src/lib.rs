//! pg-ha-proxy: TCP load balancer with active health checking (replaces HAProxy)
//!
//! Routes PostgreSQL connections based on port:
//! - Read-Write port → current Primary (health check: HTTP HEAD /primary)
//! - Read-Only port → healthy Replicas (health check: HTTP HEAD /replica)
//!
//! Features equivalent to HAProxy:
//! - Active HTTP health checks (option httpchk)
//! - fall/rise logic for backend state transitions
//! - on-marked-down shutdown-sessions
//! - maxconn per backend
//! - Round-robin load balancing for RO

pub mod proxy;

pub use proxy::{HealthCheckConfig, PgBackend, PgProxy, UpstreamState};
