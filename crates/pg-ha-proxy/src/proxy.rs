//! TCP proxy implementation for PostgreSQL connections with active health checking.
//!
//! Implements HAProxy-equivalent behavior:
//! - RW port: routes to the current primary (health check via HTTP GET /primary)
//! - RO port: round-robin across healthy replicas (health check via HTTP GET /replica)
//! - Active health checks every `inter` interval
//! - fall/rise logic: mark down after N consecutive failures, up after N successes
//! - on-marked-down: shutdown active sessions to failed backends
//! - maxconn: limit concurrent connections per backend
//!
//! Uses tokio TCP for accept/connect and bidirectional byte piping.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, RwLock, Semaphore};
use tracing::{debug, error, info, warn};

// ─────────────────── Configuration ───────────────────

/// Health check configuration (mirrors HAProxy `default-server` options)
#[derive(Debug, Clone)]
pub struct HealthCheckConfig {
    /// Interval between health checks (HAProxy `inter`)
    pub interval: Duration,
    /// Number of consecutive failures before marking down (HAProxy `fall`)
    pub fall: u32,
    /// Number of consecutive successes before marking up (HAProxy `rise`)
    pub rise: u32,
    /// HTTP timeout for health check requests
    pub timeout: Duration,
    /// Maximum concurrent connections per backend (HAProxy `maxconn`)
    pub max_connections: usize,
    /// Whether to shutdown active sessions when a backend is marked down
    /// (HAProxy `on-marked-down shutdown-sessions`)
    pub shutdown_on_down: bool,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(3),
            fall: 3,
            rise: 2,
            timeout: Duration::from_secs(5),
            max_connections: 300,
            shutdown_on_down: true,
        }
    }
}

// ─────────────────── Backend State ───────────────────

/// A PostgreSQL backend endpoint with health state
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgBackend {
    pub addr: SocketAddr,
    pub name: String,
    pub is_primary: bool,
    pub is_healthy: bool,
    /// REST API base URL for health checks (e.g. `http://node1:8008`).
    /// Prefer hostname form so Docker DNS / multi-node clusters work.
    pub api_url: String,
}

/// Internal tracked state per backend for health checking
#[derive(Debug)]
struct BackendHealth {
    /// Backend address (PG port)
    addr: SocketAddr,
    /// API URL for health checks (e.g., "http://node1:8008")
    api_url: String,
    /// Node name
    name: String,
    /// Whether this backend is currently considered healthy for RW routing
    rw_healthy: bool,
    /// Whether this backend is currently considered healthy for RO routing
    ro_healthy: bool,
    /// Consecutive RW check failures
    rw_fail_count: u32,
    /// Consecutive RW check successes
    rw_success_count: u32,
    /// Consecutive RO check failures
    ro_fail_count: u32,
    /// Consecutive RO check successes
    ro_success_count: u32,
    /// Connection limiter
    semaphore: Arc<Semaphore>,
    /// Shutdown signal for active connections (sent when marked down)
    shutdown_tx: watch::Sender<bool>,
}

/// Shared upstream state, updated by health checker
#[derive(Debug, Clone, Default)]
pub struct UpstreamState {
    pub backends: Vec<PgBackend>,
}

impl UpstreamState {
    /// Get the current primary backend
    pub fn primary(&self) -> Option<&PgBackend> {
        self.backends.iter().find(|b| b.is_primary && b.is_healthy)
    }

    /// Get all healthy replica backends
    pub fn replicas(&self) -> Vec<&PgBackend> {
        self.backends
            .iter()
            .filter(|b| !b.is_primary && b.is_healthy)
            .collect()
    }
}

// ─────────────────── Proxy ───────────────────

/// The PostgreSQL TCP proxy with active health checking
pub struct PgProxy {
    rw_addr: SocketAddr,
    ro_addr: SocketAddr,
    upstream: Arc<RwLock<UpstreamState>>,
    health_state: Arc<RwLock<HashMap<String, BackendHealth>>>,
    health_config: HealthCheckConfig,
    _ro_counter: AtomicUsize,
}

impl PgProxy {
    pub fn new(rw_addr: SocketAddr, ro_addr: SocketAddr) -> Self {
        Self::with_health_config(rw_addr, ro_addr, HealthCheckConfig::default())
    }

    pub fn with_health_config(
        rw_addr: SocketAddr,
        ro_addr: SocketAddr,
        health_config: HealthCheckConfig,
    ) -> Self {
        Self {
            rw_addr,
            ro_addr,
            upstream: Arc::new(RwLock::new(UpstreamState::default())),
            health_state: Arc::new(RwLock::new(HashMap::new())),
            health_config,
            _ro_counter: AtomicUsize::new(0),
        }
    }

    /// Get a handle to update upstream state from the HA loop
    pub fn upstream_handle(&self) -> Arc<RwLock<UpstreamState>> {
        self.upstream.clone()
    }

    /// Update the backend list and register them for health checking.
    /// Called from HA loop each cycle with the latest cluster membership.
    /// Note: This only registers/unregisters backends. The health checker
    /// is the sole authority on is_primary/is_healthy state after initial registration.
    pub async fn update_backends(&self, backends: Vec<PgBackend>) {
        // Only update the upstream state if health checker hasn't taken over yet
        // (health_state is empty means health checker hasn't run)
        let health_active = {
            let h = self.health_state.read().await;
            !h.is_empty()
        };

        if !health_active {
            // Health checker hasn't started — use HA loop's state directly
            let mut state = self.upstream.write().await;
            state.backends = backends.clone();
        }
        // If health checker is active, it manages upstream state — don't override

        // Register / refresh backends for health checking
        let mut health = self.health_state.write().await;
        for backend in &backends {
            let api_url = if backend.api_url.is_empty() {
                // Last resort: derive from resolved PG address (IPs break Docker hostnames)
                format!("http://{}:8008", backend.addr.ip())
            } else {
                backend.api_url.clone()
            };

            if let Some(existing) = health.get_mut(&backend.name) {
                existing.addr = backend.addr;
                if existing.api_url != api_url {
                    existing.api_url = api_url;
                }
            } else {
                let (shutdown_tx, _) = watch::channel(false);
                health.insert(
                    backend.name.clone(),
                    BackendHealth {
                        addr: backend.addr,
                        api_url,
                        name: backend.name.clone(),
                        rw_healthy: false,
                        ro_healthy: false,
                        rw_fail_count: 0,
                        rw_success_count: 0,
                        ro_fail_count: 0,
                        ro_success_count: 0,
                        semaphore: Arc::new(Semaphore::new(self.health_config.max_connections)),
                        shutdown_tx,
                    },
                );
            }
        }

        // Remove backends no longer in the cluster
        let names: Vec<String> = backends.iter().map(|b| b.name.clone()).collect();
        health.retain(|name, _| names.contains(name));
    }

    /// Start the proxy with health checking
    pub async fn run(&self) -> anyhow::Result<()> {
        let upstream = self.upstream.clone();
        let health_state = self.health_state.clone();
        let rw_addr = self.rw_addr;
        let ro_addr = self.ro_addr;
        let health_config = self.health_config.clone();

        info!(%rw_addr, %ro_addr, "Starting PostgreSQL TCP proxy with active health checks");
        info!(
            inter = ?health_config.interval,
            fall = health_config.fall,
            rise = health_config.rise,
            maxconn = health_config.max_connections,
            shutdown_on_down = health_config.shutdown_on_down,
            "Health check configuration"
        );

        // Spawn active health checker
        let hc_upstream = upstream.clone();
        let hc_health = health_state.clone();
        let hc_config = health_config.clone();
        tokio::spawn(async move {
            run_health_checker(hc_upstream, hc_health, hc_config).await;
        });

        // Spawn RW listener
        let rw_upstream = upstream.clone();
        let rw_health = health_state.clone();
        let rw_config = health_config.clone();
        let rw_handle = tokio::spawn(async move {
            if let Err(e) = run_listener(rw_addr, rw_upstream, rw_health, ProxyMode::ReadWrite, rw_config).await {
                error!("RW proxy error: {e}");
            }
        });

        // Spawn RO listener
        let ro_upstream = upstream.clone();
        let ro_health = health_state.clone();
        let ro_config = health_config;
        let ro_handle = tokio::spawn(async move {
            if let Err(e) = run_listener(ro_addr, ro_upstream, ro_health, ProxyMode::ReadOnly, ro_config).await {
                error!("RO proxy error: {e}");
            }
        });

        tokio::select! {
            _ = rw_handle => {},
            _ = ro_handle => {},
        }

        Ok(())
    }
}

// ─────────────────── Health Checker ───────────────────

/// Active health checker that polls backends periodically.
/// Equivalent to HAProxy's `option httpchk` with `inter`, `fall`, `rise`.
async fn run_health_checker(
    upstream: Arc<RwLock<UpstreamState>>,
    health_state: Arc<RwLock<HashMap<String, BackendHealth>>>,
    config: HealthCheckConfig,
) {
    let client = reqwest::Client::builder()
        .timeout(config.timeout)
        .build()
        .unwrap_or_default();

    let mut interval = tokio::time::interval(config.interval);

    loop {
        interval.tick().await;

        // Collect backends to check
        let backends_to_check: Vec<(String, String, SocketAddr)> = {
            let health = health_state.read().await;
            health
                .values()
                .map(|b| (b.name.clone(), b.api_url.clone(), b.addr))
                .collect()
        };

        if backends_to_check.is_empty() {
            continue;
        }

        // Check all backends concurrently (not sequentially) to avoid
        // one unreachable node blocking health checks for all others.
        let checks: Vec<_> = backends_to_check.iter().map(|(name, api_url, _addr)| {
            let client = client.clone();
            let name = name.clone();
            let api_url = api_url.clone();
            async move {
                let rw_ok = client
                    .head(format!("{api_url}/primary"))
                    .send()
                    .await
                    .map(|r| r.status().is_success())
                    .unwrap_or(false);
                let ro_ok = client
                    .head(format!("{api_url}/replica"))
                    .send()
                    .await
                    .map(|r| r.status().is_success())
                    .unwrap_or(false);
                (name, rw_ok, ro_ok)
            }
        }).collect();

        let results = futures::future::join_all(checks).await;

        let mut rw_results: Vec<(String, bool)> = Vec::new();
        let mut ro_results: Vec<(String, bool)> = Vec::new();
        for (name, rw_ok, ro_ok) in results {
            rw_results.push((name.clone(), rw_ok));
            ro_results.push((name, ro_ok));
        }

        // Apply fall/rise logic and update upstream state
        let mut upstream_changed = false;
        {
            let mut health = health_state.write().await;

            for (name, rw_ok) in &rw_results {
                if let Some(backend) = health.get_mut(name) {
                    let was_healthy = backend.rw_healthy;

                    if *rw_ok {
                        backend.rw_fail_count = 0;
                        backend.rw_success_count += 1;
                        if !backend.rw_healthy && backend.rw_success_count >= config.rise {
                            backend.rw_healthy = true;
                            info!(node = %name, "Backend marked UP for RW (rise={})", config.rise);
                            upstream_changed = true;
                        }
                    } else {
                        backend.rw_success_count = 0;
                        backend.rw_fail_count += 1;
                        if backend.rw_healthy && backend.rw_fail_count >= config.fall {
                            backend.rw_healthy = false;
                            warn!(node = %name, "Backend marked DOWN for RW (fall={})", config.fall);
                            upstream_changed = true;
                            // Shutdown active sessions if configured
                            if config.shutdown_on_down && was_healthy {
                                let _ = backend.shutdown_tx.send(true);
                            }
                        }
                    }
                }
            }

            for (name, ro_ok) in &ro_results {
                if let Some(backend) = health.get_mut(name) {
                    let was_healthy = backend.ro_healthy;

                    if *ro_ok {
                        backend.ro_fail_count = 0;
                        backend.ro_success_count += 1;
                        if !backend.ro_healthy && backend.ro_success_count >= config.rise {
                            backend.ro_healthy = true;
                            info!(node = %name, "Backend marked UP for RO (rise={})", config.rise);
                            upstream_changed = true;
                        }
                    } else {
                        backend.ro_success_count = 0;
                        backend.ro_fail_count += 1;
                        if backend.ro_healthy && backend.ro_fail_count >= config.fall {
                            backend.ro_healthy = false;
                            warn!(node = %name, "Backend marked DOWN for RO (fall={})", config.fall);
                            upstream_changed = true;
                            if config.shutdown_on_down && was_healthy {
                                let _ = backend.shutdown_tx.send(true);
                            }
                        }
                    }
                }
            }
        }

        // Rebuild upstream state from health checker results
        if upstream_changed {
            let health = health_state.read().await;
            let mut new_backends: Vec<PgBackend> = Vec::new();
            for backend in health.values() {
                // Primary: passes RW health check (regardless of RO result —
                // a primary can serve reads too)
                let is_primary = backend.rw_healthy;
                let is_replica = backend.ro_healthy && !backend.rw_healthy;
                new_backends.push(PgBackend {
                    addr: backend.addr,
                    name: backend.name.clone(),
                    is_primary,
                    is_healthy: is_primary || is_replica,
                    api_url: backend.api_url.clone(),
                });
            }
            let mut state = upstream.write().await;
            state.backends = new_backends;
            debug!("Upstream state updated from health checker");
        }
    }
}

// ─────────────────── Proxy Listener ───────────────────

#[derive(Debug, Clone, Copy)]
enum ProxyMode {
    ReadWrite,
    ReadOnly,
}

/// Run a TCP listener and proxy connections to the appropriate backend
async fn run_listener(
    listen_addr: SocketAddr,
    upstream: Arc<RwLock<UpstreamState>>,
    health_state: Arc<RwLock<HashMap<String, BackendHealth>>>,
    mode: ProxyMode,
    config: HealthCheckConfig,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(listen_addr).await?;
    info!(?mode, %listen_addr, "Proxy listener started");

    let counter = AtomicUsize::new(0);

    loop {
        let (client_stream, client_addr) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                error!("Accept error: {e}");
                continue;
            }
        };

        let upstream = upstream.clone();
        let health_state = health_state.clone();
        let counter_val = counter.fetch_add(1, Ordering::Relaxed);
        let max_conn = config.max_connections;
        let shutdown_on_down = config.shutdown_on_down;

        tokio::spawn(async move {
            if let Err(e) = handle_connection(
                client_stream,
                client_addr,
                upstream,
                health_state,
                mode,
                counter_val,
                max_conn,
                shutdown_on_down,
            )
            .await
            {
                debug!(%client_addr, "Connection ended: {e}");
            }
        });
    }
}

/// Handle a single proxied connection with maxconn and shutdown-on-down support
#[allow(clippy::too_many_arguments)]
async fn handle_connection(
    client_stream: TcpStream,
    client_addr: SocketAddr,
    upstream: Arc<RwLock<UpstreamState>>,
    health_state: Arc<RwLock<HashMap<String, BackendHealth>>>,
    mode: ProxyMode,
    counter: usize,
    _max_conn: usize,
    shutdown_on_down: bool,
) -> anyhow::Result<()> {
    // Select backend and acquire connection permit
    let (backend_addr, backend_name, semaphore, mut shutdown_rx) = {
        let state = upstream.read().await;
        let health = health_state.read().await;

        let selected = match mode {
            ProxyMode::ReadWrite => state.primary().map(|b| b.name.clone()),
            ProxyMode::ReadOnly => {
                let replicas = state.replicas();
                if replicas.is_empty() {
                    // Fallback: route RO to primary if no replicas
                    state.primary().map(|b| b.name.clone())
                } else {
                    let idx = counter % replicas.len();
                    Some(replicas[idx].name.clone())
                }
            }
        };

        let backend_name = match selected {
            Some(name) => name,
            None => {
                warn!(%client_addr, ?mode, "No backend available, closing connection");
                return Ok(());
            }
        };

        let backend_info = health.get(&backend_name);
        match backend_info {
            Some(info) => (
                info.addr,
                backend_name,
                info.semaphore.clone(),
                info.shutdown_tx.subscribe(),
            ),
            None => {
                warn!(%client_addr, "Backend '{}' not found in health state", backend_name);
                return Ok(());
            }
        }
    };

    // Acquire connection permit (maxconn enforcement)
    let _permit = match semaphore.try_acquire() {
        Ok(permit) => permit,
        Err(_) => {
            warn!(
                %client_addr,
                backend = %backend_name,
                "Backend at maxconn limit, rejecting connection"
            );
            return Ok(());
        }
    };

    // Connect to backend
    let backend_stream = match TcpStream::connect(backend_addr).await {
        Ok(s) => s,
        Err(e) => {
            debug!(%client_addr, %backend_addr, "Backend connect failed: {e}");
            return Err(e.into());
        }
    };
    debug!(%client_addr, %backend_addr, backend = %backend_name, ?mode, "Proxying connection");

    // Pipe bidirectionally with shutdown-on-down support
    let (mut client_read, mut client_write) = client_stream.into_split();
    let (mut backend_read, mut backend_write) = backend_stream.into_split();

    let client_to_backend = io::copy(&mut client_read, &mut backend_write);
    let backend_to_client = io::copy(&mut backend_read, &mut client_write);

    if shutdown_on_down {
        // Monitor for shutdown signal while piping bidirectionally.
        // Use tokio::join! inside select! to ensure both copy directions complete
        // (preventing data truncation on half-close), while still allowing
        // forced termination when the backend is marked DOWN.
        tokio::select! {
            result = async {
                let (c2b, b2c) = tokio::join!(client_to_backend, backend_to_client);
                (c2b, b2c)
            } => {
                let (c2b, b2c) = result;
                if let Err(e) = c2b {
                    debug!(%client_addr, "Client→Backend closed: {e}");
                }
                if let Err(e) = b2c {
                    debug!(%client_addr, "Backend→Client closed: {e}");
                }
            }
            _ = async {
                // Wait for shutdown signal
                loop {
                    shutdown_rx.changed().await.ok();
                    if *shutdown_rx.borrow() {
                        break;
                    }
                }
            } => {
                info!(
                    %client_addr,
                    backend = %backend_name,
                    "Shutting down session: backend marked DOWN"
                );
            }
        }
    } else {
        // No shutdown monitoring — wait for both directions to complete.
        // tokio::join! ensures half-close is handled correctly: when one direction
        // finishes (peer sends FIN), the other continues until it also completes.
        let (c2b, b2c) = tokio::join!(client_to_backend, backend_to_client);
        if let Err(e) = c2b {
            debug!(%client_addr, "Client→Backend closed: {e}");
        }
        if let Err(e) = b2c {
            debug!(%client_addr, "Backend→Client closed: {e}");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_upstream_state_primary() {
        let state = UpstreamState {
            backends: vec![
                PgBackend {
                    addr: "10.0.0.1:5432".parse().unwrap(),
                    name: "node1".into(),
                    is_primary: true,
                    is_healthy: true,
                    api_url: "http://node1:8008".into(),
                },
                PgBackend {
                    addr: "10.0.0.2:5432".parse().unwrap(),
                    name: "node2".into(),
                    is_primary: false,
                    is_healthy: true,
                    api_url: "http://node2:8008".into(),
                },
            ],
        };
        assert_eq!(state.primary().unwrap().name, "node1");
        assert_eq!(state.replicas().len(), 1);
        assert_eq!(state.replicas()[0].name, "node2");
    }

    #[test]
    fn test_upstream_state_no_primary() {
        let state = UpstreamState {
            backends: vec![PgBackend {
                addr: "10.0.0.2:5432".parse().unwrap(),
                name: "node2".into(),
                is_primary: false,
                is_healthy: true,
                api_url: "http://node2:8008".into(),
            }],
        };
        assert!(state.primary().is_none());
    }

    #[test]
    fn test_upstream_state_unhealthy_excluded() {
        let state = UpstreamState {
            backends: vec![
                PgBackend {
                    addr: "10.0.0.1:5432".parse().unwrap(),
                    name: "node1".into(),
                    is_primary: true,
                    is_healthy: false,
                    api_url: "http://node1:8008".into(),
                },
                PgBackend {
                    addr: "10.0.0.2:5432".parse().unwrap(),
                    name: "node2".into(),
                    is_primary: false,
                    is_healthy: false,
                    api_url: "http://node2:8008".into(),
                },
            ],
        };
        assert!(state.primary().is_none());
        assert!(state.replicas().is_empty());
    }

    #[test]
    fn test_round_robin_selection() {
        let state = UpstreamState {
            backends: vec![
                PgBackend {
                    addr: "10.0.0.2:5432".parse().unwrap(),
                    name: "node2".into(),
                    is_primary: false,
                    is_healthy: true,
                    api_url: "http://node2:8008".into(),
                },
                PgBackend {
                    addr: "10.0.0.3:5432".parse().unwrap(),
                    name: "node3".into(),
                    is_primary: false,
                    is_healthy: true,
                    api_url: "http://node3:8008".into(),
                },
            ],
        };
        let replicas = state.replicas();
        assert_eq!(replicas[0 % 2].name, "node2");
        assert_eq!(replicas[1 % 2].name, "node3");
        assert_eq!(replicas[2 % 2].name, "node2");
    }

    #[test]
    fn test_health_check_config_defaults() {
        let config = HealthCheckConfig::default();
        assert_eq!(config.interval, Duration::from_secs(3));
        assert_eq!(config.fall, 3);
        assert_eq!(config.rise, 2);
        assert_eq!(config.max_connections, 300);
        assert!(config.shutdown_on_down);
    }
}
