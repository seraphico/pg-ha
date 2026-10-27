//! pg-ha: PostgreSQL High Availability Agent
//!
//! Single binary containing:
//! - HA decision engine
//! - Raft DCS (embedded consensus)
//! - REST API (health checks + management)
//! - TCP Proxy (read/write splitting)

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use clap::Parser;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use pg_ha_api::{AppState, AuthConfig};
use pg_ha_core::cluster::{Member, MemberRole, MemberState};
use pg_ha_core::config::Config;
use pg_ha_core::ha::{Ha, ShutdownOutcome};
use pg_ha_core::postgresql::Postgresql;
use pg_ha_dcs::RaftDcs;
use pg_ha_proxy::{PgBackend, PgProxy};

/// pg-ha: PostgreSQL High Availability Agent
#[derive(Parser)]
#[command(name = "pg-ha", version, about)]
struct Cli {
    /// Path to configuration file (YAML)
    #[arg(default_value = "pg-ha.yml")]
    configfile: PathBuf,

    /// Validate configuration and exit
    #[arg(long)]
    validate_config: bool,

    /// Generate a sample configuration file and exit
    #[arg(long)]
    generate_sample_config: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Handle --generate-sample-config
    if cli.generate_sample_config {
        print!("{}", Config::sample());
        return Ok(());
    }

    // Load configuration
    let mut config = Config::from_file(&cli.configfile)?;
    config.apply_env_overrides();
    // Environment overrides can change ttl / loop_wait / retry_timeout.
    config.validate()?;

    // Initialize tracing: env var PG_HA_LOG_FORMAT overrides config.log_format
    let env_filter = EnvFilter::from_default_env()
        .add_directive("pg_ha=info".parse()?)
        .add_directive("openraft::replication=off".parse()?);

    let use_json = match std::env::var("PG_HA_LOG_FORMAT") {
        Ok(val) => val.eq_ignore_ascii_case("json"),
        Err(_) => config.log_format == pg_ha_core::config::LogFormat::Json,
    };

    if use_json {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer().json())
            .init();
    } else {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer())
            .init();
    }

    // Handle --validate-config
    if cli.validate_config {
        info!("Configuration is valid");
        return Ok(());
    }

    info!(name = %config.name, scope = %config.scope, "Starting pg-ha agent");

    // ─── Initialize Raft DCS ───
    // Derive node_id: use explicit config or hash from self_addr
    let all_addrs = {
        let mut addrs = vec![config.raft.self_addr.clone()];
        addrs.extend(config.raft.partner_addrs.clone());
        addrs.sort();
        addrs
    };
    let node_id: u64 = config.raft.node_id.unwrap_or_else(|| {
        // Deterministic ID: position in sorted address list + 1
        all_addrs
            .iter()
            .position(|a| a == &config.raft.self_addr)
            .unwrap_or(0) as u64
            + 1
    });
    info!(node_id, self_addr = %config.raft.self_addr, "Raft node identity");

    let dcs = RaftDcs::new(
        node_id,
        config.name.clone(),
        config.scope.clone(),
        config.namespace.clone(),
        config.ttl,
        config.loop_wait,
        config.retry_timeout,
        config.raft.data_dir.clone(),
        config.raft.tls.as_ref(),
    )
    .await
    .map_err(|e| anyhow::anyhow!("Failed to initialize Raft: {e}"))?;
    let dcs = Arc::new(dcs);

    // ─── Initialize Raft RPC server (must start BEFORE bootstrap) ───
    let raft_router = pg_ha_dcs::raft_router(dcs.raft().clone());
    let raft_addr: SocketAddr = match resolve_addr(&config.raft.self_addr).await {
        Some(addr) => addr,
        None => {
            error!(addr = %config.raft.self_addr, "Cannot resolve Raft RPC address");
            anyhow::bail!(
                "Failed to resolve Raft RPC address '{}'. Check DNS or use IP:port format.",
                config.raft.self_addr
            );
        }
    };

    // Start Raft RPC server — TLS or plain HTTP depending on config
    if let Some(ref tls_cfg) = config.raft.tls {
        use axum_server::tls_rustls::RustlsConfig;
        // Install rustls crypto provider (required by rustls 0.23+)
        let _ = rustls::crypto::ring::default_provider().install_default();
        let rustls_config = RustlsConfig::from_pem_file(&tls_cfg.cert, &tls_cfg.key)
            .await
            .map_err(|e| anyhow::anyhow!("Raft TLS config error: {e}"))?;
        info!(%raft_addr, "Raft RPC listening (TLS)");
        tokio::spawn(async move {
            if let Err(e) = axum_server::bind_rustls(raft_addr, rustls_config)
                .serve(raft_router.into_make_service())
                .await
            {
                error!("Raft RPC TLS server fatal error: {e}");
                std::process::exit(1);
            }
        });
    } else {
        let raft_listener = tokio::net::TcpListener::bind(raft_addr).await?;
        info!(%raft_addr, "Raft RPC listening");
        tokio::spawn(async move {
            if let Err(e) = axum::serve(raft_listener, raft_router).await {
                error!("Raft RPC server fatal error: {e}");
                std::process::exit(1);
            }
        });
    }

    // ─── Bootstrap Raft cluster ───
    // Build the full member list: (node_id, addr)
    let members: Vec<(u64, String)> = all_addrs
        .iter()
        .enumerate()
        .map(|(i, addr)| ((i + 1) as u64, addr.clone()))
        .collect();

    info!("Waiting for Raft leader election...");

    let bootstrap_timeout = std::time::Duration::from_secs(30);
    let deadline = tokio::time::Instant::now() + bootstrap_timeout;

    // Small staggered dely on node_id to reduce simultaneous attempts
    let stagger = std::time::Duration::from_millis(500 * node_id);
    tokio::time::sleep(stagger).await;

    let mut bootstrapped = false;
    loop {
        match dcs.wait_for_leader(2).await {
            Ok(()) => {
                info!("Raft cluster ready");
                break;
            }
            Err(_) => {}
        }

        if tokio::time::Instant::now() >= deadline {
            error!("Raft cluster not ready after {bootstrap_timeout:?}");
            break;
        }

        if !bootstrapped {
            match dcs.bootstrap_cluster(&members).await {
                Ok(()) => {
                    info!("Raft cluster bootstrapped with {} member", members.len());
                    bootstrapped = true;
                }
                Err(e) => {
                    tracing::debug!("Bootstrap attempts: {e} (expected if cluster exists)");
                    // Ignored: likely already bootstrapped
                    bootstrapped = true;
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    // ─── Initialize PostgreSQL manager ───
    let postgresql = Postgresql::new(config.postgresql.clone());

    // ─── Initialize HA engine ───
    let (mut ha, cmd_tx) = Ha::new(config.clone(), dcs.clone(), postgresql);

    // ─── Initialize REST API ───
    let app_state = AppState::with_dcs(
        config.name.clone(),
        config.scope.clone(),
        config.ttl,
        dcs.clone(),
        config.lease_timing(),
    );
    let auth_config = AuthConfig {
        username: config.restapi.username.clone(),
        password: config.restapi.password.clone(),
    };
    let api_router =
        pg_ha_api::build_router_with_commands(app_state.clone(), Some(cmd_tx), auth_config);
    let api_addr: SocketAddr = format!("{}:{}", config.restapi.listen, config.restapi.port)
        .parse()
        .map_err(|e| {
            anyhow::anyhow!(
                "Invalid REST API address '{}:{}': {e}",
                config.restapi.listen,
                config.restapi.port
            )
        })?;

    // ─── Initialize TCP Proxy ───
    let rw_addr: SocketAddr = format!("{}:{}", config.proxy.rw_listen, config.proxy.rw_port)
        .parse()
        .map_err(|e| anyhow::anyhow!("Invalid proxy RW address: {e}"))?;
    let ro_addr: SocketAddr = format!("{}:{}", config.proxy.ro_listen, config.proxy.ro_port)
        .parse()
        .map_err(|e| anyhow::anyhow!("Invalid proxy RO address: {e}"))?;
    let proxy = Arc::new(PgProxy::new(rw_addr, ro_addr));

    // ─── Initialize Raft RPC server ───
    // (Already started above before bootstrap)

    // ─── Start remaining subsystems concurrently ───
    info!(%api_addr, %rw_addr, %ro_addr, "All subsystems starting");

    let app_state_clone = app_state.clone();

    // Demotion / fence paths publish the API state from inside the cycle, so
    // /primary is 503 before PostgreSQL stops (not only after the cycle).
    let publish_state = app_state.clone();
    ha.set_state_publisher(Arc::new(move |snapshot| {
        let state = publish_state.clone();
        Box::pin(async move { state.update(|s| s.apply_ha_snapshot(&snapshot)).await })
    }));

    // Proxy backends are refreshed on their own task, fed with the latest
    // member list after each HA cycle. The task ends when the HA loop (the
    // sender) is dropped.
    let (members_tx, members_rx) = tokio::sync::watch::channel(Vec::<Member>::new());
    tokio::spawn(refresh_proxy_backends(
        members_rx,
        proxy.clone(),
        config.restapi.port,
    ));

    // REST API server: spawned so it keeps serving during graceful shutdown
    // (/primary is 503 from the signal on, the other endpoints stay truthful).
    let api_listener = match tokio::net::TcpListener::bind(api_addr).await {
        Ok(l) => l,
        Err(e) => {
            error!(%api_addr, "Failed to bind REST API: {e}");
            std::process::exit(1);
        }
    };
    info!(%api_addr, "REST API listening");
    tokio::spawn(async move {
        if let Err(e) = axum::serve(api_listener, api_router).await {
            error!("REST API server fatal error: {e}");
            std::process::exit(1);
        }
    });

    let mut signals = ShutdownSignals::new()?;

    tokio::select! {
        // HA Loop
        _ = async {
            let mut current_loop_wait = config.loop_wait;
            let mut interval = ha_interval(current_loop_wait);
            loop {
                interval.tick().await;
                ha_cycle(&mut ha, &mut current_loop_wait, &mut interval, &app_state_clone, &members_tx).await;
            }
        } => {}

        // TCP Proxy
        _ = proxy.run() => {}

        // Signal handling (SIGINT and SIGTERM)
        sig = signals.recv() => {
            info!("Received {sig}");
        }
    }

    // ─── Graceful Shutdown Sequence ───
    // The proxy listeners are tasks spawned inside proxy.run(), so dropping
    // the select does not close them; the REST API keeps serving. Further
    // signals never make pg-ha exit while PostgreSQL may be writable: they
    // only make a stuck shutdown retry the stop at once.
    let retry_now = Arc::new(tokio::sync::Notify::new());
    let notify = retry_now.clone();
    tokio::spawn(async move {
        loop {
            let sig = signals.recv().await;
            warn!(
                "Received {sig} during graceful shutdown; pg-ha exits only after PostgreSQL has stopped"
            );
            notify.notify_one();
        }
    });

    let drain = std::time::Duration::from_secs(config.shutdown.drain_timeout_secs);
    let mut outcome = ha.graceful_shutdown(drain).await;
    if outcome == ShutdownOutcome::KeepRunning {
        outcome =
            keep_running_until_stopped(&mut ha, &app_state_clone, &members_tx, &retry_now).await;
    }
    info!("Graceful shutdown complete: {outcome}");
    Ok(())
}

/// SIGTERM and SIGINT, registered once for the whole process lifetime, so no
/// signal is lost between the first one and the shutdown sequence.
struct ShutdownSignals {
    term: tokio::signal::unix::Signal,
    int: tokio::signal::unix::Signal,
}

impl ShutdownSignals {
    fn new() -> anyhow::Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Self {
            term: signal(SignalKind::terminate())
                .map_err(|e| anyhow::anyhow!("Failed to register SIGTERM handler: {e}"))?,
            int: signal(SignalKind::interrupt())
                .map_err(|e| anyhow::anyhow!("Failed to register SIGINT handler: {e}"))?,
        })
    }

    async fn recv(&mut self) -> &'static str {
        tokio::select! {
            _ = self.term.recv() => "SIGTERM",
            _ = self.int.recv() => "SIGINT",
        }
    }
}

/// One HA loop iteration after the tick: the bounded cycle, then the API
/// state and the proxy member list.
async fn ha_cycle(
    ha: &mut Ha,
    current_loop_wait: &mut u64,
    interval: &mut tokio::time::Interval,
    app_state: &AppState,
    members_tx: &tokio::sync::watch::Sender<Vec<Member>>,
) {
    let result = ha.run_cycle_bounded().await;
    info!("{result}");

    // Detect dynamic loop_wait change and recreate interval
    let new_loop_wait = ha.config().loop_wait;
    if new_loop_wait != *current_loop_wait {
        info!(
            old = *current_loop_wait,
            new = new_loop_wait,
            "loop_wait changed, recreating interval"
        );
        *current_loop_wait = new_loop_wait;
        *interval = ha_interval(new_loop_wait);
    }

    // Track DCS last seen: if the cycle didn't return an error about DCS,
    // we consider DCS as reachable
    let dcs_ok = !matches!(&result, pg_ha_core::CycleResult::Error(msg) if msg.contains("DCS"));

    // Update shared API state (state: same derivation as the state published
    // to DCS)
    let snapshot = ha.api_snapshot();
    app_state
        .update(|s| {
            s.apply_ha_snapshot(&snapshot);
            s.last_loop_at = Some(Instant::now());
            if dcs_ok {
                s.dcs_last_seen = Some(Instant::now());
            }
        })
        .await;

    // Hand the members to the proxy refresher task. Never await the refresh
    // here: its DNS lookups have no timeout, and the HA loop must not await
    // anything unbounded outside run_cycle_bounded (docs/invariants.md 1a,
    // "leader 租约").
    members_tx.send_replace(ha.cluster().members.clone());
}

/// The shutdown could not stop a PostgreSQL that may be writable: keep
/// running the normal HA loop (it keeps renewing the lock while this node
/// holds it and retries the stop every cycle through `stop_pending`) until
/// PostgreSQL is stopped, then finish the shutdown. A further SIGTERM /
/// SIGINT runs the next cycle at once.
async fn keep_running_until_stopped(
    ha: &mut Ha,
    app_state: &AppState,
    members_tx: &tokio::sync::watch::Sender<Vec<Member>>,
    retry_now: &tokio::sync::Notify,
) -> ShutdownOutcome {
    // Keep-running loop
    let mut current_loop_wait = ha.config().loop_wait;
    let mut interval = ha_interval(current_loop_wait);
    // The shutdown just tried to stop PostgreSQL: wait one loop_wait.
    interval.reset();
    loop {
        tokio::select! {
            _ = interval.tick() => {
                ha_cycle(ha, &mut current_loop_wait, &mut interval, app_state, members_tx).await;
                // Awaits the lock delete only after PostgreSQL stopped.
                let outcome = ha.continue_shutdown().await;
                if outcome != ShutdownOutcome::KeepRunning {
                    return outcome;
                }
            }
            _ = retry_now.notified() => {
                interval.reset_immediately();
            }
        }
    }
}

/// HA loop timer. After a cycle that overran `loop_wait`, the next cycle
/// starts at once and the schedule restarts from there: missed ticks are not
/// replayed as a burst of back-to-back cycles.
fn ha_interval(loop_wait: u64) -> tokio::time::Interval {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(loop_wait));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval
}

/// Rebuild the proxy backends from each member list the HA loop publishes
/// (resolves Docker DNS hostnames). Runs off the HA task because the lookups
/// have no timeout. Snapshots are handled one at a time; one published while
/// a refresh runs replaces any older pending one.
async fn refresh_proxy_backends(
    mut members_rx: tokio::sync::watch::Receiver<Vec<Member>>,
    proxy: Arc<PgProxy>,
    api_port: u16,
) {
    while members_rx.changed().await.is_ok() {
        let members = members_rx.borrow_and_update().clone();
        let mut backends: Vec<PgBackend> = Vec::new();
        for m in &members {
            let Some(addr) = resolve_pg_addr(&m.conn_url).await else {
                warn!(
                    member = %m.name,
                    conn_url = %m.conn_url,
                    "Skipping proxy backend: failed to resolve conn_url"
                );
                continue;
            };
            let api_url = if m.api_url.is_empty() {
                api_url_from_conn_url(&m.conn_url, api_port)
                    .unwrap_or_else(|| format!("http://{}:{}", addr.ip(), api_port))
            } else {
                m.api_url.clone()
            };
            backends.push(PgBackend {
                addr,
                name: m.name.clone(),
                is_primary: m.role == MemberRole::Primary,
                is_healthy: m.state == MemberState::Running,
                api_url,
            });
        }
        proxy.update_backends(backends).await;
    }
}

/// Extract host/port from a libpq-style conn_url (`host=node1 port=5432 ...`).
fn parse_pg_host_port(conn_url: &str) -> Option<(String, u16)> {
    if !conn_url.contains("host=") {
        return None;
    }
    let host = conn_url
        .split_whitespace()
        .find(|s| s.starts_with("host="))
        .and_then(|s| s.strip_prefix("host="))?
        .to_string();
    let port = conn_url
        .split_whitespace()
        .find(|s| s.starts_with("port="))
        .and_then(|s| s.strip_prefix("port="))
        .and_then(|p| p.parse().ok())
        .unwrap_or(5432);
    Some((host, port))
}

/// Resolve a PostgreSQL conn_url to a SocketAddr (supports Docker DNS hostnames).
async fn resolve_pg_addr(conn_url: &str) -> Option<SocketAddr> {
    let (host, port) = parse_pg_host_port(conn_url)?;
    resolve_addr(&format!("{host}:{port}")).await
}

/// Build REST health-check base URL from conn_url host (prefer hostname over raw IP).
fn api_url_from_conn_url(conn_url: &str, api_port: u16) -> Option<String> {
    let (host, _) = parse_pg_host_port(conn_url)?;
    Some(format!("http://{host}:{api_port}"))
}

/// Resolve a host:port string to a SocketAddr (supports DNS names)
async fn resolve_addr(addr: &str) -> Option<SocketAddr> {
    // Try direct parse first
    if let Ok(sa) = addr.parse::<SocketAddr>() {
        return Some(sa);
    }
    // DNS lookup
    tokio::net::lookup_host(addr).await.ok()?.next()
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
    /// **Validates: Requirements 3.5, 3.6**
    ///
    /// Property 2: Preservation - Normal Startup Flow
    /// For all valid IP:port inputs, `resolve_addr` returns `Some(valid_addr)` matching the input.
    /// This captures the baseline behavior of the normal startup path.

    // Strategy: generate valid IPv4 addresses and ports
    fn ipv4_addr_strategy() -> impl Strategy<Value = (Ipv4Addr, u16)> {
        (
            (any::<u8>(), any::<u8>(), any::<u8>(), any::<u8>()),
            1u16..=65535u16,
        )
            .prop_map(|((a, b, c, d), port)| (Ipv4Addr::new(a, b, c, d), port))
    }

    // Strategy: generate valid IPv6 addresses and ports
    fn ipv6_addr_strategy() -> impl Strategy<Value = (Ipv6Addr, u16)> {
        (prop::array::uniform8(any::<u16>()), 1u16..=65535u16).prop_map(|(segments, port)| {
            (
                Ipv6Addr::new(
                    segments[0],
                    segments[1],
                    segments[2],
                    segments[3],
                    segments[4],
                    segments[5],
                    segments[6],
                    segments[7],
                ),
                port,
            )
        })
    }

    // Property test: all valid IPv4:port strings resolve to the correct SocketAddr
    proptest! {
        #[test]
        fn prop_resolve_addr_ipv4_returns_correct_addr((ip, port) in ipv4_addr_strategy()) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let addr_str = format!("{ip}:{port}");
            let expected = SocketAddr::new(IpAddr::V4(ip), port);

            let result = rt.block_on(resolve_addr(&addr_str));
            prop_assert_eq!(result, Some(expected));
        }

        // Property test: all valid IPv6:port strings (bracketed) resolve to the correct SocketAddr
        #[test]
        fn prop_resolve_addr_ipv6_returns_correct_addr((ip, port) in ipv6_addr_strategy()) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            // IPv6 socket addresses use bracket notation: [::1]:8080
            let addr_str = format!("[{ip}]:{port}");
            let expected = SocketAddr::new(IpAddr::V6(ip), port);

            let result = rt.block_on(resolve_addr(&addr_str));
            prop_assert_eq!(result, Some(expected));
        }
    }

    // Unit test: resolve_addr with localhost (DNS lookup path)
    #[tokio::test]
    async fn test_resolve_addr_localhost_dns_path() {
        // localhost should resolve via DNS lookup to 127.0.0.1 or ::1
        let result = resolve_addr("localhost:2380").await;
        assert!(
            result.is_some(),
            "resolve_addr(\"localhost:2380\") should resolve to Some(addr)"
        );
        let addr = result.unwrap();
        assert_eq!(addr.port(), 2380);
        // localhost should resolve to either 127.0.0.1 or ::1
        let ip = addr.ip();
        assert!(
            ip == IpAddr::V4(Ipv4Addr::LOCALHOST) || ip == IpAddr::V6(Ipv6Addr::LOCALHOST),
            "localhost should resolve to 127.0.0.1 or ::1, got {ip}"
        );
    }

    // Unit test: resolve_addr with a typical Raft self_addr (IP:port format)
    #[tokio::test]
    async fn test_resolve_addr_typical_raft_addr() {
        let result = resolve_addr("127.0.0.1:2380").await;
        assert_eq!(
            result,
            Some(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                2380
            ))
        );
    }

    // Unit test: resolve_addr with 0.0.0.0 (bind-all address, common in configs)
    #[tokio::test]
    async fn test_resolve_addr_bind_all() {
        let result = resolve_addr("0.0.0.0:2380").await;
        assert_eq!(
            result,
            Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 2380))
        );
    }

    // Unit test: verify successful resolution enables normal startup (no error path)
    #[tokio::test]
    async fn test_normal_startup_resolution_proceeds() {
        // Simulate the startup path: resolve_addr succeeds → we get an addr for binding
        let addr = resolve_addr("127.0.0.1:2380").await;
        assert!(
            addr.is_some(),
            "Normal startup: resolve_addr must return Some"
        );

        // Verify the returned addr can be used for binding (format matches SocketAddr)
        let socket_addr = addr.unwrap();
        assert_eq!(socket_addr.port(), 2380);
        assert_eq!(socket_addr.ip(), IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));

        // Verify the addr can actually be bound (proving startup proceeds)
        let listener = tokio::net::TcpListener::bind(socket_addr).await;
        assert!(
            listener.is_ok(),
            "Normal startup: should be able to bind resolved address"
        );
    }

    // ─── Generators for unresolvable hostnames ───

    /// Generate hostnames that should never resolve via DNS.
    /// Uses RFC 6761 reserved ".invalid" TLD which is guaranteed to not resolve.
    fn arb_unresolvable_hostname() -> impl Strategy<Value = String> {
        prop_oneof![
            // RFC 6761: .invalid TLD guaranteed to not resolve
            "[a-z]{3,12}".prop_map(|host| format!("{host}.invalid:2380")),
            // Nonsensical multi-level subdomains under .invalid
            ("[a-z]{2,6}", "[a-z]{2,6}")
                .prop_map(|(sub, host)| format!("{sub}.{host}.invalid:9090")),
            // Known non-existent hostnames with random ports
            (1024u16..=65535u16).prop_map(|port| format!("nonexistent.invalid:{port}")),
            // Random gibberish hostnames that won't resolve
            "[a-z0-9]{8,16}".prop_map(|host| format!("{host}.test.invalid:2380")),
        ]
    }

    // ─── Property 1: Bug Condition — DNS Resolve Panic & Raft RPC Silent Panic ───
    //
    // **Validates: Requirements 1.5, 1.6**
    //
    // For defect 5: On UNFIXED code, `resolve_addr(...).expect(...)` would panic
    // when given an unresolvable hostname. On FIXED code, `resolve_addr` returns
    // `None` and the calling code uses `match` + `anyhow::bail!` for graceful exit.
    //
    // For defect 6: On UNFIXED code, `axum::serve(...).await.unwrap()` in a
    // spawned task would panic on I/O error. On FIXED code, the pattern uses
    // `if let Err(e)` + `process::exit(1)`.
    //
    // This test verifies:
    // 1. `resolve_addr` returns `None` (not panic) for unresolvable hostnames
    // 2. The main.rs error handling pattern is correct (no .expect() on resolve_addr,
    //    no .unwrap() on axum::serve)

    proptest! {
        /// Property: For all unresolvable hostnames, `resolve_addr` returns None
        /// without panicking. On UNFIXED code, `.expect()` after resolve_addr would
        /// panic — this test confirms the bug is fixed.
        ///
        /// Counterexample on UNFIXED code: `resolve_addr("nonexistent.invalid:2380")`
        /// returns None → `.expect()` panics
        #[test]
        fn resolve_addr_returns_none_for_unresolvable_hostnames(
            addr in arb_unresolvable_hostname()
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                // This should return None, NOT panic
                let result = resolve_addr(&addr).await;

                prop_assert_eq!(
                    result, None,
                    "resolve_addr('{}') should return None for unresolvable hostname, \
                     but got {:?}. Bug condition: if .expect() were used, this would panic.",
                    addr, result
                );
                Ok(())
            })?;
        }
    }

    /// **Validates: Requirements 1.5**
    ///
    /// Specific example test: resolve_addr with a known unresolvable address.
    /// Documents the counterexample from the bug condition.
    #[tokio::test]
    async fn test_resolve_addr_unresolvable_does_not_panic() {
        // The exact counterexample from the bug description
        let result = resolve_addr("nonexistent.invalid:2380").await;
        assert_eq!(
            result, None,
            "resolve_addr('nonexistent.invalid:2380') should return None. \
             Bug condition: `.expect(...)` on None would panic the process."
        );
    }

    /// **Validates: Requirements 1.5**
    ///
    /// Verify that the main.rs code uses `match` + error handling pattern
    /// rather than `.expect()` for resolve_addr results.
    /// This is a code-path analysis test that reads the source file and
    /// confirms the dangerous pattern has been removed.
    #[test]
    fn test_resolve_addr_no_expect_pattern_in_main() {
        let source = include_str!("main.rs");

        // Only examine the production code (before #[cfg(test)])
        let prod_code = source.split("#[cfg(test)]").next().unwrap_or(source);

        // Verify NO .expect() is called on resolve_addr result in production code
        let has_resolve_expect = prod_code.lines().any(|line| {
            let trimmed = line.trim();
            // Skip comments
            if trimmed.starts_with("//") || trimmed.starts_with("///") || trimmed.starts_with("*") {
                return false;
            }
            line.contains("resolve_addr") && line.contains(".expect(")
        });

        assert!(
            !has_resolve_expect,
            "Bug condition detected: resolve_addr result is followed by .expect() in production code. \
             This would panic on DNS failure. Expected: match/if-let pattern with error handling."
        );

        // Verify the safe pattern exists (match on resolve_addr with None arm)
        let has_safe_pattern =
            prod_code.contains("match resolve_addr(") && prod_code.contains("None =>");

        assert!(
            has_safe_pattern,
            "Expected safe pattern: `match resolve_addr(...) {{ Some(addr) => ..., None => ... }}` \
             not found in main.rs production code"
        );
    }

    /// **Validates: Requirements 1.6**
    ///
    /// Verify that the Raft RPC server spawn block uses proper error handling
    /// rather than `.unwrap()` which would cause a silent task panic.
    /// This is a code-path analysis test.
    #[test]
    fn test_raft_rpc_serve_no_unwrap_pattern_in_main() {
        let source = include_str!("main.rs");

        // Only examine the production code (before #[cfg(test)])
        let prod_code = source.split("#[cfg(test)]").next().unwrap_or(source);

        // Find lines containing axum::serve and check they don't use .unwrap()
        let has_serve_unwrap = prod_code.lines().any(|line| {
            let trimmed = line.trim();
            // Skip comments
            if trimmed.starts_with("//") || trimmed.starts_with("///") || trimmed.starts_with("*") {
                return false;
            }
            line.contains("axum::serve") && line.contains(".unwrap()")
        });

        // Also check across two consecutive lines (in case .await and .unwrap() are split)
        let lines: Vec<&str> = prod_code.lines().collect();
        let has_serve_unwrap_multiline = lines.windows(2).any(|window| {
            let trimmed0 = window[0].trim();
            let trimmed1 = window[1].trim();
            // Skip comment lines
            if trimmed0.starts_with("//") || trimmed1.starts_with("//") {
                return false;
            }
            window[0].contains("axum::serve") && window[1].contains(".unwrap()")
        });

        assert!(
            !has_serve_unwrap && !has_serve_unwrap_multiline,
            "Bug condition detected: axum::serve(...).await.unwrap() found in spawned task. \
             This causes silent task panic on I/O error. \
             Expected: `if let Err(e)` pattern with error logging and process::exit(1)."
        );

        // Verify the safe error handling pattern exists for raft_listener/raft_router serve
        let has_safe_serve_pattern =
            prod_code.contains("if let Err(e) = axum::serve(raft_listener, raft_router)");

        assert!(
            has_safe_serve_pattern,
            "Expected safe pattern: `if let Err(e) = axum::serve(raft_listener, raft_router).await` \
             not found in main.rs production code. The Raft RPC server should handle errors gracefully."
        );

        // Verify process::exit is used as the recovery action
        let has_process_exit = prod_code.contains("std::process::exit(1)");
        assert!(
            has_process_exit,
            "Expected process::exit(1) for fatal Raft RPC server errors, but not found in production code."
        );
    }

    /// **Validates: Requirements 1.5**
    ///
    /// Edge case: resolve_addr with completely invalid format strings should
    /// return None without panicking.
    #[tokio::test]
    async fn test_resolve_addr_invalid_formats_no_panic() {
        let invalid_inputs = vec![
            "",                  // empty string
            ":",                 // just colon
            ":2380",             // missing host
            "no-port-here",      // no port
            "spaces in name:80", // spaces
            "very.long.nonexistent.subdomain.that.does.not.resolve.invalid:443",
        ];

        for input in invalid_inputs {
            let result = resolve_addr(input).await;
            assert_eq!(
                result, None,
                "resolve_addr('{}') should return None for invalid input without panicking",
                input
            );
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // Bug Condition Exploration: loop_wait dynamic update (Defect 1.7)
    //
    // **Property 1: Bug Condition** - loop_wait runtime update not applied
    // **Validates: Requirements 2.7**
    //
    // The interval in main.rs is created once at startup and never recreated
    // when `loop_wait` changes via PATCH /config.
    // ─────────────────────────────────────────────────────────────────────

    /// **Property 1: Bug Condition** - loop_wait dynamic update
    ///
    /// **Validates: Requirements 2.7**
    ///
    /// Verifies that the HA loop code handles dynamic `loop_wait` changes by
    /// detecting the change and recreating/resetting the interval. On unfixed
    /// code, the interval is only created once at startup.
    /// Graceful shutdown stops PostgreSQL only through the HA engine
    /// (`Ha::graceful_shutdown`): drain with renewal, CHECKPOINT + stop
    /// raced against the leader lease, lock release after the stop. main.rs
    /// only calls it and, while PostgreSQL cannot be stopped, keeps the HA
    /// loop running with `continue_shutdown`.
    #[test]
    fn test_shutdown_stops_postgres_only_through_ha() {
        let source = include_str!("main.rs");
        let prod_code = source.split("#[cfg(test)]").next().unwrap_or(source);
        let msg = "a new direct PostgreSQL stop: use fence_primary / \
                   stop_after_demotion_write, or add it to the audited table in plan \
                   section 2.2b with a reason";
        assert_eq!(prod_code.matches(".stop(").count(), 0, "{msg}");
        assert!(prod_code.contains("ha.graceful_shutdown(drain).await"));
        assert!(prod_code.contains("ha.continue_shutdown().await"));
        for inside_core in [
            "drain_renewing_leader_lock(",
            "stop_postgres_for_shutdown(",
            "release_leader_lock_if_stopped(",
            ".checkpoint(",
        ] {
            assert!(
                !prod_code.contains(inside_core),
                "{inside_core} belongs inside Ha::graceful_shutdown"
            );
        }
        // Only KeepRunning stays in the process; every other outcome exits.
        let after = prod_code
            .split("let mut outcome = ha.graceful_shutdown(drain).await;")
            .nth(1)
            .expect("graceful_shutdown call");
        assert_before_text(
            after,
            "if outcome == ShutdownOutcome::KeepRunning {",
            "keep_running_until_stopped(",
        );
    }

    fn assert_before_text(text: &str, first: &str, second: &str) {
        let a = text.find(first).unwrap_or_else(|| panic!("no `{first}`"));
        let b = text.find(second).unwrap_or_else(|| panic!("no `{second}`"));
        assert!(a < b, "`{first}` must come before `{second}`");
    }

    fn awaits_in(text: &str) -> Vec<&str> {
        text.lines()
            .map(str::trim)
            .filter(|l| l.contains(".await"))
            .collect()
    }

    /// Outside `run_cycle_bounded` the HA loop awaits only the tick and the
    /// API state write lock (handlers hold it only for local reads); nothing
    /// there races the leader lease (docs/invariants.md 1a). Anything else,
    /// such as the proxy refresh's DNS lookups, belongs on another task. The
    /// keep-running loop of a stuck shutdown adds only `continue_shutdown`,
    /// which awaits the lock delete only after PostgreSQL stopped.
    #[test]
    fn test_ha_loop_awaits_nothing_unbounded_outside_the_bounded_cycle() {
        let source = include_str!("main.rs");
        let prod_code = source.split("#[cfg(test)]").next().unwrap_or(source);
        let msg = "a new await in the HA loop is not raced against the leader lease: \
                   move it into run_cycle_bounded or onto another task";
        let ha_loop = prod_code
            .split("// HA Loop")
            .nth(1)
            .and_then(|s| s.split("// TCP Proxy").next())
            .expect("HA loop arm not found");
        assert_eq!(
            awaits_in(ha_loop),
            vec![
                "interval.tick().await;",
                "ha_cycle(&mut ha, &mut current_loop_wait, &mut interval, &app_state_clone, &members_tx).await;",
            ],
            "{msg}"
        );
        let cycle = prod_code
            .split("async fn ha_cycle(")
            .nth(1)
            .and_then(|s| s.split("\n}\n").next())
            .expect("ha_cycle not found");
        assert_eq!(
            awaits_in(cycle),
            vec!["let result = ha.run_cycle_bounded().await;", ".await;"],
            "{msg}"
        );
        // The only other await: the API state write lock.
        assert!(cycle.contains("app_state\n        .update(|s| {"));
        let keep_running = prod_code
            .split("// Keep-running loop")
            .nth(1)
            .and_then(|s| s.split("\n}\n").next())
            .expect("keep-running loop not found");
        assert_eq!(
            awaits_in(keep_running),
            vec![
                "ha_cycle(ha, &mut current_loop_wait, &mut interval, app_state, members_tx).await;",
                "let outcome = ha.continue_shutdown().await;",
            ],
            "{msg}"
        );
        assert!(keep_running.contains("_ = interval.tick() => {"));
        assert!(keep_running.contains("_ = retry_now.notified() => {"));
    }

    /// The proxy refresher applies the published member list and ends when the
    /// HA loop drops the sender.
    #[tokio::test]
    async fn test_refresh_proxy_backends_follows_published_members() {
        let proxy = Arc::new(PgProxy::new(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:0".parse().unwrap(),
        ));
        let (tx, rx) = tokio::sync::watch::channel(Vec::<Member>::new());
        let task = tokio::spawn(refresh_proxy_backends(rx, proxy.clone(), 8008));
        tx.send_replace(vec![Member {
            name: "node1".to_string(),
            conn_url: "host=10.0.0.1 port=5433".to_string(),
            api_url: String::new(),
            state: MemberState::Running,
            role: MemberRole::Primary,
            wal_position: None,
            timeline: None,
            tags: Default::default(),
            version: None,
            rewind_check: false,
            lineage: None,
        }]);
        drop(tx);
        tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .expect("refresher did not end after the sender was dropped")
            .unwrap();
        let upstream = proxy.upstream_handle();
        let state = upstream.read().await;
        let primary = state.primary().expect("primary backend registered");
        assert_eq!(primary.name, "node1");
        assert_eq!(primary.addr, "10.0.0.1:5433".parse::<SocketAddr>().unwrap());
        assert_eq!(primary.api_url, "http://10.0.0.1:8008");
    }

    #[test]
    fn test_bug_condition_loop_wait_dynamic_update() {
        let source = include_str!("main.rs");

        // Only examine production code
        let prod_code = source.split("#[cfg(test)]").next().unwrap_or(source);

        // The fix should include logic that detects loop_wait changes and
        // recreates/resets the interval. Look for telltale patterns:
        let has_dynamic_interval = prod_code.contains("current_loop_wait")
            || prod_code.contains("loop_wait_changed")
            || (prod_code.contains("ha.config().loop_wait")
                && prod_code.contains("interval = tokio::time::interval"));

        assert!(
            has_dynamic_interval,
            "BUG DETECTED: HA loop interval is created once at startup and never updated.\n\
             When `loop_wait` is changed via PATCH /config, the interval keeps the old value.\n\
             Fix: after each run_cycle(), compare ha.config().loop_wait with current interval \
             period and recreate interval if changed."
        );
    }
}
