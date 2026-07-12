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
use pg_ha_core::cluster::{MemberRole, MemberState};
use pg_ha_core::config::Config;
use pg_ha_core::dcs::DcsAdapter;
use pg_ha_core::ha::Ha;
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
    // Initialize tracing with optional JSON output
    let env_filter = EnvFilter::from_default_env()
        .add_directive("pg_ha=info".parse()?)
        .add_directive("openraft::replication=off".parse()?);

    let log_format = std::env::var("PG_HA_LOG_FORMAT").unwrap_or_default();
    if log_format.eq_ignore_ascii_case("json") {
        // JSON structured logging
        tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer().json())
            .init();
    } else {
        // Default human-readable output
        tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer())
            .init();
    }

    let cli = Cli::parse();

    // Handle --generate-sample-config
    if cli.generate_sample_config {
        print!("{}", Config::sample());
        return Ok(());
    }

    // Load configuration
    let mut config = Config::from_file(&cli.configfile)?;
    config.apply_env_overrides();

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
        config.raft.data_dir.clone(),
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

    // Start Raft RPC server in background immediately
    let raft_listener = tokio::net::TcpListener::bind(raft_addr).await?;
    info!(%raft_addr, "Raft RPC listening");
    tokio::spawn(async move {
        if let Err(e) = axum::serve(raft_listener, raft_router).await {
            error!("Raft RPC server fatal error: {e}");
            std::process::exit(1);
        }
    });

    // ─── Bootstrap Raft cluster ───
    // Build the full member list: (node_id, addr)
    let members: Vec<(u64, String)> = all_addrs
        .iter()
        .enumerate()
        .map(|(i, addr)| ((i + 1) as u64, addr.clone()))
        .collect();

    // Only the node with the lowest ID attempts bootstrap.
    // Others will receive the membership via Raft RPC from the bootstrapper.
    if node_id == 1 {
        // Small delay to let other nodes' RPC servers start
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        match dcs.bootstrap_cluster(&members).await {
            Ok(()) => info!("Raft cluster bootstrapped with {} members", members.len()),
            Err(e) => {
                // Not fatal — cluster may already be initialized
                tracing::debug!("Bootstrap attempt: {e} (cluster may already exist)");
            }
        }
    }

    // Wait for Raft to elect a leader (all nodes wait)
    info!("Waiting for Raft leader election...");
    match dcs.wait_for_leader(30).await {
        Ok(()) => info!("Raft cluster ready"),
        Err(e) => {
            error!("Raft cluster not ready after 30s: {e}");
            // Continue anyway — HA loop will handle DCS errors
        }
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

    let proxy_clone = proxy.clone();
    let app_state_clone = app_state.clone();

    tokio::select! {
        // HA Loop
        _ = async {
            let mut interval = tokio::time::interval(
                std::time::Duration::from_secs(config.loop_wait)
            );
            loop {
                interval.tick().await;
                let result = ha.run_cycle().await;
                info!("{result}");

                // Track DCS last seen: if the cycle didn't return an error about DCS,
                // we consider DCS as reachable
                let dcs_ok = !matches!(&result, pg_ha_core::CycleResult::Error(msg) if msg.contains("DCS"));

                // Update shared API state
                app_state_clone.update(|s| {
                    s.role = ha.postgresql().role().clone();
                    s.state = if ha.postgresql().is_running() {
                        MemberState::Running
                    } else {
                        MemberState::Stopped
                    };
                    s.is_leader = ha.is_leader();
                    s.is_paused = ha.is_paused();
                    s.pending_restart = ha.pending_restart();
                    s.last_loop_at = Some(Instant::now());
                    if dcs_ok {
                        s.dcs_last_seen = Some(Instant::now());
                    }
                }).await;

                // Update proxy backends from cluster state (resolve Docker DNS hostnames)
                let mut backends: Vec<PgBackend> = Vec::new();
                for m in &ha.cluster().members {
                    let Some(addr) = resolve_pg_addr(&m.conn_url).await else {
                        warn!(
                            member = %m.name,
                            conn_url = %m.conn_url,
                            "Skipping proxy backend: failed to resolve conn_url"
                        );
                        continue;
                    };
                    let api_url = if m.api_url.is_empty() {
                        api_url_from_conn_url(&m.conn_url, config.restapi.port).unwrap_or_else(|| {
                            format!("http://{}:{}", addr.ip(), config.restapi.port)
                        })
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
                proxy_clone.update_backends(backends).await;
            }
        } => {}

        // REST API server
        _ = async {
            let listener = match tokio::net::TcpListener::bind(api_addr).await {
                Ok(l) => l,
                Err(e) => {
                    error!(%api_addr, "Failed to bind REST API: {e}");
                    std::process::exit(1);
                }
            };
            info!(%api_addr, "REST API listening");
            if let Err(e) = axum::serve(listener, api_router).await {
                error!("REST API server fatal error: {e}");
                std::process::exit(1);
            }
        } => {}

        // TCP Proxy
        _ = proxy.run() => {}

        // Signal handling (SIGINT and SIGTERM)
        _ = async {
            let mut sigterm = tokio::signal::unix::signal(
                tokio::signal::unix::SignalKind::terminate()
            ).expect("Failed to register SIGTERM handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    info!("Received SIGINT");
                }
                _ = sigterm.recv() => {
                    info!("Received SIGTERM");
                }
            }
        } => {}
    }

    // ─── Graceful Shutdown Sequence ───
    info!("Shutting down gracefully...");

    if ha.is_leader() {
        // Leader shutdown: checkpoint → release lock → stop PG
        info!("Leader shutdown: running CHECKPOINT");
        if let Err(e) = ha.postgresql().checkpoint().await {
            error!("CHECKPOINT during shutdown failed: {e}");
        }

        // Release the leader lock in DCS
        if let Some(leader) = ha.cluster().leader.as_ref() {
            info!("Releasing leader lock in DCS");
            if let Err(e) = dcs.delete_leader(leader).await {
                error!("Failed to release leader lock: {e}");
            }
        }
    }

    // Stop PostgreSQL with "fast" mode (disconnects clients, no new connections)
    info!("Stopping PostgreSQL (fast mode)");
    if let Err(e) = ha.postgresql_mut().stop("fast").await {
        error!("Failed to stop PostgreSQL during shutdown: {e}");
    }

    info!("Shutdown complete");
    Ok(())
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
}
