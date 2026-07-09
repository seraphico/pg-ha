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
use tracing::{error, info};
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
        all_addrs.iter().position(|a| a == &config.raft.self_addr).unwrap_or(0) as u64 + 1
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
    let raft_addr: SocketAddr = resolve_addr(&config.raft.self_addr)
        .await
        .expect("Cannot resolve Raft RPC address");

    // Start Raft RPC server in background immediately
    let raft_listener = tokio::net::TcpListener::bind(raft_addr).await?;
    info!(%raft_addr, "Raft RPC listening");
    tokio::spawn(async move {
        axum::serve(raft_listener, raft_router).await.unwrap();
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
    let app_state = AppState::with_dcs(config.name.clone(), config.scope.clone(), config.ttl, dcs.clone());
    let auth_config = AuthConfig {
        username: config.restapi.username.clone(),
        password: config.restapi.password.clone(),
    };
    let api_router = pg_ha_api::build_router_with_commands(app_state.clone(), Some(cmd_tx), auth_config);
    let api_addr: SocketAddr = format!("{}:{}", config.restapi.listen, config.restapi.port)
        .parse()
        .expect("Invalid REST API address");

    // ─── Initialize TCP Proxy ───
    let rw_addr: SocketAddr = format!("{}:{}", config.proxy.rw_listen, config.proxy.rw_port)
        .parse()
        .expect("Invalid proxy RW address");
    let ro_addr: SocketAddr = format!("{}:{}", config.proxy.ro_listen, config.proxy.ro_port)
        .parse()
        .expect("Invalid proxy RO address");
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

                // Update proxy backends from cluster state
                let backends: Vec<PgBackend> = ha.cluster().members.iter().map(|m| {
                    // Parse addr from conn_url (simplified)
                    let addr = parse_pg_addr(&m.conn_url).unwrap_or_else(|| "127.0.0.1:5432".parse().unwrap());
                    PgBackend {
                        addr,
                        name: m.name.clone(),
                        is_primary: m.role == MemberRole::Primary,
                        is_healthy: m.state == MemberState::Running,
                    }
                }).collect();
                proxy_clone.update_backends(backends).await;
            }
        } => {}

        // REST API server
        _ = async {
            let listener = tokio::net::TcpListener::bind(api_addr).await.unwrap();
            info!(%api_addr, "REST API listening");
            axum::serve(listener, api_router).await.unwrap();
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

/// Parse a PostgreSQL connection URL to extract host:port as SocketAddr
fn parse_pg_addr(conn_url: &str) -> Option<SocketAddr> {
    if conn_url.contains("host=") {
        let host = conn_url
            .split_whitespace()
            .find(|s| s.starts_with("host="))
            .and_then(|s| s.strip_prefix("host="))?;
        let port = conn_url
            .split_whitespace()
            .find(|s| s.starts_with("port="))
            .and_then(|s| s.strip_prefix("port="))
            .unwrap_or("5432");
        format!("{host}:{port}").parse().ok()
    } else {
        None
    }
}

/// Resolve a host:port string to a SocketAddr (supports DNS names)
async fn resolve_addr(addr: &str) -> Option<SocketAddr> {
    // Try direct parse first
    if let Ok(sa) = addr.parse::<SocketAddr>() {
        return Some(sa);
    }
    // DNS lookup
    tokio::net::lookup_host(addr)
        .await
        .ok()?
        .next()
}
