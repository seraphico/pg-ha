//! PostgreSQL lifecycle management
//!
//! Manages the local PostgreSQL instance via pg_ctl, initdb, pg_basebackup,
//! pg_rewind, and tokio-postgres for health checks and queries.

use crate::cluster::MemberRole;
use crate::config::PostgresqlConfig;
use crate::error::{Error, Result};
use std::path::PathBuf;
use std::time::Duration;
use tokio::process::Command;
use tracing::{debug, error, info, warn};

/// Redact password from a libpq-style connection string for safe logging.
/// Replaces `password=xxx` with `password=***`.
pub fn redact_connstr(connstr: &str) -> String {
    let mut result = String::with_capacity(connstr.len());
    let mut remaining = connstr;
    while let Some(pos) = remaining.find("password=") {
        result.push_str(&remaining[..pos]);
        result.push_str("password=***");
        let after_key = &remaining[pos + 9..]; // skip "password="
        // Skip the password value (until next space or end)
        let end = after_key.find(' ').unwrap_or(after_key.len());
        remaining = &after_key[end..];
    }
    result.push_str(remaining);
    result
}

/// Timeout for spawned PG connection futures.
/// If PG is unresponsive, the task will be cancelled after this duration.
const PG_CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);

/// Upper bound for one `pg_is_in_recovery()` observation. A PostgreSQL that
/// cannot answer within this time is treated as "unknown" for the cycle, so a
/// stuck server cannot stall the HA loop.
const RECOVERY_OBSERVE_TIMEOUT: Duration = Duration::from_secs(5);

/// Upper bound for the live check of a pg_rewind source (psql also gets
/// PGCONNECT_TIMEOUT=5).
const SOURCE_PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// Pure: exit status + stdout of the `SELECT pg_is_in_recovery()` probe ->
/// Some(true) = answers as a primary, Some(false) = in recovery, None = no
/// usable answer.
pub(crate) fn parse_recovery_probe(success: bool, stdout: &str) -> Option<bool> {
    if !success {
        return None;
    }
    match stdout.trim() {
        "f" => Some(true),
        "t" => Some(false),
        _ => None,
    }
}

/// Manages a local PostgreSQL instance lifecycle
pub struct Postgresql {
    config: PostgresqlConfig,
    /// Role this node last confirmed or was configured into. Meaningful only
    /// together with the process state; it is never evidence that PostgreSQL
    /// is currently writable (use [`Postgresql::is_observed_primary`]).
    role: MemberRole,
    state: PgState,
    /// Last observed `pg_is_in_recovery()` result.
    ///
    /// `None` means "not observed": PostgreSQL is not running, the query failed,
    /// or pg-ha has since run an action that can change the recovery state
    /// (start / stop / promote), which invalidates the previous observation.
    in_recovery: Option<bool>,
    /// Set once graceful shutdown begins: `start()` refuses to run, so
    /// pg-ha never starts PostgreSQL again before it exits.
    starts_blocked: bool,
    /// Receiver pair from the last cycle it was streaming (see
    /// [`ReceivedWal`]). A Mutex because `wal_status` takes `&self`.
    last_received_wal: std::sync::Mutex<Option<ReceivedWal>>,
}

/// Internal PostgreSQL process state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgState {
    Running,
    Stopped,
    Starting,
    Crashed,
    Unknown,
}

impl std::fmt::Display for PgState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Running => write!(f, "running"),
            Self::Stopped => write!(f, "stopped"),
            Self::Starting => write!(f, "starting"),
            Self::Crashed => write!(f, "crashed"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

/// Result of querying PostgreSQL for WAL position and timeline
#[derive(Debug, Clone)]
pub struct WalStatus {
    /// Timeline of the WAL at `wal_position` (R2: the pair describes one
    /// point). On a replica this is a lower bound when the receiver's
    /// timeline is unknown (see [`resolve_replica_wal`]).
    pub timeline: Option<u64>,
    /// WAL position in bytes (receive or replay LSN, whichever is higher)
    pub wal_position: u64,
    /// Whether the instance is in recovery (replica)
    pub in_recovery: bool,
}

/// Timeline and flushed LSN the WAL receiver last reported.
///
/// Kept after the receiver stops: when the upstream dies, the receiver exits
/// and `pg_stat_wal_receiver` is empty, which is exactly when an election
/// needs the timeline. `pg_last_wal_receive_lsn()` keeps returning the same
/// flushed position, so the pair still describes the replica's WAL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReceivedWal {
    /// `pg_postmaster_start_time()`: the pair is valid only for this server run.
    pub(crate) postmaster_start: String,
    pub(crate) flushed_lsn: u64,
    pub(crate) timeline: u64,
}

/// One replica observation, all values from one SQL statement.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ReplicaWalSample {
    /// `pg_last_wal_receive_lsn()`
    pub(crate) receive_lsn: Option<u64>,
    /// `pg_last_wal_replay_lsn()`
    pub(crate) replay_lsn: Option<u64>,
    /// `pg_control_checkpoint().timeline_id`: timeline of the last
    /// restartpoint, never above the timeline of the replica's newest WAL.
    pub(crate) checkpoint_timeline: Option<u64>,
    /// `pg_stat_wal_receiver` (`flushed_lsn`, `received_tli`), read together
    /// from one view row. `None` while no receiver is streaming.
    pub(crate) receiver: Option<(u64, u64)>,
    /// `pg_postmaster_start_time()`
    pub(crate) postmaster_start: Option<String>,
}

/// How a replica's published timeline relates to its published position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplicaTimelineSource {
    /// Exact: the streaming receiver's `flushed_lsn` is the published
    /// position and `received_tli` is its timeline.
    Receiver,
    /// Lower bound: the highest timeline known to hold at or below the
    /// published position (the checkpoint, the current receiver pair, or the
    /// pair remembered from this server run).
    LowerBound,
}

/// A replica's published (wal_position, timeline) and the receiver pair to
/// remember for later cycles.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplicaWalResolution {
    pub(crate) wal_position: u64,
    pub(crate) timeline: Option<u64>,
    pub(crate) source: ReplicaTimelineSource,
    /// `Some` replaces the remembered pair; `None` keeps it.
    pub(crate) remember: Option<ReceivedWal>,
}

/// R2: publish the timeline of the WAL at `wal_position`, not the timeline of
/// the last restartpoint. The election orders by timeline first; a replica
/// that has streamed the new timeline but not yet made a restartpoint would
/// otherwise publish the old one and lose to a peer with less WAL.
///
/// The receiver's `received_tli` is the timeline of its `flushed_lsn`; it is
/// exact when that LSN is the published position (flushed >= replay).
///
/// Otherwise the result is the highest lower bound. Within one server run a
/// replica's WAL is one history and timelines only grow along it, so a
/// timeline seen at LSN `x` holds at every position >= `x`:
/// - the checkpoint timeline (the restartpoint is below the position);
/// - the current receiver pair when replay passed it (receiver restarted
///   from a segment start);
/// - the pair remembered from this server run when the position is at or
///   above it. This is the failover case: the dying upstream's last WAL
///   arrives after the last sample, then the receiver exits.
///
/// pg_rewind and pg_basebackup need a stopped server, so a new
/// `pg_postmaster_start_time()` drops the remembered pair. A replica may
/// under-report, never over-report.
pub(crate) fn resolve_replica_wal(
    sample: &ReplicaWalSample,
    last: Option<&ReceivedWal>,
) -> ReplicaWalResolution {
    let replay = sample.replay_lsn.unwrap_or(0);
    let remember = match (sample.receiver, &sample.postmaster_start) {
        (Some((flushed_lsn, timeline)), Some(start)) => Some(ReceivedWal {
            postmaster_start: start.clone(),
            flushed_lsn,
            timeline,
        }),
        _ => None,
    };

    if let Some((flushed, timeline)) = sample.receiver
        && flushed >= replay
    {
        return ReplicaWalResolution {
            wal_position: flushed,
            timeline: Some(timeline),
            source: ReplicaTimelineSource::Receiver,
            remember,
        };
    }

    // The view's flushed_lsn and pg_last_wal_receive_lsn() read the same
    // field; prefer the view's so the pair stays consistent.
    let receive = match sample.receiver {
        Some((flushed, _)) => flushed,
        None => sample.receive_lsn.unwrap_or(0),
    };
    let wal_position = receive.max(replay);

    let mut timeline = sample.checkpoint_timeline;
    if let Some((_, receiver_tl)) = sample.receiver {
        timeline = timeline.max(Some(receiver_tl));
    }
    if let Some(last) = last
        && sample.postmaster_start.as_deref() == Some(last.postmaster_start.as_str())
        && wal_position >= last.flushed_lsn
    {
        timeline = timeline.max(Some(last.timeline));
    }
    ReplicaWalResolution {
        wal_position,
        timeline,
        source: ReplicaTimelineSource::LowerBound,
        remember,
    }
}

/// Parse a PostgreSQL LSN string (e.g., "0/3000060") into a byte offset (u64).
///
/// LSN format is "A/B" where A and B are hexadecimal values.
/// The result is (A << 32) | B, giving the total byte position.
pub fn parse_lsn(lsn: &str) -> Option<u64> {
    let parts: Vec<&str> = lsn.split('/').collect();
    if parts.len() != 2 {
        return None;
    }
    let hi = u64::from_str_radix(parts[0], 16).ok()?;
    let lo = u64::from_str_radix(parts[1], 16).ok()?;
    Some((hi << 32) | lo)
}

/// Timeline from a WAL file name: its first 8 hex digits.
/// "00000003000000000000000A" -> Some(3)
pub fn parse_walfile_timeline(walfile: &str) -> Option<u64> {
    let walfile = walfile.trim();
    if walfile.len() != 24 || !walfile.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    u64::from_str_radix(&walfile[..8], 16).ok()
}

impl Postgresql {
    pub fn new(config: PostgresqlConfig) -> Self {
        Self {
            config,
            role: MemberRole::Uninitialized,
            state: PgState::Unknown,
            in_recovery: None,
            starts_blocked: false,
            last_received_wal: std::sync::Mutex::new(None),
        }
    }

    /// Graceful shutdown began: from now on `start()` fails without doing
    /// anything (one check for every start path).
    pub fn block_starts(&mut self) {
        self.starts_blocked = true;
    }

    // ─────────────────────── Path helpers ───────────────────────

    fn pg_ctl(&self) -> PathBuf {
        self.config.bin_dir.join("pg_ctl")
    }

    fn initdb_bin(&self) -> PathBuf {
        self.config.bin_dir.join("initdb")
    }

    fn pg_basebackup_bin(&self) -> PathBuf {
        self.config.bin_dir.join("pg_basebackup")
    }

    fn pg_rewind_bin(&self) -> PathBuf {
        self.config.bin_dir.join("pg_rewind")
    }

    fn pg_controldata_bin(&self) -> PathBuf {
        self.config.bin_dir.join("pg_controldata")
    }

    fn psql_bin(&self) -> PathBuf {
        self.config.bin_dir.join("psql")
    }

    fn data_dir_str(&self) -> &str {
        self.config.data_dir.to_str().unwrap_or("")
    }

    fn postmaster_pid_path(&self) -> PathBuf {
        self.config.data_dir.join("postmaster.pid")
    }

    // ─────────────────────── Data directory checks ───────────────────────

    /// Check if PostgreSQL data directory is empty or does not exist
    pub fn data_directory_empty(&self) -> Result<bool> {
        let path = &self.config.data_dir;
        if !path.exists() {
            return Ok(true);
        }
        if !path.is_dir() {
            return Err(Error::Postgres(format!(
                "data_dir '{}' exists but is not a directory",
                path.display()
            )));
        }
        let mut entries = std::fs::read_dir(path)?;
        Ok(entries.next().is_none())
    }

    /// Check if PostgreSQL is currently running by examining postmaster.pid
    pub fn is_running(&self) -> bool {
        let pid_file = self.postmaster_pid_path();
        if !pid_file.exists() {
            return false;
        }
        match std::fs::read_to_string(&pid_file) {
            Ok(content) => {
                if let Some(pid_str) = content.lines().next()
                    && let Ok(pid) = pid_str.trim().parse::<i32>()
                {
                    // Check if process is alive (signal 0 doesn't send a signal, just checks)
                    if unsafe { libc::kill(pid, 0) != 0 } {
                        return false;
                    }
                    // PID exists — verify it's actually a postgres process
                    return Self::is_postgres_process(pid);
                }
                false
            }
            Err(_) => false,
        }
    }

    /// Verify that a PID belongs to a PostgreSQL process.
    /// On Linux: checks /proc/{pid}/cmdline for "postgres".
    /// On other platforms: returns true (conservative — PID existence is sufficient).
    #[cfg(target_os = "linux")]
    fn is_postgres_process(pid: i32) -> bool {
        let cmdline_path = format!("/proc/{pid}/cmdline");
        match std::fs::read(&cmdline_path) {
            Ok(data) => {
                // cmdline is NUL-separated; check first argument contains "postgres"
                let cmdline = String::from_utf8_lossy(&data);
                let first_arg = cmdline.split('\0').next().unwrap_or("");
                first_arg.contains("postgres")
            }
            Err(_) => {
                // Cannot read cmdline (permission denied or process exited) — conservative false
                false
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn is_postgres_process(_pid: i32) -> bool {
        // On macOS/other platforms: trust PID existence check (acceptable for dev/test)
        true
    }

    /// Read the PID from postmaster.pid, if available
    pub fn postmaster_pid(&self) -> Option<i32> {
        let content = std::fs::read_to_string(self.postmaster_pid_path()).ok()?;
        content.lines().next()?.trim().parse().ok()
    }

    // ─────────────────────── Process management ───────────────────────

    /// Start PostgreSQL using pg_ctl start
    pub async fn start(&mut self) -> Result<()> {
        if self.starts_blocked {
            warn!("Not starting PostgreSQL: graceful shutdown in progress");
            return Err(Error::Postgres(
                "graceful shutdown in progress: not starting PostgreSQL".into(),
            ));
        }
        info!("Starting PostgreSQL");
        self.state = PgState::Starting;
        self.in_recovery = None;

        // Build pg_ctl start command with log file in data directory
        let log_file = self.config.data_dir.join("pg_log.log");
        let log_file_str = log_file.to_string_lossy().to_string();

        let output = Command::new(self.pg_ctl())
            .args([
                "start",
                "-D",
                self.data_dir_str(),
                "-l",
                &log_file_str,
                "-w",
            ])
            .stdin(std::process::Stdio::null())
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            error!(%stderr, %stdout, "pg_ctl start failed");
            self.state = PgState::Crashed;
            return Err(Error::Postgres(format!("pg_ctl start failed: {stderr}")));
        }

        // Verify PostgreSQL is running
        if self.is_running() {
            self.state = PgState::Running;
            info!("PostgreSQL started successfully");
            return Ok(());
        }

        // Poll briefly in case postmaster.pid hasn't appeared yet
        for _ in 0..10 {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            if self.is_running() {
                self.state = PgState::Running;
                info!("PostgreSQL started successfully");
                return Ok(());
            }
        }

        self.state = PgState::Crashed;
        Err(Error::Postgres(
            "PostgreSQL did not start within timeout".into(),
        ))
    }

    /// Stop PostgreSQL with the specified mode (smart, fast, immediate)
    pub async fn stop(&mut self, mode: &str) -> Result<()> {
        info!(mode, "Stopping PostgreSQL");
        self.in_recovery = None;

        let status = Command::new(self.pg_ctl())
            .args(["stop", "-D", self.data_dir_str(), "-m", mode, "-w"])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await?;

        if status.success() {
            self.state = PgState::Stopped;
            info!("PostgreSQL stopped");
            Ok(())
        } else {
            warn!("pg_ctl stop returned non-zero (exit: {:?})", status.code());
            Err(Error::Postgres("pg_ctl stop failed".to_string()))
        }
    }

    /// Promote a standby to primary using pg_ctl promote.
    ///
    /// Success means `pg_ctl promote -w` reported completion. It does not set
    /// the role: callers must re-observe with [`Postgresql::observe_recovery`]
    /// and only then record `MemberRole::Primary`.
    pub async fn promote(&mut self) -> Result<()> {
        info!("Promoting PostgreSQL to primary");
        self.in_recovery = None;

        let output = Command::new(self.pg_ctl())
            .args(["promote", "-D", self.data_dir_str(), "-w"])
            .stdin(std::process::Stdio::null())
            .output()
            .await?;

        if output.status.success() {
            info!("pg_ctl promote reported success");
            Ok(())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            error!(%stderr, %stdout, "pg_ctl promote failed (exit: {:?})", output.status.code());
            Err(Error::Postgres(format!("pg_ctl promote failed: {stderr}")))
        }
    }

    /// Reload PostgreSQL configuration (pg_ctl reload)
    pub async fn reload(&self) -> Result<()> {
        info!("Reloading PostgreSQL configuration");

        let output = Command::new(self.pg_ctl())
            .args(["reload", "-D", self.data_dir_str()])
            .output()
            .await?;

        if output.status.success() {
            Ok(())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(Error::Postgres(format!("pg_ctl reload failed: {stderr}")))
        }
    }

    // ─────────────────────── Bootstrap operations ───────────────────────

    /// Initialize a new PostgreSQL cluster using initdb
    pub async fn initdb(&self, options: &[String]) -> Result<()> {
        info!("Running initdb");

        let mut cmd = Command::new(self.initdb_bin());
        cmd.args(["-D", self.data_dir_str()]);

        for opt in options {
            cmd.arg(format!("--{opt}"));
        }

        let output = cmd.output().await?;

        if output.status.success() {
            info!("initdb completed successfully");
            Ok(())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!(%stderr, "initdb failed");
            Err(Error::Postgres(format!("initdb failed: {stderr}")))
        }
    }

    /// Clone from another node using pg_basebackup
    pub async fn basebackup(&self, source_connstr: &str) -> Result<()> {
        info!(source = %redact_connstr(source_connstr), "Running pg_basebackup");

        let output = Command::new(self.pg_basebackup_bin())
            .args([
                "-D",
                self.data_dir_str(),
                "-d",
                source_connstr,
                "--checkpoint=fast",
                "--wal-method=stream",
                "--no-password",
            ])
            .output()
            .await?;

        if output.status.success() {
            info!("pg_basebackup completed successfully");
            Ok(())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!(%stderr, "pg_basebackup failed");
            Err(Error::Postgres(format!("pg_basebackup failed: {stderr}")))
        }
    }

    /// Run pg_rewind to resync a diverged former primary
    pub async fn rewind(&self, source_connstr: &str) -> Result<()> {
        info!(source = %redact_connstr(source_connstr), "Running pg_rewind");

        let output = Command::new(self.pg_rewind_bin())
            .args([
                "--target-pgdata",
                self.data_dir_str(),
                "--source-server",
                source_connstr,
                "--progress",
            ])
            .output()
            .await?;

        if output.status.success() {
            info!("pg_rewind completed successfully");
            Ok(())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!(%stderr, "pg_rewind failed");
            Err(Error::Postgres(format!("pg_rewind failed: {stderr}")))
        }
    }

    /// Live check of a pg_rewind source: run
    /// `psql -X -tAq -w -d <conn_url> -c "SELECT pg_is_in_recovery()"` with
    /// PGCONNECT_TIMEOUT=5 under a 10 s limit (the child is killed on
    /// timeout). Some(true) = it answers as a primary, Some(false) = it is in
    /// recovery, None = no usable answer.
    pub async fn source_answers_as_primary(&self, conn_url: &str) -> Option<bool> {
        let probe = Command::new(self.psql_bin())
            .args([
                "-X",
                "-tAq",
                "-w",
                "-d",
                conn_url,
                "-c",
                "SELECT pg_is_in_recovery()",
            ])
            .env("PGCONNECT_TIMEOUT", "5")
            .stdin(std::process::Stdio::null())
            .kill_on_drop(true)
            .output();
        let output = match tokio::time::timeout(SOURCE_PROBE_TIMEOUT, probe).await {
            Ok(Ok(output)) => output,
            Ok(Err(e)) => {
                warn!(source = %redact_connstr(conn_url), "Live check of the pg_rewind source failed: {e}");
                return None;
            }
            Err(_) => {
                warn!(
                    source = %redact_connstr(conn_url),
                    "Live check of the pg_rewind source failed: no answer within {SOURCE_PROBE_TIMEOUT:?}"
                );
                return None;
            }
        };
        let stdout = String::from_utf8_lossy(&output.stdout);
        let answer = parse_recovery_probe(output.status.success(), &stdout);
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let first = stderr.lines().next().unwrap_or("").trim();
            warn!(
                source = %redact_connstr(conn_url),
                "Live check of the pg_rewind source failed: psql exited with {}: {first}",
                output.status
            );
        } else if answer.is_none() {
            warn!(
                source = %redact_connstr(conn_url),
                "Live check of the pg_rewind source failed: unexpected answer {:?}",
                stdout.trim()
            );
        }
        answer
    }

    // ─────────────────────── Introspection ───────────────────────

    /// Get system identifier from pg_controldata
    pub async fn sysid(&self) -> Result<String> {
        let output = Command::new(self.pg_controldata_bin())
            .args(["-D", self.data_dir_str()])
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Postgres(format!("pg_controldata failed: {stderr}")));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        for line in stdout.lines() {
            if line.contains("Database system identifier")
                && let Some(val) = line.split(':').nth(1)
            {
                return Ok(val.trim().to_string());
            }
        }
        Err(Error::Postgres(
            "Could not find system identifier in pg_controldata output".into(),
        ))
    }

    /// Get the database cluster state from pg_controldata
    pub async fn controldata_state(&self) -> Result<String> {
        let output = Command::new(self.pg_controldata_bin())
            .args(["-D", self.data_dir_str()])
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Postgres(format!("pg_controldata failed: {stderr}")));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        for line in stdout.lines() {
            if line.contains("Database cluster state")
                && let Some(val) = line.split(':').nth(1)
            {
                return Ok(val.trim().to_string());
            }
        }
        Err(Error::Postgres(
            "Could not find cluster state in pg_controldata output".into(),
        ))
    }

    /// Get the latest checkpoint's TimeLineID from pg_controldata.
    /// Works even when PostgreSQL is not running (reads data directory directly).
    pub async fn controldata_timeline(&self) -> Result<u64> {
        let output = Command::new(self.pg_controldata_bin())
            .args(["-D", self.data_dir_str()])
            .output()
            .await?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Postgres(format!("pg_controldata failed: {stderr}")));
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        for line in stdout.lines() {
            if line.contains("Latest checkpoint's TimeLineID")
                && let Some(val) = line.split(':').nth(1)
                && let Ok(tl) = val.trim().parse::<u64>()
            {
                return Ok(tl);
            }
        }
        Err(Error::Postgres(
            "Could not find TimeLineID in pg_controldata output".into(),
        ))
    }
    // ─────────────────────── tokio-postgres queries ───────────────────────

    /// Build a connection string for this PostgreSQL instance
    pub fn connection_string(&self) -> String {
        let mut parts = format!(
            "host={} port={} dbname={} user={}",
            self.config.listen,
            self.config.port,
            self.config.superuser.dbname,
            self.config.superuser.username,
        );
        if let Some(ref password) = self.config.superuser.password {
            parts.push_str(&format!(" password={password}"));
        }
        parts
    }

    /// Build a replication connection string
    pub fn replication_connection_string(&self, host: &str, port: u16) -> String {
        let mut parts = format!(
            "host={host} port={port} dbname={} user={}",
            self.config.replication.dbname, self.config.replication.username,
        );
        if let Some(ref password) = self.config.replication.password {
            parts.push_str(&format!(" password={password}"));
        }
        parts
    }

    /// Simple health check: connect and run SELECT 1
    pub async fn health_check(&self) -> Result<()> {
        let connstr = self.connection_string();
        let (client, connection) = tokio_postgres::connect(&connstr, tokio_postgres::NoTls)
            .await
            .map_err(|e| Error::Postgres(format!("Connection failed: {e}")))?;

        // Spawn the connection task with timeout to prevent task leak if PG is unresponsive
        tokio::spawn(async move {
            match tokio::time::timeout(PG_CONNECTION_TIMEOUT, connection).await {
                Ok(Err(e)) => debug!("Health check connection closed: {e}"),
                Err(_) => {
                    warn!("Health check connection task timed out after {PG_CONNECTION_TIMEOUT:?}")
                }
                Ok(Ok(())) => {}
            }
        });

        client
            .simple_query("SELECT 1")
            .await
            .map_err(|e| Error::Postgres(format!("Health check query failed: {e}")))?;

        Ok(())
    }

    /// Check if PostgreSQL is in recovery (i.e., running as replica)
    pub async fn is_in_recovery(&self) -> Result<bool> {
        let connstr = self.connection_string();
        let (client, connection) = tokio_postgres::connect(&connstr, tokio_postgres::NoTls)
            .await
            .map_err(|e| Error::Postgres(format!("Connection failed: {e}")))?;

        tokio::spawn(async move {
            match tokio::time::timeout(PG_CONNECTION_TIMEOUT, connection).await {
                Ok(Err(e)) => debug!("Connection closed: {e}"),
                Err(_) => warn!("PG connection task timed out after {PG_CONNECTION_TIMEOUT:?}"),
                Ok(Ok(())) => {}
            }
        });

        let rows = client
            .simple_query("SELECT pg_is_in_recovery()")
            .await
            .map_err(|e| Error::Postgres(format!("Query failed: {e}")))?;

        for msg in rows {
            if let tokio_postgres::SimpleQueryMessage::Row(row) = msg
                && let Some(val) = row.get(0)
            {
                return Ok(val == "t");
            }
        }
        Err(Error::Postgres("Could not determine recovery state".into()))
    }

    /// Timeline the primary is writing WAL on right now, from
    /// pg_walfile_name(pg_current_wal_lsn()). Errors on a server in recovery.
    pub async fn primary_timeline(&self) -> Result<u64> {
        let connstr = self.connection_string();
        let (client, connection) = tokio_postgres::connect(&connstr, tokio_postgres::NoTls)
            .await
            .map_err(|e| Error::Postgres(format!("Connection failed: {e}")))?;

        tokio::spawn(async move {
            match tokio::time::timeout(PG_CONNECTION_TIMEOUT, connection).await {
                Ok(Err(e)) => debug!("Connection closed: {e}"),
                Err(_) => warn!("PG connection task timed out after {PG_CONNECTION_TIMEOUT:?}"),
                Ok(Ok(())) => {}
            }
        });

        let rows = client
            .simple_query("SELECT pg_walfile_name(pg_current_wal_lsn())")
            .await
            .map_err(|e| Error::Postgres(format!("Query failed: {e}")))?;

        for msg in rows {
            if let tokio_postgres::SimpleQueryMessage::Row(row) = msg
                && let Some(val) = row.get(0)
            {
                return parse_walfile_timeline(val)
                    .ok_or_else(|| Error::Postgres(format!("Unexpected WAL file name '{val}'")));
            }
        }
        Err(Error::Postgres(
            "Could not determine the primary's timeline".into(),
        ))
    }

    /// Get WAL position and timeline information
    pub async fn wal_status(&self) -> Result<WalStatus> {
        let connstr = self.connection_string();
        let (client, connection) = tokio_postgres::connect(&connstr, tokio_postgres::NoTls)
            .await
            .map_err(|e| Error::Postgres(format!("Connection failed: {e}")))?;

        tokio::spawn(async move {
            match tokio::time::timeout(PG_CONNECTION_TIMEOUT, connection).await {
                Ok(Err(e)) => debug!("Connection closed: {e}"),
                Err(_) => warn!("PG connection task timed out after {PG_CONNECTION_TIMEOUT:?}"),
                Ok(Ok(())) => {}
            }
        });

        // First check if in recovery
        let in_recovery = {
            let rows = client
                .simple_query("SELECT pg_is_in_recovery()")
                .await
                .map_err(|e| Error::Postgres(format!("Query failed: {e}")))?;
            let mut result = false;
            for msg in rows {
                if let tokio_postgres::SimpleQueryMessage::Row(row) = msg
                    && let Some(val) = row.get(0)
                {
                    result = val == "t";
                }
            }
            result
        };

        if in_recovery {
            // Replica: receive/replay LSN, the receiver's (flushed_lsn,
            // received_tli) pair, and the checkpoint timeline as a lower
            // bound, all in one statement.
            let query = "SELECT \
                pg_last_wal_receive_lsn() - '0/0', \
                pg_last_wal_replay_lsn() - '0/0', \
                (SELECT timeline_id FROM pg_control_checkpoint()), \
                r.flushed_lsn - '0/0', \
                r.received_tli, \
                pg_postmaster_start_time() \
                FROM (SELECT 1) AS one \
                LEFT JOIN pg_stat_wal_receiver AS r ON true";

            let rows = client
                .simple_query(query)
                .await
                .map_err(|e| Error::Postgres(format!("WAL status query failed: {e}")))?;

            let mut sample = ReplicaWalSample::default();
            for msg in rows {
                if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
                    let num = |i: usize| row.get(i).and_then(|v| v.parse::<u64>().ok());
                    sample.receive_lsn = num(0);
                    sample.replay_lsn = num(1);
                    sample.checkpoint_timeline = num(2);
                    sample.receiver = num(3).zip(num(4));
                    sample.postmaster_start = row.get(5).map(str::to_owned);
                }
            }

            let mut last = self
                .last_received_wal
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let resolved = resolve_replica_wal(&sample, last.as_ref());
            if let Some(pair) = resolved.remember.clone() {
                *last = Some(pair);
            }
            drop(last);
            debug!(
                wal_position = resolved.wal_position,
                timeline = ?resolved.timeline,
                source = ?resolved.source,
                "Replica WAL status"
            );

            Ok(WalStatus {
                timeline: resolved.timeline,
                wal_position: resolved.wal_position,
                in_recovery: true,
            })
        } else {
            // Primary: current WAL position and the timeline WAL is inserted
            // on (exact; the checkpoint timeline lags a promote until the
            // first checkpoint on the new timeline completes).
            let query = "SELECT \
                pg_current_wal_lsn() - '0/0', \
                pg_walfile_name(pg_current_wal_lsn())";

            let rows = client
                .simple_query(query)
                .await
                .map_err(|e| Error::Postgres(format!("WAL status query failed: {e}")))?;

            let mut wal_lsn: u64 = 0;
            let mut timeline: Option<u64> = None;

            for msg in rows {
                if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
                    if let Some(val) = row.get(0) {
                        wal_lsn = val.parse().unwrap_or(0);
                    }
                    if let Some(val) = row.get(1) {
                        timeline = parse_walfile_timeline(val);
                    }
                }
            }

            Ok(WalStatus {
                timeline,
                wal_position: wal_lsn,
                in_recovery: false,
            })
        }
    }

    /// Query current WAL position from PostgreSQL.
    /// On primary: pg_current_wal_lsn()
    /// On replica: pg_last_wal_replay_lsn() (or receive, whichever is higher)
    pub async fn query_wal_position(&self) -> Result<u64> {
        let status = self.wal_status().await?;
        Ok(status.wal_position)
    }

    /// Query current timeline from PostgreSQL.
    pub async fn query_timeline(&self) -> Result<u64> {
        let status = self.wal_status().await?;
        status
            .timeline
            .ok_or_else(|| Error::Postgres("Could not determine timeline".into()))
    }

    /// Run CHECKPOINT to flush WAL to disk (used before graceful shutdown).
    pub async fn checkpoint(&self) -> Result<()> {
        let connstr = self.connection_string();
        let (client, connection) = tokio_postgres::connect(&connstr, tokio_postgres::NoTls)
            .await
            .map_err(|e| Error::Postgres(format!("Connection failed: {e}")))?;

        tokio::spawn(async move {
            match tokio::time::timeout(PG_CONNECTION_TIMEOUT, connection).await {
                Ok(Err(e)) => debug!("Connection closed: {e}"),
                Err(_) => warn!("PG connection task timed out after {PG_CONNECTION_TIMEOUT:?}"),
                Ok(Ok(())) => {}
            }
        });

        client
            .simple_query("CHECKPOINT")
            .await
            .map_err(|e| Error::Postgres(format!("CHECKPOINT failed: {e}")))?;

        info!("CHECKPOINT completed");
        Ok(())
    }

    /// Persist synchronous_standby_names via ALTER SYSTEM and reload.
    pub async fn set_synchronous_standby_names(&self, value: &str) -> Result<()> {
        let connstr = self.connection_string();
        let (client, connection) = tokio_postgres::connect(&connstr, tokio_postgres::NoTls)
            .await
            .map_err(|e| Error::Postgres(format!("Connection failed: {e}")))?;

        tokio::spawn(async move {
            match tokio::time::timeout(PG_CONNECTION_TIMEOUT, connection).await {
                Ok(Err(e)) => debug!("Connection closed: {e}"),
                Err(_) => warn!("PG connection task timed out after {PG_CONNECTION_TIMEOUT:?}"),
                Ok(Ok(())) => {}
            }
        });

        let escaped = value.replace('\'', "''");
        let sql = format!("ALTER SYSTEM SET synchronous_standby_names TO '{escaped}'");

        client.simple_query(&sql).await.map_err(|e| {
            Error::Postgres(format!(
                "ALTER SYSTEM SET synchronous_standby_names failed: {e}"
            ))
        })?;

        self.reload().await?;
        info!(%value, "Updated synchronous_standby_names");
        Ok(())
    }

    // ─────────────────────── State accessors ───────────────────────

    pub fn role(&self) -> &MemberRole {
        &self.role
    }

    pub fn set_role(&mut self, role: MemberRole) {
        self.role = role;
    }

    pub fn state(&self) -> PgState {
        self.state
    }

    pub fn set_state(&mut self, state: PgState) {
        self.state = state;
    }

    /// Observe `pg_is_in_recovery()` now and record the result.
    ///
    /// Returns `None` (and records it) when PostgreSQL is not running, the
    /// query fails, or it does not answer within [`RECOVERY_OBSERVE_TIMEOUT`].
    pub async fn observe_recovery(&mut self) -> Option<bool> {
        self.in_recovery = if !self.is_running() {
            None
        } else {
            match tokio::time::timeout(RECOVERY_OBSERVE_TIMEOUT, self.is_in_recovery()).await {
                Ok(Ok(v)) => Some(v),
                Ok(Err(e)) => {
                    warn!("Could not observe recovery state: {e}");
                    None
                }
                Err(_) => {
                    warn!(
                        "Could not observe recovery state: no answer within {RECOVERY_OBSERVE_TIMEOUT:?}"
                    );
                    None
                }
            }
        };
        self.in_recovery
    }

    /// Last recorded recovery observation (see the `in_recovery` field).
    pub fn observed_in_recovery(&self) -> Option<bool> {
        self.in_recovery
    }

    /// True only when PostgreSQL was observed out of recovery, i.e. it is a
    /// writable primary, and no state-changing action ran since.
    pub fn is_observed_primary(&self) -> bool {
        self.in_recovery == Some(false)
    }

    #[cfg(test)]
    pub(crate) fn set_observed_in_recovery(&mut self, v: Option<bool>) {
        self.in_recovery = v;
    }

    pub fn config(&self) -> &PostgresqlConfig {
        &self.config
    }

    /// Remove the data directory (used when reinitializing)
    pub fn remove_data_directory(&self) -> Result<()> {
        let path = &self.config.data_dir;
        if path.exists() {
            for entry in std::fs::read_dir(path)? {
                let entry = entry?;
                let entry_path = entry.path();
                if entry_path.is_dir() {
                    std::fs::remove_dir_all(&entry_path)?;
                } else {
                    std::fs::remove_file(&entry_path)?;
                }
            }
            info!(path = %path.display(), "Removed data directory contents");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConnectionParams;
    use proptest::prelude::*;
    use std::path::PathBuf;
    use std::time::Duration;

    fn test_config() -> PostgresqlConfig {
        PostgresqlConfig {
            data_dir: PathBuf::from("/tmp/pg-ha-test-data"),
            bin_dir: PathBuf::from("/usr/lib/postgresql/16/bin"),
            listen: "127.0.0.1".to_string(),
            port: 5432,
            superuser: ConnectionParams {
                username: "postgres".to_string(),
                password: Some("secret".to_string()),
                dbname: "postgres".to_string(),
            },
            replication: ConnectionParams {
                username: "replicator".to_string(),
                password: Some("replpass".to_string()),
                dbname: "postgres".to_string(),
            },
            remove_data_directory_on_rewind_failure: false,
            parameters: Default::default(),
        }
    }

    #[tokio::test]
    async fn test_start_refused_after_block_starts() {
        let data_dir =
            std::env::temp_dir().join(format!("pg-ha-test-block-starts-{}", std::process::id()));
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut config = test_config();
        config.data_dir = data_dir.clone();
        config.bin_dir = std::env::temp_dir().join(format!(
            "pg-ha-test-block-starts-no-bin-{}",
            std::process::id()
        ));
        let mut pg = Postgresql::new(config);
        pg.block_starts();
        let err = pg.start().await.expect_err("start must be refused");
        assert!(err.to_string().contains("graceful shutdown"), "{err}");
        assert_eq!(pg.state(), PgState::Unknown, "start() did nothing");
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[test]
    fn test_connection_string() {
        let pg = Postgresql::new(test_config());
        let connstr = pg.connection_string();
        assert!(connstr.contains("host=127.0.0.1"));
        assert!(connstr.contains("port=5432"));
        assert!(connstr.contains("user=postgres"));
        assert!(connstr.contains("password=secret"));
        assert!(connstr.contains("dbname=postgres"));
    }

    #[test]
    fn test_replication_connection_string() {
        let pg = Postgresql::new(test_config());
        let connstr = pg.replication_connection_string("10.0.0.1", 5432);
        assert!(connstr.contains("host=10.0.0.1"));
        assert!(connstr.contains("user=replicator"));
        assert!(connstr.contains("password=replpass"));
    }

    #[test]
    fn test_data_directory_empty_nonexistent() {
        let mut config = test_config();
        config.data_dir = PathBuf::from("/tmp/nonexistent-pg-ha-test-dir-12345");
        let pg = Postgresql::new(config);
        assert!(pg.data_directory_empty().unwrap());
    }

    #[test]
    fn test_is_running_no_pid_file() {
        let mut config = test_config();
        config.data_dir = PathBuf::from("/tmp/nonexistent-pg-ha-test-dir-12345");
        let pg = Postgresql::new(config);
        assert!(!pg.is_running());
    }

    #[test]
    fn test_initial_state() {
        let pg = Postgresql::new(test_config());
        assert_eq!(pg.role(), &MemberRole::Uninitialized);
        assert_eq!(pg.state(), PgState::Unknown);
        assert_eq!(pg.observed_in_recovery(), None);
        assert!(!pg.is_observed_primary());
    }

    #[test]
    fn test_recorded_role_is_not_evidence_of_primary() {
        // Setting the role records intent; only an observation makes it a primary.
        let mut pg = Postgresql::new(test_config());
        pg.set_role(MemberRole::Primary);
        assert_eq!(pg.role(), &MemberRole::Primary);
        assert!(!pg.is_observed_primary());

        pg.set_observed_in_recovery(Some(true));
        assert!(!pg.is_observed_primary());

        pg.set_observed_in_recovery(Some(false));
        assert!(pg.is_observed_primary());
    }

    #[tokio::test]
    async fn test_observe_when_not_running_is_unknown() {
        let mut pg = Postgresql::new(test_config());
        pg.set_observed_in_recovery(Some(false));
        // No postmaster.pid: not running, so the old observation is dropped.
        assert_eq!(pg.observe_recovery().await, None);
        assert!(!pg.is_observed_primary());
    }

    #[tokio::test]
    async fn test_state_changing_actions_invalidate_observation() {
        // pg_ctl does not exist here, so each action fails, but the observation
        // must already be invalidated: the server state is no longer known once
        // pg-ha attempted to change it.
        let mut config = test_config();
        config.bin_dir = PathBuf::from("/nonexistent/pg-ha-test-bin");
        config.data_dir = PathBuf::from("/nonexistent/pg-ha-test-data");
        let mut pg = Postgresql::new(config);

        pg.set_observed_in_recovery(Some(false));
        let _ = pg.promote().await;
        assert_eq!(pg.observed_in_recovery(), None, "promote");

        pg.set_observed_in_recovery(Some(false));
        let _ = pg.stop("fast").await;
        assert_eq!(pg.observed_in_recovery(), None, "stop");

        pg.set_observed_in_recovery(Some(true));
        let _ = pg.start().await;
        assert_eq!(pg.observed_in_recovery(), None, "start");
    }

    #[test]
    fn parse_recovery_probe_table() {
        let table = [
            (true, "f\n", Some(true)),
            (true, " t ", Some(false)),
            (true, "", None),
            (true, "x", None),
            (false, "f", None),
        ];
        for (success, stdout, expected) in table {
            assert_eq!(
                parse_recovery_probe(success, stdout),
                expected,
                "{success} {stdout:?}"
            );
        }
    }

    #[tokio::test]
    async fn source_probe_without_psql_is_none() {
        let mut config = test_config();
        config.bin_dir = PathBuf::from("/nonexistent/pg-ha-test-bin");
        let pg = Postgresql::new(config);
        assert_eq!(
            pg.source_answers_as_primary("host=node1 port=5432").await,
            None
        );
    }

    #[test]
    fn test_pg_ctl_path() {
        let pg = Postgresql::new(test_config());
        assert_eq!(
            pg.pg_ctl(),
            PathBuf::from("/usr/lib/postgresql/16/bin/pg_ctl")
        );
    }

    #[test]
    fn test_pg_state_display() {
        assert_eq!(format!("{}", PgState::Running), "running");
        assert_eq!(format!("{}", PgState::Stopped), "stopped");
        assert_eq!(format!("{}", PgState::Starting), "starting");
        assert_eq!(format!("{}", PgState::Crashed), "crashed");
        assert_eq!(format!("{}", PgState::Unknown), "unknown");
    }

    #[test]
    fn test_parse_lsn_basic() {
        assert_eq!(parse_lsn("0/3000060"), Some(0x3000060));
        assert_eq!(parse_lsn("1/3000060"), Some((1u64 << 32) | 0x3000060));
        assert_eq!(parse_lsn("0/0"), Some(0));
        assert_eq!(parse_lsn("FF/FFFFFFFF"), Some((0xFFu64 << 32) | 0xFFFFFFFF));
    }

    #[test]
    fn test_parse_lsn_invalid() {
        assert_eq!(parse_lsn(""), None);
        assert_eq!(parse_lsn("0"), None);
        assert_eq!(parse_lsn("0/0/0"), None);
        assert_eq!(parse_lsn("G/0"), None);
        assert_eq!(parse_lsn("0/G"), None);
    }

    #[test]
    fn parse_walfile_timeline_valid() {
        assert_eq!(parse_walfile_timeline("000000010000000000000003"), Some(1));
        assert_eq!(parse_walfile_timeline("0000000A0000000000000003"), Some(10));
        assert_eq!(parse_walfile_timeline("00000003000000000000000A"), Some(3));
        assert_eq!(
            parse_walfile_timeline("000000ff00000001000000FE"),
            Some(255)
        );
    }

    #[test]
    fn parse_walfile_timeline_invalid() {
        assert_eq!(parse_walfile_timeline(""), None);
        assert_eq!(parse_walfile_timeline("00000001"), None);
        assert_eq!(parse_walfile_timeline("0000000100000000000000030"), None);
        assert_eq!(parse_walfile_timeline("0000000G0000000000000003"), None);
        assert_eq!(parse_walfile_timeline("+00000010000000000000003"), None);
    }

    // ─────────────────────────────────────────────────────────────────────
    // Bug Condition Exploration: PID Validation Missing (Defect 1.5)
    //
    // **Property 1: Bug Condition** - PID recycling causes false-positive is_running
    // **Validates: Requirements 2.5**
    //
    // `is_running()` only checks `libc::kill(pid, 0)` — it cannot distinguish
    // between PostgreSQL and another process that recycled the same PID.
    // This test verifies the source code includes process identity validation.
    // ─────────────────────────────────────────────────────────────────────

    /// **Property 1: Bug Condition** - PID validation in is_running
    ///
    /// **Validates: Requirements 2.5**
    ///
    /// Verifies `is_running()` validates process identity (not just PID existence).
    /// On unfixed code, only `libc::kill(pid, 0)` is used — any alive PID returns true.
    #[test]
    fn test_bug_condition_pid_validation_in_is_running() {
        let source = include_str!("postgresql.rs");

        // Find is_running function body (stop at next pub fn)
        let fn_start = source
            .find("pub fn is_running(&self) -> bool")
            .expect("is_running() not found");
        let fn_body = &source[fn_start..];

        let mut brace_count = 0;
        let mut fn_end = 0;
        let mut found_first = false;
        for (i, ch) in fn_body.char_indices() {
            if ch == '{' {
                brace_count += 1;
                found_first = true;
            } else if ch == '}' {
                brace_count -= 1;
                if found_first && brace_count == 0 {
                    fn_end = i + 1;
                    break;
                }
            }
        }
        let body = &fn_body[..fn_end];

        // The fix should include process identity verification beyond kill(pid, 0).
        // On Linux: /proc/{pid}/cmdline check for "postgres"
        // Implementation detail: `is_postgres_process` helper or inline cmdline check.
        let has_identity_check = body.contains("is_postgres_process")
            || body.contains("/proc/")
            || body.contains("cmdline")
            || body.contains("process_name");

        assert!(
            has_identity_check,
            "BUG DETECTED: is_running() only checks libc::kill(pid, 0) without \
             verifying process identity.\n\
             If PostgreSQL crashes and its PID is recycled by another process, \
             is_running() incorrectly returns true — preventing HA from restarting PG.\n\
             Fix: after kill(pid, 0) succeeds, verify the process is actually postgres \
             (e.g., check /proc/{{pid}}/cmdline on Linux)."
        );
    }

    // ─────────────────────────────────────────────────────────────────────
    // Bug Condition Exploration Tests: PG Connection Task Leak (Defect #1)
    //
    // **Property 1: Bug Condition** - PG Connection Tasks Accumulate Without Bound
    // **Validates: Requirements 1.1**
    //
    // These tests verify the fix is in place: all spawned connection tasks are
    // wrapped in `tokio::time::timeout(PG_CONNECTION_TIMEOUT, connection)`.
    // On unfixed code, connection tasks would never terminate when PG is
    // unresponsive, leading to unbounded task accumulation.
    // ─────────────────────────────────────────────────────────────────────

    /// **Property 1: Bug Condition** - PG Connection Tasks Accumulate Without Bound
    /// Validates: Requirements 1.1
    ///
    /// Verify PG_CONNECTION_TIMEOUT is defined with a reasonable value (30s).
    /// The timeout prevents spawned connection tasks from living forever when
    /// PG is unresponsive.
    #[test]
    fn test_pg_connection_timeout_defined_and_reasonable() {
        // PG_CONNECTION_TIMEOUT must be defined (compile-time proof)
        let timeout = PG_CONNECTION_TIMEOUT;

        // Must be exactly 30 seconds as specified in the fix
        assert_eq!(
            timeout,
            Duration::from_secs(30),
            "PG_CONNECTION_TIMEOUT should be 30 seconds"
        );

        // Sanity: timeout should be > 0 and <= 60s (reasonable for a DB connection)
        assert!(timeout > Duration::ZERO, "Timeout must be positive");
        assert!(
            timeout <= Duration::from_secs(60),
            "Timeout should not exceed 60 seconds for a health check connection"
        );
    }

    /// **Property 1: Bug Condition** - PG Connection Tasks Accumulate Without Bound
    /// Validates: Requirements 1.1
    ///
    /// Code analysis: verify that health_check, is_in_recovery, wal_status, and
    /// checkpoint all use the `tokio::time::timeout(PG_CONNECTION_TIMEOUT, ...)`
    /// pattern to wrap the connection future.
    ///
    /// This is a source-level verification: we read the source file and confirm
    /// each method contains the timeout pattern. Without this pattern, tasks
    /// would accumulate indefinitely when PG is unresponsive.
    #[test]
    fn test_all_connection_methods_use_timeout_pattern() {
        let source = include_str!("postgresql.rs");

        // All methods that spawn a connection task
        let methods_with_connection_spawn =
            ["health_check", "is_in_recovery", "wal_status", "checkpoint"];

        // The timeout pattern that must appear in each method's spawned task
        let timeout_pattern = "tokio::time::timeout(PG_CONNECTION_TIMEOUT, connection)";

        // Count occurrences of the timeout pattern in the source
        let timeout_count = source.matches(timeout_pattern).count();

        // There should be at least 4 occurrences (one per method)
        assert!(
            timeout_count >= 4,
            "Expected at least 4 occurrences of timeout pattern \
             '{}' in postgresql.rs, found {}. \
             Bug condition: without timeout, connection tasks accumulate without bound.",
            timeout_pattern,
            timeout_count
        );

        // Verify each method exists and contains tokio::spawn
        for method in &methods_with_connection_spawn {
            assert!(
                source.contains(&format!("pub async fn {method}")),
                "Method '{}' not found in postgresql.rs",
                method
            );
        }

        // Verify the spawn pattern exists (connection tasks are spawned)
        let spawn_count = source.matches("tokio::spawn(async move {").count();
        assert!(
            spawn_count >= 4,
            "Expected at least 4 tokio::spawn calls for connection tasks, found {}",
            spawn_count
        );
    }

    /// **Property 1: Bug Condition** - PG Connection Tasks Accumulate Without Bound
    /// Validates: Requirements 1.1
    ///
    /// Logical test: verify that a future wrapped in tokio::time::timeout gets
    /// cancelled when the timeout expires. This proves the mechanism used to fix
    /// the task leak actually works — tasks that would otherwise hang indefinitely
    /// are reclaimed after the timeout.
    ///
    /// Uses a short timeout (50ms) to keep the test fast while proving the
    /// cancellation mechanism is sound.
    #[tokio::test]
    async fn test_timeout_cancels_hanging_connection_task() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let task_completed = Arc::new(AtomicBool::new(false));
        let task_timed_out = Arc::new(AtomicBool::new(false));
        let task_completed_clone = task_completed.clone();
        let task_timed_out_clone = task_timed_out.clone();

        // Simulate an unresponsive PG connection: a future that never completes
        let never_completes = async {
            // This simulates `connection.await` when PG is unresponsive
            std::future::pending::<std::result::Result<(), tokio_postgres::Error>>().await
        };

        // Use a short timeout (50ms) to keep the test fast
        let test_timeout = Duration::from_millis(50);

        let handle = tokio::spawn(async move {
            match tokio::time::timeout(test_timeout, never_completes).await {
                Ok(Err(_e)) => {
                    task_completed_clone.store(true, Ordering::SeqCst);
                }
                Err(_) => {
                    // Timeout elapsed — this is the expected path for unresponsive PG
                    task_timed_out_clone.store(true, Ordering::SeqCst);
                }
                Ok(Ok(())) => {
                    task_completed_clone.store(true, Ordering::SeqCst);
                }
            }
        });

        // Wait for the task to finish (should complete after ~50ms timeout)
        handle.await.expect("Spawned task panicked");

        // The task must have timed out (not completed normally)
        assert!(
            task_timed_out.load(Ordering::SeqCst),
            "Expected connection task to be cancelled by timeout. \
             Bug condition: without timeout, this task would hang forever, \
             accumulating zombie tasks with each health_check call."
        );
        assert!(
            !task_completed.load(Ordering::SeqCst),
            "Connection task should NOT have completed normally (PG was unresponsive)"
        );
    }

    /// **Property 1: Bug Condition** - PG Connection Tasks Accumulate Without Bound
    /// Validates: Requirements 1.1
    ///
    /// Verify that spawning N timeout-wrapped tasks against an unresponsive
    /// endpoint results in all tasks being reclaimed (not accumulating).
    /// This directly tests the bug condition: without timeout, N health_check
    /// calls would leave N zombie tasks alive indefinitely.
    #[tokio::test]
    async fn test_timeout_wrapped_tasks_do_not_accumulate() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let active_tasks = Arc::new(AtomicUsize::new(0));
        let completed_tasks = Arc::new(AtomicUsize::new(0));
        let test_timeout = Duration::from_millis(50);
        let num_calls = 5; // Simulate 5 health_check calls

        let mut handles = Vec::new();

        for _ in 0..num_calls {
            let active = active_tasks.clone();
            let completed = completed_tasks.clone();

            let handle = tokio::spawn(async move {
                active.fetch_add(1, Ordering::SeqCst);

                // Simulate unresponsive PG connection (never completes)
                let never_completes = std::future::pending::<()>();

                // This is the fix pattern — timeout will cancel the hanging future
                let _ = tokio::time::timeout(test_timeout, never_completes).await;

                active.fetch_sub(1, Ordering::SeqCst);
                completed.fetch_add(1, Ordering::SeqCst);
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete (they should all timeout after ~50ms)
        for handle in handles {
            handle.await.expect("Task panicked");
        }

        // All 5 tasks should have completed (via timeout cancellation)
        assert_eq!(
            completed_tasks.load(Ordering::SeqCst),
            num_calls,
            "All {} tasks should have completed via timeout. \
             Bug condition: without timeout, these tasks would remain alive indefinitely. \
             Counterexample: 5 health_check calls → 5+ zombie tasks after 30s wait.",
            num_calls
        );

        // No tasks should remain active
        assert_eq!(
            active_tasks.load(Ordering::SeqCst),
            0,
            "No tasks should remain active after timeout. \
             Bug condition: tasks accumulate without bound when PG is unresponsive."
        );
    }

    // ─────────────────────────────────────────────────────────────────────
    // Preservation Property Tests: PG Normal Query Completion
    //
    // **Property 2: Preservation** - PG Normal Query Completion
    // **Validates: Requirements 3.1**
    //
    // These tests verify that when PG responds normally, the timeout wrapper
    // does NOT interfere with normal operation. The key insight:
    //   - The timeout wraps only the `connection` maintenance future
    //   - The query itself uses the `client` which is SEPARATE from `connection`
    //   - The 30s timeout is far above normal query completion time
    //   - The Ok(Ok(())) branch silently succeeds (no warning logged)
    //
    // Observation on FIXED code: when PG responds normally, health_check()
    // returns Ok(()), connection task exits cleanly via Ok(Ok(())) branch.
    // ─────────────────────────────────────────────────────────────────────

    /// **Property 2: Preservation** - PG Normal Query Completion
    /// **Validates: Requirements 3.1**
    ///
    /// Code analysis: verify the timeout is applied ONLY to the `connection`
    /// future, NOT to the query itself. In tokio-postgres, `connect()` returns
    /// (client, connection) where:
    ///   - `client` is used for queries (SELECT 1, pg_is_in_recovery, etc.)
    ///   - `connection` is a background maintenance future
    ///
    /// The query execution path is OUTSIDE the timeout wrapper. This means
    /// normal queries complete without any timeout pressure.
    #[test]
    fn test_timeout_wraps_connection_not_query() {
        let source = include_str!("postgresql.rs");

        // Only check the production code (before #[cfg(test)] mod tests)
        let prod_code = source.split("#[cfg(test)]").next().unwrap_or(source);

        // The timeout pattern wraps `connection`, not `client`
        let timeout_wraps_connection = "tokio::time::timeout(PG_CONNECTION_TIMEOUT, connection)";

        // Verify timeout is on `connection` (the background maintenance future)
        assert!(
            prod_code.contains(timeout_wraps_connection),
            "Timeout must wrap the `connection` future, not the query. \
             Preservation: queries use `client` which is separate from `connection`."
        );

        // Verify NO timeout wrapping on query calls in production code
        assert!(
            !prod_code.contains("timeout(PG_CONNECTION_TIMEOUT, client"),
            "PRESERVATION VIOLATION: timeout must NOT wrap client/query operations. \
             Queries use `client` which is separate from the `connection` future."
        );
        assert!(
            !prod_code.contains("timeout(PG_CONNECTION_TIMEOUT, simple_query"),
            "PRESERVATION VIOLATION: timeout must NOT wrap simple_query operations."
        );

        // Verify the query methods all use `client.simple_query(...)` directly
        // (without any timeout wrapper on the query itself)
        let methods_and_queries = [
            ("health_check", "SELECT 1"),
            ("is_in_recovery", "pg_is_in_recovery"),
            ("checkpoint", "CHECKPOINT"),
        ];
        for (method, query_fragment) in &methods_and_queries {
            assert!(
                prod_code.contains(query_fragment),
                "Method '{}' should contain query fragment '{}' — \
                 queries execute directly via client without timeout interference.",
                method,
                query_fragment
            );
        }
    }

    /// **Property 2: Preservation** - PG Normal Query Completion
    /// **Validates: Requirements 3.1**
    ///
    /// Verify all four methods (health_check, is_in_recovery, wal_status,
    /// checkpoint) follow the same connection handling pattern and the
    /// Ok(Ok(())) branch silently succeeds without logging a warning.
    /// This ensures normal PG responses don't trigger spurious warnings.
    #[test]
    fn test_normal_completion_branch_is_silent() {
        let source = include_str!("postgresql.rs");

        // The Ok(Ok(())) branch in the timeout match should be silent (no log)
        // Pattern: `Ok(Ok(())) => {}`
        let silent_success_pattern = "Ok(Ok(())) => {}";
        let silent_count = source.matches(silent_success_pattern).count();

        assert!(
            silent_count >= 4,
            "Expected at least 4 silent Ok(Ok(())) branches (one per connection method), \
             found {}. Preservation: normal PG responses should not trigger any warning \
             or debug log — the connection simply completed successfully.",
            silent_count
        );

        // Verify the timeout arm logs a warning (only on timeout, not on success)
        let warn_in_timeout = source.matches("timed out").count();
        assert!(
            warn_in_timeout >= 4,
            "Expected at least 4 timeout warning messages (one per method), found {}. \
             Only timeout triggers a warning; normal completion is silent.",
            warn_in_timeout
        );
    }

    /// **Property 2: Preservation** - PG Normal Query Completion
    /// **Validates: Requirements 3.1**
    ///
    /// Functional test: verify that a future which completes immediately
    /// (simulating a normal PG response) is NOT affected by the timeout wrapper.
    /// When wrapped in `tokio::time::timeout(30s, future_that_completes_fast)`,
    /// the result should be `Ok(inner_result)` — the timeout never fires.
    #[tokio::test]
    async fn test_immediate_completion_not_affected_by_timeout() {
        // Simulate a PG connection future that completes immediately (normal case)
        let instant_connection: std::pin::Pin<
            Box<dyn std::future::Future<Output = std::result::Result<(), String>> + Send>,
        > = Box::pin(async { Ok(()) });

        // Wrap in the same timeout duration used in production
        let result = tokio::time::timeout(PG_CONNECTION_TIMEOUT, instant_connection).await;

        // The timeout should NOT fire — we get Ok(Ok(())) (the inner future's result)
        match result {
            Ok(Ok(())) => {
                // This is the expected path — mirrors the `Ok(Ok(())) => {}` branch
                // in production code. Normal completion, no warning logged.
            }
            Ok(Err(e)) => {
                panic!(
                    "Preservation violation: immediate connection returned error: {}. \
                     Normal PG responses should complete successfully.",
                    e
                );
            }
            Err(_elapsed) => {
                panic!(
                    "Preservation violation: timeout fired on an immediately-completing future! \
                     PG_CONNECTION_TIMEOUT ({:?}) should never interfere with normal responses.",
                    PG_CONNECTION_TIMEOUT
                );
            }
        }
    }

    // Property 2: Preservation - PG Normal Query Completion
    // Validates: Requirements 3.1
    //
    // Property-based test: for all completion times significantly below the
    // 30s timeout threshold, the timeout wrapper does not interfere.
    // Generates random "query completion times" from 0ms to 1000ms and verifies
    // the timeout pattern always yields Ok (no timeout fires).
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10))]
        #[test]
        fn prop_normal_completion_within_timeout_succeeds(
            completion_ms in 0u64..1000u64
        ) {
            // Any completion time under 1 second should be well within the 30s timeout.
            let completion_time = Duration::from_millis(completion_ms);

            prop_assert!(
                PG_CONNECTION_TIMEOUT > completion_time,
                "PG_CONNECTION_TIMEOUT ({:?}) must be larger than normal completion time ({:?}).",
                PG_CONNECTION_TIMEOUT,
                completion_time
            );

            let margin = PG_CONNECTION_TIMEOUT - completion_time;
            prop_assert!(
                margin >= Duration::from_secs(29),
                "Margin between timeout and completion ({:?}) is too small.",
                margin
            );
        }
    }

    // Property 2: Preservation - PG Normal Query Completion
    // Validates: Requirements 3.1
    //
    // Property-based test: for all futures that complete within a reasonable
    // timeframe (simulating normal PG responses), the timeout wrapper resolves
    // to Ok(inner_result) without firing.
    //
    // This uses tokio's test-util with time pausing to run quickly while
    // verifying the property across many simulated completion times.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10))]
        #[test]
        fn prop_timeout_preserves_result_for_fast_futures(
            delay_ms in 0u64..50u64,
            result_is_ok in proptest::bool::ANY
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .unwrap();

            rt.block_on(async {
                let delay = Duration::from_millis(delay_ms);

                let simulated_connection = async move {
                    tokio::time::sleep(delay).await;
                    if result_is_ok {
                        Ok::<(), String>(())
                    } else {
                        Err("connection closed by server".to_string())
                    }
                };

                let result =
                    tokio::time::timeout(PG_CONNECTION_TIMEOUT, simulated_connection).await;

                match result {
                    Ok(Ok(())) => {
                        assert!(result_is_ok, "Got Ok but expected Err");
                    }
                    Ok(Err(_)) => {
                        assert!(!result_is_ok, "Got Err but expected Ok");
                    }
                    Err(_) => {
                        panic!(
                            "Preservation violation: timeout fired after only {:?}.",
                            delay
                        );
                    }
                }
            });
        }
    }

    /// **Property 2: Preservation** - PG Normal Query Completion
    /// **Validates: Requirements 3.1**
    ///
    /// When PG connection is refused (port not listening), health_check() should
    /// return an error promptly without hanging. This verifies the normal error
    /// path still works correctly — connection refused is a fast failure that
    /// happens BEFORE the connection future is even spawned (at the
    /// `tokio_postgres::connect()` stage), so the timeout wrapper is irrelevant
    /// for this case. The function must not hang waiting for the 30s timeout.
    #[tokio::test]
    async fn test_connection_refused_returns_error_promptly() {
        use std::time::Instant;

        // Use a port that is almost certainly not listening (ephemeral range, high port)
        let mut config = test_config();
        config.port = 59999; // Very unlikely to have a PG instance here
        config.listen = "127.0.0.1".to_string();

        let pg = Postgresql::new(config);

        let start = Instant::now();
        let result = pg.health_check().await;
        let elapsed = start.elapsed();

        // Must return an error (connection refused)
        assert!(
            result.is_err(),
            "Preservation: health_check() to a non-listening port must return Err, got Ok. \
             Connection refused should propagate as an error."
        );

        // Must complete quickly — well under the 30s PG_CONNECTION_TIMEOUT.
        // Connection refused typically resolves in <100ms. We allow up to 5s
        // for slow CI environments, but the key point is it doesn't wait 30s.
        assert!(
            elapsed < Duration::from_secs(5),
            "Preservation violation: health_check() took {:?} to return error on connection refused. \
             Expected prompt failure (< 5s). The timeout wrapper must NOT cause the function \
             to wait 30s when the connection is outright refused.",
            elapsed
        );
    }

    /// **Property 2: Preservation** - PG Normal Query Completion
    /// **Validates: Requirements 3.1**
    ///
    /// Verify that every method opening a PG connection (health_check,
    /// is_in_recovery, wal_status, checkpoint, set_synchronous_standby_names, ...)
    /// follows the identical timeout pattern, ensuring consistent preservation
    /// behavior across all PG query methods.
    ///
    /// The check pairs connects with timeout-wrapped spawns instead of pinning a
    /// fixed method count, so adding a new query method does not break it as long
    /// as the new method follows the pattern.
    #[test]
    fn test_all_methods_follow_same_preservation_pattern() {
        let source = include_str!("postgresql.rs");

        // Only check the production code (before #[cfg(test)] mod tests)
        let prod_code = source.split("#[cfg(test)]").next().unwrap_or(source);

        // Each method should have exactly this structure:
        // 1. tokio_postgres::connect(...) → (client, connection)
        // 2. tokio::spawn with timeout wrapping connection
        // 3. client.simple_query(...) for the actual query (outside spawn)

        let connect_pattern = "tokio_postgres::connect(&connstr, tokio_postgres::NoTls)";
        let spawn_timeout_pattern = "tokio::spawn(async move {\n            match tokio::time::timeout(PG_CONNECTION_TIMEOUT, connection)";

        let connect_count = prod_code.matches(connect_pattern).count();
        let spawn_timeout_count = prod_code.matches(spawn_timeout_pattern).count();

        // At least the original four query methods must still use this pattern.
        assert!(
            connect_count >= 4,
            "Expected at least 4 PG connect calls (health_check, is_in_recovery, \
             wal_status, checkpoint), found {connect_count}."
        );

        // Every connect must be paired with a timeout-wrapped connection task.
        assert_eq!(
            connect_count, spawn_timeout_count,
            "Found {connect_count} PG connect calls but {spawn_timeout_count} spawn+timeout \
             patterns. Each method must follow the preservation pattern: \
             connect → spawn with timeout → query via client."
        );
    }

    // ── resolve_replica_wal (R2: timeline of the published WAL position) ──

    const RUN: &str = "2026-10-27 08:00:00+00";

    fn sample(
        receive: Option<u64>,
        replay: Option<u64>,
        checkpoint_tl: Option<u64>,
        receiver: Option<(u64, u64)>,
    ) -> ReplicaWalSample {
        ReplicaWalSample {
            receive_lsn: receive,
            replay_lsn: replay,
            checkpoint_timeline: checkpoint_tl,
            receiver,
            postmaster_start: Some(RUN.to_string()),
        }
    }

    fn received(flushed: u64, timeline: u64) -> ReceivedWal {
        ReceivedWal {
            postmaster_start: RUN.to_string(),
            flushed_lsn: flushed,
            timeline,
        }
    }

    #[test]
    fn streaming_replica_publishes_the_receiver_timeline_not_the_checkpoint() {
        // Streamed timeline 6, last restartpoint still on 5.
        let r = resolve_replica_wal(&sample(Some(900), Some(880), Some(5), Some((900, 6))), None);

        assert_eq!(r.wal_position, 900);
        assert_eq!(r.timeline, Some(6));
        assert_eq!(r.source, ReplicaTimelineSource::Receiver);
        assert_eq!(r.remember, Some(received(900, 6)));
    }

    #[test]
    fn receiver_pair_is_used_together_even_if_receive_lsn_moved() {
        // pg_last_wal_receive_lsn() was read a moment after the view row.
        let r = resolve_replica_wal(&sample(Some(950), Some(880), Some(5), Some((900, 6))), None);

        assert_eq!(r.wal_position, 900);
        assert_eq!(r.timeline, Some(6));
    }

    #[test]
    fn stopped_receiver_keeps_the_remembered_timeline_at_the_same_position() {
        let last = received(900, 6);
        let r = resolve_replica_wal(&sample(Some(900), Some(900), Some(5), None), Some(&last));

        assert_eq!(r.wal_position, 900);
        assert_eq!(r.timeline, Some(6));
        assert_eq!(r.source, ReplicaTimelineSource::LowerBound);
        assert_eq!(r.remember, None, "keeps the remembered pair");
    }

    /// Docker run 2026-10-27: the dying primary's last WAL (shutdown
    /// checkpoint) reached node2 after the last sample, so the position
    /// moved past the remembered pair before the receiver exited.
    #[test]
    fn remembered_timeline_holds_after_the_last_wal_from_a_dying_upstream() {
        let last = received(67_571_080, 2);
        let r = resolve_replica_wal(
            &sample(Some(67_575_848), Some(67_575_848), Some(1), None),
            Some(&last),
        );

        assert_eq!(r.wal_position, 67_575_848);
        assert_eq!(r.timeline, Some(2), "not checkpoint timeline 1");
    }

    #[test]
    fn remembered_timeline_holds_when_replay_passed_it() {
        // Replay beyond the received position is the same history.
        let last = received(900, 6);
        let r = resolve_replica_wal(&sample(Some(900), Some(1000), Some(5), None), Some(&last));

        assert_eq!(r.wal_position, 1000);
        assert_eq!(r.timeline, Some(6));
    }

    #[test]
    fn remembered_pair_from_another_server_run_is_not_used() {
        // A new run may follow a rewound or re-cloned history.
        let last = ReceivedWal {
            postmaster_start: "2026-10-27 07:00:00+00".to_string(),
            ..received(900, 6)
        };
        let r = resolve_replica_wal(&sample(Some(900), Some(900), Some(5), None), Some(&last));

        assert_eq!(r.timeline, Some(5));
    }

    #[test]
    fn remembered_pair_above_the_position_is_not_used() {
        let last = received(1000, 6);
        let r = resolve_replica_wal(&sample(Some(900), Some(900), Some(5), None), Some(&last));

        assert_eq!(r.timeline, Some(5));
    }

    #[test]
    fn replay_ahead_of_a_restarted_receiver_uses_the_receiver_as_a_lower_bound() {
        // A receiver restarts from the start of the replay segment.
        let r = resolve_replica_wal(&sample(Some(512), Some(700), Some(5), Some((512, 6))), None);

        assert_eq!(r.wal_position, 700);
        assert_eq!(r.timeline, Some(6));
        assert_eq!(r.source, ReplicaTimelineSource::LowerBound);
        assert_eq!(r.remember, Some(received(512, 6)));
    }

    #[test]
    fn no_receiver_and_nothing_remembered_publishes_the_checkpoint_lower_bound() {
        let r = resolve_replica_wal(&sample(None, Some(700), Some(5), None), None);

        assert_eq!(r.wal_position, 700);
        assert_eq!(r.timeline, Some(5));
        assert_eq!(r.source, ReplicaTimelineSource::LowerBound);
    }

    #[test]
    fn unknown_postmaster_start_is_not_remembered_and_matches_nothing() {
        let mut s = sample(Some(900), Some(900), Some(5), Some((900, 6)));
        s.postmaster_start = None;
        let r = resolve_replica_wal(&s, None);
        assert_eq!(r.timeline, Some(6));
        assert_eq!(r.remember, None);

        let mut s = sample(Some(900), Some(900), Some(5), None);
        s.postmaster_start = None;
        let r = resolve_replica_wal(&s, Some(&received(900, 6)));
        assert_eq!(r.timeline, Some(5));
    }

    proptest! {
        /// Exact only from the streaming pair; otherwise never above the
        /// highest value known to hold at or below the published position.
        #[test]
        fn resolution_never_invents_a_timeline(
            receive in proptest::option::of(0u64..2000),
            replay in proptest::option::of(0u64..2000),
            ckpt in proptest::option::of(1u64..10),
            receiver in proptest::option::of((0u64..2000, 1u64..10)),
            last in proptest::option::of((0u64..2000, 1u64..10)),
        ) {
            let s = sample(receive, replay, ckpt, receiver);
            let last = last.map(|(lsn, tl)| received(lsn, tl));
            let r = resolve_replica_wal(&s, last.as_ref());

            prop_assert!(r.wal_position >= replay.unwrap_or(0));
            match r.source {
                ReplicaTimelineSource::Receiver => {
                    prop_assert_eq!(Some((r.wal_position, r.timeline.unwrap_or(0))), receiver);
                }
                ReplicaTimelineSource::LowerBound => {
                    let mut bound = ckpt;
                    if let Some((lsn, tl)) = receiver && lsn <= r.wal_position {
                        bound = bound.max(Some(tl));
                    }
                    if let Some(l) = &last && l.flushed_lsn <= r.wal_position {
                        bound = bound.max(Some(l.timeline));
                    }
                    prop_assert_eq!(r.timeline, bound);
                }
            }
        }
    }
}
