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

/// Timeout for spawned PG connection futures.
/// If PG is unresponsive, the task will be cancelled after this duration.
const PG_CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);

/// Manages a local PostgreSQL instance lifecycle
pub struct Postgresql {
    config: PostgresqlConfig,
    role: MemberRole,
    state: PgState,
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
    /// Current timeline
    pub timeline: Option<u64>,
    /// WAL position in bytes (receive or replay LSN, whichever is higher)
    pub wal_position: u64,
    /// Whether the instance is in recovery (replica)
    pub in_recovery: bool,
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

impl Postgresql {
    pub fn new(config: PostgresqlConfig) -> Self {
        Self {
            config,
            role: MemberRole::Uninitialized,
            state: PgState::Unknown,
        }
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
                    && let Ok(pid) = pid_str.trim().parse::<i32>() {
                        // Check if process is alive (signal 0 doesn't send a signal, just checks)
                        return unsafe { libc::kill(pid, 0) == 0 };
                    }
                false
            }
            Err(_) => false,
        }
    }

    /// Read the PID from postmaster.pid, if available
    pub fn postmaster_pid(&self) -> Option<i32> {
        let content = std::fs::read_to_string(self.postmaster_pid_path()).ok()?;
        content.lines().next()?.trim().parse().ok()
    }

    // ─────────────────────── Process management ───────────────────────

    /// Start PostgreSQL using pg_ctl start
    pub async fn start(&mut self) -> Result<()> {
        info!("Starting PostgreSQL");
        self.state = PgState::Starting;

        // Build pg_ctl start command with log file in data directory
        let log_file = self.config.data_dir.join("pg_log.log");
        let log_file_str = log_file.to_string_lossy().to_string();

        let output = Command::new(self.pg_ctl())
            .args(["start", "-D", self.data_dir_str(), "-l", &log_file_str, "-w"])
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
        Err(Error::Postgres("PostgreSQL did not start within timeout".into()))
    }

    /// Stop PostgreSQL with the specified mode (smart, fast, immediate)
    pub async fn stop(&mut self, mode: &str) -> Result<()> {
        info!(mode, "Stopping PostgreSQL");

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

    /// Promote a standby to primary using pg_ctl promote
    pub async fn promote(&mut self) -> Result<()> {
        info!("Promoting PostgreSQL to primary");

        let output = Command::new(self.pg_ctl())
            .args(["promote", "-D", self.data_dir_str(), "-w"])
            .stdin(std::process::Stdio::null())
            .output()
            .await?;

        if output.status.success() {
            self.role = MemberRole::Primary;
            info!("Promotion successful");
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
        info!(source = source_connstr, "Running pg_basebackup");

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
        info!(source = source_connstr, "Running pg_rewind");

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
                && let Some(val) = line.split(':').nth(1) {
                    return Ok(val.trim().to_string());
                }
        }
        Err(Error::Postgres("Could not find system identifier in pg_controldata output".into()))
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
                && let Some(val) = line.split(':').nth(1) {
                    return Ok(val.trim().to_string());
                }
        }
        Err(Error::Postgres("Could not find cluster state in pg_controldata output".into()))
    }

    // ─────────────────────── tokio-postgres queries ───────────────────────

    /// Build a connection string for this PostgreSQL instance
    pub fn connection_string(&self) -> String {
        let mut parts = format!(
            "host={} port={} dbname={} user={}",
            self.config.listen, self.config.port,
            self.config.superuser.dbname, self.config.superuser.username,
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
        let (client, connection) =
            tokio_postgres::connect(&connstr, tokio_postgres::NoTls)
                .await
                .map_err(|e| Error::Postgres(format!("Connection failed: {e}")))?;

        // Spawn the connection task with timeout to prevent task leak if PG is unresponsive
        tokio::spawn(async move {
            match tokio::time::timeout(PG_CONNECTION_TIMEOUT, connection).await {
                Ok(Err(e)) => debug!("Health check connection closed: {e}"),
                Err(_) => warn!("Health check connection task timed out after {PG_CONNECTION_TIMEOUT:?}"),
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
        let (client, connection) =
            tokio_postgres::connect(&connstr, tokio_postgres::NoTls)
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
                && let Some(val) = row.get(0) {
                    return Ok(val == "t");
                }
        }
        Err(Error::Postgres("Could not determine recovery state".into()))
    }

    /// Get WAL position and timeline information
    pub async fn wal_status(&self) -> Result<WalStatus> {
        let connstr = self.connection_string();
        let (client, connection) =
            tokio_postgres::connect(&connstr, tokio_postgres::NoTls)
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
                    && let Some(val) = row.get(0) {
                        result = val == "t";
                    }
            }
            result
        };

        if in_recovery {
            // Replica: get receive and replay LSN
            let query = "SELECT \
                pg_last_wal_receive_lsn() - '0/0', \
                pg_last_wal_replay_lsn() - '0/0', \
                (SELECT timeline_id FROM pg_control_checkpoint())";

            let rows = client
                .simple_query(query)
                .await
                .map_err(|e| Error::Postgres(format!("WAL status query failed: {e}")))?;

            let mut receive_lsn: u64 = 0;
            let mut replay_lsn: u64 = 0;
            let mut timeline: Option<u64> = None;

            for msg in rows {
                if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
                    if let Some(val) = row.get(0) {
                        receive_lsn = val.parse().unwrap_or(0);
                    }
                    if let Some(val) = row.get(1) {
                        replay_lsn = val.parse().unwrap_or(0);
                    }
                    if let Some(val) = row.get(2) {
                        timeline = val.parse().ok();
                    }
                }
            }

            Ok(WalStatus {
                timeline,
                wal_position: receive_lsn.max(replay_lsn),
                in_recovery: true,
            })
        } else {
            // Primary: get current WAL position
            let query = "SELECT \
                pg_current_wal_lsn() - '0/0', \
                (SELECT timeline_id FROM pg_control_checkpoint())";

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
                        timeline = val.parse().ok();
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
        status.timeline.ok_or_else(|| Error::Postgres("Could not determine timeline".into()))
    }

    /// Run CHECKPOINT to flush WAL to disk (used before graceful shutdown).
    pub async fn checkpoint(&self) -> Result<()> {
        let connstr = self.connection_string();
        let (client, connection) =
            tokio_postgres::connect(&connstr, tokio_postgres::NoTls)
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

    pub fn is_primary(&self) -> bool {
        self.role == MemberRole::Primary
    }

    pub fn config(&self) -> &PostgresqlConfig {
        &self.config
    }

    /// Remove the data directory (used when reinitializing)
    pub fn remove_data_directory(&self) -> Result<()> {
        let path = &self.config.data_dir;
        if path.exists() {
            std::fs::remove_dir_all(path)?;
            info!(path = %path.display(), "Removed data directory");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConnectionParams;
    use std::path::PathBuf;

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
            parameters: Default::default(),
        }
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
        assert!(!pg.is_primary());
    }

    #[test]
    fn test_role_mutation() {
        let mut pg = Postgresql::new(test_config());
        pg.set_role(MemberRole::Primary);
        assert!(pg.is_primary());
        pg.set_role(MemberRole::Replica);
        assert!(!pg.is_primary());
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
}
