//! Helper methods: member heartbeat, standby signal detection, reconfiguration guard,
//! upstream reading, and lock ownership verification.

use tracing::warn;

use crate::error::Result;

use super::Ha;

impl Ha {
    /// Update member info in DCS (heartbeat)
    pub(super) async fn touch_member(&self) -> Result<()> {
        // Use the raft self_addr host for conn_url (it's the reachable address)
        let reachable_host = self
            .config
            .raft
            .self_addr
            .split(':')
            .next()
            .unwrap_or("127.0.0.1");
        let mut conn_url = format!(
            "host={} port={} dbname={} user={}",
            reachable_host,
            self.config.postgresql.port,
            self.config.postgresql.superuser.dbname,
            self.config.postgresql.superuser.username,
        );
        if let Some(ref pw) = self.config.postgresql.superuser.password {
            conn_url.push_str(&format!(" password={pw}"));
        }

        // Query real-time WAL position and timeline from PostgreSQL
        let (wal_position, timeline) = if self.postgresql.is_running() {
            match self.postgresql.wal_status().await {
                Ok(status) => (Some(status.wal_position), status.timeline),
                Err(e) => {
                    warn!("Failed to query WAL position: {e}");
                    (None, None)
                }
            }
        } else {
            (None, None)
        };

        let mut data = serde_json::json!({
            "name": self.config.name,
            "conn_url": conn_url,
            "api_url": format!("http://{}:{}", reachable_host, self.config.restapi.port),
            "state": format!("{}", self.postgresql.state()),
            "role": format!("{}", self.postgresql.role()),
        });

        if let Some(pos) = wal_position {
            data["wal_position"] = serde_json::json!(pos);
        }
        if let Some(tl) = timeline {
            data["timeline"] = serde_json::json!(tl);
        }

        self.dcs.touch_member(&data).await?;
        Ok(())
    }

    /// Check if standby.signal file exists in the data directory.
    pub(super) fn has_standby_signal(&self) -> bool {
        self.config
            .postgresql
            .data_dir
            .join("standby.signal")
            .exists()
    }

    /// Check if this node recently rejoined/reconfigured and should not be disturbed.
    /// Returns true for `retry_timeout` seconds after the last standby config was written.
    ///
    /// This replaces the old `is_pg_streaming()` heuristic which used an unreliable
    /// 30-second file modification time window. Instead, we use the configured
    /// `retry_timeout` (default 10s) as the grace period after writing
    /// postgresql.auto.conf, preventing unnecessary reconfiguration churn.
    pub(super) fn is_recently_reconfigured(&self) -> bool {
        let auto_conf_path = self.config.postgresql.data_dir.join("postgresql.auto.conf");
        if let Ok(metadata) = std::fs::metadata(&auto_conf_path)
            && let Ok(modified) = metadata.modified()
            && let Ok(elapsed) = modified.elapsed()
        {
            // Don't reconfigure within retry_timeout of the last config write
            return elapsed.as_secs() < self.config.retry_timeout;
        }
        false
    }

    /// Read the current upstream host from postgresql.auto.conf (primary_conninfo).
    /// Returns the host name/IP the replica is currently streaming from.
    pub(super) fn read_current_upstream(&self) -> Option<String> {
        let auto_conf_path = self.config.postgresql.data_dir.join("postgresql.auto.conf");
        let content = std::fs::read_to_string(&auto_conf_path).ok()?;

        // Parse primary_conninfo line: primary_conninfo = 'host=XXX port=...'
        for line in content.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with("primary_conninfo") {
                // Extract host= value from the connection string
                if let Some(start) = trimmed.find("host=") {
                    let after_host = &trimmed[start + 5..];
                    let host = after_host
                        .split_whitespace()
                        .next()
                        .unwrap_or("")
                        .trim_matches('\'')
                        .trim_matches('"');
                    if !host.is_empty() {
                        return Some(host.to_string());
                    }
                }
            }
        }
        None
    }

    /// Check if this node is the lock owner
    pub(super) fn is_lock_owner(&self) -> bool {
        self.cluster
            .leader
            .as_ref()
            .is_some_and(|l| l.name == self.config.name)
    }
}
