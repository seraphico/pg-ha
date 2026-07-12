//! Bootstrap and Cluster Initialization
//!
//! Orchestrates the full bootstrap flow for new clusters:
//! - `bootstrap_new_cluster()` — initdb with options from config, write pg config, start PG, run post-bootstrap SQL
//! - `clone_from_member()` — pg_basebackup from the best clone source (prefer clonefrom tagged members)
//! - `custom_bootstrap()` — run a user-supplied command instead of initdb
//! - `cleanup_on_failure()` — remove partial data directory
//! - `select_clone_source()` — pick best member (clonefrom tag > primary)
//!
//! The Bootstrap module is used by the HA engine's `handle_empty_data_dir()` to
//! coordinate cluster initialization with DCS race semantics.

use std::path::Path;
use std::sync::Arc;

use tokio::process::Command;
use tracing::{error, info, warn};

use crate::cluster::{Cluster, Member};
use crate::config::{BootstrapConfig, Config, InitdbOption};
use crate::dcs::DcsAdapter;
use crate::error::{Error, Result};
use crate::postgresql::Postgresql;

/// Orchestrates the bootstrap flow for new PostgreSQL clusters.
pub struct Bootstrap<'a> {
    config: &'a Config,
    postgresql: &'a mut Postgresql,
    dcs: &'a Arc<dyn DcsAdapter>,
}

/// Result of a bootstrap operation
#[derive(Debug)]
pub enum BootstrapResult {
    /// Successfully bootstrapped as a new primary
    InitializedAsPrimary,
    /// Successfully cloned from an existing member
    ClonedAsReplica,
    /// Lost the initialization race to another node
    LostRace,
    /// Bootstrap failed (error message included)
    Failed(String),
}

impl<'a> Bootstrap<'a> {
    /// Create a new Bootstrap orchestrator.
    pub fn new(
        config: &'a Config,
        postgresql: &'a mut Postgresql,
        dcs: &'a Arc<dyn DcsAdapter>,
    ) -> Self {
        Self {
            config,
            postgresql,
            dcs,
        }
    }

    /// Race for cluster initialization via DCS and bootstrap a new cluster.
    ///
    /// This is called when the data directory is empty AND no cluster exists in DCS.
    /// The node races to write the /initialize key atomically. The winner runs initdb.
    pub async fn bootstrap_new_cluster(&mut self) -> BootstrapResult {
        // Race for initialization via atomic DCS write
        match self.dcs.initialize("").await {
            Ok(true) => {
                info!("Won initialization race — bootstrapping new cluster");
            }
            Ok(false) => {
                info!("Lost initialization race — another node is bootstrapping");
                return BootstrapResult::LostRace;
            }
            Err(e) => {
                return BootstrapResult::Failed(format!("DCS initialize error: {e}"));
            }
        }

        // Determine bootstrap method: custom command or initdb
        let bootstrap_config = self.config.bootstrap.clone().unwrap_or_default();

        let result = if let Some(ref custom_cmd) = bootstrap_config.custom_command {
            self.run_custom_bootstrap(custom_cmd).await
        } else {
            self.run_initdb(&bootstrap_config).await
        };

        if let Err(e) = result {
            error!("Bootstrap failed: {e}");
            self.cleanup_on_failure();
            return BootstrapResult::Failed(e.to_string());
        }

        // Write post-initdb PG configuration (postgresql.conf, pg_hba.conf)
        self.write_pg_config();

        // Start PostgreSQL
        if let Err(e) = self.postgresql.start().await {
            error!("Failed to start PostgreSQL after bootstrap: {e}");
            self.cleanup_on_failure();
            return BootstrapResult::Failed(format!("Start after bootstrap failed: {e}"));
        }

        // Execute post-bootstrap SQL if configured
        if !bootstrap_config.post_bootstrap_sql.is_empty()
            && let Err(e) = self
                .run_post_bootstrap_sql(&bootstrap_config.post_bootstrap_sql)
                .await
        {
            warn!("Post-bootstrap SQL failed (non-fatal): {e}");
            // Not fatal — cluster is still usable
        }

        // Write DCS config if bootstrap.dcs is set
        if !bootstrap_config.dcs.is_empty() {
            let dcs_value = serde_json::to_string(&bootstrap_config.dcs).unwrap_or_default();
            if !dcs_value.is_empty()
                && dcs_value != "{}"
                && let Err(e) = self.dcs.set_config_value(&dcs_value).await
            {
                warn!("Failed to write bootstrap DCS config: {e}");
            }
        }

        BootstrapResult::InitializedAsPrimary
    }

    /// Clone from an existing cluster member via pg_basebackup.
    ///
    /// Selects the best clone source (preferring members with clonefrom tag),
    /// runs pg_basebackup, and starts PostgreSQL as a replica.
    pub async fn clone_from_member(&mut self, cluster: &Cluster) -> BootstrapResult {
        let source = match Self::select_clone_source(cluster, &self.config.name) {
            Some(member) => member.clone(),
            None => {
                return BootstrapResult::Failed("No suitable clone source found".to_string());
            }
        };

        info!(source = %source.name, "Cloning from existing member");

        match self.postgresql.basebackup(&source.conn_url).await {
            Ok(()) => {
                // Write replication config for standby
                self.write_pg_config();
                // Configure as a streaming standby
                self.write_standby_config(&source.conn_url);
                BootstrapResult::ClonedAsReplica
            }
            Err(e) => {
                error!("pg_basebackup from {} failed: {e}", source.name);
                self.cleanup_on_failure();
                BootstrapResult::Failed(format!("Clone from {} failed: {e}", source.name))
            }
        }
    }

    /// Select the best clone source from the cluster.
    ///
    /// Preference order:
    /// 1. Members with the `clonefrom` tag set to true
    /// 2. The current primary (leader)
    /// 3. Any running member
    ///
    /// Excludes the requesting node itself and members with `noclone` tag.
    pub fn select_clone_source<'b>(cluster: &'b Cluster, exclude_name: &str) -> Option<&'b Member> {
        // First, try members with clonefrom tag
        let clonefrom_member = cluster.members.iter().find(|m| {
            m.name != exclude_name && !Self::has_noclone_tag(m) && Self::has_clonefrom_tag(m)
        });
        if clonefrom_member.is_some() {
            return clonefrom_member;
        }

        // Fallback: use the primary (leader)
        if let Some(leader) = &cluster.leader {
            let leader_member = cluster.members.iter().find(|m| {
                m.name == leader.name && m.name != exclude_name && !Self::has_noclone_tag(m)
            });
            if leader_member.is_some() {
                return leader_member;
            }
        }

        // Last resort: any running member that isn't excluded
        cluster.members.iter().find(|m| {
            m.name != exclude_name
                && !Self::has_noclone_tag(m)
                && m.state == crate::cluster::MemberState::Running
        })
    }

    /// Run initdb with options from bootstrap configuration.
    async fn run_initdb(&self, bootstrap_config: &BootstrapConfig) -> Result<()> {
        let options = Self::parse_initdb_options(&bootstrap_config.initdb);
        self.postgresql.initdb(&options).await
    }

    /// Run a custom bootstrap command instead of initdb.
    ///
    /// The command is executed with the data directory path as an environment variable.
    async fn run_custom_bootstrap(&self, command: &str) -> Result<()> {
        info!(command, "Running custom bootstrap command");

        let data_dir = self
            .config
            .postgresql
            .data_dir
            .to_string_lossy()
            .to_string();

        let output = Command::new("sh")
            .args(["-c", command])
            .env("PGDATA", &data_dir)
            .env("PG_DATA_DIR", &data_dir)
            .output()
            .await?;

        if output.status.success() {
            // Validate the data directory was created
            if !Path::new(&data_dir).exists() || Self::is_dir_empty(Path::new(&data_dir)) {
                return Err(Error::Postgres(
                    "Custom bootstrap command succeeded but data directory is empty".to_string(),
                ));
            }
            info!("Custom bootstrap command completed successfully");
            Ok(())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!(%stderr, "Custom bootstrap command failed");
            Err(Error::Postgres(format!(
                "Custom bootstrap command failed: {stderr}"
            )))
        }
    }

    /// Execute post-bootstrap SQL statements against the running PostgreSQL instance.
    async fn run_post_bootstrap_sql(&self, statements: &[String]) -> Result<()> {
        info!(count = statements.len(), "Running post-bootstrap SQL");

        let connstr = self.postgresql.connection_string();
        let (client, connection) = tokio_postgres::connect(&connstr, tokio_postgres::NoTls)
            .await
            .map_err(|e| Error::Postgres(format!("Post-bootstrap SQL connect failed: {e}")))?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                warn!("Post-bootstrap SQL connection closed: {e}");
            }
        });

        for sql in statements {
            info!(sql = %sql, "Executing post-bootstrap SQL");
            if let Err(e) = client.batch_execute(sql).await {
                error!(sql = %sql, error = %e, "Post-bootstrap SQL statement failed");
                return Err(Error::Postgres(format!(
                    "Post-bootstrap SQL failed on '{sql}': {e}"
                )));
            }
        }

        info!("Post-bootstrap SQL completed successfully");
        Ok(())
    }

    /// Write postgresql.conf and pg_hba.conf for replication after bootstrap.
    fn write_pg_config(&self) {
        let data_dir = &self.config.postgresql.data_dir;
        let pg_conf_path = data_dir.join("postgresql.conf");
        let listen_addr = &self.config.postgresql.listen;
        let port = self.config.postgresql.port;

        // Build the managed settings block
        let mut managed_block = String::from("# pg-ha managed settings\n");
        managed_block.push_str(&format!("listen_addresses = '{listen_addr}'\n"));
        managed_block.push_str(&format!("port = {port}\n"));
        managed_block.push_str("wal_level = replica\n");
        managed_block.push_str("max_wal_senders = 10\n");
        managed_block.push_str("max_replication_slots = 10\n");
        managed_block.push_str("hot_standby = on\n");

        // Apply any additional parameters from config (skip ones we already set)
        let builtin_keys = [
            "listen_addresses",
            "port",
            "wal_level",
            "max_wal_senders",
            "max_replication_slots",
            "hot_standby",
        ];
        for (key, value) in &self.config.postgresql.parameters {
            if builtin_keys.contains(&key.as_str()) {
                continue; // Already set above, skip duplicates
            }
            let clean_value = value.trim_matches('\'').trim_matches('"');
            let needs_quoting = clean_value.parse::<f64>().is_err()
                && !matches!(clean_value, "on" | "off" | "true" | "false" | "yes" | "no");
            if needs_quoting {
                managed_block.push_str(&format!("{key} = '{clean_value}'\n"));
            } else {
                managed_block.push_str(&format!("{key} = {clean_value}\n"));
            }
        }

        // Read existing postgresql.conf, strip any previous managed block, then append new one
        let existing = std::fs::read_to_string(&pg_conf_path).unwrap_or_default();
        let cleaned: String = if existing.contains("# pg-ha managed settings") {
            // Remove everything from "# pg-ha managed settings" to end
            existing
                .split("# pg-ha managed settings")
                .next()
                .unwrap_or("")
                .to_string()
        } else {
            existing
        };

        let final_content = format!("{}\n{}", cleaned.trim_end(), managed_block);
        if let Err(e) = std::fs::write(&pg_conf_path, final_content) {
            warn!("Failed to write postgresql.conf: {e}");
        }

        // Write pg_hba.conf allowing replication connections
        let hba_path = data_dir.join("pg_hba.conf");
        let repl_user = &self.config.postgresql.replication.username;
        let hba_content = format!(
            "# pg-ha managed\n\
             local   all             all                                     trust\n\
             host    all             all             0.0.0.0/0               trust\n\
             host    all             all             ::/0                    trust\n\
             local   replication     all                                     trust\n\
             host    replication     {repl_user}     0.0.0.0/0               trust\n\
             host    replication     {repl_user}     ::/0                    trust\n"
        );
        if let Err(e) = std::fs::write(&hba_path, hba_content) {
            warn!("Failed to write pg_hba.conf: {e}");
        }
    }

    /// Configure PostgreSQL as a streaming standby replica.
    ///
    /// Creates `standby.signal` and writes `primary_conninfo` to `postgresql.auto.conf`.
    /// Required for PG 12+ to start in recovery mode after pg_basebackup.
    fn write_standby_config(&self, source_connstr: &str) {
        let data_dir = &self.config.postgresql.data_dir;

        // Create standby.signal (empty file tells PG to start in standby mode)
        let standby_signal = data_dir.join("standby.signal");
        if let Err(e) = std::fs::write(&standby_signal, "") {
            warn!("Failed to create standby.signal: {e}");
        }

        // Build primary_conninfo from source connection string
        // The source_connstr is in libpq format: "host=X port=Y dbname=Z user=W password=P"
        // We need to ensure it has the replication user
        let repl_user = &self.config.postgresql.replication.username;
        let repl_pass = self
            .config
            .postgresql
            .replication
            .password
            .as_deref()
            .unwrap_or("");

        // Parse host and port from source_connstr
        let mut host = "127.0.0.1".to_string();
        let mut port = "5432".to_string();
        for part in source_connstr.split_whitespace() {
            if let Some(val) = part.strip_prefix("host=") {
                host = val.to_string();
            } else if let Some(val) = part.strip_prefix("port=") {
                port = val.to_string();
            }
        }

        let primary_conninfo = format!(
            "host={host} port={port} user={repl_user} password={repl_pass} application_name={}",
            self.config.name
        );

        // Write to postgresql.auto.conf (PG reads this after postgresql.conf)
        let auto_conf_path = data_dir.join("postgresql.auto.conf");
        let auto_conf_content = format!(
            "# pg-ha managed standby configuration\nprimary_conninfo = '{primary_conninfo}'\nrecovery_target_timeline = 'latest'\n"
        );
        if let Err(e) = std::fs::write(&auto_conf_path, auto_conf_content) {
            warn!("Failed to write postgresql.auto.conf: {e}");
        }

        info!(
            primary_conninfo = %primary_conninfo,
            "Configured as streaming standby"
        );
    }

    /// Clean up partial data directory on bootstrap failure.
    ///
    /// Removes the data directory so the next HA cycle can retry cleanly.
    pub fn cleanup_on_failure(&self) {
        let data_dir = &self.config.postgresql.data_dir;
        if data_dir.exists() {
            info!(path = %data_dir.display(), "Cleaning up partial data directory after bootstrap failure");
            if let Err(e) = std::fs::remove_dir_all(data_dir) {
                error!(
                    path = %data_dir.display(),
                    error = %e,
                    "Failed to clean up data directory"
                );
            }
        }
    }

    // ─────────────────── Helper functions ───────────────────

    /// Parse InitdbOption list into command-line argument strings.
    pub fn parse_initdb_options(options: &[InitdbOption]) -> Vec<String> {
        options
            .iter()
            .flat_map(|opt| match opt {
                InitdbOption::Flag(f) => vec![f.clone()],
                InitdbOption::KeyValue(kv) => kv.iter().map(|(k, v)| format!("{k}={v}")).collect(),
            })
            .collect()
    }

    /// Check if a member has the clonefrom tag set to true.
    fn has_clonefrom_tag(member: &Member) -> bool {
        member
            .tags
            .get("clonefrom")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }

    /// Check if a member has the noclone tag set to true.
    fn has_noclone_tag(member: &Member) -> bool {
        member
            .tags
            .get("noclone")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }

    /// Check if a directory is empty.
    fn is_dir_empty(path: &Path) -> bool {
        match std::fs::read_dir(path) {
            Ok(mut entries) => entries.next().is_none(),
            Err(_) => true,
        }
    }
}

/// Default implementation for BootstrapConfig allows using it without explicit config.
impl Default for BootstrapConfig {
    fn default() -> Self {
        Self {
            initdb: vec![],
            dcs: Default::default(),
            post_init: vec![],
            custom_command: None,
            post_bootstrap_sql: vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{Leader, Member, MemberRole, MemberState};
    use std::collections::HashMap;

    fn make_member(name: &str, tags: HashMap<String, serde_json::Value>) -> Member {
        Member {
            name: name.to_string(),
            conn_url: format!("host={name} port=5432 dbname=postgres"),
            api_url: format!("http://{name}:8008"),
            state: MemberState::Running,
            role: MemberRole::Replica,
            wal_position: None,
            timeline: None,
            tags,
            version: None,
        }
    }

    #[test]
    fn test_parse_initdb_options_flags() {
        let options = vec![InitdbOption::Flag("data-checksums".to_string())];
        let result = Bootstrap::parse_initdb_options(&options);
        assert_eq!(result, vec!["data-checksums"]);
    }

    #[test]
    fn test_parse_initdb_options_key_values() {
        let options = vec![
            InitdbOption::KeyValue(HashMap::from([(
                "encoding".to_string(),
                "UTF8".to_string(),
            )])),
            InitdbOption::KeyValue(HashMap::from([(
                "locale".to_string(),
                "en_US.UTF-8".to_string(),
            )])),
        ];
        let result = Bootstrap::parse_initdb_options(&options);
        assert!(result.contains(&"encoding=UTF8".to_string()));
        assert!(result.contains(&"locale=en_US.UTF-8".to_string()));
    }

    #[test]
    fn test_parse_initdb_options_mixed() {
        let options = vec![
            InitdbOption::Flag("data-checksums".to_string()),
            InitdbOption::KeyValue(HashMap::from([(
                "encoding".to_string(),
                "UTF8".to_string(),
            )])),
            InitdbOption::KeyValue(HashMap::from([(
                "wal-segsize".to_string(),
                "64".to_string(),
            )])),
        ];
        let result = Bootstrap::parse_initdb_options(&options);
        assert_eq!(result.len(), 3);
        assert!(result.contains(&"data-checksums".to_string()));
        assert!(result.contains(&"encoding=UTF8".to_string()));
        assert!(result.contains(&"wal-segsize=64".to_string()));
    }

    #[test]
    fn test_parse_initdb_options_empty() {
        let options: Vec<InitdbOption> = vec![];
        let result = Bootstrap::parse_initdb_options(&options);
        assert!(result.is_empty());
    }

    #[test]
    fn test_select_clone_source_prefers_clonefrom_tag() {
        let cluster = Cluster {
            leader: Some(Leader {
                name: "node1".to_string(),
                version: 1,
            }),
            members: vec![
                make_member("node1", HashMap::new()),
                make_member(
                    "node2",
                    HashMap::from([("clonefrom".to_string(), serde_json::json!(true))]),
                ),
                make_member("node3", HashMap::new()),
            ],
            ..Default::default()
        };

        let source = Bootstrap::select_clone_source(&cluster, "node3").unwrap();
        assert_eq!(source.name, "node2");
    }

    #[test]
    fn test_select_clone_source_falls_back_to_leader() {
        let cluster = Cluster {
            leader: Some(Leader {
                name: "node1".to_string(),
                version: 1,
            }),
            members: vec![
                make_member("node1", HashMap::new()),
                make_member("node2", HashMap::new()),
                make_member("node3", HashMap::new()),
            ],
            ..Default::default()
        };

        let source = Bootstrap::select_clone_source(&cluster, "node3").unwrap();
        assert_eq!(source.name, "node1"); // Falls back to leader
    }

    #[test]
    fn test_select_clone_source_excludes_self() {
        let cluster = Cluster {
            leader: Some(Leader {
                name: "node1".to_string(),
                version: 1,
            }),
            members: vec![
                make_member("node1", HashMap::new()),
                make_member(
                    "node2",
                    HashMap::from([("clonefrom".to_string(), serde_json::json!(true))]),
                ),
            ],
            ..Default::default()
        };

        // Requesting as node2 — should not pick itself even though it has clonefrom
        let source = Bootstrap::select_clone_source(&cluster, "node2").unwrap();
        assert_eq!(source.name, "node1");
    }

    #[test]
    fn test_select_clone_source_excludes_noclone() {
        let cluster = Cluster {
            leader: Some(Leader {
                name: "node1".to_string(),
                version: 1,
            }),
            members: vec![
                make_member(
                    "node1",
                    HashMap::from([("noclone".to_string(), serde_json::json!(true))]),
                ),
                make_member("node2", HashMap::new()),
            ],
            ..Default::default()
        };

        // node1 is leader but has noclone — should fall through to "any running member"
        let source = Bootstrap::select_clone_source(&cluster, "node3").unwrap();
        assert_eq!(source.name, "node2");
    }

    #[test]
    fn test_select_clone_source_no_suitable_source() {
        let cluster = Cluster {
            leader: None,
            members: vec![make_member(
                "node1",
                HashMap::from([("noclone".to_string(), serde_json::json!(true))]),
            )],
            ..Default::default()
        };

        // node1 has noclone, and we're node2 — no source available
        let source = Bootstrap::select_clone_source(&cluster, "node2");
        assert!(source.is_none());
    }

    #[test]
    fn test_select_clone_source_multiple_clonefrom() {
        let cluster = Cluster {
            leader: Some(Leader {
                name: "node1".to_string(),
                version: 1,
            }),
            members: vec![
                make_member("node1", HashMap::new()),
                make_member(
                    "node2",
                    HashMap::from([("clonefrom".to_string(), serde_json::json!(true))]),
                ),
                make_member(
                    "node3",
                    HashMap::from([("clonefrom".to_string(), serde_json::json!(true))]),
                ),
            ],
            ..Default::default()
        };

        // Should pick first clonefrom member found (node2)
        let source = Bootstrap::select_clone_source(&cluster, "node4").unwrap();
        assert_eq!(source.name, "node2");
    }

    #[test]
    fn test_default_bootstrap_config() {
        let config = BootstrapConfig::default();
        assert!(config.initdb.is_empty());
        assert!(config.dcs.is_empty());
        assert!(config.post_init.is_empty());
        assert!(config.custom_command.is_none());
        assert!(config.post_bootstrap_sql.is_empty());
    }
}
