//! Configuration parsing and validation
//!
//! Loads from YAML file with environment variable overrides (PG_HA_ prefix).
//! Supports --validate-config and --generate-sample-config CLI flags.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Top-level configuration for a pg-ha node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Unique name of this node in the cluster
    pub name: String,

    /// Cluster scope/namespace
    pub scope: String,

    /// Namespace in DCS key path (default: "service")
    #[serde(default = "default_namespace")]
    pub namespace: String,

    /// HA loop interval in seconds
    #[serde(default = "default_loop_wait")]
    pub loop_wait: u64,

    /// TTL for leader lock in seconds
    #[serde(default = "default_ttl")]
    pub ttl: u64,

    /// Timeout for retrying DCS/PG operations in seconds
    #[serde(default = "default_retry_timeout")]
    pub retry_timeout: u64,

    /// PostgreSQL configuration
    pub postgresql: PostgresqlConfig,

    /// REST API configuration
    pub restapi: RestApiConfig,

    /// Raft DCS configuration
    pub raft: RaftConfig,

    /// Proxy configuration (replaces HAProxy)
    pub proxy: ProxyConfig,

    /// Watchdog configuration
    #[serde(default)]
    pub watchdog: WatchdogConfig,

    /// Node tags controlling HA behavior
    #[serde(default)]
    pub tags: Tags,

    /// Bootstrap configuration (used when initializing a new cluster)
    #[serde(default)]
    pub bootstrap: Option<BootstrapConfig>,
}

/// PostgreSQL instance configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresqlConfig {
    /// Path to PostgreSQL data directory
    pub data_dir: PathBuf,

    /// Path to directory containing pg_ctl, initdb, pg_basebackup, etc.
    pub bin_dir: PathBuf,

    /// Listen address for PostgreSQL
    #[serde(default = "default_pg_listen")]
    pub listen: String,

    /// Port for PostgreSQL
    #[serde(default = "default_pg_port")]
    pub port: u16,

    /// Superuser connection parameters
    pub superuser: ConnectionParams,

    /// Replication user connection parameters
    pub replication: ConnectionParams,

    /// Additional PostgreSQL parameters to set in postgresql.conf
    #[serde(default)]
    pub parameters: HashMap<String, String>,
}

/// Database connection parameters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionParams {
    pub username: String,

    #[serde(default)]
    pub password: Option<String>,

    #[serde(default = "default_dbname")]
    pub dbname: String,
}

/// REST API server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestApiConfig {
    /// Listen address
    #[serde(default = "default_api_listen")]
    pub listen: String,

    /// Listen port
    #[serde(default = "default_api_port")]
    pub port: u16,

    /// HTTP Basic Auth username (optional, if set enables auth)
    #[serde(default)]
    pub username: Option<String>,

    /// HTTP Basic Auth password
    #[serde(default)]
    pub password: Option<String>,
}

/// Raft consensus configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftConfig {
    /// This node's Raft RPC address (host:port)
    pub self_addr: String,

    /// Other nodes' Raft RPC addresses
    pub partner_addrs: Vec<String>,

    /// Directory for Raft state persistence (logs, snapshots)
    #[serde(default)]
    pub data_dir: Option<PathBuf>,

    /// Explicit node ID (if not set, derived from position in cluster)
    #[serde(default)]
    pub node_id: Option<u64>,
}

/// TCP proxy configuration (replaces HAProxy)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyConfig {
    /// Listen address for read-write traffic (routes to primary)
    #[serde(default = "default_proxy_listen")]
    pub rw_listen: String,

    /// Port for read-write traffic
    #[serde(default = "default_proxy_rw_port")]
    pub rw_port: u16,

    /// Listen address for read-only traffic (routes to replicas)
    #[serde(default = "default_proxy_listen")]
    pub ro_listen: String,

    /// Port for read-only traffic
    #[serde(default = "default_proxy_ro_port")]
    pub ro_port: u16,
}

/// Watchdog configuration for split-brain prevention
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchdogConfig {
    /// Watchdog mode: off, automatic, required
    #[serde(default = "default_watchdog_mode")]
    pub mode: WatchdogMode,

    /// Path to watchdog device
    #[serde(default = "default_watchdog_device")]
    pub device: String,

    /// Safety margin in seconds (watchdog fires before TTL expires)
    #[serde(default = "default_watchdog_safety_margin")]
    pub safety_margin: u64,
}

impl Default for WatchdogConfig {
    fn default() -> Self {
        Self {
            mode: WatchdogMode::Off,
            device: default_watchdog_device(),
            safety_margin: default_watchdog_safety_margin(),
        }
    }
}

/// Watchdog operating mode
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WatchdogMode {
    Off,
    Automatic,
    Required,
}

/// Node tags that influence HA behavior
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tags {
    /// If true, this node will never be promoted to primary
    #[serde(default)]
    pub nofailover: bool,

    /// If true, this node won't receive read traffic from the proxy
    #[serde(default)]
    pub noloadbalance: bool,

    /// If true, this node cannot be used as a clone source
    #[serde(default)]
    pub noclone: bool,

    /// If true, this node should not participate in sync replication
    #[serde(default)]
    pub nosync: bool,

    /// If true, use WAL file-based recovery instead of streaming
    #[serde(default)]
    pub nostream: bool,

    /// If true, this node is preferred as a clone source
    #[serde(default)]
    pub clonefrom: bool,

    /// Replicate from this named node instead of primary (cascading)
    #[serde(default)]
    pub replicatefrom: Option<String>,

    /// Failover priority (higher = preferred, 0 = nofailover equivalent)
    #[serde(default = "default_failover_priority")]
    pub failover_priority: u32,

    /// Synchronous replication priority (higher = preferred)
    #[serde(default)]
    pub sync_priority: u32,
}

impl Default for Tags {
    fn default() -> Self {
        Self {
            nofailover: false,
            noloadbalance: false,
            noclone: false,
            nosync: false,
            nostream: false,
            clonefrom: false,
            replicatefrom: None,
            failover_priority: 1,
            sync_priority: 0,
        }
    }
}

/// Bootstrap configuration for new cluster initialization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapConfig {
    /// initdb options (encoding, locale, data-checksums, wal-segsize, etc.)
    #[serde(default)]
    pub initdb: Vec<InitdbOption>,

    /// Dynamic configuration to set on first bootstrap (written to DCS /config key)
    #[serde(default)]
    pub dcs: HashMap<String, serde_json::Value>,

    /// Post-init SQL scripts to execute (legacy field, same as post_bootstrap_sql)
    #[serde(default)]
    pub post_init: Vec<String>,

    /// Custom bootstrap command (alternative to initdb).
    /// When set, this command is executed instead of initdb.
    /// The PGDATA environment variable is set to the data directory path.
    #[serde(default)]
    pub custom_command: Option<String>,

    /// SQL statements to execute after successful bootstrap and PostgreSQL start.
    /// These are run once during initial cluster creation (e.g., CREATE USER, CREATE DATABASE).
    #[serde(default)]
    pub post_bootstrap_sql: Vec<String>,
}

/// A single initdb option (e.g., "--encoding=UTF8" or "--data-checksums")
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum InitdbOption {
    /// Key-value option like encoding: UTF8
    KeyValue(HashMap<String, String>),
    /// Flag option like "data-checksums"
    Flag(String),
}

// ──────────────────────────────── Defaults ────────────────────────────────

fn default_namespace() -> String {
    "service".to_string()
}

fn default_loop_wait() -> u64 {
    10
}

fn default_ttl() -> u64 {
    30
}

fn default_retry_timeout() -> u64 {
    10
}

fn default_pg_listen() -> String {
    "0.0.0.0".to_string()
}

fn default_pg_port() -> u16 {
    5432
}

fn default_dbname() -> String {
    "postgres".to_string()
}

fn default_api_listen() -> String {
    "0.0.0.0".to_string()
}

fn default_api_port() -> u16 {
    8008
}

fn default_proxy_listen() -> String {
    "0.0.0.0".to_string()
}

fn default_proxy_rw_port() -> u16 {
    6432
}

fn default_proxy_ro_port() -> u16 {
    6433
}

fn default_watchdog_mode() -> WatchdogMode {
    WatchdogMode::Off
}

fn default_watchdog_device() -> String {
    "/dev/watchdog".to_string()
}

fn default_watchdog_safety_margin() -> u64 {
    5
}

fn default_failover_priority() -> u32 {
    1
}

// ──────────────────────────────── Implementation ────────────────────────────────

/// Environment variable prefix for configuration overrides
const ENV_PREFIX: &str = "PG_HA_";

impl Config {
    /// Load configuration from a YAML file
    pub fn from_file(path: &Path) -> crate::Result<Self> {
        let content = std::fs::read_to_string(path).map_err(|e| {
            crate::Error::Config(format!("Cannot read config file '{}': {}", path.display(), e))
        })?;
        Self::from_yaml(&content)
    }

    /// Parse configuration from a YAML string
    pub fn from_yaml(yaml: &str) -> crate::Result<Self> {
        let config: Self = serde_yaml::from_str(yaml).map_err(|e| {
            crate::Error::Config(format!("Invalid YAML: {e}"))
        })?;
        config.validate()?;
        Ok(config)
    }

    /// Apply environment variable overrides with PG_HA_ prefix.
    ///
    /// Convention: PG_HA_NAME, PG_HA_SCOPE, PG_HA_POSTGRESQL__PORT, etc.
    /// Double underscore (__) separates nested levels.
    pub fn apply_env_overrides(&mut self) {
        if let Ok(val) = std::env::var(format!("{ENV_PREFIX}NAME")) {
            self.name = val;
        }
        if let Ok(val) = std::env::var(format!("{ENV_PREFIX}SCOPE")) {
            self.scope = val;
        }
        if let Ok(val) = std::env::var(format!("{ENV_PREFIX}LOOP_WAIT"))
            && let Ok(v) = val.parse() {
                self.loop_wait = v;
            }
        if let Ok(val) = std::env::var(format!("{ENV_PREFIX}TTL"))
            && let Ok(v) = val.parse() {
                self.ttl = v;
            }
        if let Ok(val) = std::env::var(format!("{ENV_PREFIX}RETRY_TIMEOUT"))
            && let Ok(v) = val.parse() {
                self.retry_timeout = v;
            }
        if let Ok(val) = std::env::var(format!("{ENV_PREFIX}POSTGRESQL__PORT"))
            && let Ok(v) = val.parse() {
                self.postgresql.port = v;
            }
        if let Ok(val) = std::env::var(format!("{ENV_PREFIX}POSTGRESQL__LISTEN")) {
            self.postgresql.listen = val;
        }
        if let Ok(val) = std::env::var(format!("{ENV_PREFIX}RESTAPI__PORT"))
            && let Ok(v) = val.parse() {
                self.restapi.port = v;
            }
        if let Ok(val) = std::env::var(format!("{ENV_PREFIX}RAFT__SELF_ADDR")) {
            self.raft.self_addr = val;
        }
    }

    /// Validate configuration values
    pub fn validate(&self) -> crate::Result<()> {
        if self.name.is_empty() {
            return Err(crate::Error::Config("'name' is required and cannot be empty".into()));
        }
        if self.scope.is_empty() {
            return Err(crate::Error::Config("'scope' is required and cannot be empty".into()));
        }
        if self.ttl <= self.loop_wait {
            return Err(crate::Error::Config(format!(
                "ttl ({}) must be greater than loop_wait ({})",
                self.ttl, self.loop_wait
            )));
        }
        if self.raft.self_addr.is_empty() {
            return Err(crate::Error::Config("raft.self_addr is required".into()));
        }
        if self.raft.partner_addrs.is_empty() {
            return Err(crate::Error::Config(
                "raft.partner_addrs must contain at least one peer address".into(),
            ));
        }
        Ok(())
    }

    /// Generate a sample configuration YAML string
    pub fn sample() -> String {
        let sample = Self {
            name: "node1".to_string(),
            scope: "pg-cluster".to_string(),
            namespace: "service".to_string(),
            loop_wait: 10,
            ttl: 30,
            retry_timeout: 10,
            postgresql: PostgresqlConfig {
                data_dir: PathBuf::from("/var/lib/postgresql/16/data"),
                bin_dir: PathBuf::from("/usr/lib/postgresql/16/bin"),
                listen: "0.0.0.0".to_string(),
                port: 5432,
                superuser: ConnectionParams {
                    username: "postgres".to_string(),
                    password: Some("secret".to_string()),
                    dbname: "postgres".to_string(),
                },
                replication: ConnectionParams {
                    username: "replicator".to_string(),
                    password: Some("secret".to_string()),
                    dbname: "postgres".to_string(),
                },
                parameters: HashMap::from([
                    ("max_connections".to_string(), "100".to_string()),
                    ("wal_level".to_string(), "replica".to_string()),
                    ("max_wal_senders".to_string(), "10".to_string()),
                    ("max_replication_slots".to_string(), "10".to_string()),
                    ("hot_standby".to_string(), "on".to_string()),
                ]),
            },
            restapi: RestApiConfig {
                listen: "0.0.0.0".to_string(),
                port: 8008,
                username: None,
                password: None,
            },
            raft: RaftConfig {
                self_addr: "node1:2380".to_string(),
                partner_addrs: vec!["node2:2380".to_string(), "node3:2380".to_string()],
                data_dir: Some(PathBuf::from("/var/lib/pg-ha/raft")),
                node_id: None,
            },
            proxy: ProxyConfig {
                rw_listen: "0.0.0.0".to_string(),
                rw_port: 6432,
                ro_listen: "0.0.0.0".to_string(),
                ro_port: 6433,
            },
            watchdog: WatchdogConfig {
                mode: WatchdogMode::Off,
                device: "/dev/watchdog".to_string(),
                safety_margin: 5,
            },
            tags: Tags::default(),
            bootstrap: Some(BootstrapConfig {
                initdb: vec![
                    InitdbOption::Flag("data-checksums".to_string()),
                    InitdbOption::KeyValue(HashMap::from([
                        ("encoding".to_string(), "UTF8".to_string()),
                    ])),
                ],
                dcs: HashMap::from([
                    ("loop_wait".into(), serde_json::json!(10)),
                    ("ttl".into(), serde_json::json!(30)),
                    ("maximum_lag_on_failover".into(), serde_json::json!(1048576)),
                ]),
                post_init: vec![],
                custom_command: None,
                post_bootstrap_sql: vec![],
            }),
        };
        serde_yaml::to_string(&sample).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL_YAML: &str = r#"
name: node1
scope: my-cluster
postgresql:
  data_dir: /tmp/pgdata
  bin_dir: /usr/bin
  superuser:
    username: postgres
  replication:
    username: replicator
restapi:
  listen: "0.0.0.0"
  port: 8008
raft:
  self_addr: "127.0.0.1:2380"
  partner_addrs:
    - "127.0.0.2:2380"
    - "127.0.0.3:2380"
proxy:
  rw_listen: "0.0.0.0"
  rw_port: 6432
  ro_listen: "0.0.0.0"
  ro_port: 6433
"#;

    #[test]
    fn test_parse_minimal_config() {
        let config = Config::from_yaml(MINIMAL_YAML).unwrap();
        assert_eq!(config.name, "node1");
        assert_eq!(config.scope, "my-cluster");
        assert_eq!(config.loop_wait, 10); // default
        assert_eq!(config.ttl, 30); // default
        assert_eq!(config.postgresql.port, 5432); // default
        assert_eq!(config.raft.partner_addrs.len(), 2);
    }

    #[test]
    fn test_validation_empty_name() {
        let yaml = MINIMAL_YAML.replace("name: node1", "name: \"\"");
        let result = Config::from_yaml(&yaml);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("name"));
    }

    #[test]
    fn test_validation_ttl_must_exceed_loop_wait() {
        let yaml = format!("{MINIMAL_YAML}\nttl: 5\nloop_wait: 10");
        let result = Config::from_yaml(&yaml);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("ttl"));
    }

    #[test]
    fn test_validation_empty_raft_partners() {
        let yaml = MINIMAL_YAML.replace(
            "partner_addrs:\n    - \"127.0.0.2:2380\"\n    - \"127.0.0.3:2380\"",
            "partner_addrs: []",
        );
        let result = Config::from_yaml(&yaml);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("partner_addrs"));
    }

    #[test]
    fn test_sample_config_is_valid_yaml() {
        let sample = Config::sample();
        let parsed: Result<Config, _> = serde_yaml::from_str(&sample);
        assert!(parsed.is_ok(), "Sample config should be valid YAML: {:?}", parsed.err());
    }

    #[test]
    fn test_tags_defaults() {
        let config = Config::from_yaml(MINIMAL_YAML).unwrap();
        assert!(!config.tags.nofailover);
        assert!(!config.tags.noloadbalance);
        assert!(!config.tags.nosync);
        // When tags section is omitted, Default is used → failover_priority = 1 from serde default
        assert_eq!(config.tags.failover_priority, 1);
        assert_eq!(config.tags.replicatefrom, None);
    }

    #[test]
    fn test_tags_custom() {
        let yaml = format!(
            "{MINIMAL_YAML}\ntags:\n  nofailover: true\n  failover_priority: 0\n  replicatefrom: node2"
        );
        let config = Config::from_yaml(&yaml).unwrap();
        assert!(config.tags.nofailover);
        assert_eq!(config.tags.failover_priority, 0);
        assert_eq!(config.tags.replicatefrom, Some("node2".to_string()));
    }

    #[test]
    fn test_watchdog_defaults() {
        let config = Config::from_yaml(MINIMAL_YAML).unwrap();
        assert_eq!(config.watchdog.mode, WatchdogMode::Off);
        assert_eq!(config.watchdog.device, "/dev/watchdog");
        assert_eq!(config.watchdog.safety_margin, 5);
    }
}
