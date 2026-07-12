//! Dynamic Configuration Management
//!
//! Handles cluster-wide configuration stored in the DCS /config key.
//! Changes are detected each HA cycle and applied:
//! - HA parameters (loop_wait, ttl, etc.) apply immediately
//! - PG parameters that need only a reload are applied via pg_ctl reload
//! - PG parameters that need a restart set a pending_restart flag

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{info, warn};

use crate::standby_cluster::StandbyClusterConfig;

/// Dynamic cluster-wide configuration stored in DCS /config key.
///
/// This supplements the static YAML config with runtime-adjustable parameters.
/// All cluster members read this and apply changes each HA cycle.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GlobalConfig {
    /// HA loop interval in seconds
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub loop_wait: Option<u64>,

    /// Leader lock TTL in seconds
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl: Option<u64>,

    /// Timeout for retrying DCS/PG operations in seconds
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_timeout: Option<u64>,

    /// Maximum replication lag (bytes) for failover candidates
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maximum_lag_on_failover: Option<u64>,

    /// Enable synchronous replication mode
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub synchronous_mode: Option<bool>,

    /// Strict synchronous mode (no async fallback)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub synchronous_mode_strict: Option<bool>,

    /// Number of synchronous standbys required
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub synchronous_node_count: Option<u32>,

    /// Enable failsafe mode for DCS outage handling
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failsafe_mode: Option<bool>,

    /// Whether the cluster is paused
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pause: Option<bool>,

    /// PostgreSQL parameters section
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub postgresql: Option<PostgresqlDynamicConfig>,

    /// Standby cluster configuration (when present, cluster operates in standby mode)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub standby_cluster: Option<StandbyClusterConfig>,
}

/// Dynamic PostgreSQL parameter configuration
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PostgresqlDynamicConfig {
    /// PostgreSQL parameters (name -> value)
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub parameters: HashMap<String, String>,
}

/// Classification of how a PostgreSQL parameter change must be applied
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgParamClassification {
    /// Parameter can be applied with pg_ctl reload (SIGHUP)
    NeedsReload,
    /// Parameter requires a full PostgreSQL restart
    NeedsRestart,
}

/// Known PostgreSQL parameters that require a restart (postmaster context).
/// These cannot be changed without stopping and restarting PostgreSQL.
const RESTART_PARAMS: &[&str] = &[
    "shared_buffers",
    "max_connections",
    "max_prepared_transactions",
    "max_worker_processes",
    "max_wal_senders",
    "max_replication_slots",
    "wal_level",
    "wal_buffers",
    "max_locks_per_transaction",
    "max_pred_locks_per_transaction",
    "track_commit_timestamp",
    "shared_preload_libraries",
    "listen_addresses",
    "port",
    "superuser_reserved_connections",
    "unix_socket_directories",
    "huge_pages",
    "cluster_name",
    "bonjour",
    "bonjour_name",
    "ssl",
    "ssl_ca_file",
    "ssl_cert_file",
    "ssl_key_file",
    "ssl_crl_file",
    "password_encryption",
    "hot_standby",
    "max_files_per_process",
    "archive_mode",
    "restore_command",
    "recovery_target",
    "recovery_target_name",
    "recovery_target_time",
    "recovery_target_xid",
    "recovery_target_lsn",
    "recovery_target_inclusive",
    "recovery_target_timeline",
    "recovery_target_action",
    "primary_conninfo",
    "primary_slot_name",
];

/// Classify a PostgreSQL parameter as needing reload or restart.
pub fn classify_pg_param(name: &str) -> PgParamClassification {
    let lower = name.to_lowercase();
    if RESTART_PARAMS.iter().any(|&p| p == lower) {
        PgParamClassification::NeedsRestart
    } else {
        PgParamClassification::NeedsReload
    }
}

/// Detected changes between two GlobalConfig instances
#[derive(Debug, Clone, Default)]
pub struct ConfigChanges {
    /// HA-level parameters that changed (applied immediately)
    pub ha_params_changed: bool,

    /// PG parameters that only need a reload
    pub pg_reload_params: HashMap<String, String>,

    /// PG parameters that need a restart
    pub pg_restart_params: HashMap<String, String>,

    /// PG parameters that were removed
    pub pg_removed_params: Vec<String>,
}

impl ConfigChanges {
    /// Returns true if there are any changes
    pub fn has_changes(&self) -> bool {
        self.ha_params_changed
            || !self.pg_reload_params.is_empty()
            || !self.pg_restart_params.is_empty()
            || !self.pg_removed_params.is_empty()
    }

    /// Returns true if any PG parameter changes require a restart
    pub fn needs_restart(&self) -> bool {
        !self.pg_restart_params.is_empty()
    }

    /// Returns true if any PG parameter changes require only a reload
    pub fn needs_reload(&self) -> bool {
        !self.pg_reload_params.is_empty() || !self.pg_removed_params.is_empty()
    }
}

/// Compare old and new GlobalConfig and produce a ConfigChanges result.
pub fn detect_changes(old: &GlobalConfig, new: &GlobalConfig) -> ConfigChanges {
    let mut changes = ConfigChanges::default();

    // Check HA-level parameter changes
    if old.loop_wait != new.loop_wait
        || old.ttl != new.ttl
        || old.retry_timeout != new.retry_timeout
        || old.maximum_lag_on_failover != new.maximum_lag_on_failover
        || old.synchronous_mode != new.synchronous_mode
        || old.synchronous_mode_strict != new.synchronous_mode_strict
        || old.synchronous_node_count != new.synchronous_node_count
        || old.failsafe_mode != new.failsafe_mode
        || old.pause != new.pause
        || old.standby_cluster != new.standby_cluster
    {
        changes.ha_params_changed = true;
    }

    // Check PG parameter changes
    let old_params = old
        .postgresql
        .as_ref()
        .map(|p| &p.parameters)
        .cloned()
        .unwrap_or_default();
    let new_params = new
        .postgresql
        .as_ref()
        .map(|p| &p.parameters)
        .cloned()
        .unwrap_or_default();

    // Detect added or modified params
    for (name, new_val) in &new_params {
        let changed = match old_params.get(name) {
            Some(old_val) => old_val != new_val,
            None => true, // new param added
        };
        if changed {
            match classify_pg_param(name) {
                PgParamClassification::NeedsReload => {
                    changes
                        .pg_reload_params
                        .insert(name.clone(), new_val.clone());
                }
                PgParamClassification::NeedsRestart => {
                    changes
                        .pg_restart_params
                        .insert(name.clone(), new_val.clone());
                }
            }
        }
    }

    // Detect removed params
    for name in old_params.keys() {
        if !new_params.contains_key(name) {
            changes.pg_removed_params.push(name.clone());
        }
    }

    changes
}

/// Tracks the pending_restart state and last known global config.
#[derive(Debug, Clone)]
pub struct DynamicConfigState {
    /// Last known global config from DCS
    pub last_config: GlobalConfig,

    /// Whether a PG restart is pending (params changed that need restart)
    pub pending_restart: bool,

    /// Parameters that triggered the pending restart
    pub pending_restart_params: HashMap<String, String>,
}

impl DynamicConfigState {
    pub fn new() -> Self {
        Self {
            last_config: GlobalConfig::default(),
            pending_restart: false,
            pending_restart_params: HashMap::new(),
        }
    }

    /// Process a new config from DCS. Returns the detected changes.
    /// Caller is responsible for acting on the changes (reload PG, etc.)
    pub fn apply_new_config(&mut self, new_config: GlobalConfig) -> ConfigChanges {
        let changes = detect_changes(&self.last_config, &new_config);

        if changes.has_changes() {
            info!("Dynamic configuration change detected");

            if changes.needs_restart() {
                warn!(
                    params = ?changes.pg_restart_params.keys().collect::<Vec<_>>(),
                    "PostgreSQL restart required for parameter changes — setting pending_restart"
                );
                self.pending_restart = true;
                self.pending_restart_params
                    .extend(changes.pg_restart_params.clone());
            }

            if changes.ha_params_changed {
                info!("HA parameters changed, applying immediately");
            }
        }

        self.last_config = new_config;
        changes
    }

    /// Clear the pending_restart flag (called after an explicit restart)
    pub fn clear_pending_restart(&mut self) {
        self.pending_restart = false;
        self.pending_restart_params.clear();
    }
}

impl Default for DynamicConfigState {
    fn default() -> Self {
        Self::new()
    }
}

/// Apply a partial update (PATCH semantics) to a GlobalConfig.
/// Keys set to null in the patch are removed from the config.
pub fn patch_config(
    base: &GlobalConfig,
    patch: &serde_json::Value,
) -> Result<GlobalConfig, String> {
    // Serialize base to a JSON Value
    let mut base_value =
        serde_json::to_value(base).map_err(|e| format!("Failed to serialize base config: {e}"))?;

    // Apply patch using recursive merge
    merge_json_patch(&mut base_value, patch);

    // Deserialize back to GlobalConfig
    serde_json::from_value(base_value).map_err(|e| format!("Invalid config after patch: {e}"))
}

/// Recursively merge a JSON patch into a base value.
/// Keys set to null in the patch are removed from the base.
fn merge_json_patch(base: &mut serde_json::Value, patch: &serde_json::Value) {
    if let (serde_json::Value::Object(base_map), serde_json::Value::Object(patch_map)) =
        (base, patch)
    {
        for (key, value) in patch_map {
            if value.is_null() {
                base_map.remove(key);
            } else if value.is_object() {
                // Recurse into nested objects
                let entry = base_map
                    .entry(key.clone())
                    .or_insert_with(|| serde_json::json!({}));
                merge_json_patch(entry, value);
            } else {
                base_map.insert(key.clone(), value.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_global_config_serialization() {
        let config = GlobalConfig {
            loop_wait: Some(10),
            ttl: Some(30),
            retry_timeout: Some(10),
            maximum_lag_on_failover: Some(1048576),
            synchronous_mode: Some(false),
            synchronous_mode_strict: None,
            synchronous_node_count: Some(1),
            failsafe_mode: Some(true),
            pause: None,
            postgresql: Some(PostgresqlDynamicConfig {
                parameters: HashMap::from([
                    ("max_connections".to_string(), "100".to_string()),
                    ("work_mem".to_string(), "64MB".to_string()),
                ]),
            }),
            standby_cluster: None,
        };

        let json = serde_json::to_string(&config).unwrap();
        let parsed: GlobalConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, parsed);
    }

    #[test]
    fn test_global_config_deserialization_empty() {
        let json = "{}";
        let config: GlobalConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config, GlobalConfig::default());
    }

    #[test]
    fn test_global_config_deserialization_partial() {
        let json = r#"{"loop_wait": 15, "ttl": 45}"#;
        let config: GlobalConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.loop_wait, Some(15));
        assert_eq!(config.ttl, Some(45));
        assert_eq!(config.retry_timeout, None);
    }

    #[test]
    fn test_classify_restart_params() {
        assert_eq!(
            classify_pg_param("shared_buffers"),
            PgParamClassification::NeedsRestart
        );
        assert_eq!(
            classify_pg_param("max_connections"),
            PgParamClassification::NeedsRestart
        );
        assert_eq!(
            classify_pg_param("wal_level"),
            PgParamClassification::NeedsRestart
        );
        assert_eq!(
            classify_pg_param("max_worker_processes"),
            PgParamClassification::NeedsRestart
        );
        assert_eq!(
            classify_pg_param("max_wal_senders"),
            PgParamClassification::NeedsRestart
        );
    }

    #[test]
    fn test_classify_reload_params() {
        assert_eq!(
            classify_pg_param("work_mem"),
            PgParamClassification::NeedsReload
        );
        assert_eq!(
            classify_pg_param("maintenance_work_mem"),
            PgParamClassification::NeedsReload
        );
        assert_eq!(
            classify_pg_param("effective_cache_size"),
            PgParamClassification::NeedsReload
        );
        assert_eq!(
            classify_pg_param("log_min_duration_statement"),
            PgParamClassification::NeedsReload
        );
        assert_eq!(
            classify_pg_param("synchronous_commit"),
            PgParamClassification::NeedsReload
        );
    }

    #[test]
    fn test_classify_case_insensitive() {
        assert_eq!(
            classify_pg_param("Shared_Buffers"),
            PgParamClassification::NeedsRestart
        );
        assert_eq!(
            classify_pg_param("WORK_MEM"),
            PgParamClassification::NeedsReload
        );
    }

    #[test]
    fn test_detect_changes_no_changes() {
        let config = GlobalConfig {
            loop_wait: Some(10),
            ttl: Some(30),
            ..Default::default()
        };
        let changes = detect_changes(&config, &config);
        assert!(!changes.has_changes());
    }

    #[test]
    fn test_detect_changes_ha_params() {
        let old = GlobalConfig {
            loop_wait: Some(10),
            ttl: Some(30),
            ..Default::default()
        };
        let new = GlobalConfig {
            loop_wait: Some(15),
            ttl: Some(30),
            ..Default::default()
        };
        let changes = detect_changes(&old, &new);
        assert!(changes.ha_params_changed);
        assert!(!changes.needs_restart());
        assert!(!changes.needs_reload());
    }

    #[test]
    fn test_detect_changes_pg_reload_param() {
        let old = GlobalConfig {
            postgresql: Some(PostgresqlDynamicConfig {
                parameters: HashMap::from([("work_mem".to_string(), "32MB".to_string())]),
            }),
            ..Default::default()
        };
        let new = GlobalConfig {
            postgresql: Some(PostgresqlDynamicConfig {
                parameters: HashMap::from([("work_mem".to_string(), "64MB".to_string())]),
            }),
            ..Default::default()
        };
        let changes = detect_changes(&old, &new);
        assert!(!changes.ha_params_changed);
        assert!(changes.needs_reload());
        assert!(!changes.needs_restart());
        assert_eq!(changes.pg_reload_params.get("work_mem").unwrap(), "64MB");
    }

    #[test]
    fn test_detect_changes_pg_restart_param() {
        let old = GlobalConfig {
            postgresql: Some(PostgresqlDynamicConfig {
                parameters: HashMap::from([("max_connections".to_string(), "100".to_string())]),
            }),
            ..Default::default()
        };
        let new = GlobalConfig {
            postgresql: Some(PostgresqlDynamicConfig {
                parameters: HashMap::from([("max_connections".to_string(), "200".to_string())]),
            }),
            ..Default::default()
        };
        let changes = detect_changes(&old, &new);
        assert!(changes.needs_restart());
        assert!(!changes.needs_reload());
        assert_eq!(
            changes.pg_restart_params.get("max_connections").unwrap(),
            "200"
        );
    }

    #[test]
    fn test_detect_changes_pg_param_removed() {
        let old = GlobalConfig {
            postgresql: Some(PostgresqlDynamicConfig {
                parameters: HashMap::from([
                    ("work_mem".to_string(), "32MB".to_string()),
                    ("shared_buffers".to_string(), "256MB".to_string()),
                ]),
            }),
            ..Default::default()
        };
        let new = GlobalConfig {
            postgresql: Some(PostgresqlDynamicConfig {
                parameters: HashMap::from([("shared_buffers".to_string(), "256MB".to_string())]),
            }),
            ..Default::default()
        };
        let changes = detect_changes(&old, &new);
        assert!(changes.pg_removed_params.contains(&"work_mem".to_string()));
    }

    #[test]
    fn test_detect_changes_mixed() {
        let old = GlobalConfig {
            loop_wait: Some(10),
            postgresql: Some(PostgresqlDynamicConfig {
                parameters: HashMap::from([
                    ("work_mem".to_string(), "32MB".to_string()),
                    ("max_connections".to_string(), "100".to_string()),
                ]),
            }),
            ..Default::default()
        };
        let new = GlobalConfig {
            loop_wait: Some(15),
            postgresql: Some(PostgresqlDynamicConfig {
                parameters: HashMap::from([
                    ("work_mem".to_string(), "64MB".to_string()),
                    ("max_connections".to_string(), "200".to_string()),
                ]),
            }),
            ..Default::default()
        };
        let changes = detect_changes(&old, &new);
        assert!(changes.ha_params_changed);
        assert!(changes.needs_reload());
        assert!(changes.needs_restart());
    }

    #[test]
    fn test_dynamic_config_state_apply() {
        let mut state = DynamicConfigState::new();

        // First apply — everything is a change from default
        let config = GlobalConfig {
            loop_wait: Some(10),
            postgresql: Some(PostgresqlDynamicConfig {
                parameters: HashMap::from([("work_mem".to_string(), "32MB".to_string())]),
            }),
            ..Default::default()
        };
        let changes = state.apply_new_config(config.clone());
        assert!(changes.has_changes());
        assert!(!state.pending_restart);

        // Same config again — no changes
        let changes = state.apply_new_config(config);
        assert!(!changes.has_changes());

        // Restart param change
        let new_config = GlobalConfig {
            loop_wait: Some(10),
            postgresql: Some(PostgresqlDynamicConfig {
                parameters: HashMap::from([
                    ("work_mem".to_string(), "32MB".to_string()),
                    ("max_connections".to_string(), "200".to_string()),
                ]),
            }),
            ..Default::default()
        };
        let changes = state.apply_new_config(new_config);
        assert!(changes.needs_restart());
        assert!(state.pending_restart);
        assert!(state.pending_restart_params.contains_key("max_connections"));
    }

    #[test]
    fn test_dynamic_config_state_clear_restart() {
        let mut state = DynamicConfigState::new();
        state.pending_restart = true;
        state
            .pending_restart_params
            .insert("max_connections".to_string(), "200".to_string());

        state.clear_pending_restart();
        assert!(!state.pending_restart);
        assert!(state.pending_restart_params.is_empty());
    }

    #[test]
    fn test_patch_config_basic() {
        let base = GlobalConfig {
            loop_wait: Some(10),
            ttl: Some(30),
            ..Default::default()
        };
        let patch = serde_json::json!({
            "loop_wait": 15,
            "maximum_lag_on_failover": 2097152
        });

        let result = patch_config(&base, &patch).unwrap();
        assert_eq!(result.loop_wait, Some(15));
        assert_eq!(result.ttl, Some(30)); // unchanged
        assert_eq!(result.maximum_lag_on_failover, Some(2097152));
    }

    #[test]
    fn test_patch_config_null_removes_key() {
        let base = GlobalConfig {
            loop_wait: Some(10),
            ttl: Some(30),
            maximum_lag_on_failover: Some(1048576),
            ..Default::default()
        };
        let patch = serde_json::json!({
            "maximum_lag_on_failover": null
        });

        let result = patch_config(&base, &patch).unwrap();
        assert_eq!(result.loop_wait, Some(10));
        assert_eq!(result.ttl, Some(30));
        assert_eq!(result.maximum_lag_on_failover, None); // removed
    }

    #[test]
    fn test_patch_config_postgresql_parameters() {
        let base = GlobalConfig {
            postgresql: Some(PostgresqlDynamicConfig {
                parameters: HashMap::from([
                    ("work_mem".to_string(), "32MB".to_string()),
                    ("shared_buffers".to_string(), "256MB".to_string()),
                ]),
            }),
            ..Default::default()
        };
        let patch = serde_json::json!({
            "postgresql": {
                "parameters": {
                    "work_mem": "64MB",
                    "shared_buffers": null
                }
            }
        });

        let result = patch_config(&base, &patch).unwrap();
        let params = &result.postgresql.unwrap().parameters;
        assert_eq!(params.get("work_mem").unwrap(), "64MB");
        assert!(!params.contains_key("shared_buffers")); // removed by null
    }

    #[test]
    fn test_patch_config_invalid_patch() {
        let base = GlobalConfig::default();
        let patch = serde_json::json!("not an object");
        // A non-object patch doesn't change anything, the base is returned as-is
        let result = patch_config(&base, &patch).unwrap();
        assert_eq!(result, base);
    }
}
