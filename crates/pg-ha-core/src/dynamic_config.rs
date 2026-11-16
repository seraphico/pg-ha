//! Dynamic Configuration Management
//!
//! Handles cluster-wide configuration stored in the DCS /config key.
//! Changes are detected each HA cycle and applied:
//! - HA parameters (loop_wait, ttl, etc.) apply immediately
//! - PG parameters that need only a reload are applied via pg_ctl reload
//! - PG parameters that need a restart set a pending_restart flag

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{info, warn};

use crate::config::LeaseTiming;
use crate::standby_cluster::StandbyClusterConfig;
use crate::synchronous_mode::SynchronousMode;

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

    /// Maximum replication lag (bytes) tolerated on a synchronous standby
    /// before it is excluded from the synchronous set.
    ///
    /// `None` = off: because `u64` cannot be negative there is no numeric
    /// "disable" sentinel, so `None` is the only off state. Units are bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maximum_lag_on_syncnode: Option<u64>,

    /// Synchronous replication mode: `"async"` (default), `"sync"` or
    /// `"quorum"`. Anything else is rejected when the document is parsed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub synchronous_mode: Option<SynchronousMode>,

    /// Strict synchronous mode (no async fallback)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub synchronous_mode_strict: Option<bool>,

    /// Number of synchronous standbys required
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub synchronous_node_count: Option<u32>,

    /// Accepted for backward compatibility but ignored. Failsafe mode is not
    /// applicable under pg-ha's embedded RaftDcs (unreachable-consensus is
    /// handled by lease-expiry fencing, R19); see docs/invariants.md P3.
    /// Existing configs that set it keep parsing; the value has no effect.
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
        || old.maximum_lag_on_syncnode != new.maximum_lag_on_syncnode
        || old.synchronous_mode != new.synchronous_mode
        || old.synchronous_mode_strict != new.synchronous_mode_strict
        || old.synchronous_node_count != new.synchronous_node_count
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

    /// True once a valid `/config` (or a legitimately absent one) was seen.
    /// Without a baseline `last_config` is only the type default, not
    /// something the operator chose.
    pub has_baseline: bool,

    /// The stored `/config` that failed to parse, if it is still invalid.
    pub invalid: Option<InvalidConfig>,
}

/// A stored `/config` that could not be parsed, and when it was last logged.
#[derive(Debug, Clone)]
pub struct InvalidConfig {
    pub reason: String,
    raw_fingerprint: u64,
    last_logged: Option<Instant>,
}

/// How often the same invalid `/config` is logged again.
pub const CONFIG_ERROR_LOG_INTERVAL: Duration = Duration::from_secs(60);

impl DynamicConfigState {
    pub fn new() -> Self {
        Self {
            last_config: GlobalConfig::default(),
            pending_restart: false,
            pending_restart_params: HashMap::new(),
            has_baseline: false,
            invalid: None,
        }
    }

    /// Record that the stored `/config` (`raw`) does not parse. Returns true
    /// when the caller should log it now: the first time, when the stored text
    /// changed, and every `CONFIG_ERROR_LOG_INTERVAL`. `last_config` is not
    /// touched, so the last valid configuration stays in force.
    pub fn note_invalid(&mut self, raw: &str, reason: String, now: Instant) -> bool {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        raw.hash(&mut h);
        let fp = h.finish();
        let log_now = match &self.invalid {
            Some(i) if i.raw_fingerprint == fp => i
                .last_logged
                .is_none_or(|t| now.duration_since(t) >= CONFIG_ERROR_LOG_INTERVAL),
            _ => true,
        };
        let last_logged = if log_now {
            Some(now)
        } else {
            self.invalid.as_ref().and_then(|i| i.last_logged)
        };
        self.invalid = Some(InvalidConfig {
            reason,
            raw_fingerprint: fp,
            last_logged,
        });
        log_now
    }

    /// The stored `/config` is absent: that is the default (async) baseline.
    pub fn note_absent(&mut self) {
        self.has_baseline = true;
        self.invalid = None;
    }

    /// True when the stored `/config` is invalid and no valid one was ever
    /// applied: nothing says whether synchronous replication is wanted.
    pub fn sync_config_unresolved(&self) -> bool {
        !self.has_baseline && self.invalid.is_some()
    }

    /// Process a new config from DCS. Returns the detected changes.
    /// Caller is responsible for acting on the changes (reload PG, etc.)
    pub fn apply_new_config(&mut self, new_config: GlobalConfig) -> ConfigChanges {
        if self.invalid.take().is_some() {
            info!("stored /config is valid again; applying it");
        }
        self.has_baseline = true;
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

impl GlobalConfig {
    /// The effective synchronous mode (`async` when the key is absent).
    pub fn sync_mode(&self) -> SynchronousMode {
        self.synchronous_mode.unwrap_or_default()
    }

    /// Lease timing with this configuration's `ttl`, `loop_wait` and
    /// `retry_timeout` over a node's static values (`base`).
    pub fn lease_timing(&self, base: LeaseTiming) -> LeaseTiming {
        LeaseTiming {
            ttl: self.ttl.unwrap_or(base.ttl),
            loop_wait: self.loop_wait.unwrap_or(base.loop_wait),
            retry_timeout: self.retry_timeout.unwrap_or(base.retry_timeout),
            safety_margin: base.safety_margin,
        }
    }
}

/// PATCH / PUT /config: when the update changes `ttl`, `loop_wait` or
/// `retry_timeout`, the result must satisfy
/// `ttl >= loop_wait + retry_timeout + safety_margin` on the node that
/// receives it (`base` = its static values). Err carries the reason; the
/// caller must not write the update.
pub fn check_lease_timing_change(
    current: &GlobalConfig,
    new: &GlobalConfig,
    base: LeaseTiming,
) -> Result<(), String> {
    if current.ttl == new.ttl
        && current.loop_wait == new.loop_wait
        && current.retry_timeout == new.retry_timeout
    {
        return Ok(());
    }
    match new.lease_timing(base).violation() {
        Some(problem) => Err(problem),
        None => Ok(()),
    }
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
            maximum_lag_on_syncnode: Some(8388608),
            synchronous_mode: Some(SynchronousMode::Async),
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
    fn test_maximum_lag_on_syncnode_roundtrip_and_detect_and_patch() {
        // Round-trip: the field survives serialize/deserialize.
        let config = GlobalConfig {
            maximum_lag_on_syncnode: Some(8388608),
            ..Default::default()
        };
        let json = serde_json::to_string(&config).unwrap();
        let parsed: GlobalConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, parsed);
        assert_eq!(parsed.maximum_lag_on_syncnode, Some(8388608));

        // detect_changes flags ha_params_changed when only this field differs.
        let old = GlobalConfig::default();
        let changes = detect_changes(&old, &config);
        assert!(changes.ha_params_changed);

        // patch_config with null removes the field.
        let patch = serde_json::json!({ "maximum_lag_on_syncnode": null });
        let result = patch_config(&config, &patch).unwrap();
        assert_eq!(result.maximum_lag_on_syncnode, None);
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

    const ACCEPTED_TEXT: &str = r#"expected one of "async", "sync", "quorum" (lowercase)"#;

    #[test]
    fn test_synchronous_mode_parses_the_three_strings_and_absent_null() {
        for (raw, want) in [
            (
                r#"{"synchronous_mode":"async"}"#,
                Some(SynchronousMode::Async),
            ),
            (
                r#"{"synchronous_mode":"sync"}"#,
                Some(SynchronousMode::Sync),
            ),
            (
                r#"{"synchronous_mode":"quorum"}"#,
                Some(SynchronousMode::Quorum),
            ),
            (r#"{"synchronous_mode":null}"#, None),
            (r#"{}"#, None),
        ] {
            let c: GlobalConfig = serde_json::from_str(raw).unwrap();
            assert_eq!(c.synchronous_mode, want, "{raw}");
        }
        assert_eq!(GlobalConfig::default().sync_mode(), SynchronousMode::Async);
    }

    #[test]
    fn test_synchronous_mode_rejects_every_other_value() {
        for v in [
            "true",
            "false",
            "1",
            "0",
            "1.5",
            r#""on""#,
            r#""off""#,
            r#""true""#,
            r#""Sync""#,
            r#""QUORUM""#,
            r#""Async""#,
            r#""""#,
            r#"" sync""#,
            "[]",
            "{}",
        ] {
            let raw = format!(r#"{{"synchronous_mode":{v}}}"#);
            let err = serde_json::from_str::<GlobalConfig>(&raw)
                .expect_err(&raw)
                .to_string();
            assert!(
                err.contains("synchronous_mode: invalid value"),
                "{raw}: {err}"
            );
            assert!(err.contains(ACCEPTED_TEXT), "{raw}: {err}");
        }
    }

    #[test]
    fn test_patch_config_synchronous_mode() {
        let base = GlobalConfig::default();
        for bad in [
            serde_json::json!(true),
            serde_json::json!(false),
            serde_json::json!("on"),
            serde_json::json!("Sync"),
            serde_json::json!("QUORUM"),
            serde_json::json!(""),
            serde_json::json!(2),
        ] {
            let err =
                patch_config(&base, &serde_json::json!({ "synchronous_mode": bad })).unwrap_err();
            assert!(err.contains(ACCEPTED_TEXT), "{bad}: {err}");
        }
        let q = patch_config(&base, &serde_json::json!({"synchronous_mode": "quorum"})).unwrap();
        assert_eq!(q.synchronous_mode, Some(SynchronousMode::Quorum));
        assert_eq!(
            serde_json::to_value(&q).unwrap()["synchronous_mode"],
            serde_json::json!("quorum")
        );
        let removed = patch_config(&q, &serde_json::json!({"synchronous_mode": null})).unwrap();
        assert_eq!(removed.synchronous_mode, None);
    }

    #[test]
    fn test_detect_changes_flags_a_sync_to_quorum_switch() {
        let sync = GlobalConfig {
            synchronous_mode: Some(SynchronousMode::Sync),
            ..Default::default()
        };
        let quorum = GlobalConfig {
            synchronous_mode: Some(SynchronousMode::Quorum),
            ..Default::default()
        };
        assert!(detect_changes(&sync, &quorum).ha_params_changed);
        assert!(!detect_changes(&quorum, &quorum.clone()).ha_params_changed);
    }

    #[test]
    fn test_note_invalid_rate_limits_the_error_log() {
        let mut st = DynamicConfigState::new();
        let t0 = Instant::now();
        assert!(st.note_invalid("raw-a", "why".into(), t0));
        assert!(!st.note_invalid("raw-a", "why".into(), t0 + Duration::from_secs(5)));
        assert!(!st.note_invalid("raw-a", "why".into(), t0 + Duration::from_secs(59)));
        // The stored text changed: log at once.
        assert!(st.note_invalid("raw-b", "why".into(), t0 + Duration::from_secs(6)));
        assert!(!st.note_invalid("raw-b", "why".into(), t0 + Duration::from_secs(7)));
        // 60 s after the last log of the same text.
        assert!(st.note_invalid("raw-b", "why".into(), t0 + Duration::from_secs(66)));
        // Nothing here touched the applied config.
        assert_eq!(st.last_config, GlobalConfig::default());
        assert!(!st.has_baseline);
        assert!(st.sync_config_unresolved());
    }

    #[test]
    fn test_a_valid_config_clears_the_invalid_state() {
        let mut st = DynamicConfigState::new();
        st.note_invalid("x", "why".into(), Instant::now());
        assert!(st.sync_config_unresolved());
        st.apply_new_config(GlobalConfig::default());
        assert!(st.invalid.is_none());
        assert!(st.has_baseline);
        assert!(!st.sync_config_unresolved());
        // With a baseline, a later invalid document is not "unresolved".
        st.note_invalid("y", "why".into(), Instant::now());
        assert!(!st.sync_config_unresolved());
        st.note_absent();
        assert!(st.invalid.is_none() && st.has_baseline);
    }
}
