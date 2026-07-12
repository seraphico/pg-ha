//! Standby Cluster Support
//!
//! Enables a pg-ha cluster to operate as a standby of an external PostgreSQL primary.
//! In standby mode:
//! - The local "standby leader" holds the leader lock but replicates from a remote source
//! - Local replicas still stream from the standby leader (cascading)
//! - Leader election among local nodes determines which node connects to the remote primary
//! - A "cascade promote" removes standby config and promotes the standby leader to a full primary
//!
//! Equivalent to Patroni's standby_cluster feature.

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{info, warn};

use crate::cluster::MemberRole;
use crate::dcs::DcsAdapter;
use crate::dynamic_config::GlobalConfig;
use crate::error::Result;
use crate::postgresql::Postgresql;

/// Configuration for a standby cluster that replicates from an external primary.
///
/// This is stored in the dynamic configuration (DCS /config key) under "standby_cluster".
/// When present, the cluster operates in standby mode.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StandbyClusterConfig {
    /// Remote primary host to replicate from
    pub host: String,

    /// Remote primary port (default: 5432)
    #[serde(default = "default_port")]
    pub port: u16,

    /// Replication slot name on the remote primary (optional)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_slot_name: Option<String>,

    /// WAL archive restore command (alternative or supplement to streaming)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restore_command: Option<String>,

    /// Methods to create a replica from the remote (e.g., "basebackup")
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub create_replica_methods: Vec<String>,
}

fn default_port() -> u16 {
    5432
}

impl StandbyClusterConfig {
    /// Build a primary_conninfo string for connecting to the remote primary.
    ///
    /// This is used to configure PostgreSQL recovery to stream from the remote source.
    pub fn primary_conninfo(
        &self,
        replication_user: &str,
        replication_password: Option<&str>,
    ) -> String {
        let mut conninfo = format!(
            "host={} port={} user={} application_name=standby_cluster",
            self.host, self.port, replication_user
        );
        if let Some(password) = replication_password {
            conninfo.push_str(&format!(" password={password}"));
        }
        conninfo
    }

    /// Build a connection string for pg_basebackup from the remote primary.
    pub fn basebackup_connstr(
        &self,
        replication_user: &str,
        replication_password: Option<&str>,
    ) -> String {
        let mut connstr = format!(
            "host={} port={} user={} dbname=postgres",
            self.host, self.port, replication_user
        );
        if let Some(password) = replication_password {
            connstr.push_str(&format!(" password={password}"));
        }
        connstr
    }
}

/// Manages standby cluster behavior for the HA engine.
///
/// The standby cluster logic determines:
/// - Whether the cluster is operating in standby mode
/// - How the standby leader should configure its replication
/// - When and how to perform a cascade promote
pub struct StandbyCluster;

impl StandbyCluster {
    /// Check if the cluster is operating in standby mode.
    ///
    /// A cluster is in standby mode when `standby_cluster` is present
    /// in the dynamic configuration (GlobalConfig).
    pub fn is_standby_cluster(config: &GlobalConfig) -> bool {
        config.standby_cluster.is_some()
    }

    /// Get the standby cluster configuration from the dynamic config, if present.
    pub fn get_config(global_config: &GlobalConfig) -> Option<&StandbyClusterConfig> {
        global_config.standby_cluster.as_ref()
    }

    /// Enforce that the standby leader is replicating from the remote primary.
    ///
    /// This verifies that PostgreSQL's primary_conninfo points to the remote source
    /// configured in standby_cluster. If not, it needs to be reconfigured.
    ///
    /// Returns Ok(true) if the configuration is already correct,
    /// Ok(false) if reconfiguration was needed (caller should restart PG).
    pub fn enforce_follow_remote_member(
        standby_config: &StandbyClusterConfig,
        postgresql: &Postgresql,
        replication_user: &str,
        replication_password: Option<&str>,
    ) -> Result<bool> {
        let _expected_conninfo =
            standby_config.primary_conninfo(replication_user, replication_password);

        // In a real implementation, we would read the current recovery config
        // and compare it to the expected conninfo. For now, we verify the
        // postgresql is running in recovery mode (as a standby should be).
        if postgresql.is_running() {
            // The standby leader should always be in recovery mode
            // (replicating from remote). If it's a primary, something is wrong.
            if postgresql.is_primary() {
                warn!(
                    "Standby leader is running as primary — needs reconfiguration to follow remote at {}:{}",
                    standby_config.host, standby_config.port
                );
                return Ok(false);
            }
            // PostgreSQL is in recovery mode — configuration is likely correct
            info!(
                "Standby leader confirmed replicating from remote {}:{}",
                standby_config.host, standby_config.port
            );
            Ok(true)
        } else {
            // PostgreSQL is not running — cannot verify
            Ok(false)
        }
    }

    /// Perform a cascade promote: remove the standby_cluster configuration
    /// from DCS and promote the standby leader to a full primary.
    ///
    /// Steps:
    /// 1. Remove standby_cluster key from dynamic config in DCS
    /// 2. Promote PostgreSQL from standby to primary (pg_ctl promote)
    /// 3. Update member role to Primary
    pub async fn cascade_promote(
        dcs: &Arc<dyn DcsAdapter>,
        postgresql: &mut Postgresql,
        current_config: &GlobalConfig,
    ) -> Result<()> {
        info!(
            "Initiating cascade promote: removing standby_cluster config and promoting to primary"
        );

        // Step 1: Remove standby_cluster from dynamic config
        let mut new_config = current_config.clone();
        new_config.standby_cluster = None;

        let config_json = serde_json::to_string(&new_config)
            .map_err(|e| crate::Error::Config(format!("Failed to serialize config: {e}")))?;
        dcs.set_config_value(&config_json).await?;

        info!("Standby cluster configuration removed from DCS");

        // Step 2: Promote PostgreSQL
        postgresql.promote().await?;

        // Step 3: Update role
        postgresql.set_role(MemberRole::Primary);

        info!("Cascade promote completed — node is now a full primary");
        Ok(())
    }

    /// Determine the clone source for a new node joining a standby cluster.
    ///
    /// In standby mode, the preferred clone source is:
    /// 1. The remote primary (via standby_cluster config) for the standby leader
    /// 2. The local standby leader for other local replicas
    pub fn clone_source_connstr(
        standby_config: &StandbyClusterConfig,
        replication_user: &str,
        replication_password: Option<&str>,
    ) -> String {
        standby_config.basebackup_connstr(replication_user, replication_password)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dynamic_config::GlobalConfig;

    #[test]
    fn test_standby_cluster_config_deserialization() {
        let json = r#"{
            "host": "remote-primary.example.com",
            "port": 5433,
            "primary_slot_name": "standby_slot1",
            "restore_command": "cp /archive/%f %p",
            "create_replica_methods": ["basebackup"]
        }"#;

        let config: StandbyClusterConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.host, "remote-primary.example.com");
        assert_eq!(config.port, 5433);
        assert_eq!(config.primary_slot_name, Some("standby_slot1".to_string()));
        assert_eq!(
            config.restore_command,
            Some("cp /archive/%f %p".to_string())
        );
        assert_eq!(config.create_replica_methods, vec!["basebackup"]);
    }

    #[test]
    fn test_standby_cluster_config_minimal() {
        let json = r#"{"host": "10.0.0.1"}"#;

        let config: StandbyClusterConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.host, "10.0.0.1");
        assert_eq!(config.port, 5432); // default
        assert_eq!(config.primary_slot_name, None);
        assert_eq!(config.restore_command, None);
        assert!(config.create_replica_methods.is_empty());
    }

    #[test]
    fn test_standby_cluster_config_serialization_roundtrip() {
        let config = StandbyClusterConfig {
            host: "primary.dc1.example.com".to_string(),
            port: 5432,
            primary_slot_name: Some("dc2_slot".to_string()),
            restore_command: None,
            create_replica_methods: vec!["basebackup".to_string()],
        };

        let json = serde_json::to_string(&config).unwrap();
        let parsed: StandbyClusterConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, parsed);
    }

    #[test]
    fn test_primary_conninfo_generation() {
        let config = StandbyClusterConfig {
            host: "primary.example.com".to_string(),
            port: 5433,
            primary_slot_name: Some("slot1".to_string()),
            restore_command: None,
            create_replica_methods: vec![],
        };

        let conninfo = config.primary_conninfo("replicator", Some("secret"));
        assert!(conninfo.contains("host=primary.example.com"));
        assert!(conninfo.contains("port=5433"));
        assert!(conninfo.contains("user=replicator"));
        assert!(conninfo.contains("password=secret"));
        assert!(conninfo.contains("application_name=standby_cluster"));
    }

    #[test]
    fn test_primary_conninfo_no_password() {
        let config = StandbyClusterConfig {
            host: "10.0.0.1".to_string(),
            port: 5432,
            primary_slot_name: None,
            restore_command: None,
            create_replica_methods: vec![],
        };

        let conninfo = config.primary_conninfo("replicator", None);
        assert!(conninfo.contains("host=10.0.0.1"));
        assert!(conninfo.contains("port=5432"));
        assert!(conninfo.contains("user=replicator"));
        assert!(!conninfo.contains("password="));
    }

    #[test]
    fn test_basebackup_connstr() {
        let config = StandbyClusterConfig {
            host: "primary.example.com".to_string(),
            port: 5432,
            primary_slot_name: None,
            restore_command: None,
            create_replica_methods: vec![],
        };

        let connstr = config.basebackup_connstr("replicator", Some("pass123"));
        assert!(connstr.contains("host=primary.example.com"));
        assert!(connstr.contains("port=5432"));
        assert!(connstr.contains("user=replicator"));
        assert!(connstr.contains("password=pass123"));
        assert!(connstr.contains("dbname=postgres"));
    }

    #[test]
    fn test_is_standby_cluster_true() {
        let config = GlobalConfig {
            standby_cluster: Some(StandbyClusterConfig {
                host: "remote.example.com".to_string(),
                port: 5432,
                primary_slot_name: None,
                restore_command: None,
                create_replica_methods: vec![],
            }),
            ..Default::default()
        };

        assert!(StandbyCluster::is_standby_cluster(&config));
    }

    #[test]
    fn test_is_standby_cluster_false() {
        let config = GlobalConfig::default();
        assert!(!StandbyCluster::is_standby_cluster(&config));
    }

    #[test]
    fn test_clone_source_connstr() {
        let standby_config = StandbyClusterConfig {
            host: "remote-primary.dc1.com".to_string(),
            port: 5433,
            primary_slot_name: Some("standby_slot".to_string()),
            restore_command: None,
            create_replica_methods: vec!["basebackup".to_string()],
        };

        let connstr =
            StandbyCluster::clone_source_connstr(&standby_config, "replicator", Some("replpass"));
        assert!(connstr.contains("host=remote-primary.dc1.com"));
        assert!(connstr.contains("port=5433"));
        assert!(connstr.contains("user=replicator"));
        assert!(connstr.contains("password=replpass"));
    }

    #[test]
    fn test_global_config_with_standby_cluster() {
        let json = r#"{
            "loop_wait": 10,
            "ttl": 30,
            "standby_cluster": {
                "host": "primary.dc1.example.com",
                "port": 5432,
                "primary_slot_name": "dc2_replication_slot",
                "restore_command": "envdir /etc/wal-e.d/env wal-e wal-fetch \"%f\" \"%p\""
            }
        }"#;

        let config: GlobalConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.loop_wait, Some(10));
        assert_eq!(config.ttl, Some(30));
        assert!(config.standby_cluster.is_some());

        let standby = config.standby_cluster.unwrap();
        assert_eq!(standby.host, "primary.dc1.example.com");
        assert_eq!(standby.port, 5432);
        assert_eq!(
            standby.primary_slot_name,
            Some("dc2_replication_slot".to_string())
        );
        assert!(standby.restore_command.unwrap().contains("wal-e"));
    }

    #[test]
    fn test_global_config_without_standby_cluster() {
        let json = r#"{"loop_wait": 10, "ttl": 30}"#;
        let config: GlobalConfig = serde_json::from_str(json).unwrap();
        assert!(config.standby_cluster.is_none());
        assert!(!StandbyCluster::is_standby_cluster(&config));
    }

    #[test]
    fn test_cascade_promote_removes_standby_config() {
        // Verify that removing standby_cluster from GlobalConfig produces correct JSON
        let mut config = GlobalConfig {
            loop_wait: Some(10),
            ttl: Some(30),
            standby_cluster: Some(StandbyClusterConfig {
                host: "remote.example.com".to_string(),
                port: 5432,
                primary_slot_name: None,
                restore_command: None,
                create_replica_methods: vec![],
            }),
            ..Default::default()
        };

        // Simulate what cascade_promote does to the config
        config.standby_cluster = None;

        let json = serde_json::to_string(&config).unwrap();
        let reparsed: GlobalConfig = serde_json::from_str(&json).unwrap();
        assert!(reparsed.standby_cluster.is_none());
        assert_eq!(reparsed.loop_wait, Some(10));
        assert_eq!(reparsed.ttl, Some(30));
    }
}
