//! Failsafe mode: survive temporary DCS outages without unnecessary failover.
//!
//! When enabled, the primary contacts all known replicas via REST API before
//! deciding to demote. If ALL replicas confirm the primary is alive, it continues.
//! If any replica doesn't respond, demotion proceeds.
//!
//! This prevents unnecessary failovers during brief Raft/network hiccups.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tracing::{debug, info, warn};

/// Failsafe state tracker
pub struct Failsafe {
    /// Whether failsafe mode is enabled (from dynamic config)
    enabled: bool,
    /// Known cluster members (name → api_url) for failsafe pings
    members: HashMap<String, String>,
    /// Last time the failsafe check passed
    last_success: Option<Instant>,
    /// TTL for caching failsafe result
    ttl: Duration,
    /// HTTP client for pinging members
    client: reqwest::Client,
}

/// Result of a failsafe topology check
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailsafeResult {
    /// All members confirmed — safe to continue as primary
    AllConfirmed,
    /// Some members did not respond — must demote
    NotAllConfirmed { failed: Vec<String> },
    /// Failsafe mode is disabled
    Disabled,
}

impl Failsafe {
    pub fn new(ttl_secs: u64) -> Self {
        Self {
            enabled: false,
            members: HashMap::new(),
            last_success: None,
            ttl: Duration::from_secs(ttl_secs),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(2))
                .build()
                .unwrap_or_default(),
        }
    }

    /// Update the failsafe enabled flag (from dynamic config)
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// Update the known members list (called by leader each cycle)
    pub fn update_members(&mut self, members: HashMap<String, String>) {
        self.members = members;
    }

    /// Whether failsafe mode is currently enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Whether the failsafe check passed recently (within TTL)
    pub fn is_active(&self) -> bool {
        self.last_success
            .is_some_and(|t| t.elapsed() < self.ttl)
    }

    /// Check the failsafe topology: ping all known members via POST /failsafe.
    /// Returns whether all members confirmed the primary is alive.
    ///
    /// This is called when the DCS is unreachable and we need to decide
    /// whether to continue as primary or demote.
    pub async fn check_topology(&mut self, my_name: &str) -> FailsafeResult {
        if !self.enabled {
            return FailsafeResult::Disabled;
        }

        if self.members.is_empty() {
            warn!("Failsafe enabled but no members known");
            return FailsafeResult::NotAllConfirmed {
                failed: vec!["no members".into()],
            };
        }

        let payload = serde_json::json!({
            "name": my_name,
            "conn_url": "",
            "api_url": "",
        });

        let mut failed = Vec::new();

        for (name, api_url) in &self.members {
            if name == my_name {
                continue; // Don't ping ourselves
            }

            let url = format!("{api_url}/failsafe");
            debug!(target_node = %name, %url, "Failsafe ping");

            match self.client.post(&url).json(&payload).send().await {
                Ok(resp) if resp.status().is_success() => {
                    debug!(target_node = %name, "Failsafe ping OK");
                }
                Ok(resp) => {
                    warn!(target_node = %name, status = %resp.status(), "Failsafe ping rejected");
                    failed.push(name.clone());
                }
                Err(e) => {
                    warn!(target_node = %name, error = %e, "Failsafe ping failed");
                    failed.push(name.clone());
                }
            }
        }

        if failed.is_empty() {
            info!("Failsafe check passed: all members confirmed");
            self.last_success = Some(Instant::now());
            FailsafeResult::AllConfirmed
        } else {
            warn!(?failed, "Failsafe check failed: some members not reachable");
            FailsafeResult::NotAllConfirmed { failed }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_failsafe_disabled_by_default() {
        let fs = Failsafe::new(30);
        assert!(!fs.is_enabled());
        assert!(!fs.is_active());
    }

    #[test]
    fn test_failsafe_enable_disable() {
        let mut fs = Failsafe::new(30);
        fs.set_enabled(true);
        assert!(fs.is_enabled());
        fs.set_enabled(false);
        assert!(!fs.is_enabled());
    }

    #[tokio::test]
    async fn test_failsafe_disabled_returns_disabled() {
        let mut fs = Failsafe::new(30);
        let result = fs.check_topology("node1").await;
        assert_eq!(result, FailsafeResult::Disabled);
    }

    #[tokio::test]
    async fn test_failsafe_no_members_fails() {
        let mut fs = Failsafe::new(30);
        fs.set_enabled(true);
        let result = fs.check_topology("node1").await;
        assert!(matches!(result, FailsafeResult::NotAllConfirmed { .. }));
    }

    #[tokio::test]
    async fn test_failsafe_unreachable_member_fails() {
        let mut fs = Failsafe::new(30);
        fs.set_enabled(true);
        fs.update_members(HashMap::from([
            ("node1".into(), "http://127.0.0.1:8008".into()),
            ("node2".into(), "http://127.0.0.99:9999".into()), // unreachable
        ]));
        let result = fs.check_topology("node1").await;
        match result {
            FailsafeResult::NotAllConfirmed { failed } => {
                assert!(failed.contains(&"node2".to_string()));
            }
            _ => panic!("Expected NotAllConfirmed"),
        }
    }

    #[test]
    fn test_is_active_after_success() {
        let mut fs = Failsafe::new(30);
        assert!(!fs.is_active());
        fs.last_success = Some(Instant::now());
        assert!(fs.is_active());
    }

    #[test]
    fn test_is_active_expired() {
        let mut fs = Failsafe::new(1); // 1 second TTL
        fs.last_success = Some(Instant::now() - Duration::from_secs(5));
        assert!(!fs.is_active()); // expired
    }
}
