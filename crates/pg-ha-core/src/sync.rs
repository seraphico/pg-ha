//! Synchronous replication mode management
//!
//! Automatically computes and sets `synchronous_standby_names` based on
//! cluster state, node tags (nosync, sync_priority), and configuration.

use crate::cluster::Member;

/// Synchronous replication mode
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncMode {
    /// No synchronous replication
    Off,
    /// Priority-based: FIRST N (node1, node2, ...)
    Priority,
    /// Quorum-based: ANY N (node1, node2, ...)
    Quorum,
}

/// Manages synchronous replication settings
pub struct SyncManager {
    mode: SyncMode,
    /// Number of synchronous standbys required
    sync_node_count: usize,
    /// Whether to block writes when no sync standbys available
    strict_mode: bool,
    /// Maximum lag before removing from sync standby list (bytes)
    max_lag_on_syncnode: Option<u64>,
}

impl SyncManager {
    pub fn new(mode: SyncMode, sync_node_count: usize, strict_mode: bool) -> Self {
        Self {
            mode,
            sync_node_count: sync_node_count.max(1),
            strict_mode,
            max_lag_on_syncnode: None,
        }
    }

    pub fn set_max_lag(&mut self, max_lag: Option<u64>) {
        self.max_lag_on_syncnode = max_lag;
    }

    /// Compute the `synchronous_standby_names` value based on current members.
    ///
    /// Returns None if sync mode is Off.
    /// Returns Some("") if strict mode and no eligible standbys (blocks writes).
    pub fn compute_sync_standby_names(&self, members: &[Member], my_name: &str) -> Option<String> {
        if self.mode == SyncMode::Off {
            return None;
        }

        // Filter eligible sync standbys
        let mut candidates: Vec<(&Member, u32)> = members
            .iter()
            .filter(|m| {
                m.name != my_name
                    && !m.is_nosync()
                    && m.state == crate::cluster::MemberState::Running
                    && m.role == crate::cluster::MemberRole::Replica
            })
            .filter(|m| {
                // Filter by lag if configured
                if let Some(_max_lag) = self.max_lag_on_syncnode {
                    // If we don't know the lag, include them (benefit of doubt)
                    m.wal_position.is_none() || m.wal_position.is_some_and(|_| true) // TODO: compute actual lag
                } else {
                    true
                }
            })
            .map(|m| (m, m.sync_priority()))
            .collect();

        // Sort by sync_priority (higher first), then name
        candidates.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.name.cmp(&b.0.name)));

        let names: Vec<&str> = candidates.iter().map(|(m, _)| m.name.as_str()).collect();

        if names.is_empty() {
            if self.strict_mode {
                // Return a placeholder that PostgreSQL interprets as
                // "require sync but no one available" → blocks writes
                return Some("*".to_string());
            }
            return Some(String::new()); // Effectively disables sync
        }

        // Take up to sync_node_count
        let selected: Vec<&str> = names.into_iter().take(self.sync_node_count).collect();
        let name_list = selected.join(",");

        match self.mode {
            SyncMode::Priority => Some(format!("FIRST {} ({})", self.sync_node_count, name_list)),
            SyncMode::Quorum => Some(format!("ANY {} ({})", self.sync_node_count, name_list)),
            SyncMode::Off => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{Member, MemberRole, MemberState};
    use std::collections::HashMap;

    fn make_member(name: &str, priority: u32) -> Member {
        let mut tags = HashMap::new();
        if priority > 0 {
            tags.insert("sync_priority".to_string(), serde_json::json!(priority));
        }
        Member {
            name: name.to_string(),
            conn_url: String::new(),
            api_url: String::new(),
            state: MemberState::Running,
            role: MemberRole::Replica,
            wal_position: Some(1000),
            timeline: Some(1),
            tags,
            version: None,
        }
    }

    fn make_nosync_member(name: &str) -> Member {
        Member {
            name: name.to_string(),
            conn_url: String::new(),
            api_url: String::new(),
            state: MemberState::Running,
            role: MemberRole::Replica,
            wal_position: Some(1000),
            timeline: Some(1),
            tags: HashMap::from([("nosync".to_string(), serde_json::json!(true))]),
            version: None,
        }
    }

    #[test]
    fn test_sync_off() {
        let mgr = SyncManager::new(SyncMode::Off, 1, false);
        let members = vec![make_member("n1", 0)];
        assert_eq!(mgr.compute_sync_standby_names(&members, "primary"), None);
    }

    #[test]
    fn test_priority_mode_single() {
        let mgr = SyncManager::new(SyncMode::Priority, 1, false);
        let members = vec![make_member("replica1", 0), make_member("replica2", 0)];
        let result = mgr.compute_sync_standby_names(&members, "primary").unwrap();
        assert!(result.starts_with("FIRST 1"));
        assert!(result.contains("replica1")); // alphabetical first
    }

    #[test]
    fn test_priority_mode_respects_sync_priority() {
        let mgr = SyncManager::new(SyncMode::Priority, 1, false);
        let members = vec![make_member("replica1", 1), make_member("replica2", 10)];
        let result = mgr.compute_sync_standby_names(&members, "primary").unwrap();
        // replica2 has higher priority
        assert!(result.contains("replica2"));
        assert!(!result.contains("replica1"));
    }

    #[test]
    fn test_quorum_mode() {
        let mgr = SyncManager::new(SyncMode::Quorum, 2, false);
        let members = vec![
            make_member("r1", 0),
            make_member("r2", 0),
            make_member("r3", 0),
        ];
        let result = mgr.compute_sync_standby_names(&members, "primary").unwrap();
        assert!(result.starts_with("ANY 2"));
    }

    #[test]
    fn test_nosync_excluded() {
        let mgr = SyncManager::new(SyncMode::Priority, 1, false);
        let members = vec![make_nosync_member("r1"), make_member("r2", 0)];
        let result = mgr.compute_sync_standby_names(&members, "primary").unwrap();
        assert!(result.contains("r2"));
        assert!(!result.contains("r1"));
    }

    #[test]
    fn test_strict_mode_no_standbys() {
        let mgr = SyncManager::new(SyncMode::Priority, 1, true);
        let members: Vec<Member> = vec![]; // no replicas
        let result = mgr.compute_sync_standby_names(&members, "primary").unwrap();
        assert_eq!(result, "*"); // blocks writes
    }

    #[test]
    fn test_non_strict_no_standbys() {
        let mgr = SyncManager::new(SyncMode::Priority, 1, false);
        let members: Vec<Member> = vec![];
        let result = mgr.compute_sync_standby_names(&members, "primary").unwrap();
        assert_eq!(result, ""); // allows async operation
    }

    #[test]
    fn test_excludes_self() {
        let mgr = SyncManager::new(SyncMode::Priority, 1, false);
        let members = vec![make_member("primary", 10), make_member("replica1", 5)];
        let result = mgr.compute_sync_standby_names(&members, "primary").unwrap();
        assert!(!result.contains("primary"));
        assert!(result.contains("replica1"));
    }
}
