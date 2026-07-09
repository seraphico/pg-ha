//! Replication slot management
//!
//! Maintains physical replication slots for each cluster member on the primary.
//! Handles slot name translation, creation, deletion, and TTL-based retention
//! for absent members.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tracing::{debug, info};

/// Manages replication slots on the primary
pub struct SlotManager {
    /// Retained slots for members that disappeared (name → last_seen time)
    retained: HashMap<String, Instant>,
    /// How long to keep slots for absent members
    retention_ttl: Duration,
    /// Slot name patterns to ignore (not managed by pg-ha)
    ignore_patterns: Vec<String>,
}

impl SlotManager {
    pub fn new(retention_ttl_secs: u64) -> Self {
        Self {
            retained: HashMap::new(),
            retention_ttl: Duration::from_secs(retention_ttl_secs),
            ignore_patterns: Vec::new(),
        }
    }

    /// Set patterns for slots that pg-ha should not manage
    pub fn set_ignore_patterns(&mut self, patterns: Vec<String>) {
        self.ignore_patterns = patterns;
    }

    /// Translate a member name to a valid PostgreSQL replication slot name.
    ///
    /// Rules (same as Patroni):
    /// - Lowercase
    /// - Replace dashes and periods with underscores
    /// - Truncate to 63 characters
    pub fn slot_name_from_member(name: &str) -> String {
        let slot_name: String = name
            .to_lowercase()
            .chars()
            .map(|c| match c {
                'a'..='z' | '0'..='9' | '_' => c,
                '-' | '.' => '_',
                _ => '_',
            })
            .collect();
        slot_name[..slot_name.len().min(63)].to_string()
    }

    /// Compute which slots should exist based on current cluster members.
    ///
    /// Returns: (slots_to_create, slots_to_drop)
    pub fn compute_slot_diff(
        &mut self,
        current_members: &[String],
        existing_slots: &[String],
        my_name: &str,
    ) -> (Vec<String>, Vec<String>) {
        // Desired slots: one per member except ourselves
        let desired: HashMap<String, &str> = current_members
            .iter()
            .filter(|m| *m != my_name)
            .map(|m| (Self::slot_name_from_member(m), m.as_str()))
            .collect();

        // Slots to create: desired but not existing
        let to_create: Vec<String> = desired
            .keys()
            .filter(|s| !existing_slots.contains(s))
            .cloned()
            .collect();

        // Slots to drop: existing but not desired AND not retained AND not ignored
        let now = Instant::now();
        let mut to_drop = Vec::new();

        for slot in existing_slots {
            if desired.contains_key(slot) {
                // Still needed — remove from retention tracking
                self.retained.remove(slot);
                continue;
            }
            if self.is_ignored(slot) {
                continue;
            }

            // Track when this slot became orphaned
            let first_seen_absent = self.retained.entry(slot.clone()).or_insert(now);
            if now.duration_since(*first_seen_absent) >= self.retention_ttl {
                to_drop.push(slot.clone());
                self.retained.remove(slot);
                info!(slot = %slot, "Dropping slot after retention TTL expired");
            } else {
                debug!(
                    slot = %slot,
                    remaining_secs = (self.retention_ttl - now.duration_since(*first_seen_absent)).as_secs(),
                    "Retaining slot for absent member"
                );
            }
        }

        (to_create, to_drop)
    }

    /// Check if a slot name matches any ignore pattern
    fn is_ignored(&self, slot_name: &str) -> bool {
        self.ignore_patterns.iter().any(|pattern| {
            if pattern.contains('*') {
                // Simple glob: only support prefix* and *suffix
                if let Some(prefix) = pattern.strip_suffix('*') {
                    slot_name.starts_with(prefix)
                } else if let Some(suffix) = pattern.strip_prefix('*') {
                    slot_name.ends_with(suffix)
                } else {
                    slot_name == pattern
                }
            } else {
                slot_name == pattern
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slot_name_basic() {
        assert_eq!(SlotManager::slot_name_from_member("node1"), "node1");
        assert_eq!(SlotManager::slot_name_from_member("node-1"), "node_1");
        assert_eq!(SlotManager::slot_name_from_member("my.node"), "my_node");
        assert_eq!(
            SlotManager::slot_name_from_member("Node-1.example"),
            "node_1_example"
        );
    }

    #[test]
    fn test_slot_name_truncation() {
        let long_name = "a".repeat(100);
        let slot = SlotManager::slot_name_from_member(&long_name);
        assert_eq!(slot.len(), 63);
    }

    #[test]
    fn test_slot_name_special_chars() {
        assert_eq!(
            SlotManager::slot_name_from_member("node@host:5432"),
            "node_host_5432"
        );
    }

    #[test]
    fn test_compute_diff_create() {
        let mut mgr = SlotManager::new(300);
        let members = vec!["node1".into(), "node2".into(), "node3".into()];
        let existing: Vec<String> = vec![];
        let (create, drop) = mgr.compute_slot_diff(&members, &existing, "node1");
        assert_eq!(create.len(), 2); // node2, node3 (not self)
        assert!(create.contains(&"node2".to_string()));
        assert!(create.contains(&"node3".to_string()));
        assert!(drop.is_empty());
    }

    #[test]
    fn test_compute_diff_drop_after_ttl() {
        let mut mgr = SlotManager::new(0); // 0s TTL = immediate drop
        let members = vec!["node1".into()]; // only self
        let existing = vec!["node2".to_string()]; // orphaned slot
        let (create, drop) = mgr.compute_slot_diff(&members, &existing, "node1");
        assert!(create.is_empty());
        assert_eq!(drop, vec!["node2"]);
    }

    #[test]
    fn test_compute_diff_retain_before_ttl() {
        let mut mgr = SlotManager::new(300); // 5 min TTL
        let members = vec!["node1".into()];
        let existing = vec!["node2".to_string()];
        let (create, drop) = mgr.compute_slot_diff(&members, &existing, "node1");
        assert!(create.is_empty());
        assert!(drop.is_empty()); // retained, TTL not expired
    }

    #[test]
    fn test_ignore_pattern() {
        let mut mgr = SlotManager::new(0);
        mgr.set_ignore_patterns(vec!["pg_*".into(), "*_logical".into()]);
        let members = vec!["node1".into()];
        let existing = vec![
            "pg_basebackup_12345".into(),
            "my_logical".into(),
            "orphan_slot".into(),
        ];
        let (_, drop) = mgr.compute_slot_diff(&members, &existing, "node1");
        // Only orphan_slot should be dropped; pg_* and *_logical are ignored
        assert_eq!(drop, vec!["orphan_slot"]);
    }

    #[test]
    fn test_no_self_slot() {
        let mut mgr = SlotManager::new(300);
        let members = vec!["node1".into(), "node2".into()];
        let existing: Vec<String> = vec![];
        let (create, _) = mgr.compute_slot_diff(&members, &existing, "node1");
        // Should not create a slot for self
        assert!(!create.contains(&"node1".to_string()));
        assert!(create.contains(&"node2".to_string()));
    }
}
