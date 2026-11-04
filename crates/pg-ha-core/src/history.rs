//! Cluster history recording.
//!
//! The cluster history is the DCS `/history` key: a JSON array of [`HistoryEntry`],
//! oldest first, capped at [`MAX_HISTORY_ENTRIES`]. [`History`] is the capped buffer
//! that is read from and written back to that key.

use serde::{Deserialize, Serialize};

/// Maximum number of history entries kept in the DCS `/history` key
pub const MAX_HISTORY_ENTRIES: usize = 100;

/// A single recorded cluster event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEntry {
    /// Unix timestamp (seconds since epoch) when the event occurred
    pub timestamp: u64,
    /// Type of event
    pub event_type: HistoryEventType,
    /// Previous leader node name (if applicable)
    pub old_leader: Option<String>,
    /// New leader node name (if applicable)
    pub new_leader: Option<String>,
    /// Human-readable reason for the event
    pub reason: String,
    /// Timeline the new primary created (absent in entries written before this
    /// field existed, or when it could not be determined)
    #[serde(default)]
    pub timeline: Option<u64>,
    /// LSN where that timeline began, i.e. the promote LSN (same caveat)
    #[serde(default)]
    pub lsn: Option<u64>,
}

/// Types of history events
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HistoryEventType {
    Failover,
    Switchover,
    Promote,
}

impl std::fmt::Display for HistoryEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Failover => write!(f, "failover"),
            Self::Switchover => write!(f, "switchover"),
            Self::Promote => write!(f, "promote"),
        }
    }
}

/// Bounded history buffer for cluster events
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct History {
    entries: Vec<HistoryEntry>,
}

impl History {
    /// Create an empty history
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Build a history from stored entries (oldest first), keeping the newest
    /// `MAX_HISTORY_ENTRIES`
    pub fn from_entries(mut entries: Vec<HistoryEntry>) -> Self {
        if entries.len() > MAX_HISTORY_ENTRIES {
            entries.drain(..entries.len() - MAX_HISTORY_ENTRIES);
        }
        Self { entries }
    }
    /// Append an entry, evicting the oldest one at capacity. Returns false (and
    /// changes nothing) when the same event is already recorded, which makes a
    /// retried append after an uncertain write idempotent.
    pub fn push(&mut self, entry: HistoryEntry) -> bool {
        if self.entries.iter().any(|e| {
            e.timestamp == entry.timestamp
                && e.event_type == entry.event_type
                && e.new_leader == entry.new_leader
        }) {
            return false;
        }
        if self.entries.len() >= MAX_HISTORY_ENTRIES {
            self.entries.remove(0);
        }
        self.entries.push(entry);
        true
    }
    /// Take the entries (oldest first)
    pub fn into_entries(self) -> Vec<HistoryEntry> {
        self.entries
    }
    /// Get all history entries (oldest first)
    pub fn entries(&self) -> &[HistoryEntry] {
        &self.entries
    }

    /// Serialize to JSON string (for DCS persistence)
    pub fn to_json(&self) -> String {
        serde_json::to_string(&self.entries).unwrap_or_else(|_| "[]".to_string())
    }

    /// Deserialize from JSON string (from DCS)
    pub fn from_json(json: &str) -> Self {
        let entries: Vec<HistoryEntry> = serde_json::from_str(json).unwrap_or_default();
        Self { entries }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn entry(i: u64) -> HistoryEntry {
        HistoryEntry {
            timestamp: 1_000 + i,
            event_type: HistoryEventType::Failover,
            old_leader: Some("node1".into()),
            new_leader: Some(format!("node{i}")),
            reason: format!("test event {i}"),
            timeline: Some(i),
            lsn: Some(i * 16),
        }
    }
    #[test]
    fn test_push() {
        let mut history = History::new();
        assert!(history.push(entry(2)));
        assert_eq!(history.entries().len(), 1);
        assert_eq!(history.entries()[0].event_type, HistoryEventType::Failover);
        assert_eq!(history.entries()[0].new_leader.as_deref(), Some("node2"));
    }
    #[test]
    fn test_bounded_capacity() {
        let mut history = History::new();
        for i in 0..150 {
            assert!(history.push(entry(i)));
        }
        assert_eq!(history.entries().len(), MAX_HISTORY_ENTRIES);
        // Oldest entries are evicted; the first remaining entry is #50
        assert_eq!(history.entries()[0].new_leader.as_deref(), Some("node50"));
        assert_eq!(history.entries()[99].new_leader.as_deref(), Some("node149"));
    }
    #[test]
    fn test_from_entries_keeps_newest() {
        let history = History::from_entries((0..130).map(entry).collect());
        assert_eq!(history.entries().len(), MAX_HISTORY_ENTRIES);
        assert_eq!(history.entries()[0].new_leader.as_deref(), Some("node30"));
    }
    #[test]
    fn test_push_same_event_twice_is_ignored() {
        let mut history = History::new();
        assert!(history.push(entry(1)));
        assert!(!history.push(entry(1)));
        assert_eq!(history.entries().len(), 1);
    }
    #[test]
    fn test_json_roundtrip() {
        let mut e = entry(1);
        e.event_type = HistoryEventType::Switchover;
        e.reason = "planned maintenance".into();
        let mut history = History::new();
        history.push(e);
        let restored = History::from_json(&history.to_json());
        assert_eq!(restored.entries().len(), 1);
        assert_eq!(
            restored.entries()[0].event_type,
            HistoryEventType::Switchover
        );
        assert_eq!(restored.entries()[0].reason, "planned maintenance");
        assert_eq!(restored.entries()[0].timeline, Some(1));
        assert_eq!(restored.entries()[0].lsn, Some(16));
    }
    #[test]
    fn test_entry_without_timeline_and_lsn_still_parses() {
        let json = r#"[{"timestamp":5,"event_type":"failover","old_leader":"a","new_leader":"b","reason":"old"}]"#;
        let history = History::from_json(json);
        assert_eq!(history.entries().len(), 1);
        assert_eq!(history.entries()[0].timeline, None);
        assert_eq!(history.entries()[0].lsn, None);
    }
    #[test]
    fn test_from_invalid_json() {
        let history = History::from_json("not valid json");
        assert_eq!(history.entries().len(), 0);
    }
}
