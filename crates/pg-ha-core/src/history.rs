//! Cluster history recording.
//!
//! Records significant cluster events (failover, switchover, promote) with bounded
//! in-memory storage and optional DCS persistence under /history key.

use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Maximum number of history entries to retain in memory
const MAX_HISTORY_ENTRIES: usize = 100;

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

    /// Record a new event, evicting the oldest entry if at capacity
    pub fn record_event(
        &mut self,
        event_type: HistoryEventType,
        old_leader: Option<String>,
        new_leader: Option<String>,
        reason: String,
    ) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let entry = HistoryEntry {
            timestamp,
            event_type,
            old_leader,
            new_leader,
            reason,
        };

        if self.entries.len() >= MAX_HISTORY_ENTRIES {
            self.entries.remove(0);
        }
        self.entries.push(entry);
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
        let entries: Vec<HistoryEntry> =
            serde_json::from_str(json).unwrap_or_default();
        Self { entries }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_event() {
        let mut history = History::new();
        history.record_event(
            HistoryEventType::Failover,
            Some("node1".into()),
            Some("node2".into()),
            "leader lock expired".into(),
        );

        assert_eq!(history.entries().len(), 1);
        assert_eq!(history.entries()[0].event_type, HistoryEventType::Failover);
        assert_eq!(history.entries()[0].old_leader.as_deref(), Some("node1"));
        assert_eq!(history.entries()[0].new_leader.as_deref(), Some("node2"));
    }

    #[test]
    fn test_bounded_capacity() {
        let mut history = History::new();
        for i in 0..150 {
            history.record_event(
                HistoryEventType::Promote,
                None,
                Some(format!("node{i}")),
                format!("test event {i}"),
            );
        }

        assert_eq!(history.entries().len(), MAX_HISTORY_ENTRIES);
        // Oldest entries should have been evicted; first entry should be #50
        assert_eq!(
            history.entries()[0].new_leader.as_deref(),
            Some("node50")
        );
    }

    #[test]
    fn test_json_roundtrip() {
        let mut history = History::new();
        history.record_event(
            HistoryEventType::Switchover,
            Some("old".into()),
            Some("new".into()),
            "planned maintenance".into(),
        );

        let json = history.to_json();
        let restored = History::from_json(&json);

        assert_eq!(restored.entries().len(), 1);
        assert_eq!(restored.entries()[0].event_type, HistoryEventType::Switchover);
        assert_eq!(restored.entries()[0].reason, "planned maintenance");
    }

    #[test]
    fn test_from_invalid_json() {
        let history = History::from_json("not valid json");
        assert_eq!(history.entries().len(), 0);
    }
}
