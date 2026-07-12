//! Raft State Machine: KV store with TTL and CAS semantics
//!
//! This is the core data structure replicated across all Raft nodes.
//! It provides etcd-like semantics: key-value storage with TTL expiry
//! and Compare-And-Swap (CAS) operations for atomic updates.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// A key-value entry with metadata and optional TTL
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct KvEntry {
    /// The stored value (usually JSON)
    pub value: String,
    /// Timestamp when this key was first created (millis since epoch)
    pub created_at: u64,
    /// Timestamp of the last update (millis since epoch)
    pub updated_at: u64,
    /// Optional expiry timestamp (millis since epoch). None = never expires.
    pub expire_at: Option<u64>,
    /// Monotonically increasing version (set to raft log index on each write)
    pub version: u64,
}

/// Request types for the Raft state machine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    /// Set a key with optional TTL and CAS conditions
    Set {
        key: String,
        value: String,
        /// TTL in seconds (None = no expiry)
        ttl: Option<u64>,
        /// CAS: only succeed if key existence matches this flag
        prev_exist: Option<bool>,
        /// CAS: only succeed if current value equals this
        prev_value: Option<String>,
        /// CAS: only succeed if current version equals this
        prev_version: Option<u64>,
        /// Deterministic timestamp (millis since epoch), filled by Raft leader at proposal time.
        /// Defaults to 0 for backward compatibility with existing log entries.
        #[serde(default)]
        now: u64,
    },
    /// Delete a key with optional CAS and recursive mode
    Delete {
        key: String,
        /// CAS: only delete if current value matches
        prev_value: Option<String>,
        /// If true, delete all keys with this prefix
        recursive: bool,
    },
    /// Expire all keys past their TTL (proposed periodically by Raft leader)
    ExpireKeys { now: u64 },
}

/// Response from the state machine after applying a request
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Response {
    /// Operation succeeded, returning the new version
    Ok { version: u64 },
    /// CAS condition was not met — no change made
    NotChanged,
}

/// The KV state machine with TTL support
///
/// This is the data structure replicated via Raft. Every write goes through
/// Raft consensus before being applied here.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct KvStateMachine {
    data: HashMap<String, KvEntry>,
    last_applied_log: u64,
}

impl KvStateMachine {
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply a request to the state machine (called after Raft commits the entry)
    pub fn apply(&mut self, request: &Request, log_index: u64) -> Response {
        self.last_applied_log = log_index;
        match request {
            Request::Set {
                key,
                value,
                ttl,
                prev_exist,
                prev_value,
                prev_version,
                now,
            } => self.apply_set(key, value, *ttl, *prev_exist, prev_value.as_deref(), *prev_version, *now),
            Request::Delete {
                key,
                prev_value,
                recursive,
            } => self.apply_delete(key, prev_value.as_deref(), *recursive),
            Request::ExpireKeys { now } => self.apply_expire(*now),
        }
    }

    fn apply_set(
        &mut self,
        key: &str,
        value: &str,
        ttl: Option<u64>,
        prev_exist: Option<bool>,
        prev_value: Option<&str>,
        prev_version: Option<u64>,
        now: u64,
    ) -> Response {
        // Backward compat: if now == 0 (old log entries without the field), fall back to wall-clock
        let now = if now == 0 { current_millis() } else { now };

        let existing = self.data.get(key);

        // Treat expired entries as non-existent for CAS purposes (deterministic: uses request's now)
        let effectively_exists = existing.is_some_and(|e| {
            e.expire_at.is_none_or(|exp| exp > now)
        });
        let effective_existing = if effectively_exists { existing } else { None };

        // CAS check: prev_exist
        if let Some(should_exist) = prev_exist
            && should_exist != effectively_exists {
                return Response::NotChanged;
            }

        // CAS check: prev_value
        if let Some(expected_val) = prev_value {
            match effective_existing {
                Some(entry) if entry.value == expected_val => {}
                _ => return Response::NotChanged,
            }
        }

        // CAS check: prev_version
        if let Some(expected_ver) = prev_version {
            match effective_existing {
                Some(entry) if entry.version == expected_ver => {}
                _ => return Response::NotChanged,
            }
        }

        // All CAS checks passed — apply the write using deterministic timestamp
        let created_at = existing
            .filter(|_| effectively_exists)
            .map(|e| e.created_at)
            .unwrap_or(now);
        let updated_at = now;
        let version = self.last_applied_log;

        let entry = KvEntry {
            value: value.to_string(),
            created_at,
            updated_at,
            expire_at: ttl.map(|t| now + t * 1000),
            version,
        };

        self.data.insert(key.to_string(), entry);
        Response::Ok { version }
    }
    fn apply_delete(&mut self, key: &str, prev_value: Option<&str>, recursive: bool) -> Response {
        if recursive {
            let keys_to_remove: Vec<String> = self
                .data
                .keys()
                .filter(|k| k.starts_with(key))
                .cloned()
                .collect();
            for k in keys_to_remove {
                self.data.remove(&k);
            }
            return Response::Ok {
                version: self.last_applied_log,
            };
        }

        // CAS check: prev_value
        if let Some(expected_val) = prev_value {
            match self.data.get(key) {
                Some(entry) if entry.value == expected_val => {}
                _ => return Response::NotChanged,
            }
        }

        if self.data.remove(key).is_some() {
            Response::Ok {
                version: self.last_applied_log,
            }
        } else {
            Response::NotChanged
        }
    }

    fn apply_expire(&mut self, now: u64) -> Response {
        self.data.retain(|_, entry| {
            entry.expire_at.is_none_or(|exp| exp > now)
        });
        Response::Ok {
            version: self.last_applied_log,
        }
    }

    // ─────────────────────── Read operations (local, no Raft) ───────────────────────

    /// Get a single key, respecting TTL
    pub fn get(&self, key: &str) -> Option<&KvEntry> {
        let entry = self.data.get(key)?;
        if is_expired(entry) {
            return None;
        }
        Some(entry)
    }

    /// Get all keys matching a prefix, respecting TTL
    pub fn get_prefix(&self, prefix: &str) -> HashMap<String, &KvEntry> {
        self.data
            .iter()
            .filter(|(k, v)| k.starts_with(prefix) && !is_expired(v))
            .map(|(k, v)| (k.clone(), v))
            .collect()
    }

    /// Get the last applied log index
    pub fn last_applied_log(&self) -> u64 {
        self.last_applied_log
    }

    /// Get total number of keys (including potentially expired ones)
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the state machine has no keys
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    // ─────────────────────── Persistence ───────────────────────

    /// Save the full state machine to a JSON file on disk.
    pub fn save_to_disk(&self, path: &Path) {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        match serde_json::to_string_pretty(self) {
            Ok(json) => {
                if let Err(e) = std::fs::write(path, json) {
                    tracing::warn!("Failed to persist state machine to {}: {e}", path.display());
                }
            }
            Err(e) => {
                tracing::warn!("Failed to serialize state machine: {e}");
            }
        }
    }

    /// Load the state machine from a JSON file on disk.
    /// Returns a default (empty) state machine if the file doesn't exist or can't be parsed.
    pub fn load_from_disk(path: &Path) -> Self {
        match std::fs::read_to_string(path) {
            Ok(json) => match serde_json::from_str(&json) {
                Ok(sm) => {
                    tracing::info!("Loaded state machine from {}", path.display());
                    sm
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to parse state machine from {}: {e} — starting fresh",
                        path.display()
                    );
                    Self::default()
                }
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::info!("No persisted state machine at {} — starting fresh", path.display());
                Self::default()
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to read state machine from {}: {e} — starting fresh",
                    path.display()
                );
                Self::default()
            }
        }
    }
}

fn is_expired(entry: &KvEntry) -> bool {
    entry
        .expire_at
        .is_some_and(|exp| exp <= current_millis())
}

pub fn current_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A reasonable test timestamp (approx 2023-11-14)
    const TEST_NOW: u64 = 1_700_000_000_000;

    fn make_set(key: &str, value: &str) -> Request {
        Request::Set {
            key: key.to_string(),
            value: value.to_string(),
            ttl: None,
            prev_exist: None,
            prev_value: None,
            prev_version: None,
            now: TEST_NOW,
        }
    }

    fn make_set_with_ttl(key: &str, value: &str, ttl: u64) -> Request {
        Request::Set {
            key: key.to_string(),
            value: value.to_string(),
            ttl: Some(ttl),
            prev_exist: None,
            prev_value: None,
            prev_version: None,
            now: TEST_NOW,
        }
    }

    fn make_set_prev_exist(key: &str, value: &str, prev_exist: bool) -> Request {
        Request::Set {
            key: key.to_string(),
            value: value.to_string(),
            ttl: None,
            prev_exist: Some(prev_exist),
            prev_value: None,
            prev_version: None,
            now: TEST_NOW,
        }
    }

    fn make_set_prev_value(key: &str, value: &str, prev_value: &str) -> Request {
        Request::Set {
            key: key.to_string(),
            value: value.to_string(),
            ttl: None,
            prev_exist: None,
            prev_value: Some(prev_value.to_string()),
            prev_version: None,
            now: TEST_NOW,
        }
    }

    fn make_delete(key: &str) -> Request {
        Request::Delete {
            key: key.to_string(),
            prev_value: None,
            recursive: false,
        }
    }

    fn make_delete_prev_value(key: &str, prev_value: &str) -> Request {
        Request::Delete {
            key: key.to_string(),
            prev_value: Some(prev_value.to_string()),
            recursive: false,
        }
    }

    fn make_delete_recursive(prefix: &str) -> Request {
        Request::Delete {
            key: prefix.to_string(),
            prev_value: None,
            recursive: true,
        }
    }

    // ─── Basic set/get/delete ───

    #[test]
    fn test_set_and_get() {
        let mut sm = KvStateMachine::new();
        let resp = sm.apply(&make_set("/leader", "node1"), 1);
        assert_eq!(resp, Response::Ok { version: 1 });

        let entry = sm.get("/leader").unwrap();
        assert_eq!(entry.value, "node1");
        assert_eq!(entry.version, 1);
    }

    #[test]
    fn test_overwrite() {
        let mut sm = KvStateMachine::new();
        sm.apply(&make_set("/key", "v1"), 1);
        sm.apply(&make_set("/key", "v2"), 2);

        let entry = sm.get("/key").unwrap();
        assert_eq!(entry.value, "v2");
        assert_eq!(entry.version, 2);
    }

    #[test]
    fn test_delete() {
        let mut sm = KvStateMachine::new();
        sm.apply(&make_set("/key", "v1"), 1);
        let resp = sm.apply(&make_delete("/key"), 2);
        assert_eq!(resp, Response::Ok { version: 2 });
        assert!(sm.get("/key").is_none());
    }

    #[test]
    fn test_delete_nonexistent() {
        let mut sm = KvStateMachine::new();
        let resp = sm.apply(&make_delete("/nope"), 1);
        assert_eq!(resp, Response::NotChanged);
    }

    #[test]
    fn test_delete_recursive() {
        let mut sm = KvStateMachine::new();
        sm.apply(&make_set("/members/a", "a"), 1);
        sm.apply(&make_set("/members/b", "b"), 2);
        sm.apply(&make_set("/leader", "x"), 3);

        let resp = sm.apply(&make_delete_recursive("/members/"), 4);
        assert_eq!(resp, Response::Ok { version: 4 });
        assert!(sm.get("/members/a").is_none());
        assert!(sm.get("/members/b").is_none());
        assert!(sm.get("/leader").is_some()); // not under /members/
    }

    // ─── CAS: prev_exist ───

    #[test]
    fn test_cas_prev_exist_false_succeeds_when_absent() {
        let mut sm = KvStateMachine::new();
        let resp = sm.apply(&make_set_prev_exist("/leader", "node1", false), 1);
        assert_eq!(resp, Response::Ok { version: 1 });
    }

    #[test]
    fn test_cas_prev_exist_false_fails_when_present() {
        let mut sm = KvStateMachine::new();
        sm.apply(&make_set("/leader", "node1"), 1);
        let resp = sm.apply(&make_set_prev_exist("/leader", "node2", false), 2);
        assert_eq!(resp, Response::NotChanged);
        // Value unchanged
        assert_eq!(sm.get("/leader").unwrap().value, "node1");
    }

    #[test]
    fn test_cas_prev_exist_true_succeeds_when_present() {
        let mut sm = KvStateMachine::new();
        sm.apply(&make_set("/leader", "node1"), 1);
        let resp = sm.apply(&make_set_prev_exist("/leader", "node2", true), 2);
        assert_eq!(resp, Response::Ok { version: 2 });
        assert_eq!(sm.get("/leader").unwrap().value, "node2");
    }

    #[test]
    fn test_cas_prev_exist_true_fails_when_absent() {
        let mut sm = KvStateMachine::new();
        let resp = sm.apply(&make_set_prev_exist("/leader", "node1", true), 1);
        assert_eq!(resp, Response::NotChanged);
    }

    // ─── CAS: prev_value ───

    #[test]
    fn test_cas_prev_value_succeeds() {
        let mut sm = KvStateMachine::new();
        sm.apply(&make_set("/leader", "node1"), 1);
        let resp = sm.apply(&make_set_prev_value("/leader", "node1", "node1"), 2);
        assert_eq!(resp, Response::Ok { version: 2 });
    }

    #[test]
    fn test_cas_prev_value_fails_wrong_value() {
        let mut sm = KvStateMachine::new();
        sm.apply(&make_set("/leader", "node1"), 1);
        let resp = sm.apply(&make_set_prev_value("/leader", "node2", "wrong"), 2);
        assert_eq!(resp, Response::NotChanged);
    }

    #[test]
    fn test_cas_prev_value_fails_key_absent() {
        let mut sm = KvStateMachine::new();
        let resp = sm.apply(&make_set_prev_value("/leader", "node1", "anything"), 1);
        assert_eq!(resp, Response::NotChanged);
    }

    #[test]
    fn test_cas_delete_with_prev_value() {
        let mut sm = KvStateMachine::new();
        sm.apply(&make_set("/leader", "node1"), 1);

        // Wrong prev_value → no delete
        let resp = sm.apply(&make_delete_prev_value("/leader", "wrong"), 2);
        assert_eq!(resp, Response::NotChanged);
        assert!(sm.get("/leader").is_some());

        // Correct prev_value → delete
        let resp = sm.apply(&make_delete_prev_value("/leader", "node1"), 3);
        assert_eq!(resp, Response::Ok { version: 3 });
        assert!(sm.get("/leader").is_none());
    }

    // ─── CAS: prev_version ───

    #[test]
    fn test_cas_prev_version_succeeds() {
        let mut sm = KvStateMachine::new();
        sm.apply(&make_set("/key", "v1"), 5);
        let req = Request::Set {
            key: "/key".to_string(),
            value: "v2".to_string(),
            ttl: None,
            prev_exist: None,
            prev_value: None,
            prev_version: Some(5), // matches version set at log_index=5
            now: TEST_NOW,
        };
        let resp = sm.apply(&req, 6);
        assert_eq!(resp, Response::Ok { version: 6 });
    }

    #[test]
    fn test_cas_prev_version_fails() {
        let mut sm = KvStateMachine::new();
        sm.apply(&make_set("/key", "v1"), 5);
        let req = Request::Set {
            key: "/key".to_string(),
            value: "v2".to_string(),
            ttl: None,
            prev_exist: None,
            prev_value: None,
            prev_version: Some(99), // wrong version
            now: TEST_NOW,
        };
        let resp = sm.apply(&req, 6);
        assert_eq!(resp, Response::NotChanged);
    }

    // ─── TTL and expiry ───

    #[test]
    fn test_ttl_sets_expire_at() {
        let mut sm = KvStateMachine::new();
        // Use a future timestamp so the entry won't appear expired via get()
        let future_now = current_millis() + 1_000_000;
        let req = Request::Set {
            key: "/leader".to_string(),
            value: "node1".to_string(),
            ttl: Some(30),
            prev_exist: None,
            prev_value: None,
            prev_version: None,
            now: future_now,
        };
        sm.apply(&req, 1);
        let entry = sm.get("/leader").unwrap();
        assert!(entry.expire_at.is_some());
        // expire_at should be exactly future_now + 30 * 1000
        let expire = entry.expire_at.unwrap();
        assert_eq!(expire, future_now + 30_000);
    }

    #[test]
    fn test_expire_keys_removes_expired() {
        let mut sm = KvStateMachine::new();
        // Insert key that expires immediately
        sm.apply(&make_set_with_ttl("/short", "val", 0), 1);
        // Insert key that won't expire
        sm.apply(&make_set("/permanent", "val"), 2);

        // Manually set expire_at to the past for testing
        if let Some(entry) = sm.data.get_mut("/short") {
            entry.expire_at = Some(1); // epoch + 1ms = definitely past
        }

        let far_future = current_millis() + 100_000;
        sm.apply(&Request::ExpireKeys { now: far_future }, 3);

        assert!(sm.data.get("/short").is_none());
        assert!(sm.data.get("/permanent").is_some());
    }

    #[test]
    fn test_get_respects_ttl() {
        let mut sm = KvStateMachine::new();
        sm.apply(&make_set_with_ttl("/key", "val", 30), 1);

        // Manually expire it
        if let Some(entry) = sm.data.get_mut("/key") {
            entry.expire_at = Some(1); // in the past
        }

        // get() should treat it as absent
        assert!(sm.get("/key").is_none());
    }

    // ─── Prefix queries ───

    #[test]
    fn test_get_prefix() {
        let mut sm = KvStateMachine::new();
        sm.apply(&make_set("/service/cluster/members/a", "a"), 1);
        sm.apply(&make_set("/service/cluster/members/b", "b"), 2);
        sm.apply(&make_set("/service/cluster/leader", "a"), 3);

        let results = sm.get_prefix("/service/cluster/members/");
        assert_eq!(results.len(), 2);
        assert!(results.contains_key("/service/cluster/members/a"));
        assert!(results.contains_key("/service/cluster/members/b"));
    }

    // ─── Leader lock lifecycle (integration-style) ───

    #[test]
    fn test_leader_lock_lifecycle() {
        let mut sm = KvStateMachine::new();

        // Acquire: prev_exist=false (key must not exist)
        let acquire = make_set_prev_exist("/leader", "node1", false);
        assert_eq!(sm.apply(&acquire, 1), Response::Ok { version: 1 });

        // Another node tries to acquire — fails
        let acquire2 = make_set_prev_exist("/leader", "node2", false);
        assert_eq!(sm.apply(&acquire2, 2), Response::NotChanged);

        // Leader renews: prev_value=node1
        let renew = make_set_prev_value("/leader", "node1", "node1");
        assert_eq!(sm.apply(&renew, 3), Response::Ok { version: 3 });

        // Wrong node tries to renew — fails
        let bad_renew = make_set_prev_value("/leader", "node2", "node2");
        assert_eq!(sm.apply(&bad_renew, 4), Response::NotChanged);

        // Leader releases: delete with prev_value=node1
        let release = make_delete_prev_value("/leader", "node1");
        assert_eq!(sm.apply(&release, 5), Response::Ok { version: 5 });

        // Now node2 can acquire
        let acquire3 = make_set_prev_exist("/leader", "node2", false);
        assert_eq!(sm.apply(&acquire3, 6), Response::Ok { version: 6 });
    }

    #[test]
    fn test_created_at_preserved_on_update() {
        let mut sm = KvStateMachine::new();
        sm.apply(&make_set("/key", "v1"), 1);
        let created = sm.get("/key").unwrap().created_at;

        // Small delay to ensure timestamps differ
        std::thread::sleep(std::time::Duration::from_millis(2));

        sm.apply(&make_set("/key", "v2"), 2);
        let entry = sm.get("/key").unwrap();
        assert_eq!(entry.created_at, created); // preserved
        assert!(entry.updated_at >= created); // updated
    }
}
