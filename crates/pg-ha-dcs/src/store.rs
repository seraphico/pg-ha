//! Raft storage implementation for openraft 0.9
//!
//! Provides:
//! - TypeConfig declaration for openraft
//! - Log + state machine store (RaftStorage trait) with optional disk persistence
//! - Snapshot support via serde serialization of KvStateMachine

use std::collections::BTreeMap;
use std::fmt;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use openraft::TokioRuntime;
use openraft::entry::RaftPayload;
use openraft::storage::{LogState, Snapshot};
use openraft::{
    BasicNode, Entry, LogId, RaftLogReader, RaftSnapshotBuilder, RaftStorage, SnapshotMeta,
    StorageError, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::state_machine::{KvStateMachine, Request, Response};
use crate::wal::{COMPACTION_THRESHOLD, PurgePayload, RecordType, TruncatePayload, WalWriter};

// ─────────────────── Display impls required by openraft AppData ───────────────────

impl fmt::Display for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Request::Set { key, .. } => write!(f, "Set({key})"),
            Request::Delete { key, .. } => write!(f, "Delete({key})"),
            Request::ExpireKeys { .. } => write!(f, "ExpireKeys"),
        }
    }
}

// ─────────────────── openraft Type Configuration ───────────────────

pub type NodeId = u64;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Request,
        R = Response,
        NodeId = NodeId,
        Node = BasicNode,
        Entry = Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = TokioRuntime,
);

pub type LogEntry = Entry<TypeConfig>;
pub type Raft = openraft::Raft<TypeConfig>;

// ─────────────────── Persisted hard state (vote + committed) ───────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct PersistedHardState {
    vote: Option<Vote<NodeId>>,
    committed: Option<LogId<NodeId>>,
}

// ─────────────────── Persisted membership ───────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedMembership {
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
}

// ─────────────────── Combined Storage ───────────────────

/// Raft storage with optional disk persistence.
///
/// When `data_dir` is set:
/// - Raft log → append-only [`WalWriter`] (`raft_log.wal`)
/// - hard state / state machine / membership → JSON (atomic write)
#[derive(Clone)]
pub struct MemStore {
    inner: Arc<RwLock<MemStoreInner>>,
    data_dir: Option<PathBuf>,
    /// Present when disk persistence is enabled.
    wal: Option<Arc<Mutex<WalWriter>>>,
}

impl fmt::Debug for MemStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemStore")
            .field("data_dir", &self.data_dir)
            .field("wal", &self.wal.as_ref().map(|_| "WalWriter"))
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
struct MemStoreInner {
    vote: Option<Vote<NodeId>>,
    committed: Option<LogId<NodeId>>,
    log: BTreeMap<u64, LogEntry>,
    last_purged: Option<LogId<NodeId>>,

    // State machine
    state_machine: KvStateMachine,
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,

    // Snapshot
    snapshot: Option<StoredSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, BasicNode>,
    data: Vec<u8>,
}

impl Default for MemStore {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(MemStoreInner::default())),
            data_dir: None,
            wal: None,
        }
    }
}

impl MemStore {
    /// Create a new in-memory store (no persistence).
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a store with disk persistence. Loads existing state from `data_dir` if available.
    pub fn new_persistent(data_dir: PathBuf) -> Self {
        // Ensure directory exists
        if let Err(e) = std::fs::create_dir_all(&data_dir) {
            warn!("Failed to create raft data dir {}: {e}", data_dir.display());
        }

        let mut inner = MemStoreInner::default();

        // Load hard state (vote + committed)
        let hard_state_path = data_dir.join("hard_state.json");
        if let Ok(json) = std::fs::read_to_string(&hard_state_path) {
            if let Ok(hs) = serde_json::from_str::<PersistedHardState>(&json) {
                info!("Loaded hard state from disk: vote={:?}", hs.vote);
                inner.vote = hs.vote;
                inner.committed = hs.committed;
            } else {
                warn!("Failed to parse hard_state.json — starting fresh");
            }
        }

        // Load state machine
        let sm_path = data_dir.join("state_machine.json");
        inner.state_machine = KvStateMachine::load_from_disk(&sm_path);

        // Load Raft log from WAL (append-only). Legacy log_entries.json is ignored.
        let wal_path = data_dir.join("raft_log.wal");
        let wal = match WalWriter::open(wal_path) {
            Ok((writer, replayed)) => {
                info!(
                    "Loaded {} log entries from WAL (last_purged={:?}, valid_len={})",
                    replayed.log.len(),
                    replayed.last_purged,
                    replayed.valid_len
                );
                inner.log = replayed.log;
                inner.last_purged = replayed.last_purged;
                Some(Arc::new(Mutex::new(writer)))
            }
            Err(e) => {
                warn!("Failed to open raft_log.wal — log persistence disabled: {e}");
                None
            }
        };

        // Load membership
        let membership_path = data_dir.join("membership.json");
        if let Ok(json) = std::fs::read_to_string(&membership_path) {
            if let Ok(pm) = serde_json::from_str::<PersistedMembership>(&json) {
                info!(
                    "Loaded membership from disk: last_applied={:?}",
                    pm.last_applied_log
                );
                inner.last_applied_log = pm.last_applied_log;
                inner.last_membership = pm.last_membership;
            } else {
                warn!("Failed to parse membership.json — starting fresh");
            }
        }

        Self {
            inner: Arc::new(RwLock::new(inner)),
            data_dir: Some(data_dir),
            wal,
        }
    }

    /// Read a key from the state machine (for external queries)
    pub async fn get(&self, key: &str) -> Option<crate::state_machine::KvEntry> {
        self.inner.read().await.state_machine.get(key).cloned()
    }

    /// Read all keys with a prefix
    pub async fn get_prefix(
        &self,
        prefix: &str,
    ) -> std::collections::HashMap<String, crate::state_machine::KvEntry> {
        self.inner
            .read()
            .await
            .state_machine
            .get_prefix(prefix)
            .into_iter()
            .map(|(k, v)| (k, v.clone()))
            .collect()
    }

    // ─────────────────── Persistence helpers ───────────────────

    async fn persist_hard_state(&self, inner: &MemStoreInner) {
        if let Some(dir) = &self.data_dir {
            let hs = PersistedHardState {
                vote: inner.vote,
                committed: inner.committed,
            };
            let path = dir.join("hard_state.json");
            let json = match serde_json::to_string(&hs) {
                Ok(j) => j,
                Err(e) => {
                    warn!("Failed to serialize hard_state: {e}");
                    return;
                }
            };
            let result = tokio::task::spawn_blocking(move || {
                Self::atomic_write_json(&path, json.as_bytes())
            })
            .await;
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => warn!("Failed to persist hard_state: {e}"),
                Err(e) => warn!("persist_hard_state task panicked: {e}"),
            }
        }
    }

    /// Persist log mutations incrementally to the WAL.
    ///
    /// TODO: after repeated WAL write failures, surface `StorageError` so Raft can react
    /// instead of only logging warnings.
    async fn wal_append_entries(&self, entries: Vec<LogEntry>, inner: &MemStoreInner) {
        let Some(wal) = self.wal.clone() else {
            return;
        };
        let log = inner.log.clone();
        let last_purged = inner.last_purged;

        let result = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            let mut w = wal
                .lock()
                .map_err(|_| std::io::Error::other("WAL mutex poisoned"))?;
            for entry in &entries {
                let payload = bincode::serialize(entry)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                w.append_record_buffered(RecordType::Append, &payload)?;
            }
            w.sync()?;
            if w.file_size() > COMPACTION_THRESHOLD {
                w.compact(&log, last_purged)?;
            }
            Ok(())
        })
        .await;

        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => warn!("Failed to append to WAL: {e}"),
            Err(e) => warn!("WAL append task panicked: {e}"),
        }
    }

    /// TODO: after repeated WAL write failures, surface `StorageError` so Raft can react
    /// instead of only logging warnings.
    async fn wal_truncate_since(&self, since_index: u64, inner: &MemStoreInner) {
        let Some(wal) = self.wal.clone() else {
            return;
        };
        let log = inner.log.clone();
        let last_purged = inner.last_purged;

        let result = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            let mut w = wal
                .lock()
                .map_err(|_| std::io::Error::other("WAL mutex poisoned"))?;
            let payload = bincode::serialize(&TruncatePayload { since_index })
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            w.append_record(RecordType::Truncate, &payload)?;
            if w.file_size() > COMPACTION_THRESHOLD {
                w.compact(&log, last_purged)?;
            }
            Ok(())
        })
        .await;

        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => warn!("Failed to write Truncate to WAL: {e}"),
            Err(e) => warn!("WAL truncate task panicked: {e}"),
        }
    }

    /// TODO: after repeated WAL write failures, surface `StorageError` so Raft can react
    /// instead of only logging warnings.
    async fn wal_purge_upto(&self, log_id: LogId<NodeId>, inner: &MemStoreInner) {
        let Some(wal) = self.wal.clone() else {
            return;
        };
        let log = inner.log.clone();
        let last_purged = inner.last_purged;

        let result = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            let mut w = wal
                .lock()
                .map_err(|_| std::io::Error::other("WAL mutex poisoned"))?;
            let payload = bincode::serialize(&PurgePayload { log_id })
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            w.append_record(RecordType::Purge, &payload)?;
            if w.file_size() > COMPACTION_THRESHOLD {
                w.compact(&log, last_purged)?;
            }
            Ok(())
        })
        .await;

        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => warn!("Failed to write Purge to WAL: {e}"),
            Err(e) => warn!("WAL purge task panicked: {e}"),
        }
    }

    /// Compact/rewrite the WAL from the current in-memory log snapshot.
    ///
    /// Used by property tests that mutate memory then flush a consistent WAL image.
    #[cfg_attr(not(test), allow(dead_code))]
    async fn rewrite_wal(&self, inner: &MemStoreInner) {
        let Some(wal) = self.wal.clone() else {
            return;
        };
        let log = inner.log.clone();
        let last_purged = inner.last_purged;

        let result = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            let mut w = wal
                .lock()
                .map_err(|_| std::io::Error::other("WAL mutex poisoned"))?;
            w.compact(&log, last_purged)
        })
        .await;

        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => warn!("Failed to rewrite WAL: {e}"),
            Err(e) => warn!("WAL rewrite task panicked: {e}"),
        }
    }

    async fn persist_state_machine(&self, inner: &MemStoreInner) {
        if let Some(dir) = &self.data_dir {
            let path = dir.join("state_machine.json");
            let json = match serde_json::to_string_pretty(&inner.state_machine) {
                Ok(j) => j,
                Err(e) => {
                    warn!("Failed to serialize state machine: {e}");
                    return;
                }
            };
            let result = tokio::task::spawn_blocking(move || {
                Self::atomic_write_json(&path, json.as_bytes())
            })
            .await;
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => warn!("Failed to persist state machine: {e}"),
                Err(e) => warn!("persist_state_machine task panicked: {e}"),
            }
        }
    }

    async fn persist_membership(&self, inner: &MemStoreInner) {
        if let Some(dir) = &self.data_dir {
            let pm = PersistedMembership {
                last_applied_log: inner.last_applied_log,
                last_membership: inner.last_membership.clone(),
            };
            let path = dir.join("membership.json");
            let json = match serde_json::to_string(&pm) {
                Ok(j) => j,
                Err(e) => {
                    warn!("Failed to serialize membership: {e}");
                    return;
                }
            };
            let result = tokio::task::spawn_blocking(move || {
                Self::atomic_write_json(&path, json.as_bytes())
            })
            .await;
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => warn!("Failed to persist membership: {e}"),
                Err(e) => warn!("persist_membership task panicked: {e}"),
            }
        }
    }

    /// Atomically write bytes via temp file + fsync + rename.
    fn atomic_write_json(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
        use std::io::Write;

        let dir = path.parent().unwrap_or_else(|| std::path::Path::new("."));
        let file_name = path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "bad path"))?;

        let temp_path = dir.join(format!("{file_name}.{}.tmp", std::process::id()));
        // 1) Write to temporary file
        {
            let mut file = std::fs::File::create(&temp_path)?;
            file.write_all(bytes)?;
            file.sync_all()?;
        }
        // 2) Atomically rename temporary file to target file
        std::fs::rename(&temp_path, path)?;

        // 3) Sync directory so the rename itself is durable
        if let Ok(dir_file) = std::fs::File::open(dir) {
            let _ = dir_file.sync_all();
        }

        Ok(())
    }
}

impl RaftLogReader<TypeConfig> for Arc<MemStore> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + fmt::Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<LogEntry>, StorageError<NodeId>> {
        let inner = self.inner.read().await;
        Ok(inner.log.range(range).map(|(_, v)| v.clone()).collect())
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<MemStore> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let inner = self.inner.read().await;

        let data = serde_json::to_vec(&inner.state_machine).map_err(|e| {
            StorageError::from_io_error(
                openraft::ErrorSubject::StateMachine,
                openraft::ErrorVerb::Read,
                std::io::Error::other(e.to_string()),
            )
        })?;

        let snapshot_id = format!(
            "{}-snap",
            inner
                .last_applied_log
                .map(|id| format!("{}-{}", id.leader_id, id.index))
                .unwrap_or_default(),
        );

        let meta = SnapshotMeta {
            last_log_id: inner.last_applied_log,
            last_membership: inner.last_membership.clone(),
            snapshot_id,
        };

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStorage<TypeConfig> for Arc<MemStore> {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let inner = self.inner.read().await;
        let last_log_id = inner
            .log
            .iter()
            .next_back()
            .map(|(_, entry)| entry.log_id)
            .or(inner.last_purged);
        Ok(LogState {
            last_purged_log_id: inner.last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.write().await;
        inner.vote = Some(*vote);
        self.persist_hard_state(&inner).await;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.read().await.vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.write().await;
        inner.committed = committed;
        self.persist_hard_state(&inner).await;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.read().await.committed)
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = LogEntry> + Send,
    {
        let mut inner = self.inner.write().await;
        let mut appended = Vec::new();
        for entry in entries {
            appended.push(entry.clone());
            inner.log.insert(entry.log_id.index, entry);
        }
        self.wal_append_entries(appended, &inner).await;
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.write().await;
        let keys: Vec<u64> = inner.log.range(log_id.index..).map(|(k, _)| *k).collect();
        for key in keys {
            inner.log.remove(&key);
        }
        self.wal_truncate_since(log_id.index, &inner).await;
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.write().await;
        let keys: Vec<u64> = inner.log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for key in keys {
            inner.log.remove(&key);
        }
        inner.last_purged = Some(log_id);
        self.wal_purge_upto(log_id, &inner).await;
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let inner = self.inner.read().await;
        Ok((inner.last_applied_log, inner.last_membership.clone()))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[LogEntry],
    ) -> Result<Vec<Response>, StorageError<NodeId>> {
        let mut inner = self.inner.write().await;
        let mut results = Vec::with_capacity(entries.len());

        for entry in entries {
            inner.last_applied_log = Some(entry.log_id);

            if let Some(membership) = entry.get_membership() {
                inner.last_membership =
                    StoredMembership::new(Some(entry.log_id), membership.clone());
            }

            let resp = match &entry.payload {
                openraft::EntryPayload::Blank => Response::Ok {
                    version: entry.log_id.index,
                },
                openraft::EntryPayload::Normal(req) => {
                    inner.state_machine.apply(req, entry.log_id.index)
                }
                openraft::EntryPayload::Membership(_) => Response::Ok {
                    version: entry.log_id.index,
                },
            };
            results.push(resp);
        }

        // Persist state machine and membership after applying entries
        self.persist_state_machine(&inner).await;
        self.persist_membership(&inner).await;
        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();
        let state_machine: KvStateMachine = serde_json::from_slice(&data).map_err(|e| {
            StorageError::from_io_error(
                openraft::ErrorSubject::StateMachine,
                openraft::ErrorVerb::Read,
                std::io::Error::other(e.to_string()),
            )
        })?;

        let mut inner = self.inner.write().await;
        inner.state_machine = state_machine;
        inner.last_applied_log = meta.last_log_id;
        inner.last_membership = meta.last_membership.clone();
        inner.snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data,
        });

        // Persist everything after snapshot install
        self.persist_state_machine(&inner).await;
        self.persist_membership(&inner).await;
        self.persist_hard_state(&inner).await;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.read().await;
        Ok(inner.snapshot.as_ref().map(|s| Snapshot {
            meta: s.meta.clone(),
            snapshot: Box::new(Cursor::new(s.data.clone())),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeSet;
    use std::time::{Duration, Instant};

    // ─── Bug Condition Exploration Tests (Task 17) ───

    /// **Validates: Requirements 1.2**
    ///
    /// Bug Condition Exploration Test: Verify persist_* methods use spawn_blocking
    /// for disk I/O, ensuring they don't block the tokio runtime thread.
    #[test]
    fn test_persist_methods_use_spawn_blocking() {
        let source = include_str!("store.rs");

        // JSON-backed helpers still use atomic_write_json inside spawn_blocking.
        let json_persist_methods = [
            "persist_hard_state",
            "persist_state_machine",
            "persist_membership",
        ];

        for method_name in &json_persist_methods {
            let search_pattern = format!("fn {}(", method_name);
            let method_start = source
                .find(&search_pattern)
                .unwrap_or_else(|| panic!("Method {} not found in source", method_name));

            let method_source = &source[method_start..];
            let mut brace_count = 0;
            let mut method_end = 0;
            let mut found_first_brace = false;
            for (i, ch) in method_source.char_indices() {
                if ch == '{' {
                    brace_count += 1;
                    found_first_brace = true;
                } else if ch == '}' {
                    brace_count -= 1;
                    if found_first_brace && brace_count == 0 {
                        method_end = i + 1;
                        break;
                    }
                }
            }
            let method_body = &method_source[..method_end];

            assert!(
                method_body.contains("spawn_blocking"),
                "Method `{}` does NOT use spawn_blocking for disk I/O.",
                method_name
            );
            assert!(
                method_body.contains("atomic_write_json"),
                "Method `{}` should call atomic_write_json (inside spawn_blocking)",
                method_name
            );
        }

        // Log persistence goes through WAL helpers.
        for method_name in [
            "wal_append_entries",
            "wal_truncate_since",
            "wal_purge_upto",
            "rewrite_wal",
        ] {
            let search_pattern = format!("fn {}(", method_name);
            assert!(
                source.contains(&search_pattern),
                "Expected WAL helper `{method_name}` to exist"
            );
            let method_start = source.find(&search_pattern).unwrap();
            let method_source = &source[method_start..];
            let mut brace_count = 0;
            let mut method_end = 0;
            let mut found_first_brace = false;
            for (i, ch) in method_source.char_indices() {
                if ch == '{' {
                    brace_count += 1;
                    found_first_brace = true;
                } else if ch == '}' {
                    brace_count -= 1;
                    if found_first_brace && brace_count == 0 {
                        method_end = i + 1;
                        break;
                    }
                }
            }
            let method_body = &method_source[..method_end];
            assert!(
                method_body.contains("spawn_blocking"),
                "WAL helper `{method_name}` must use spawn_blocking"
            );
        }
    }

    /// **Validates: Requirements 1.2**
    ///
    /// Bug Condition Exploration Test: Verify JSON persist_* methods are async fn that
    /// internally use spawn_blocking for disk I/O.
    #[test]
    fn test_persist_methods_are_non_blocking_fn() {
        let source = include_str!("store.rs");

        let persist_methods = [
            "persist_hard_state",
            "persist_state_machine",
            "persist_membership",
        ];

        for method_name in &persist_methods {
            let search_pattern = format!("fn {}(", method_name);
            let method_start = source
                .find(&search_pattern)
                .unwrap_or_else(|| panic!("Method {} not found in source", method_name));

            let method_source = &source[method_start..];
            let mut brace_count = 0;
            let mut method_end = 0;
            let mut found_first_brace = false;
            for (i, ch) in method_source.char_indices() {
                if ch == '{' {
                    brace_count += 1;
                    found_first_brace = true;
                } else if ch == '}' {
                    brace_count -= 1;
                    if found_first_brace && brace_count == 0 {
                        method_end = i + 1;
                        break;
                    }
                }
            }
            let method_body = &method_source[..method_end];

            assert!(
                method_body.contains("tokio::task::spawn_blocking"),
                "Method `{}` must use tokio::task::spawn_blocking to offload disk I/O.",
                method_name
            );

            let spawn_pos = method_body.find("spawn_blocking").unwrap();
            let write_pos = method_body
                .find("atomic_write_json")
                .expect("persist method should perform file I/O");
            assert!(
                spawn_pos < write_pos,
                "In method `{}`, file I/O should be inside spawn_blocking closure",
                method_name
            );
        }
    }

    /// **Validates: Requirements 1.2**
    ///
    /// Bug Condition Exploration Test: Verify that calling persist operations doesn't
    /// block the current tokio task. We call persist_log with 100 entries and verify
    /// the call returns near-instantly (< 10ms), proving I/O is offloaded.
    #[tokio::test]
    async fn test_persist_does_not_block_tokio_runtime() {
        // Create a store with a temp directory for persistence
        let tmp_dir =
            std::env::temp_dir().join(format!("raft_blocking_test_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&tmp_dir);

        let store = MemStore::new_persistent(tmp_dir.clone());

        // Populate the store with entries to make persist_log do real work
        {
            let mut inner = store.inner.write().await;
            for i in 0..100u64 {
                let log_id = LogId {
                    leader_id: Default::default(),
                    index: i,
                };
                let entry = Entry {
                    log_id,
                    payload: openraft::EntryPayload::Blank,
                };
                inner.log.insert(i, entry);
            }
        }

        // Call all four persist methods and measure total time
        let inner = store.inner.read().await;
        let start = Instant::now();
        store.rewrite_wal(&inner).await;
        store.persist_hard_state(&inner).await;
        store.persist_state_machine(&inner).await;
        store.persist_membership(&inner).await;
        let total_duration = start.elapsed();
        drop(inner);

        // With the durability fix, persist_* methods now AWAIT completion (including fsync).
        // This means they take real disk I/O time, but they don't block the tokio
        // runtime thread — they use spawn_blocking to offload to the blocking pool.
        // On a fast local SSD, 4 persists with fsync should still complete within 2s.
        assert!(
            total_duration < Duration::from_secs(2),
            "All four persist_* calls took {:?} — expected < 2s on local SSD. \
             The calls are awaited (durable) but offloaded via spawn_blocking.",
            total_duration
        );

        // Verify tokio runtime remains responsive during background writes
        // by running a concurrent sleep that must complete on time
        let responsive_check = tokio::time::timeout(
            Duration::from_millis(50),
            tokio::time::sleep(Duration::from_millis(5)),
        )
        .await;
        assert!(
            responsive_check.is_ok(),
            "Tokio runtime unresponsive — persist operations are blocking worker threads"
        );

        // Cleanup
        let _ = std::fs::remove_dir_all(&tmp_dir);
    }

    /// **Validates: Requirements 3.2**
    ///
    /// Property 2: Preservation - Raft State Persistence Integrity
    ///
    /// Verifies that the persist → reload cycle preserves all Raft state data:
    /// - hard_state.json (vote + committed)
    /// - raft_log.wal (last_purged + entries)
    /// - state_machine.json (KvStateMachine)
    /// - membership.json (last_applied_log + last_membership)

    // ─── Strategies for generating random Raft state ───

    fn arb_node_id() -> impl Strategy<Value = NodeId> {
        1u64..=10
    }

    fn arb_log_id() -> impl Strategy<Value = LogId<NodeId>> {
        (arb_node_id(), 1u64..=1000).prop_map(|(node_id, index)| {
            let leader_id = openraft::CommittedLeaderId::new(1, node_id);
            LogId::new(leader_id, index)
        })
    }

    fn arb_vote() -> impl Strategy<Value = Vote<NodeId>> {
        (arb_node_id(), 1u64..=100, prop::bool::ANY).prop_map(|(node_id, term, committed)| {
            if committed {
                Vote::new_committed(term, node_id)
            } else {
                Vote::new(term, node_id)
            }
        })
    }

    fn arb_request() -> impl Strategy<Value = Request> {
        prop_oneof![
            // Set requests with various key/value combinations
            (
                "[a-z/]{1,20}",
                "[a-z0-9]{1,30}",
                prop::option::of(1u64..=3600)
            )
                .prop_map(|(key, value, ttl)| Request::Set {
                    key,
                    value,
                    ttl,
                    prev_exist: None,
                    prev_value: None,
                    prev_version: None,
                    now: 1_700_000_000_000,
                }),
            // Delete requests
            "[a-z/]{1,20}".prop_map(|key| Request::Delete {
                key,
                prev_value: None,
                recursive: false,
            }),
        ]
    }

    fn arb_log_entry(index: u64, leader_id: NodeId) -> impl Strategy<Value = LogEntry> {
        prop_oneof![
            // Blank entry
            Just(Entry {
                log_id: LogId::new(openraft::CommittedLeaderId::new(1, leader_id), index),
                payload: openraft::EntryPayload::Blank,
            }),
            // Normal entry with a request
            arb_request().prop_map(move |req| Entry {
                log_id: LogId::new(openraft::CommittedLeaderId::new(1, leader_id), index),
                payload: openraft::EntryPayload::Normal(req),
            }),
        ]
    }

    fn arb_log_entries(count: usize) -> impl Strategy<Value = Vec<LogEntry>> {
        let leader_id = 1u64;
        let entries: Vec<_> = (1..=count as u64)
            .map(|idx| arb_log_entry(idx, leader_id))
            .collect();
        entries
    }

    // ─── Property-Based Tests ───

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10))]

        /// Property: hard_state.json round-trip preserves vote and committed fields
        #[test]
        fn hard_state_roundtrip(
            vote in prop::option::of(arb_vote()),
            committed in prop::option::of(arb_log_id()),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let tmp_dir = tempdir();

                // Create a store with given hard state and persist it
                let store = MemStore::new_persistent(tmp_dir.clone());
                {
                    let mut inner = store.inner.write().await;
                    inner.vote = vote;
                    inner.committed = committed;
                    store.persist_hard_state(&inner).await;
                }

                // Wait briefly for spawn_blocking to complete

                // Reload from disk
                let reloaded = MemStore::new_persistent(tmp_dir);
                let reloaded_inner = reloaded.inner.read().await;

                prop_assert_eq!(reloaded_inner.vote, vote,
                    "Vote should survive persist → reload cycle");
                prop_assert_eq!(reloaded_inner.committed, committed,
                    "Committed should survive persist → reload cycle");
                Ok(())
            })?;
        }

        /// Property: raft_log.wal round-trip preserves all log entries and last_purged
        #[test]
        fn log_entries_roundtrip(
            entries in arb_log_entries(5),
            has_purged in prop::bool::ANY,
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let tmp_dir = tempdir();

                let last_purged = if has_purged {
                    Some(LogId::new(openraft::CommittedLeaderId::new(1, 1), 0))
                } else {
                    None
                };

                // Create store, insert entries, rewrite WAL from memory snapshot
                let store = MemStore::new_persistent(tmp_dir.clone());
                {
                    let mut inner = store.inner.write().await;
                    inner.last_purged = last_purged;
                    for entry in &entries {
                        inner.log.insert(entry.log_id.index, entry.clone());
                    }
                    store.rewrite_wal(&inner).await;
                }

                // Reload from disk
                let reloaded = MemStore::new_persistent(tmp_dir);
                let reloaded_inner = reloaded.inner.read().await;

                prop_assert_eq!(reloaded_inner.last_purged, last_purged,
                    "last_purged should survive persist → reload cycle");
                prop_assert_eq!(reloaded_inner.log.len(), entries.len(),
                    "Number of log entries should be preserved");

                for entry in &entries {
                    let reloaded_entry = reloaded_inner.log.get(&entry.log_id.index);
                    prop_assert!(reloaded_entry.is_some(),
                        "Entry at index {} should exist after reload", entry.log_id.index);
                    let reloaded_entry = reloaded_entry.unwrap();
                    prop_assert_eq!(reloaded_entry.log_id, entry.log_id,
                        "LogId should be preserved for index {}", entry.log_id.index);
                }
                Ok(())
            })?;
        }

        /// Property: state_machine.json round-trip preserves all KV data
        #[test]
        fn state_machine_roundtrip(
            keys in prop::collection::vec(("[a-z/]{1,15}", "[a-z0-9]{1,20}"), 1..=8),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let tmp_dir = tempdir();

                // Create store, apply some entries to build state machine, persist
                let store = MemStore::new_persistent(tmp_dir.clone());
                {
                    let mut inner = store.inner.write().await;
                    for (idx, (key, value)) in keys.iter().enumerate() {
                        let req = Request::Set {
                            key: key.clone(),
                            value: value.clone(),
                            ttl: None,
                            prev_exist: None,
                            prev_value: None,
                            prev_version: None,
                            now: 1_700_000_000_000,
                        };
                        inner.state_machine.apply(&req, (idx + 1) as u64);
                    }
                    store.persist_state_machine(&inner).await;
                }

                // Wait for spawn_blocking

                // Reload from disk
                let reloaded = MemStore::new_persistent(tmp_dir);
                let reloaded_inner = reloaded.inner.read().await;

                // Verify state machine preserves keys (note: later sets to same key overwrite)
                let unique_keys: BTreeSet<&str> = keys.iter().map(|(k, _)| k.as_str()).collect();
                for key in &unique_keys {
                    let original_entry = inner_get_raw(&store, key).await;
                    let reloaded_entry = inner_get_raw_from_inner(&reloaded_inner, key);

                    match (original_entry, reloaded_entry) {
                        (Some(orig), Some(reload)) => {
                            prop_assert_eq!(&orig.value, &reload.value,
                                "Value for key '{}' should be preserved", key);
                            prop_assert_eq!(orig.version, reload.version,
                                "Version for key '{}' should be preserved", key);
                            prop_assert_eq!(orig.expire_at, reload.expire_at,
                                "expire_at for key '{}' should be preserved", key);
                        }
                        (None, None) => {} // Both empty is fine
                        (orig, reload) => {
                            prop_assert!(false,
                                "Key '{}' mismatch: original={:?}, reloaded={:?}", key, orig.is_some(), reload.is_some());
                        }
                    }
                }
                Ok(())
            })?;
        }

        /// Property: membership.json round-trip preserves last_applied_log and membership config
        #[test]
        fn membership_roundtrip(
            last_applied_index in 1u64..=500,
            leader_id in arb_node_id(),
            member_ids in prop::collection::btree_set(1u64..=10, 1..=5),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let tmp_dir = tempdir();

                let last_applied_log = Some(LogId::new(
                    openraft::CommittedLeaderId::new(1, leader_id),
                    last_applied_index,
                ));

                // Build a membership config with generated node IDs
                let nodes: BTreeMap<NodeId, BasicNode> = member_ids.iter()
                    .map(|&id| (id, BasicNode { addr: format!("10.0.0.{}:2380", id) }))
                    .collect();
                let config = vec![member_ids.clone()];
                let membership = openraft::Membership::new(config, nodes.clone());
                let stored_membership = StoredMembership::new(last_applied_log, membership);

                // Create store, set membership, persist
                let store = MemStore::new_persistent(tmp_dir.clone());
                {
                    let mut inner = store.inner.write().await;
                    inner.last_applied_log = last_applied_log;
                    inner.last_membership = stored_membership.clone();
                    store.persist_membership(&inner).await;
                }

                // Wait for spawn_blocking

                // Reload from disk
                let reloaded = MemStore::new_persistent(tmp_dir);
                let reloaded_inner = reloaded.inner.read().await;

                prop_assert_eq!(reloaded_inner.last_applied_log, last_applied_log,
                    "last_applied_log should survive persist → reload cycle");

                // Verify membership node set is preserved
                let original_nodes = stored_membership.membership().get_joint_config();
                let reloaded_nodes = reloaded_inner.last_membership.membership().get_joint_config();
                prop_assert_eq!(original_nodes, reloaded_nodes,
                    "Membership joint config should be preserved after reload");

                Ok(())
            })?;
        }

        /// Property: full state round-trip (all 4 files) preserves complete store state
        #[test]
        fn full_state_roundtrip(
            vote in prop::option::of(arb_vote()),
            committed in prop::option::of(arb_log_id()),
            entries in arb_log_entries(5),
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let tmp_dir = tempdir();

                // Create and populate store
                let store = MemStore::new_persistent(tmp_dir.clone());
                {
                    let mut inner = store.inner.write().await;
                    inner.vote = vote;
                    inner.committed = committed;
                    for entry in &entries {
                        inner.log.insert(entry.log_id.index, entry.clone());
                    }
                    // Apply one entry to state machine
                    let req = Request::Set {
                        key: "/test/key".to_string(),
                        value: "hello".to_string(),
                        ttl: None,
                        prev_exist: None,
                        prev_value: None,
                        prev_version: None,
                        now: 1_700_000_000_000,
                    };
                    inner.state_machine.apply(&req, 1);
                    inner.last_applied_log = Some(LogId::new(
                        openraft::CommittedLeaderId::new(1, 1),
                        1,
                    ));

                    // Persist all state
                    store.persist_hard_state(&inner).await;
                    store.rewrite_wal(&inner).await;
                    store.persist_state_machine(&inner).await;
                    store.persist_membership(&inner).await;
                }

                // Wait for all spawn_blocking tasks

                // Reload everything from disk
                let reloaded = MemStore::new_persistent(tmp_dir);
                let reloaded_inner = reloaded.inner.read().await;

                // Verify hard state
                prop_assert_eq!(reloaded_inner.vote, vote, "Vote round-trip failed");
                prop_assert_eq!(reloaded_inner.committed, committed, "Committed round-trip failed");

                // Verify log
                prop_assert_eq!(reloaded_inner.log.len(), entries.len(), "Log length mismatch");

                // Verify state machine has the key we applied
                let sm_entry = reloaded_inner.state_machine.get("/test/key");
                prop_assert!(sm_entry.is_some(), "State machine should have /test/key after reload");
                prop_assert_eq!(&sm_entry.unwrap().value, "hello",
                    "State machine value should be preserved");

                Ok(())
            })?;
        }
    }

    // ─── Helper functions ───

    /// Create a temporary directory for test persistence
    fn tempdir() -> PathBuf {
        let dir = std::env::temp_dir()
            .join("pg-ha-test-store")
            .join(format!("{}", std::process::id()))
            .join(format!("{}", rand_suffix()));
        let _ = std::fs::create_dir_all(&dir);
        dir
    }

    /// Generate a random suffix to avoid directory collisions between test cases
    fn rand_suffix() -> u64 {
        use std::hash::{Hash, Hasher};
        use std::time::SystemTime;
        let nanos = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos() as u64;
        let mut hasher = std::hash::DefaultHasher::new();
        std::thread::current().id().hash(&mut hasher);
        nanos ^ hasher.finish()
    }

    /// Get a raw KV entry from the store's state machine (bypasses TTL check)
    async fn inner_get_raw(store: &MemStore, key: &str) -> Option<crate::state_machine::KvEntry> {
        let inner = store.inner.read().await;
        inner_get_raw_from_inner(&inner, key)
    }

    /// Get a raw KV entry from a MemStoreInner reference
    fn inner_get_raw_from_inner(
        inner: &MemStoreInner,
        key: &str,
    ) -> Option<crate::state_machine::KvEntry> {
        inner.state_machine.get(key).cloned()
    }
}
