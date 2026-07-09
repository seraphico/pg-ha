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
use std::sync::Arc;

use openraft::entry::RaftPayload;
use openraft::storage::{LogState, Snapshot};
use openraft::{
    BasicNode, Entry, LogId, RaftLogReader, RaftSnapshotBuilder, RaftStorage, SnapshotMeta,
    StorageError, StoredMembership, Vote,
};
use openraft::TokioRuntime;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::state_machine::{KvStateMachine, Request, Response};

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

// ─────────────────── Persisted log entries ───────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct PersistedLog {
    last_purged: Option<LogId<NodeId>>,
    entries: Vec<LogEntry>,
}

// ─────────────────── Persisted membership ───────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedMembership {
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
}

// ─────────────────── Combined Storage ───────────────────

/// Raft storage with optional disk persistence.
/// When `data_dir` is set, state is persisted to JSON files after every mutation.
#[derive(Debug, Clone)]
pub struct MemStore {
    inner: Arc<RwLock<MemStoreInner>>,
    data_dir: Option<PathBuf>,
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

        // Load log entries
        let log_path = data_dir.join("log_entries.json");
        if let Ok(json) = std::fs::read_to_string(&log_path) {
            if let Ok(pl) = serde_json::from_str::<PersistedLog>(&json) {
                info!(
                    "Loaded {} log entries from disk (last_purged={:?})",
                    pl.entries.len(),
                    pl.last_purged
                );
                inner.last_purged = pl.last_purged;
                for entry in pl.entries {
                    inner.log.insert(entry.log_id.index, entry);
                }
            } else {
                warn!("Failed to parse log_entries.json — starting with empty log");
            }
        }

        // Load membership
        let membership_path = data_dir.join("membership.json");
        if let Ok(json) = std::fs::read_to_string(&membership_path) {
            if let Ok(pm) = serde_json::from_str::<PersistedMembership>(&json) {
                info!("Loaded membership from disk: last_applied={:?}", pm.last_applied_log);
                inner.last_applied_log = pm.last_applied_log;
                inner.last_membership = pm.last_membership;
            } else {
                warn!("Failed to parse membership.json — starting fresh");
            }
        }

        Self {
            inner: Arc::new(RwLock::new(inner)),
            data_dir: Some(data_dir),
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

    fn persist_hard_state(&self, inner: &MemStoreInner) {
        if let Some(dir) = &self.data_dir {
            let hs = PersistedHardState {
                vote: inner.vote,
                committed: inner.committed,
            };
            let path = dir.join("hard_state.json");
            if let Ok(json) = serde_json::to_string(&hs)
                && let Err(e) = std::fs::write(&path, json) {
                    warn!("Failed to persist hard_state: {e}");
                }
        }
    }

    fn persist_log(&self, inner: &MemStoreInner) {
        if let Some(dir) = &self.data_dir {
            let pl = PersistedLog {
                last_purged: inner.last_purged,
                entries: inner.log.values().cloned().collect(),
            };
            let path = dir.join("log_entries.json");
            if let Ok(json) = serde_json::to_string(&pl)
                && let Err(e) = std::fs::write(&path, json) {
                    warn!("Failed to persist log entries: {e}");
                }
        }
    }

    fn persist_state_machine(&self, inner: &MemStoreInner) {
        if let Some(dir) = &self.data_dir {
            let path = dir.join("state_machine.json");
            inner.state_machine.save_to_disk(&path);
        }
    }

    fn persist_membership(&self, inner: &MemStoreInner) {
        if let Some(dir) = &self.data_dir {
            let pm = PersistedMembership {
                last_applied_log: inner.last_applied_log,
                last_membership: inner.last_membership.clone(),
            };
            let path = dir.join("membership.json");
            if let Ok(json) = serde_json::to_string(&pm)
                && let Err(e) = std::fs::write(&path, json) {
                    warn!("Failed to persist membership: {e}");
                }
        }
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
        self.persist_hard_state(&inner);
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
        self.persist_hard_state(&inner);
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
        for entry in entries {
            inner.log.insert(entry.log_id.index, entry);
        }
        self.persist_log(&inner);
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
        self.persist_log(&inner);
        Ok(())
    }

    async fn purge_logs_upto(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.write().await;
        let keys: Vec<u64> = inner.log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for key in keys {
            inner.log.remove(&key);
        }
        inner.last_purged = Some(log_id);
        self.persist_log(&inner);
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
        self.persist_state_machine(&inner);
        self.persist_membership(&inner);
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
        let state_machine: KvStateMachine =
            serde_json::from_slice(&data).map_err(|e| {
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
        self.persist_state_machine(&inner);
        self.persist_membership(&inner);
        self.persist_hard_state(&inner);
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
