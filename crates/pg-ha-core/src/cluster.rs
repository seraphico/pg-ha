//! Cluster state types representing the DCS view of the cluster
//!
//! These types are deserialized from DCS keys and used by the HA engine
//! to make decisions about failover, promotion, and replication topology.

use crate::history::HistoryEntry;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents the current state of the cluster as read from DCS
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Cluster {
    /// Current leader, if any
    pub leader: Option<Leader>,

    /// All known cluster members
    pub members: Vec<Member>,

    /// Cluster initialization state (PostgreSQL system identifier)
    pub initialize: Option<String>,

    /// Dynamic cluster configuration stored in DCS /config key
    pub config: Option<ClusterConfig>,

    /// Synchronous replication state from DCS /sync key
    pub sync_state: Option<SyncState>,

    /// Pending failover/switchover request from DCS /failover key
    pub failover: Option<Failover>,

    /// Cluster history (failovers and switchovers), oldest first; the DCS `/history` key
    pub history: Vec<HistoryEntry>,

    /// The DCS `/timeline` key (D6): identity of the latest confirmed
    /// timeline and any promote that may still be creating a newer one.
    #[serde(default)]
    pub timeline: ClusterTimeline,

    /// Number of Raft voters (pg-ha nodes), when the DCS knows it. Used only
    /// to tell whether every node has published its timeline evidence.
    #[serde(default)]
    pub dcs_voters: Option<usize>,
    /// SB-37: the operator's one-shot quorum recovery grant (DCS `/recovery`,
    /// TTL = lock ttl). None when absent, expired or unparsable.
    #[serde(default)]
    pub recovery: Option<Recovery>,
}
/// SB-37: what a recovery grant was planned against. The grant is used only
/// while the live `/sync` and `/timeline` still equal it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoverySnapshot {
    /// `/sync` leader, lower-cased.
    pub sync_leader: String,
    /// `/sync` voters, lower-cased and sorted.
    pub voters: Vec<String>,
    /// `/sync` quorum `q`.
    pub quorum: u32,
    /// `/timeline` record: timeline, begin LSN and key version (None: no
    /// current-format record).
    pub timeline: Option<TimelineId>,
    pub timeline_version: Option<u64>,
}
impl RecoverySnapshot {
    /// The snapshot of a live `/sync` and `/timeline`.
    pub fn of(sync: &SyncState, timeline: &ClusterTimeline) -> Self {
        let mut voters: Vec<String> = sync
            .sync_standby_names()
            .into_iter()
            .map(str::to_ascii_lowercase)
            .collect();
        voters.sort();
        voters.dedup();
        Self {
            sync_leader: sync.leader.to_ascii_lowercase(),
            voters,
            quorum: sync.quorum,
            timeline: timeline.record().map(TimelineRecord::id),
            timeline_version: timeline.version(),
        }
    }
    /// Does the live state still equal this snapshot?
    pub fn matches(&self, sync: Option<&SyncState>, timeline: &ClusterTimeline) -> bool {
        sync.is_some_and(|s| Self::of(s, timeline) == *self)
    }
}
/// SB-37: DCS `/recovery`, written create-only by `POST /recovery` and
/// deleted by any leader lock acquisition, by `DELETE /recovery`, or by its
/// TTL. It lets `candidate` win the vote with every voter outside `lost`
/// instead of `q` voters, accepting that commits held only by `lost` may be
/// gone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Recovery {
    pub candidate: String,
    /// Voters the operator declared lost (lower-cased, sorted).
    pub lost: Vec<String>,
    pub snapshot: RecoverySnapshot,
    /// The candidate is below the recorded timeline: the newer history is
    /// abandoned (same effect as `pg-ha.accept_timeline_loss`).
    pub timeline_loss: bool,
    /// The digest the operator confirmed.
    pub digest: String,
    /// Node whose REST API wrote the grant.
    pub created_by: String,
    /// pg-ha version of that node; a candidate of another version ignores it.
    pub pg_ha_version: String,
    /// Expiry (millis since the epoch) as the DCS reports it; not stored.
    #[serde(skip)]
    pub expire_at_ms: Option<u64>,
}
impl Recovery {
    /// Is `name` this grant's candidate?
    pub fn names_candidate(&self, name: &str) -> bool {
        self.candidate.eq_ignore_ascii_case(name)
    }
    /// The grant as stored in DCS; None when the value does not parse (a
    /// malformed grant is treated as absent).
    pub fn parse(value: &str) -> Option<Self> {
        serde_json::from_str(value).ok()
    }
}

/// D6: the DCS `/timeline` key as read, with the version for CAS writes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClusterTimeline {
    /// No key.
    #[default]
    Absent,
    /// Pre-D6 format: a bare number, an upper bound that may exceed every
    /// real timeline. Never trusted as an identity.
    Legacy { value: u64, version: u64 },
    /// Current format.
    Record {
        record: TimelineRecord,
        version: u64,
    },
    /// Neither format parses. Treated like a missing key, but CAS writes
    /// replace it by version.
    Unreadable { version: u64 },
}

impl ClusterTimeline {
    /// Parse the stored value. A bare number is the legacy format.
    pub fn parse(value: &str, version: u64) -> Self {
        let trimmed = value.trim();
        if let Ok(value) = trimmed.parse::<u64>() {
            return Self::Legacy { value, version };
        }
        match serde_json::from_str::<TimelineRecord>(trimmed) {
            Ok(record) => Self::Record { record, version },
            Err(_) => Self::Unreadable { version },
        }
    }

    /// The current-format record, if any.
    pub fn record(&self) -> Option<&TimelineRecord> {
        match self {
            Self::Record { record, .. } => Some(record),
            _ => None,
        }
    }

    /// Version of the stored key (None when absent), for CAS writes.
    pub fn version(&self) -> Option<u64> {
        match self {
            Self::Absent => None,
            Self::Legacy { version, .. }
            | Self::Record { version, .. }
            | Self::Unreadable { version } => Some(*version),
        }
    }

    /// History generation the cluster is in (0 before any override).
    pub fn generation(&self) -> u64 {
        self.record().map_or(0, |r| r.generation)
    }
}

/// A timeline identified by its number and the LSN where it began (the
/// switchpoint in its history file; 0 for timeline 1). With pg-ha's
/// invariants the number alone is unique; the begin LSN detects histories
/// created outside them (older versions, manual promotes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TimelineId {
    pub timeline: u64,
    pub begin: u64,
}

/// D6 `/timeline` value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimelineRecord {
    /// Latest confirmed timeline: the one the last confirmed primary wrote on.
    pub timeline: u64,
    pub begin: u64,
    /// Node that confirmed it (informational).
    #[serde(default)]
    pub primary: String,
    /// History generation. Raised only by an operator override that accepted
    /// losing the newer history; data of an older generation must be cloned
    /// again, never rewound.
    #[serde(default)]
    pub generation: u64,
    /// A promote recorded before `pg_ctl promote` and not yet confirmed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending: Option<PendingPromote>,
}

impl TimelineRecord {
    pub fn id(&self) -> TimelineId {
        TimelineId {
            timeline: self.timeline,
            begin: self.begin,
        }
    }
}

/// A promote that may have created a timeline above `base`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingPromote {
    pub node: String,
    pub base_timeline: u64,
    pub base_begin: u64,
    /// Generation the promote creates (record generation + 1 for an override).
    pub generation: u64,
    /// The promoter's server run (PID and start time from `postmaster.pid`)
    /// when it wrote this. A later server run cannot still be finishing this
    /// promote: the postmaster removes promote signal files at startup.
    #[serde(default)]
    pub postmaster_start: Option<String>,
}

impl PendingPromote {
    pub fn base(&self) -> TimelineId {
        TimelineId {
            timeline: self.base_timeline,
            begin: self.base_begin,
        }
    }
}

/// D6 evidence a member publishes about its timelines.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lineage {
    /// Begin LSN of the member's published `timeline`, from its local
    /// history file (0 for timeline 1). None when the file is missing.
    #[serde(default)]
    pub timeline_begin: Option<u64>,
    /// Newest timeline history known on disk (`pg_wal/*.history`).
    #[serde(default)]
    pub history: Option<HistoryEvidence>,
    /// Server run (PID and start time from `postmaster.pid`); None while
    /// PostgreSQL is stopped.
    #[serde(default)]
    pub postmaster_start: Option<String>,
    /// History generation of the data directory (`pg-ha.generation`;
    /// missing file = 0).
    #[serde(default)]
    pub generation: u64,
}

/// Newest timeline history file in a data directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryEvidence {
    pub timeline: u64,
    pub begin: u64,
    /// A WAL segment named with this timeline exists: the node has WAL on
    /// it, not only its history file.
    pub wal: bool,
}

impl HistoryEvidence {
    pub fn id(&self) -> TimelineId {
        TimelineId {
            timeline: self.timeline,
            begin: self.begin,
        }
    }
}

impl Cluster {
    /// Returns true if the cluster has no leader (unlocked)
    pub fn is_unlocked(&self) -> bool {
        self.leader.is_none()
    }

    /// Check if a named member exists in the cluster
    pub fn has_member(&self, name: &str) -> bool {
        self.members.iter().any(|m| m.name == name)
    }

    /// Find a member by name
    pub fn get_member(&self, name: &str) -> Option<&Member> {
        self.members.iter().find(|m| m.name == name)
    }

    /// Get suitable clone sources (prefer clonefrom-tagged, then leader).
    /// Members tagged `noclone` are never returned, like `select_clone_source`.
    pub fn get_clone_member(&self, exclude_name: &str) -> Option<&Member> {
        let cloneable = |m: &Member| {
            m.name != exclude_name && m.tags.get("noclone").and_then(|v| v.as_bool()) != Some(true)
        };
        // Prefer members with clonefrom tag
        let clone_source = self.members.iter().find(|m| {
            cloneable(m) && m.tags.get("clonefrom").and_then(|v| v.as_bool()) == Some(true)
        });
        if clone_source.is_some() {
            return clone_source;
        }
        // Fallback to leader
        if let Some(leader) = &self.leader {
            return self
                .members
                .iter()
                .find(|m| m.name == leader.name && cloneable(m));
        }
        None
    }

    /// Create an empty cluster state
    pub fn empty() -> Self {
        Self::default()
    }
}

/// Leader information from the DCS /leader key
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Leader {
    /// Name of the leader node
    pub name: String,

    /// Version/index of the leader key (used for CAS operations)
    pub version: u64,
}

/// A cluster member registered in DCS under /members/{name}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Member {
    /// Member name (unique within cluster)
    pub name: String,

    /// PostgreSQL connection URL
    pub conn_url: String,

    /// REST API URL for this member
    pub api_url: String,

    /// Current state of PostgreSQL on this member
    pub state: MemberState,

    /// Current role of PostgreSQL on this member
    pub role: MemberRole,

    /// WAL LSN position (bytes)
    #[serde(default)]
    pub wal_position: Option<u64>,

    /// Current timeline
    #[serde(default)]
    pub timeline: Option<u64>,

    /// Node tags
    #[serde(default)]
    pub tags: HashMap<String, serde_json::Value>,

    /// Patroni/pg-ha version running on this member
    #[serde(default)]
    pub version: Option<String>,

    /// The member has the pg-ha.rewind_check marker: it may hold WAL beyond a
    /// fork point and has not been verified with pg_rewind yet. Members that do
    /// not publish it (older versions) count as unmarked.
    #[serde(default)]
    pub rewind_check: bool,

    /// D6 timeline evidence. None from older versions: such a member cannot
    /// prove it is on the latest timeline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage: Option<Lineage>,

    /// W2: the member's watchdog is `required` but cannot be armed, so it
    /// will not take the leader role. Older versions do not publish it.
    #[serde(default)]
    pub watchdog_failed: bool,
}

impl Member {
    /// Check if this member has nofailover set
    pub fn is_nofailover(&self) -> bool {
        self.tags
            .get("nofailover")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }

    /// Check if this member has noloadbalance set
    pub fn is_noloadbalance(&self) -> bool {
        self.tags
            .get("noloadbalance")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }

    /// Check if this member has nosync set
    pub fn is_nosync(&self) -> bool {
        self.tags
            .get("nosync")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }

    /// Get failover priority (default 1, 0 means nofailover)
    pub fn failover_priority(&self) -> u32 {
        self.tags
            .get("failover_priority")
            .and_then(|v| v.as_u64())
            .unwrap_or(1) as u32
    }

    /// Get sync replication priority
    pub fn sync_priority(&self) -> u32 {
        self.tags
            .get("sync_priority")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32
    }
}

/// PostgreSQL member state
#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemberState {
    Running,
    Stopped,
    Starting,
    Crashed,

    #[default]
    Unknown,
}

/// PostgreSQL member role
#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemberRole {
    Primary,
    Replica,
    StandbyLeader,

    #[default]
    Uninitialized,
}

impl std::fmt::Display for MemberRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Primary => write!(f, "primary"),
            Self::Replica => write!(f, "replica"),
            Self::StandbyLeader => write!(f, "standby_leader"),
            Self::Uninitialized => write!(f, "uninitialized"),
        }
    }
}

/// Synchronous replication state stored in DCS /sync key
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncState {
    /// Leader that manages the sync key
    pub leader: String,

    /// Comma-separated list of synchronous standbys
    pub sync_standby: Option<String>,

    /// Quorum requirement for leader election from sync standbys
    #[serde(default)]
    pub quorum: u32,

    /// Which synchronous-replication semantics the `quorum` field and the
    /// `sync_standby` list follow (SB-13). Absent in old `/sync` values and old
    /// Raft snapshots, which means [`SyncKind::Priority`]. It is skipped on
    /// serialization while `Priority`, so a Priority `/sync` value stays
    /// byte-for-byte what older versions wrote and read.
    #[serde(default, skip_serializing_if = "SyncKind::is_priority")]
    pub kind: SyncKind,
}

/// Semantics of a `/sync` value (see [`SyncState::kind`]).
///
/// `Priority`: every name in `sync_standby` holds every acknowledged commit
/// (`FIRST n`); the `quorum` field is legacy and must be ignored.
/// `Quorum`: `sync_standby` is the voter set V of `ANY n (V)` and `quorum` is
/// `|V| - n`; membership alone no longer proves a node holds every
/// acknowledged commit (see `crate::quorum`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SyncKind {
    #[default]
    Priority,
    Quorum,
}

impl SyncKind {
    /// Used by `skip_serializing_if` to keep the Priority wire format unchanged.
    pub fn is_priority(&self) -> bool {
        *self == SyncKind::Priority
    }
}

/// SB-13: has `q` been relaxed to `|V| - n`? `voters` is `|V|`, `n` the
/// number PostgreSQL enforces (`ANY n`, 0 when `synchronous_standby_names` is
/// empty). While voters exist but `n` is still 0 the value is not settled:
/// `q = |V|` there is the "not relaxed yet" state, not a finished one.
pub fn quorum_is_settled(voters: usize, q: usize, n: usize) -> bool {
    if voters > 0 && n == 0 {
        return false;
    }
    q == voters.saturating_sub(n)
}

impl SyncState {
    /// Number of distinct voters (names, case-insensitive).
    pub fn voter_count(&self) -> usize {
        self.sync_standby_names()
            .iter()
            .map(|n| n.to_ascii_lowercase())
            .collect::<std::collections::BTreeSet<_>>()
            .len()
    }

    /// SB-13: is this a Quorum `/sync` whose `q` equals `|V| - n`, given the
    /// `n` the leader observed with `SHOW synchronous_standby_names`? `None`
    /// when the kind is not Quorum or `n` is unknown.
    pub fn quorum_settled(&self, observed_n: Option<usize>) -> Option<bool> {
        if self.kind != SyncKind::Quorum {
            return None;
        }
        Some(quorum_is_settled(
            self.voter_count(),
            self.quorum as usize,
            observed_n?,
        ))
    }

    /// Check if sync state is effectively empty/unset
    pub fn is_empty(&self) -> bool {
        self.leader.is_empty()
    }

    /// Get the synchronous standby names as a list
    pub fn sync_standby_names(&self) -> Vec<&str> {
        self.sync_standby
            .as_deref()
            .map(|s| {
                s.split(',')
                    .map(|n| n.trim())
                    .filter(|n| !n.is_empty())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Check if a given name is in the sync standby list (case-insensitive)
    pub fn matches(&self, name: &str) -> bool {
        self.sync_standby_names()
            .iter()
            .any(|n| n.eq_ignore_ascii_case(name))
    }
}

/// Dynamic cluster configuration stored in DCS /config key
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Version of the config key (for CAS updates)
    pub version: u64,

    /// Arbitrary configuration data
    pub data: HashMap<String, serde_json::Value>,
}

/// Failover/switchover request stored in DCS /failover key
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Failover {
    /// If set, this is a switchover from the named leader
    #[serde(default)]
    pub leader: Option<String>,

    /// Target candidate for failover/switchover.
    /// Accepts both `candidate` (native) and `member` (Patroni-compatible) JSON keys.
    #[serde(default, alias = "member")]
    pub candidate: Option<String>,

    /// Scheduled time (ISO 8601) for the operation
    #[serde(default)]
    pub scheduled_at: Option<String>,
}

impl Failover {
    /// Returns true if this is a switchover (has a leader field)
    pub fn is_switchover(&self) -> bool {
        self.leader.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quorum_is_settled_means_q_equals_voters_minus_n_and_n_is_on() {
        // 3 nodes, ANY 1 of 2 voters: q = 1 is settled, q = 2 is the window.
        assert!(quorum_is_settled(2, 1, 1));
        assert!(!quorum_is_settled(2, 2, 1));
        // 5 nodes, ANY 2 of 4.
        assert!(quorum_is_settled(4, 2, 2));
        assert!(!quorum_is_settled(4, 4, 2));
        // Voters but PostgreSQL still asynchronous (n = 0): not settled.
        assert!(!quorum_is_settled(2, 2, 0));
        // No voters, nothing to relax.
        assert!(quorum_is_settled(0, 0, 0));
    }

    #[test]
    fn sync_state_quorum_settled_needs_a_quorum_kind_and_a_known_n() {
        let state = |kind, q| SyncState {
            leader: "node1".into(),
            sync_standby: Some("node2,Node3,node2".into()),
            quorum: q,
            kind,
        };
        assert_eq!(state(SyncKind::Quorum, 1).voter_count(), 2);
        assert_eq!(
            state(SyncKind::Quorum, 1).quorum_settled(Some(1)),
            Some(true)
        );
        assert_eq!(
            state(SyncKind::Quorum, 2).quorum_settled(Some(1)),
            Some(false)
        );
        assert_eq!(state(SyncKind::Quorum, 1).quorum_settled(None), None);
        assert_eq!(state(SyncKind::Priority, 1).quorum_settled(Some(1)), None);
    }

    #[test]
    fn test_failover_deserializes_member_alias() {
        let native: Failover =
            serde_json::from_str(r#"{"leader":"node1","candidate":"node2"}"#).unwrap();
        assert_eq!(native.candidate.as_deref(), Some("node2"));

        let patroni: Failover =
            serde_json::from_str(r#"{"leader":"node1","member":"node3"}"#).unwrap();
        assert_eq!(patroni.candidate.as_deref(), Some("node3"));
        assert!(patroni.is_switchover());
    }

    #[test]
    fn member_without_rewind_check_field_deserializes_as_unmarked() {
        let old: Member = serde_json::from_str(
            r#"{"name":"n1","conn_url":"host=n1","api_url":"","state":"running","role":"replica","timeline":2}"#,
        )
        .unwrap();
        assert!(!old.rewind_check);
        let new: Member = serde_json::from_str(
            r#"{"name":"n1","conn_url":"host=n1","api_url":"","state":"running","role":"replica","rewind_check":true}"#,
        )
        .unwrap();
        assert!(new.rewind_check);
    }

    #[test]
    fn test_cluster_is_unlocked() {
        let cluster = Cluster::empty();
        assert!(cluster.is_unlocked());

        let cluster = Cluster {
            leader: Some(Leader {
                name: "node1".to_string(),
                version: 1,
            }),
            ..Default::default()
        };
        assert!(!cluster.is_unlocked());
    }

    #[test]
    fn test_sync_state_matches() {
        let sync = SyncState {
            leader: "node1".to_string(),
            sync_standby: Some("node2,node3".to_string()),
            quorum: 0,
            kind: Default::default(),
        };
        assert!(sync.matches("node2"));
        assert!(sync.matches("NODE3")); // case-insensitive
        assert!(!sync.matches("node1"));
        assert!(!sync.matches("node4"));
    }

    #[test]
    fn test_sync_state_kind_backward_compatible() {
        // Old /sync value and old Raft snapshot content: no `kind` -> Priority.
        let old = r#"{"leader":"node1","sync_standby":"node2","quorum":1}"#;
        let s: SyncState = serde_json::from_str(old).unwrap();
        assert_eq!(s.kind, SyncKind::Priority);
        assert_eq!(s.quorum, 1);
        // Even older: no `quorum` either.
        let older: SyncState =
            serde_json::from_str(r#"{"leader":"n","sync_standby":null}"#).unwrap();
        assert_eq!(older.kind, SyncKind::Priority);

        // Priority serializes exactly as before (no `kind` key), so what old
        // nodes read and what a Priority leader writes does not change.
        let json = serde_json::to_string(&s).unwrap();
        assert_eq!(
            json,
            r#"{"leader":"node1","sync_standby":"node2","quorum":1}"#
        );

        // Quorum round-trips, and an old reader (no deny_unknown_fields; the
        // pre-SB-13 shape is simulated by a map) ignores the extra key.
        let q = SyncState {
            kind: SyncKind::Quorum,
            ..s
        };
        let json = serde_json::to_string(&q).unwrap();
        assert!(json.contains(r#""kind":"quorum""#));
        let back: SyncState = serde_json::from_str(&json).unwrap();
        assert_eq!(back.kind, SyncKind::Quorum);
        #[derive(Deserialize)]
        #[allow(dead_code)]
        struct PreSb13 {
            leader: String,
            sync_standby: Option<String>,
            #[serde(default)]
            quorum: u32,
        }
        let old_reader: PreSb13 = serde_json::from_str(&json).unwrap();
        assert_eq!(old_reader.sync_standby.as_deref(), Some("node2"));
    }

    #[test]
    fn test_member_tag_helpers() {
        let member = Member {
            name: "n1".to_string(),
            conn_url: String::new(),
            api_url: String::new(),
            state: MemberState::Running,
            role: MemberRole::Replica,
            wal_position: None,
            timeline: None,
            tags: HashMap::from([
                ("nofailover".to_string(), serde_json::json!(true)),
                ("failover_priority".to_string(), serde_json::json!(0)),
            ]),
            version: None,
            rewind_check: false,
            lineage: None,
            watchdog_failed: false,
        };
        assert!(member.is_nofailover());
        assert_eq!(member.failover_priority(), 0);
        assert!(!member.is_noloadbalance());
    }

    #[test]
    fn test_cluster_get_clone_member() {
        let cluster = Cluster {
            leader: Some(Leader {
                name: "node1".to_string(),
                version: 1,
            }),
            members: vec![
                Member {
                    name: "node1".to_string(),
                    conn_url: "postgres://node1".to_string(),
                    api_url: String::new(),
                    state: MemberState::Running,
                    role: MemberRole::Primary,
                    wal_position: None,
                    timeline: None,
                    tags: HashMap::new(),
                    version: None,
                    rewind_check: false,
                    lineage: None,
                    watchdog_failed: false,
                },
                Member {
                    name: "node2".to_string(),
                    conn_url: "postgres://node2".to_string(),
                    api_url: String::new(),
                    state: MemberState::Running,
                    role: MemberRole::Replica,
                    wal_position: None,
                    timeline: None,
                    tags: HashMap::from([("clonefrom".to_string(), serde_json::json!(true))]),
                    version: None,
                    rewind_check: false,
                    lineage: None,
                    watchdog_failed: false,
                },
            ],
            ..Default::default()
        };

        // Should prefer node2 (has clonefrom tag)
        let source = cluster.get_clone_member("node3").unwrap();
        assert_eq!(source.name, "node2");
    }

    #[test]
    fn test_cluster_get_clone_member_skips_noclone() {
        let member = |name: &str, tags: &[(&str, bool)]| Member {
            name: name.to_string(),
            conn_url: String::new(),
            api_url: String::new(),
            state: MemberState::Running,
            role: MemberRole::Replica,
            wal_position: None,
            timeline: None,
            tags: tags
                .iter()
                .map(|(k, v)| (k.to_string(), serde_json::json!(v)))
                .collect(),
            version: None,
            rewind_check: false,
            lineage: None,
            watchdog_failed: false,
        };
        let cluster = Cluster {
            leader: Some(Leader {
                name: "node1".to_string(),
                version: 1,
            }),
            members: vec![
                member("node1", &[("noclone", true)]),
                member("node2", &[("clonefrom", true), ("noclone", true)]),
                member("node3", &[("clonefrom", true)]),
            ],
            ..Default::default()
        };
        // node2 is clonefrom but noclone: node3 is chosen.
        assert_eq!(cluster.get_clone_member("node9").unwrap().name, "node3");
        // Only noclone members left (leader included): no source.
        assert!(cluster.get_clone_member("node3").is_none());
    }
}
