//! Cluster state types representing the DCS view of the cluster
//!
//! These types are deserialized from DCS keys and used by the HA engine
//! to make decisions about failover, promotion, and replication topology.

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

    /// Failsafe topology from DCS /failsafe key
    pub failsafe: Option<HashMap<String, String>>,

    /// Cluster history (timeline changes)
    pub history: Vec<HistoryEntry>,

    /// The DCS `/timeline` key (D6): identity of the latest confirmed
    /// timeline and any promote that may still be creating a newer one.
    #[serde(default)]
    pub timeline: ClusterTimeline,

    /// Number of Raft voters (pg-ha nodes), when the DCS knows it. Used only
    /// to tell whether every node has published its timeline evidence.
    #[serde(default)]
    pub dcs_voters: Option<usize>,
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

    /// Get suitable clone sources (prefer clonefrom-tagged, then leader)
    pub fn get_clone_member(&self, exclude_name: &str) -> Option<&Member> {
        // Prefer members with clonefrom tag
        let clone_source = self.members.iter().find(|m| {
            m.name != exclude_name
                && m.tags.get("clonefrom").and_then(|v| v.as_bool()) == Some(true)
        });
        if clone_source.is_some() {
            return clone_source;
        }
        // Fallback to leader
        if let Some(leader) = &self.leader {
            return self
                .members
                .iter()
                .find(|m| m.name == leader.name && m.name != exclude_name);
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
}

impl SyncState {
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

/// A cluster history entry (timeline change record)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEntry {
    pub timeline: u64,
    pub lsn: u64,
    pub reason: String,
    #[serde(default)]
    pub timestamp: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

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
        };
        assert!(sync.matches("node2"));
        assert!(sync.matches("NODE3")); // case-insensitive
        assert!(!sync.matches("node1"));
        assert!(!sync.matches("node4"));
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
                },
            ],
            ..Default::default()
        };

        // Should prefer node2 (has clonefrom tag)
        let source = cluster.get_clone_member("node3").unwrap();
        assert_eq!(source.name, "node2");
    }
}
