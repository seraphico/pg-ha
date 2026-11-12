//! Synchronous replication mode management
//!
//! Automatically computes and sets `synchronous_standby_names` based on
//! cluster state, node tags (nosync, sync_priority), real replication lag
//! from `pg_stat_replication`, and configuration.

use crate::cluster::Member;
use crate::postgresql::ReplicationSnapshot;

/// Placeholder emitted in strict mode when no standby is eligible.
///
/// It is a quoted name that matches no real member, so PostgreSQL keeps
/// every commit WAITing (writes block) while `sync_failover_allowed`
/// (`ha/election.rs`) and `SyncState::matches` still return false for it —
/// no node becomes promotable when nobody is eligible.
///
/// Using `'*'` here would be wrong: `'*'` means "any connected standby" and
/// does NOT block commits, so it must never be used to represent the
/// unsatisfiable strict case.
pub const STRICT_NO_ELIGIBLE_PLACEHOLDER: &str = "FIRST 1 (\"_pg_ha_no_eligible_sync_standby_\")";

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
}

impl SyncManager {
    pub fn new(mode: SyncMode, sync_node_count: usize, strict_mode: bool) -> Self {
        Self {
            mode,
            sync_node_count: sync_node_count.max(1),
            strict_mode,
        }
    }

    /// Compute the `synchronous_standby_names` value based on current members
    /// and a primary-side replication snapshot.
    ///
    /// A candidate must be a Running DCS replica that is NOT this node, not
    /// tagged `nosync`, AND actually streaming (present in `snapshot.replicas`
    /// by `application_name` == member name, case-insensitive, with a known
    /// `flush_lsn`). A Running member absent from the snapshot is excluded.
    ///
    /// `threshold` is the maximum tolerated lag in bytes; when `Some(t)`, a
    /// candidate whose `flush_lsn` trails the baseline by more than `t` is
    /// dropped. When `None`, no lag filtering is applied.
    ///
    /// `current_voters` are the names currently recorded in `/sync`; they are
    /// kept sticky on a priority tie to avoid flapping.
    ///
    /// Returns `None` if sync mode is Off.
    /// Returns `Some("")` in non-strict mode when nobody is eligible (the
    /// cluster degrades to async; failover stays forbidden).
    /// Returns the unsatisfiable placeholder in strict mode when nobody is
    /// eligible (every commit blocks).
    pub fn compute_sync_standby_names(
        &self,
        members: &[Member],
        my_name: &str,
        snapshot: &ReplicationSnapshot,
        threshold: Option<u64>,
        current_voters: &[String],
    ) -> Option<String> {
        let selection =
            self.select_sync_standbys(members, my_name, snapshot, threshold, current_voters)?;
        self.format_sync_standby_names(&selection)
    }

    /// Selection half of [`Self::compute_sync_standby_names`]: decide WHICH
    /// standbys are listed and with what `count`, without formatting.
    ///
    /// Eligibility, lag filtering and ordering are identical for every mode.
    /// The modes differ only in how the ordered candidates become a list:
    /// - `Priority`: the first `count` candidates (`FIRST count (...)`).
    /// - `Quorum`: ALL eligible candidates, sorted by name, with `count` as the
    ///   `ANY n` confirmation number (`ANY n (V)`, `quorum = |V| - n`).
    ///   `count` is never used to truncate the list (SB-13: truncating made
    ///   every listed name mandatory, which is not a quorum).
    ///
    /// Returns `None` if sync mode is Off.
    pub fn select_sync_standbys(
        &self,
        members: &[Member],
        my_name: &str,
        snapshot: &ReplicationSnapshot,
        threshold: Option<u64>,
        current_voters: &[String],
    ) -> Option<SyncSelection> {
        if self.mode == SyncMode::Off {
            return None;
        }

        // Candidate set: Running DCS replicas that are actually streaming
        // (present in the snapshot by application_name) with a known flush_lsn.
        let mut candidates: Vec<Candidate> = members
            .iter()
            .filter(|m| {
                m.name != my_name
                    && !m.is_nosync()
                    && m.state == crate::cluster::MemberState::Running
                    && m.role == crate::cluster::MemberRole::Replica
            })
            .filter_map(|m| {
                snapshot
                    .replicas
                    .iter()
                    .find(|r| r.application_name.eq_ignore_ascii_case(&m.name))
                    .map(|r| Candidate {
                        member: m,
                        priority: m.sync_priority(),
                        flush_lsn: r.flush_lsn,
                        is_voter: current_voters
                            .iter()
                            .any(|v| v.eq_ignore_ascii_case(&m.name)),
                    })
            })
            .collect();

        // Baseline (decision 4): with more than one candidate, use the furthest
        // candidate; with exactly one, fall back to the primary's flush LSN.
        let baseline = match candidates.len() {
            0 => snapshot.primary_flush_lsn,
            1 => snapshot.primary_flush_lsn,
            _ => candidates
                .iter()
                .map(|c| c.flush_lsn)
                .max()
                .unwrap_or(snapshot.primary_flush_lsn),
        };

        // Lag filter: drop candidates trailing the baseline by more than the
        // threshold. No filtering when the threshold is None.
        if let Some(t) = threshold {
            candidates.retain(|c| baseline.saturating_sub(c.flush_lsn) <= t);
        }

        // Ordering (decision 6): sync_priority desc, current voters first
        // (sticky), flush_lsn desc, then name asc.
        candidates.sort_by(|a, b| {
            b.priority
                .cmp(&a.priority)
                .then_with(|| b.is_voter.cmp(&a.is_voter))
                .then_with(|| b.flush_lsn.cmp(&a.flush_lsn))
                .then_with(|| a.member.name.cmp(&b.member.name))
        });

        if candidates.is_empty() {
            return Some(SyncSelection::NoEligible {
                strict: self.strict_mode,
            });
        }

        let eligible_len = candidates.len();
        // Non-strict downscales the count to the number actually eligible so
        // we never emit `FIRST N (...)` with fewer than N names; strict keeps
        // the raw configured count (decision 8).
        let count = if self.strict_mode {
            self.sync_node_count
        } else {
            self.sync_node_count.min(eligible_len)
        };

        let names = match self.mode {
            SyncMode::Priority => candidates
                .iter()
                .take(count)
                .map(|c| c.member.name.clone())
                .collect(),
            SyncMode::Quorum => {
                // All eligible voters; order carries no meaning for `ANY`, so
                // sort by name (case-folded) to keep the string stable.
                let mut names: Vec<String> =
                    candidates.iter().map(|c| c.member.name.clone()).collect();
                names.sort_by(|a, b| {
                    a.to_ascii_lowercase()
                        .cmp(&b.to_ascii_lowercase())
                        .then_with(|| a.cmp(b))
                });
                names
            }
            SyncMode::Off => return None,
        };
        Some(SyncSelection::Voters { names, count })
    }

    /// Formatting half of [`Self::compute_sync_standby_names`]: render a
    /// selection as a `synchronous_standby_names` value.
    ///
    /// Returns `None` if sync mode is Off. `NoEligible` renders as the
    /// unsatisfiable placeholder in strict mode (every commit WAITs, not
    /// `'*'`, which does not block) and as the empty string otherwise (async).
    pub fn format_sync_standby_names(&self, selection: &SyncSelection) -> Option<String> {
        if self.mode == SyncMode::Off {
            return None;
        }
        match selection {
            SyncSelection::NoEligible { strict: true } => {
                Some(STRICT_NO_ELIGIBLE_PLACEHOLDER.to_string())
            }
            SyncSelection::NoEligible { strict: false } => Some(String::new()),
            SyncSelection::Voters { names, count } => {
                // Double-quote each member name so special characters cannot
                // break ALTER SYSTEM (decision 11).
                let name_list = names
                    .iter()
                    .map(|n| quote_ident(n))
                    .collect::<Vec<_>>()
                    .join(",");
                match self.mode {
                    SyncMode::Priority => Some(format!("FIRST {count} ({name_list})")),
                    SyncMode::Quorum => Some(format!("ANY {count} ({name_list})")),
                    SyncMode::Off => None,
                }
            }
        }
    }
}

/// Result of standby selection, before formatting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncSelection {
    /// Nobody is eligible. `strict` records which fallback applies when
    /// formatting (placeholder that blocks writes vs. empty = async).
    NoEligible { strict: bool },
    /// The listed standbys (raw, unquoted member names) and the `FIRST`/`ANY`
    /// number. For `Priority` `names.len() <= count` after non-strict
    /// downscaling; for `Quorum` `names` is every eligible voter.
    Voters { names: Vec<String>, count: usize },
}

impl SyncSelection {
    /// Voter names (empty for `NoEligible`).
    pub fn voters(&self) -> &[String] {
        match self {
            SyncSelection::NoEligible { .. } => &[],
            SyncSelection::Voters { names, .. } => names,
        }
    }

    /// The `n` of `ANY n (...)` / `FIRST n (...)` (0 for `NoEligible`).
    pub fn confirmations(&self) -> usize {
        match self {
            SyncSelection::NoEligible { .. } => 0,
            SyncSelection::Voters { count, .. } => *count,
        }
    }

    /// Quorum-semantics `q = |V| - n` (design section 2.2); 0 when the list
    /// cannot satisfy `n` (strict with fewer than `n` voters: writes block, so
    /// nothing is ever acknowledged and nothing can be lost).
    ///
    /// This is NOT what Priority writes into `/sync.quorum` today (that is the
    /// configured N, unchanged in PR1; decision D10).
    pub fn quorum(&self) -> usize {
        self.voters().len().saturating_sub(self.confirmations())
    }
}

/// A streaming, eligible sync-standby candidate.
struct Candidate<'a> {
    member: &'a Member,
    priority: u32,
    flush_lsn: u64,
    is_voter: bool,
}

/// Double-quote an identifier for `synchronous_standby_names`, escaping any
/// embedded double quotes by doubling them (PostgreSQL identifier rules).
pub(crate) fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{Member, MemberRole, MemberState};
    use crate::postgresql::{ReplicaRow, ReplicationSnapshot};
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
            rewind_check: false,
            lineage: None,
            watchdog_failed: false,
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
            rewind_check: false,
            lineage: None,
            watchdog_failed: false,
        }
    }

    /// Build a snapshot where each named standby is streaming at the given
    /// flush LSN; the primary flush LSN is set explicitly.
    fn snapshot(primary_flush_lsn: u64, replicas: &[(&str, u64)]) -> ReplicationSnapshot {
        ReplicationSnapshot {
            primary_flush_lsn,
            replicas: replicas
                .iter()
                .map(|(name, lsn)| ReplicaRow {
                    application_name: name.to_string(),
                    flush_lsn: *lsn,
                })
                .collect(),
        }
    }

    #[test]
    fn test_sync_off() {
        let mgr = SyncManager::new(SyncMode::Off, 1, false);
        let members = vec![make_member("n1", 0)];
        let snap = snapshot(1000, &[("n1", 1000)]);
        assert_eq!(
            mgr.compute_sync_standby_names(&members, "primary", &snap, None, &[]),
            None
        );
    }

    #[test]
    fn test_priority_mode_single() {
        let mgr = SyncManager::new(SyncMode::Priority, 1, false);
        let members = vec![make_member("replica1", 0), make_member("replica2", 0)];
        let snap = snapshot(1000, &[("replica1", 1000), ("replica2", 1000)]);
        let result = mgr
            .compute_sync_standby_names(&members, "primary", &snap, None, &[])
            .unwrap();
        assert!(result.starts_with("FIRST 1"));
        assert!(result.contains("\"replica1\"")); // alphabetical first on tie
    }

    #[test]
    fn test_priority_mode_respects_sync_priority() {
        let mgr = SyncManager::new(SyncMode::Priority, 1, false);
        let members = vec![make_member("replica1", 1), make_member("replica2", 10)];
        let snap = snapshot(1000, &[("replica1", 1000), ("replica2", 1000)]);
        let result = mgr
            .compute_sync_standby_names(&members, "primary", &snap, None, &[])
            .unwrap();
        assert!(result.contains("\"replica2\""));
        assert!(!result.contains("\"replica1\""));
    }

    #[test]
    fn test_quorum_mode() {
        // SB-13: Quorum lists ALL eligible voters (no truncation to N) and
        // `quorum = |V| - n`. Previously this emitted `ANY 2 ("r1","r2")`.
        let mgr = SyncManager::new(SyncMode::Quorum, 2, false);
        let members = vec![
            make_member("r3", 0),
            make_member("r1", 0),
            make_member("r2", 0),
        ];
        let snap = snapshot(1000, &[("r1", 1000), ("r2", 1000), ("r3", 1000)]);
        let result = mgr
            .compute_sync_standby_names(&members, "primary", &snap, None, &[])
            .unwrap();
        assert_eq!(result, "ANY 2 (\"r1\",\"r2\",\"r3\")");

        let sel = mgr
            .select_sync_standbys(&members, "primary", &snap, None, &[])
            .unwrap();
        assert_eq!(sel.voters(), ["r1", "r2", "r3"]);
        assert_eq!(sel.confirmations(), 2);
        assert_eq!(sel.quorum(), 1);
    }

    #[test]
    fn test_quorum_ignores_priority_and_stickiness() {
        // Quorum voters are peers: sync_priority and current voters do not
        // choose who is listed or the order (design D7). Order is by name.
        let mgr = SyncManager::new(SyncMode::Quorum, 1, false);
        let members = vec![
            make_member("b", 9),
            make_member("a", 1),
            make_member("c", 5),
        ];
        let snap = snapshot(1000, &[("a", 1000), ("b", 1000), ("c", 1000)]);
        let voters = vec!["c".to_string()];
        let result = mgr
            .compute_sync_standby_names(&members, "primary", &snap, None, &voters)
            .unwrap();
        assert_eq!(result, "ANY 1 (\"a\",\"b\",\"c\")");
    }

    #[test]
    fn test_quorum_count_clamped_and_strict() {
        let members = vec![make_member("r1", 0), make_member("r2", 0)];
        let snap = snapshot(1000, &[("r1", 1000), ("r2", 1000)]);

        // Non-strict: n=3 > M=2 -> n=2, q=0 (everyone must confirm).
        let mgr = SyncManager::new(SyncMode::Quorum, 3, false);
        let sel = mgr
            .select_sync_standbys(&members, "primary", &snap, None, &[])
            .unwrap();
        assert_eq!(
            mgr.format_sync_standby_names(&sel).unwrap(),
            "ANY 2 (\"r1\",\"r2\")"
        );
        assert_eq!(sel.quorum(), 0);

        // Strict: n stays 3, list stays 2 names (unsatisfiable, writes block), q=0.
        let mgr = SyncManager::new(SyncMode::Quorum, 3, true);
        let sel = mgr
            .select_sync_standbys(&members, "primary", &snap, None, &[])
            .unwrap();
        assert_eq!(
            mgr.format_sync_standby_names(&sel).unwrap(),
            "ANY 3 (\"r1\",\"r2\")"
        );
        assert_eq!(sel.quorum(), 0);
    }

    #[test]
    fn test_quorum_no_eligible_fallbacks_match_priority() {
        let snap = snapshot(1000, &[]);
        let members: Vec<Member> = vec![];
        let strict = SyncManager::new(SyncMode::Quorum, 1, true);
        assert_eq!(
            strict.compute_sync_standby_names(&members, "primary", &snap, None, &[]),
            Some(STRICT_NO_ELIGIBLE_PLACEHOLDER.to_string())
        );
        let loose = SyncManager::new(SyncMode::Quorum, 1, false);
        assert_eq!(
            loose.compute_sync_standby_names(&members, "primary", &snap, None, &[]),
            Some(String::new())
        );
    }

    #[test]
    fn test_quorum_lag_filter_still_applies() {
        // The eligibility/lag filter is shared with Priority: r3 lags and is
        // not a voter, so it must not appear in the ANY list either.
        let mgr = SyncManager::new(SyncMode::Quorum, 1, false);
        let members = vec![
            make_member("r1", 0),
            make_member("r2", 0),
            make_member("r3", 0),
        ];
        let snap = snapshot(2000, &[("r1", 1000), ("r2", 1000), ("r3", 100)]);
        let result = mgr
            .compute_sync_standby_names(&members, "primary", &snap, Some(100), &[])
            .unwrap();
        assert_eq!(result, "ANY 1 (\"r1\",\"r2\")");
    }

    #[test]
    fn test_priority_selection_is_truncated_not_quorum() {
        // Regression pin: Priority still lists only the first N by its
        // ordering, and its selection reports quorum() = |names| - n = 0.
        let mgr = SyncManager::new(SyncMode::Priority, 2, false);
        let members = vec![
            make_member("r1", 1),
            make_member("r2", 9),
            make_member("r3", 5),
        ];
        let snap = snapshot(1000, &[("r1", 1000), ("r2", 1000), ("r3", 1000)]);
        let sel = mgr
            .select_sync_standbys(&members, "primary", &snap, None, &[])
            .unwrap();
        assert_eq!(sel.voters(), ["r2", "r3"]);
        assert_eq!(sel.quorum(), 0);
        assert_eq!(
            mgr.format_sync_standby_names(&sel).unwrap(),
            "FIRST 2 (\"r2\",\"r3\")"
        );
    }

    #[test]
    fn test_nosync_excluded() {
        let mgr = SyncManager::new(SyncMode::Priority, 1, false);
        let members = vec![make_nosync_member("r1"), make_member("r2", 0)];
        let snap = snapshot(1000, &[("r1", 1000), ("r2", 1000)]);
        let result = mgr
            .compute_sync_standby_names(&members, "primary", &snap, None, &[])
            .unwrap();
        assert!(result.contains("\"r2\""));
        assert!(!result.contains("\"r1\""));
    }

    #[test]
    fn test_strict_mode_no_standbys() {
        let mgr = SyncManager::new(SyncMode::Priority, 1, true);
        let members: Vec<Member> = vec![]; // no replicas
        let snap = snapshot(1000, &[]);
        let result = mgr
            .compute_sync_standby_names(&members, "primary", &snap, None, &[])
            .unwrap();
        // Unsatisfiable placeholder (blocks writes), NOT '*'.
        assert_eq!(result, STRICT_NO_ELIGIBLE_PLACEHOLDER);
        assert_ne!(result, "*");
    }

    #[test]
    fn test_non_strict_no_standbys() {
        let mgr = SyncManager::new(SyncMode::Priority, 1, false);
        let members: Vec<Member> = vec![];
        let snap = snapshot(1000, &[]);
        let result = mgr
            .compute_sync_standby_names(&members, "primary", &snap, None, &[])
            .unwrap();
        assert_eq!(result, ""); // allows async operation
    }

    #[test]
    fn test_excludes_self() {
        let mgr = SyncManager::new(SyncMode::Priority, 1, false);
        let members = vec![make_member("primary", 10), make_member("replica1", 5)];
        let snap = snapshot(1000, &[("primary", 1000), ("replica1", 1000)]);
        let result = mgr
            .compute_sync_standby_names(&members, "primary", &snap, None, &[])
            .unwrap();
        assert!(!result.contains("primary"));
        assert!(result.contains("\"replica1\""));
    }

    #[test]
    fn test_running_member_absent_from_snapshot_excluded() {
        let mgr = SyncManager::new(SyncMode::Priority, 2, false);
        let members = vec![make_member("r1", 0), make_member("r2", 0)];
        // r2 is Running in DCS but NOT streaming (absent from snapshot).
        let snap = snapshot(1000, &[("r1", 1000)]);
        let result = mgr
            .compute_sync_standby_names(&members, "primary", &snap, None, &[])
            .unwrap();
        assert!(result.contains("\"r1\""));
        assert!(!result.contains("\"r2\""));
        // Only one eligible → non-strict downscales to FIRST 1.
        assert!(result.starts_with("FIRST 1"));
    }

    #[test]
    fn test_high_lag_excluded_multi_candidate_baseline() {
        let mgr = SyncManager::new(SyncMode::Priority, 2, false);
        let members = vec![make_member("r1", 0), make_member("r2", 0)];
        // Two candidates → baseline = max candidate flush_lsn = 1000.
        // r2 lags by 500 > threshold 100 → excluded.
        let snap = snapshot(2000, &[("r1", 1000), ("r2", 500)]);
        let result = mgr
            .compute_sync_standby_names(&members, "primary", &snap, Some(100), &[])
            .unwrap();
        assert!(result.contains("\"r1\""));
        assert!(!result.contains("\"r2\""));
    }

    #[test]
    fn test_low_lag_kept_single_candidate_primary_baseline() {
        let mgr = SyncManager::new(SyncMode::Priority, 1, false);
        let members = vec![make_member("r1", 0)];
        // Single candidate → baseline = primary_flush_lsn = 1000.
        // r1 lags by 50 <= threshold 100 → kept.
        let snap = snapshot(1000, &[("r1", 950)]);
        let result = mgr
            .compute_sync_standby_names(&members, "primary", &snap, Some(100), &[])
            .unwrap();
        assert!(result.contains("\"r1\""));
    }

    #[test]
    fn test_single_candidate_high_lag_vs_primary_excluded() {
        let mgr = SyncManager::new(SyncMode::Priority, 1, false);
        let members = vec![make_member("r1", 0)];
        // Single candidate → baseline = primary_flush_lsn = 1000.
        // r1 lags by 400 > threshold 100 → excluded, non-strict degrades.
        let snap = snapshot(1000, &[("r1", 600)]);
        let result = mgr
            .compute_sync_standby_names(&members, "primary", &snap, Some(100), &[])
            .unwrap();
        assert_eq!(result, "");
    }

    #[test]
    fn test_sticky_current_voter_wins_priority_tie() {
        let mgr = SyncManager::new(SyncMode::Priority, 1, false);
        // Equal priority and equal flush_lsn: r2 is the current voter, so it
        // must stay even though r1 sorts first by name.
        let members = vec![make_member("r1", 5), make_member("r2", 5)];
        let snap = snapshot(1000, &[("r1", 1000), ("r2", 1000)]);
        let voters = vec!["r2".to_string()];
        let result = mgr
            .compute_sync_standby_names(&members, "primary", &snap, None, &voters)
            .unwrap();
        assert!(result.contains("\"r2\""));
        assert!(!result.contains("\"r1\""));
    }

    #[test]
    fn test_name_order_breaks_tie_without_voter() {
        let mgr = SyncManager::new(SyncMode::Priority, 1, false);
        let members = vec![make_member("r2", 5), make_member("r1", 5)];
        let snap = snapshot(1000, &[("r1", 1000), ("r2", 1000)]);
        let result = mgr
            .compute_sync_standby_names(&members, "primary", &snap, None, &[])
            .unwrap();
        // No voter → name asc wins: r1.
        assert!(result.contains("\"r1\""));
        assert!(!result.contains("\"r2\""));
    }

    #[test]
    fn test_count_downscale_non_strict_not_strict() {
        let members = vec![make_member("r1", 0), make_member("r2", 0)];
        let snap = snapshot(1000, &[("r1", 1000), ("r2", 1000)]);

        // Non-strict: count 3 > 2 eligible → downscale to FIRST 2.
        let mgr = SyncManager::new(SyncMode::Priority, 3, false);
        let result = mgr
            .compute_sync_standby_names(&members, "primary", &snap, None, &[])
            .unwrap();
        assert!(result.starts_with("FIRST 2 ("));

        // Strict: count 3 kept raw even with 2 eligible.
        let mgr = SyncManager::new(SyncMode::Priority, 3, true);
        let result = mgr
            .compute_sync_standby_names(&members, "primary", &snap, None, &[])
            .unwrap();
        assert!(result.starts_with("FIRST 3 ("));
    }

    #[test]
    fn test_member_names_are_quoted() {
        let mgr = SyncManager::new(SyncMode::Priority, 1, false);
        let members = vec![make_member("node-with-dash", 0)];
        let snap = snapshot(1000, &[("node-with-dash", 1000)]);
        let result = mgr
            .compute_sync_standby_names(&members, "primary", &snap, None, &[])
            .unwrap();
        assert_eq!(result, "FIRST 1 (\"node-with-dash\")");
    }

    #[test]
    fn test_strict_no_eligible_returns_placeholder() {
        let mgr = SyncManager::new(SyncMode::Priority, 2, true);
        let members = vec![make_member("r1", 0)];
        // r1 is Running but not streaming → no eligible candidate.
        let snap = snapshot(1000, &[]);
        let result = mgr
            .compute_sync_standby_names(&members, "primary", &snap, None, &[])
            .unwrap();
        assert_eq!(result, STRICT_NO_ELIGIBLE_PLACEHOLDER);
    }
}
