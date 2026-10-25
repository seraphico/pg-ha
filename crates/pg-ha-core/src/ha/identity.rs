//! What PostgreSQL is right now, and what pg-ha may do about it.
//!
//! Every HA decision that depends on "is this node a primary" goes through
//! this module. The identity is derived from two facts only:
//!
//! - the `pg_is_in_recovery()` observation taken this cycle, and
//! - whether `standby.signal` exists (pg-ha writes it for every standby it
//!   configures: clone, rejoin, and every demotion before it stops PostgreSQL).
//!
//! It is never derived from the in-memory role, which only records what pg-ha
//! did earlier. The rule behind the tables below:
//!
//! > pg-ha only promotes a node it configured as a standby. Any recovery state
//! > pg-ha did not create is left to the operator: no promote, no rewind, no
//! > election, and a held leader lock is released.

use crate::cluster::{MemberRole, MemberState};
use crate::postgresql::PgState;
use crate::standby_config::conn_url_has_host;

/// PostgreSQL identity for one HA cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PgIdentity {
    /// Observed out of recovery: a writable primary.
    Primary,
    /// Observed in recovery with `standby.signal`: a standby pg-ha configured.
    ManagedStandby,
    /// Observed in recovery without `standby.signal`: recovery pg-ha did not
    /// create (`recovery.signal` archive / point-in-time recovery, or
    /// `standby.signal` removed while the server was running).
    UnmanagedRecovery,
    /// Recovery state not observed this cycle (not running, query failed or
    /// timed out, or invalidated by a start / stop / promote).
    Unknown { has_standby_signal: bool },
}

impl PgIdentity {
    pub(crate) fn classify(in_recovery: Option<bool>, has_standby_signal: bool) -> Self {
        match in_recovery {
            Some(false) => Self::Primary,
            Some(true) if has_standby_signal => Self::ManagedStandby,
            Some(true) => Self::UnmanagedRecovery,
            None => Self::Unknown { has_standby_signal },
        }
    }

    /// Short description for logs and cycle messages.
    pub(crate) fn describe(self) -> &'static str {
        match self {
            Self::Primary => "a primary (not in recovery)",
            Self::ManagedStandby => "a standby configured by pg-ha",
            Self::UnmanagedRecovery => "in recovery without standby.signal",
            Self::Unknown { .. } => "in an unobserved recovery state",
        }
    }
}

/// What the leader-lock holder does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LeaderAction {
    /// Already a primary: keep the lock and act as primary.
    Hold,
    /// A standby pg-ha configured, which won the lock: promote, then
    /// re-observe before claiming primary.
    Promote,
    /// Recovery pg-ha did not create: release the lock and leave the node to
    /// the operator (in pause mode: keep the lock, but still never claim
    /// primary).
    Relinquish,
    /// Cannot observe: do nothing this cycle and do not claim primary.
    Wait,
}

pub(crate) fn leader_action(identity: PgIdentity) -> LeaderAction {
    match identity {
        PgIdentity::Primary => LeaderAction::Hold,
        PgIdentity::ManagedStandby => LeaderAction::Promote,
        PgIdentity::UnmanagedRecovery => LeaderAction::Relinquish,
        PgIdentity::Unknown { .. } => LeaderAction::Wait,
    }
}

/// What a node does while another node holds the leader lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FollowerAction {
    /// A primary that lost the lock (old primary, S4): rejoin as a replica.
    Rejoin,
    /// A standby pg-ha configured: keep following the upstream.
    Follow,
    /// A standby pg-ha configured that may hold WAL beyond a fork point
    /// (pg-ha.rewind_check): verify with pg_rewind against the leader before
    /// following it (D4).
    RewindCheck,
    /// Recovery pg-ha did not create: touch nothing, report to the operator.
    HandOff,
    /// Unobserved and no `standby.signal`, so nothing proves it is not a
    /// writable primary while another node holds the lock (L5): stop it.
    /// Stopping destroys no data; the next cycle observes again.
    Fence,
    /// Unobserved but configured as a standby (`standby.signal`), so it cannot
    /// be writable unless promoted outside pg-ha: wait for the next cycle.
    Wait,
}

/// `stop_pending`: a stop after writing standby.signal on a demotion / fence
/// path failed and PostgreSQL was not confirmed stopped since. The signal was
/// written on a server that may be writable, so it no longer proves the
/// server is read-only: an unobserved server is fenced again (A19).
///
/// `has_rewind_marker` (D4) only decides whether a managed standby runs a
/// rewind check before following; it never changes the identity or any other
/// row.
pub(crate) fn follower_action(
    identity: PgIdentity,
    stop_pending: bool,
    has_rewind_marker: bool,
) -> FollowerAction {
    match identity {
        PgIdentity::Primary => FollowerAction::Rejoin,
        PgIdentity::ManagedStandby if has_rewind_marker => FollowerAction::RewindCheck,
        PgIdentity::ManagedStandby => FollowerAction::Follow,
        PgIdentity::UnmanagedRecovery => FollowerAction::HandOff,
        PgIdentity::Unknown {
            has_standby_signal: false,
        } => FollowerAction::Fence,
        PgIdentity::Unknown {
            has_standby_signal: true,
        } => {
            if stop_pending {
                FollowerAction::Fence
            } else {
                FollowerAction::Wait
            }
        }
    }
}

/// Whether a node takes part in an election when no leader lock exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ElectionStance {
    /// Run the normal election logic (stale-primary detection, then the
    /// healthiest-node check).
    Participate,
    /// Recovery pg-ha did not create: never campaign, never rewind.
    StandAside,
    /// Cannot observe: skip this cycle.
    Wait,
    /// A demotion stop failed and PostgreSQL may still be writable (observed
    /// primary, or unobserved): stop it again (L5) and skip the election.
    Fence,
}

/// `stop_pending` as in [`follower_action`]. A primary pg-ha already decided
/// to stop must not campaign: a failed lock acquisition does not stop it.
pub(crate) fn election_stance(identity: PgIdentity, stop_pending: bool) -> ElectionStance {
    match identity {
        PgIdentity::Primary | PgIdentity::Unknown { .. } if stop_pending => ElectionStance::Fence,
        PgIdentity::Primary | PgIdentity::ManagedStandby => ElectionStance::Participate,
        PgIdentity::UnmanagedRecovery => ElectionStance::StandAside,
        PgIdentity::Unknown { .. } => ElectionStance::Wait,
    }
}

/// Graceful shutdown (A27): must the stop go through `fence_primary`? Yes
/// unless PostgreSQL is observed in recovery: the lock was released first, so
/// a primary (or a server pg-ha cannot observe) may be writable without it.
pub(crate) fn shutdown_needs_fence(identity: PgIdentity) -> bool {
    match identity {
        PgIdentity::Primary | PgIdentity::Unknown { .. } => true,
        PgIdentity::ManagedStandby | PgIdentity::UnmanagedRecovery => false,
    }
}

/// D1: does the last recorded cluster state say this data directory last ran
/// as a primary (so it may hold WAL beyond a fork point)? `None` =
/// pg_controldata could not be read. Only the two recovery states say "no";
/// unknown values count as "yes" (A7).
pub(crate) fn last_ran_as_primary(controldata_state: Option<&str>) -> bool {
    !matches!(
        controldata_state.map(str::trim),
        Some("shut down in recovery" | "in archive recovery")
    )
}

/// D1 / A8 / A31: which conn_url to point primary_conninfo at when starting
/// as a standby. None = write standby.signal only and keep the existing
/// primary_conninfo (leader unknown, this node, missing member, or a conn_url
/// without `host=`).
pub(crate) fn standby_start_upstream<'a>(
    leader: Option<&str>,
    self_name: &str,
    leader_conn_url: Option<&'a str>,
) -> Option<&'a str> {
    match (leader, leader_conn_url) {
        (Some(name), Some(url)) if name != self_name && conn_url_has_host(url) => Some(url),
        _ => None,
    }
}

/// D6: may this node take part in an election, judged by the rewind-check
/// marker and the recorded cluster timeline (DCS `/timeline`)?
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MarkedElectionGate {
    /// No marker, or timeline >= recorded cluster timeline.
    Allowed,
    /// Marker, no /timeline key: compared with member timelines only (WARN).
    AllowedIncompleteEvidence,
    /// Marker and own timeline behind the evidence: stand aside.
    Behind { own: u64, cluster: u64 },
    /// Marker and own timeline not published this cycle: stand aside.
    OwnTimelineUnknown,
}

/// `own`: the timeline this node published in its member record (the value
/// other nodes see, A9); `recorded`: the `/timeline` key; `max_peer`: the
/// highest timeline other members published (used only without the key).
/// Nodes without the marker get no timeline check.
pub(crate) fn marked_election_gate(
    has_marker: bool,
    own: Option<u64>,
    recorded: Option<u64>,
    max_peer: Option<u64>,
) -> MarkedElectionGate {
    if !has_marker {
        return MarkedElectionGate::Allowed;
    }
    let Some(own) = own else {
        return MarkedElectionGate::OwnTimelineUnknown;
    };
    match (recorded, max_peer) {
        (Some(k), _) if own >= k => MarkedElectionGate::Allowed,
        (Some(k), _) => MarkedElectionGate::Behind { own, cluster: k },
        (None, Some(m)) if own < m => MarkedElectionGate::Behind { own, cluster: m },
        (None, _) => MarkedElectionGate::AllowedIncompleteEvidence,
    }
}

/// What a node does with its [`MarkedElectionGate`] result (A20).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MarkedGateAction {
    /// Go on with stale-primary detection and the normal election.
    Proceed,
    /// Do not campaign this cycle; PostgreSQL keeps running read-only.
    StandAside,
    /// Not eligible while observed writable without the lock (L5): stop
    /// PostgreSQL first, then do not campaign.
    StopThenStandAside,
}

pub(crate) fn marked_gate_action(
    gate: MarkedElectionGate,
    identity: PgIdentity,
) -> MarkedGateAction {
    match gate {
        MarkedElectionGate::Allowed | MarkedElectionGate::AllowedIncompleteEvidence => {
            MarkedGateAction::Proceed
        }
        MarkedElectionGate::Behind { .. } | MarkedElectionGate::OwnTimelineUnknown => {
            if identity == PgIdentity::Primary {
                MarkedGateAction::StopThenStandAside
            } else {
                MarkedGateAction::StandAside
            }
        }
    }
}

/// A18: may this peer win an election under the same rule the peer applies to
/// itself? Used by is_healthiest_node so nodes do not defer to a peer that
/// will not campaign.
pub(crate) fn peer_may_campaign(
    rewind_check: bool,
    timeline: Option<u64>,
    recorded: Option<u64>,
    max_other: Option<u64>,
) -> bool {
    matches!(
        marked_election_gate(rewind_check, timeline, recorded, max_other),
        MarkedElectionGate::Allowed | MarkedElectionGate::AllowedIncompleteEvidence
    )
}

/// Highest published timeline among members other than `name`.
pub(crate) fn max_timeline_excluding(
    members: &[crate::cluster::Member],
    name: &str,
) -> Option<u64> {
    members
        .iter()
        .filter(|m| m.name != name)
        .filter_map(|m| m.timeline)
        .max()
}

/// A25: should this node honour the designated switchover/failover
/// candidate? `candidate_may_campaign` is None when the candidate is not in
/// this cycle's member list (keep today's behaviour: defer).
pub(crate) fn honour_designated_candidate(
    is_self: bool,
    candidate_may_campaign: Option<bool>,
) -> bool {
    is_self || candidate_may_campaign != Some(false)
}

/// D6: value to record before promoting: max(own, recorded) + 1, None when
/// both are unknown.
pub(crate) fn planned_promote_timeline(own: Option<u64>, recorded: Option<u64>) -> Option<u64> {
    own.max(recorded).map(|t| t.saturating_add(1))
}

/// What a primary holding the lock does with `/timeline` (D6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TimelineRecord {
    /// Key missing or lower than the primary's timeline: raise it.
    Write,
    /// Key equals the primary's timeline.
    UpToDate,
    /// Key higher than the primary's timeline (a promote attempt recorded
    /// it in advance): never lowered, only reported.
    RecordedAhead,
}

pub(crate) fn timeline_record_action(primary: u64, recorded: Option<u64>) -> TimelineRecord {
    match recorded {
        Some(k) if k == primary => TimelineRecord::UpToDate,
        Some(k) if k > primary => TimelineRecord::RecordedAhead,
        _ => TimelineRecord::Write,
    }
}

/// Warn about `/timeline` = `k` above the primary's timeline `tl` only when
/// the pair changed since the last warning (`last`).
pub(crate) fn should_warn_timeline_ahead(last: Option<(u64, u64)>, k: u64, tl: u64) -> bool {
    last != Some((k, tl))
}

/// A12, DCS part: the rewind source is this cycle's lock holder and its
/// member record says running primary. Cheap; used where no live probe runs.
pub(crate) fn leader_ready_in_dcs(
    lock_holder: Option<&str>,
    source: &str,
    source_member: Option<&crate::cluster::Member>,
) -> bool {
    lock_holder == Some(source)
        && source_member
            .is_some_and(|m| m.state == MemberState::Running && m.role == MemberRole::Primary)
}

/// A12: ready for pg_rewind = the DCS part AND a live probe of the source
/// answered `pg_is_in_recovery() = false` (`Some(true)`). `None` = probe not
/// run, failed or timed out; `Some(false)` = the source is in recovery.
pub(crate) fn leader_ready_for_rewind(
    lock_holder: Option<&str>,
    source: &str,
    source_member: Option<&crate::cluster::Member>,
    source_answers_as_primary: Option<bool>,
) -> bool {
    leader_ready_in_dcs(lock_holder, source, source_member)
        && source_answers_as_primary == Some(true)
}

/// Why the rewind source is not ready, for logs and cycle messages.
pub(crate) fn rewind_source_not_ready_reason(dcs_ready: bool, probe: Option<bool>) -> &'static str {
    match (dcs_ready, probe) {
        (false, _) => "is not the leader-lock holder running as a primary yet",
        (true, Some(false)) => "does not answer as a primary yet (it is in recovery)",
        (true, None) => "does not answer as a primary yet (connection or query failed)",
        (true, Some(true)) => "",
    }
}

/// Outcome of the start after a successful pg_rewind (A28).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AfterRewindStart {
    /// pg_ctl start -w succeeded.
    Started,
    /// pg_ctl gave up waiting but the postmaster is running (still replaying
    /// to the consistent point): not a failure.
    StillStarting,
    /// PostgreSQL is not running: the start after pg_rewind failed.
    Failed,
}

pub(crate) fn after_rewind_start(start_ok: bool, running_after: bool) -> AfterRewindStart {
    match (start_ok, running_after) {
        (true, _) => AfterRewindStart::Started,
        (false, true) => AfterRewindStart::StillStarting,
        (false, false) => AfterRewindStart::Failed,
    }
}

/// What a node does given the rewind hold (D5, A11).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HeldNodeAction {
    /// Not held: carry on.
    NotHeld,
    /// Held, rebuilding disabled: keep PostgreSQL stopped and report.
    Hold,
    /// Held, rebuilding enabled, leader ready: rebuild from it.
    Rebuild,
    /// Held, rebuilding enabled, no ready leader: stay stopped and wait.
    WaitForLeader,
}

pub(crate) fn held_node_action(
    hold: bool,
    rebuild_enabled: bool,
    leader_ready: bool,
) -> HeldNodeAction {
    match (hold, rebuild_enabled, leader_ready) {
        (false, _, _) => HeldNodeAction::NotHeld,
        (true, false, _) => HeldNodeAction::Hold,
        (true, true, true) => HeldNodeAction::Rebuild,
        (true, true, false) => HeldNodeAction::WaitForLeader,
    }
}

/// Is the node held? The hold file is the source of truth; the in-memory flag
/// only covers a failed file write (`persisted == false`). Returns (held,
/// operator_removed): `operator_removed` = the file pg-ha wrote is gone, so
/// the in-memory state must be cleared (A11, M2).
pub(crate) fn rewind_hold_state(
    file_exists: bool,
    in_memory: bool,
    persisted: bool,
) -> (bool, bool) {
    match (file_exists, in_memory, persisted) {
        (true, _, _) => (true, false),
        (false, true, false) => (true, false),
        (false, true, true) => (false, true),
        (false, false, _) => (false, false),
    }
}

/// Bring the recorded role in line with the observation.
///
/// A server observed in recovery is not a primary, whatever pg-ha did before.
/// The role is never raised to `Primary` here: only the leader path does that,
/// after observing the server out of recovery.
pub(crate) fn reconcile_role(current: MemberRole, identity: PgIdentity) -> MemberRole {
    match (current, identity) {
        (MemberRole::Primary, PgIdentity::ManagedStandby | PgIdentity::UnmanagedRecovery) => {
            MemberRole::Replica
        }
        (role, _) => role,
    }
}

/// Member state this node advertises in DCS and in its REST API.
///
/// `Running` means "running, and pg-ha can vouch for its role". Other nodes
/// only count `Running` members as election candidates, sync standbys or
/// switchover targets, and `/replica` requires it, so a server pg-ha cannot
/// vouch for is advertised as `Unknown` even though its process is alive.
pub(crate) fn advertised_state(
    is_running: bool,
    pg_state: PgState,
    identity: PgIdentity,
) -> MemberState {
    if !is_running {
        return if pg_state == PgState::Crashed {
            MemberState::Crashed
        } else {
            MemberState::Stopped
        };
    }
    match identity {
        PgIdentity::Primary | PgIdentity::ManagedStandby => MemberState::Running,
        PgIdentity::UnmanagedRecovery | PgIdentity::Unknown { .. } => MemberState::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL_IDENTITIES: [PgIdentity; 5] = [
        PgIdentity::Primary,
        PgIdentity::ManagedStandby,
        PgIdentity::UnmanagedRecovery,
        PgIdentity::Unknown {
            has_standby_signal: false,
        },
        PgIdentity::Unknown {
            has_standby_signal: true,
        },
    ];

    #[test]
    fn classify_covers_every_observation() {
        use PgIdentity::*;
        assert_eq!(PgIdentity::classify(Some(false), false), Primary);
        assert_eq!(PgIdentity::classify(Some(false), true), Primary);
        assert_eq!(PgIdentity::classify(Some(true), true), ManagedStandby);
        assert_eq!(PgIdentity::classify(Some(true), false), UnmanagedRecovery);
        assert_eq!(
            PgIdentity::classify(None, false),
            Unknown {
                has_standby_signal: false
            }
        );
        assert_eq!(
            PgIdentity::classify(None, true),
            Unknown {
                has_standby_signal: true
            }
        );
    }

    #[test]
    fn standby_signal_never_overrides_observation() {
        // A file on disk says how the server will start, not what it is now.
        assert_eq!(PgIdentity::classify(Some(false), true), PgIdentity::Primary);
    }

    #[test]
    fn leader_table() {
        use LeaderAction::*;
        assert_eq!(leader_action(PgIdentity::Primary), Hold);
        assert_eq!(leader_action(PgIdentity::ManagedStandby), Promote);
        assert_eq!(leader_action(PgIdentity::UnmanagedRecovery), Relinquish);
        for s in [false, true] {
            assert_eq!(
                leader_action(PgIdentity::Unknown {
                    has_standby_signal: s
                }),
                Wait
            );
        }
    }

    #[test]
    fn only_a_managed_standby_is_ever_promoted() {
        for id in ALL_IDENTITIES {
            let promotes = leader_action(id) == LeaderAction::Promote;
            assert_eq!(promotes, id == PgIdentity::ManagedStandby, "{id:?}");
        }
    }

    const UNKNOWN_NO_SIGNAL: PgIdentity = PgIdentity::Unknown {
        has_standby_signal: false,
    };
    const UNKNOWN_SIGNAL: PgIdentity = PgIdentity::Unknown {
        has_standby_signal: true,
    };

    #[test]
    fn follower_table() {
        use FollowerAction::*;
        // (identity, stop_pending, marker, expected)
        let table = [
            (PgIdentity::Primary, false, false, Rejoin),
            (PgIdentity::Primary, false, true, Rejoin),
            (PgIdentity::Primary, true, false, Rejoin),
            (PgIdentity::Primary, true, true, Rejoin),
            (PgIdentity::ManagedStandby, false, false, Follow),
            (PgIdentity::ManagedStandby, false, true, RewindCheck),
            (PgIdentity::ManagedStandby, true, false, Follow),
            (PgIdentity::ManagedStandby, true, true, RewindCheck),
            (PgIdentity::UnmanagedRecovery, false, false, HandOff),
            (PgIdentity::UnmanagedRecovery, false, true, HandOff),
            (PgIdentity::UnmanagedRecovery, true, false, HandOff),
            (PgIdentity::UnmanagedRecovery, true, true, HandOff),
            (UNKNOWN_NO_SIGNAL, false, false, Fence),
            (UNKNOWN_NO_SIGNAL, false, true, Fence),
            (UNKNOWN_NO_SIGNAL, true, false, Fence),
            (UNKNOWN_NO_SIGNAL, true, true, Fence),
            (UNKNOWN_SIGNAL, false, false, Wait),
            (UNKNOWN_SIGNAL, false, true, Wait),
            (UNKNOWN_SIGNAL, true, false, Fence),
            (UNKNOWN_SIGNAL, true, true, Fence),
        ];
        assert_eq!(table.len(), ALL_IDENTITIES.len() * 4);
        for (id, pending, marker, expected) in table {
            assert_eq!(
                follower_action(id, pending, marker),
                expected,
                "{id:?} {pending} {marker}"
            );
        }
    }

    #[test]
    fn marker_only_turns_follow_into_rewind_check() {
        for id in ALL_IDENTITIES {
            for pending in [false, true] {
                let without = follower_action(id, pending, false);
                let with = follower_action(id, pending, true);
                if id == PgIdentity::ManagedStandby {
                    assert_eq!(
                        (without, with),
                        (FollowerAction::Follow, FollowerAction::RewindCheck)
                    );
                } else {
                    assert_eq!(without, with, "{id:?} {pending}");
                    assert_ne!(with, FollowerAction::RewindCheck, "{id:?} {pending}");
                }
            }
        }
    }

    #[test]
    fn unmanaged_recovery_is_never_rewound_or_campaigned() {
        let id = PgIdentity::UnmanagedRecovery;
        for s in [false, true] {
            for marker in [false, true] {
                let action = follower_action(id, s, marker);
                assert_ne!(action, FollowerAction::Rejoin);
                assert_ne!(action, FollowerAction::RewindCheck);
            }
            assert_eq!(election_stance(id, s), ElectionStance::StandAside);
        }
        assert_ne!(leader_action(id), LeaderAction::Promote);
    }

    #[test]
    fn election_table() {
        use ElectionStance::*;
        let table = [
            (PgIdentity::Primary, false, Participate),
            (PgIdentity::Primary, true, Fence),
            (PgIdentity::ManagedStandby, false, Participate),
            (PgIdentity::ManagedStandby, true, Participate),
            (PgIdentity::UnmanagedRecovery, false, StandAside),
            (PgIdentity::UnmanagedRecovery, true, StandAside),
            (UNKNOWN_NO_SIGNAL, false, Wait),
            (UNKNOWN_NO_SIGNAL, true, Fence),
            (UNKNOWN_SIGNAL, false, Wait),
            (UNKNOWN_SIGNAL, true, Fence),
        ];
        assert_eq!(table.len(), ALL_IDENTITIES.len() * 2);
        for (id, pending, expected) in table {
            assert_eq!(election_stance(id, pending), expected, "{id:?} {pending}");
        }
    }

    #[test]
    fn stop_pending_only_changes_unknown_and_primary_election() {
        for id in ALL_IDENTITIES {
            for marker in [false, true] {
                let follower_changes =
                    follower_action(id, false, marker) != follower_action(id, true, marker);
                assert_eq!(
                    follower_changes,
                    matches!(id, PgIdentity::Unknown { .. })
                        && follower_action(id, false, marker) != FollowerAction::Fence,
                    "{id:?} {marker}"
                );
                if follower_changes {
                    assert_eq!(
                        follower_action(id, true, marker),
                        FollowerAction::Fence,
                        "{id:?} {marker}"
                    );
                }
            }
            let election_changes = election_stance(id, false) != election_stance(id, true);
            assert_eq!(
                election_changes,
                matches!(id, PgIdentity::Unknown { .. } | PgIdentity::Primary),
                "{id:?}"
            );
            if election_changes {
                assert_eq!(election_stance(id, true), ElectionStance::Fence, "{id:?}");
            }
        }
    }

    #[test]
    fn unknown_with_stop_pending_is_always_fenced() {
        for s in [false, true] {
            let id = PgIdentity::Unknown {
                has_standby_signal: s,
            };
            for marker in [false, true] {
                assert_eq!(
                    follower_action(id, true, marker),
                    FollowerAction::Fence,
                    "{id:?}"
                );
            }
            assert_eq!(election_stance(id, true), ElectionStance::Fence, "{id:?}");
        }
    }

    #[test]
    fn shutdown_needs_fence_table() {
        for id in ALL_IDENTITIES {
            let expected = match id {
                PgIdentity::Primary | PgIdentity::Unknown { .. } => true,
                PgIdentity::ManagedStandby | PgIdentity::UnmanagedRecovery => false,
            };
            assert_eq!(shutdown_needs_fence(id), expected, "{id:?}");
        }
        assert!(shutdown_needs_fence(PgIdentity::Primary));
        assert!(shutdown_needs_fence(UNKNOWN_NO_SIGNAL));
        assert!(shutdown_needs_fence(UNKNOWN_SIGNAL));
        assert!(!shutdown_needs_fence(PgIdentity::ManagedStandby));
        assert!(!shutdown_needs_fence(PgIdentity::UnmanagedRecovery));
    }

    #[test]
    fn start_marker_table() {
        let table = [
            (Some("in production"), true),
            (Some("shut down"), true),
            (Some("shutting down"), true),
            (Some("in crash recovery"), true),
            (Some("starting up"), true),
            (Some("shut down in recovery"), false),
            (Some("in archive recovery"), false),
            (Some("something else"), true),
            (None, true),
        ];
        for (state, expected) in table {
            assert_eq!(last_ran_as_primary(state), expected, "{state:?}");
        }
        // Compared after trim().
        assert!(!last_ran_as_primary(Some("  in archive recovery ")));
    }

    #[test]
    fn only_standby_states_skip_the_marker() {
        // Every DBState value PostgreSQL 16 prints (pg_controldata.c dbState()).
        let all = [
            "starting up",
            "shut down",
            "shut down in recovery",
            "shutting down",
            "in crash recovery",
            "in archive recovery",
            "in production",
            "unrecognized status code",
        ];
        let skipped: Vec<&str> = all
            .into_iter()
            .filter(|s| !last_ran_as_primary(Some(s)))
            .collect();
        assert_eq!(skipped, ["shut down in recovery", "in archive recovery"]);
    }

    #[test]
    fn standby_start_upstream_table() {
        let leader_url = "host=node1 port=5432";
        let table = [
            (None, Some(leader_url), None),
            (Some("self"), Some("host=self port=5432"), None),
            (Some("node1"), None, None),
            (Some("node1"), Some(""), None),
            (Some("node1"), Some("port=5432"), None),
            (Some("node1"), Some(leader_url), Some(leader_url)),
        ];
        for (leader, url, expected) in table {
            assert_eq!(
                standby_start_upstream(leader, "self", url),
                expected,
                "{leader:?} {url:?}"
            );
        }
    }

    #[test]
    fn reconcile_only_lowers_primary_when_observed_in_recovery() {
        let roles = [
            MemberRole::Primary,
            MemberRole::Replica,
            MemberRole::StandbyLeader,
            MemberRole::Uninitialized,
        ];
        for role in roles {
            for id in ALL_IDENTITIES {
                let got = reconcile_role(role.clone(), id);
                let in_recovery = matches!(
                    id,
                    PgIdentity::ManagedStandby | PgIdentity::UnmanagedRecovery
                );
                if role == MemberRole::Primary && in_recovery {
                    assert_eq!(got, MemberRole::Replica, "{role:?} {id:?}");
                } else {
                    assert_eq!(got, role, "{role:?} {id:?}");
                }
                // Reconciliation never manufactures a primary.
                if role != MemberRole::Primary {
                    assert_ne!(got, MemberRole::Primary);
                }
            }
        }
    }

    #[test]
    fn advertised_state_table() {
        for id in ALL_IDENTITIES {
            assert_eq!(
                advertised_state(false, PgState::Running, id),
                MemberState::Stopped
            );
            assert_eq!(
                advertised_state(false, PgState::Crashed, id),
                MemberState::Crashed
            );
            let expected = match id {
                PgIdentity::Primary | PgIdentity::ManagedStandby => MemberState::Running,
                _ => MemberState::Unknown,
            };
            // The in-memory PgState does not matter while the process runs:
            // after an agent restart it is still `Unknown`.
            for pg_state in [PgState::Unknown, PgState::Running, PgState::Stopped] {
                assert_eq!(advertised_state(true, pg_state, id), expected, "{id:?}");
            }
        }
    }

    // ─── D6: /timeline and the marked-node election gate ───

    const TL: [Option<u64>; 4] = [None, Some(1), Some(2), Some(3)];

    #[test]
    fn marked_election_gate_table() {
        use MarkedElectionGate::*;
        let table = [
            // (has_marker, own, recorded, max_peer, expected)
            (false, None, Some(3), Some(3), Allowed),
            (false, Some(1), Some(3), None, Allowed),
            (true, Some(3), Some(3), None, Allowed), // t == k
            (true, Some(4), Some(3), Some(9), Allowed), // max_peer ignored with the key
            (true, Some(2), Some(3), None, Behind { own: 2, cluster: 3 }),
            (
                true,
                Some(2),
                Some(3),
                Some(1),
                Behind { own: 2, cluster: 3 },
            ),
            (true, None, Some(3), Some(1), OwnTimelineUnknown),
            (true, Some(2), None, Some(2), AllowedIncompleteEvidence), // t == m
            (true, Some(3), None, Some(2), AllowedIncompleteEvidence),
            (true, Some(1), None, Some(2), Behind { own: 1, cluster: 2 }),
            (true, None, None, Some(2), OwnTimelineUnknown),
            (true, Some(1), None, None, AllowedIncompleteEvidence),
            (true, None, None, None, OwnTimelineUnknown),
        ];
        for (marker, own, recorded, max_peer, expected) in table {
            assert_eq!(
                marked_election_gate(marker, own, recorded, max_peer),
                expected,
                "{marker} {own:?} {recorded:?} {max_peer:?}"
            );
        }
    }

    #[test]
    fn marked_election_gate_exhaustive() {
        use MarkedElectionGate::*;
        for own in TL {
            for recorded in TL {
                for max_peer in TL {
                    let got = marked_election_gate(true, own, recorded, max_peer);
                    let expected = match (own, recorded.or(max_peer)) {
                        (None, _) => OwnTimelineUnknown,
                        (Some(t), Some(k)) if t < k => Behind { own: t, cluster: k },
                        _ if recorded.is_some() => Allowed,
                        _ => AllowedIncompleteEvidence,
                    };
                    assert_eq!(got, expected, "{own:?} {recorded:?} {max_peer:?}");
                }
            }
        }
    }

    #[test]
    fn unmarked_nodes_get_no_timeline_check() {
        for own in TL {
            for recorded in TL {
                for max_peer in TL {
                    assert_eq!(
                        marked_election_gate(false, own, recorded, max_peer),
                        MarkedElectionGate::Allowed
                    );
                }
            }
        }
    }

    const GATES: [MarkedElectionGate; 4] = [
        MarkedElectionGate::Allowed,
        MarkedElectionGate::AllowedIncompleteEvidence,
        MarkedElectionGate::Behind { own: 1, cluster: 2 },
        MarkedElectionGate::OwnTimelineUnknown,
    ];

    #[test]
    fn marked_gate_action_table() {
        use MarkedGateAction::*;
        let mut cells = 0;
        for gate in GATES {
            for id in ALL_IDENTITIES {
                let expected = match (gate, id) {
                    (
                        MarkedElectionGate::Allowed | MarkedElectionGate::AllowedIncompleteEvidence,
                        _,
                    ) => Proceed,
                    (_, PgIdentity::Primary) => StopThenStandAside,
                    (_, PgIdentity::ManagedStandby) => StandAside,
                    (_, PgIdentity::UnmanagedRecovery | PgIdentity::Unknown { .. }) => StandAside,
                };
                assert_eq!(marked_gate_action(gate, id), expected, "{gate:?} {id:?}");
                cells += 1;
            }
        }
        assert_eq!(cells, 20);
        // Spot checks of the table in the plan.
        let behind = MarkedElectionGate::Behind { own: 1, cluster: 2 };
        assert_eq!(
            marked_gate_action(behind, PgIdentity::Primary),
            StopThenStandAside
        );
        assert_eq!(
            marked_gate_action(behind, PgIdentity::ManagedStandby),
            StandAside
        );
        assert_eq!(
            marked_gate_action(MarkedElectionGate::Allowed, PgIdentity::Primary),
            Proceed
        );
    }

    #[test]
    fn only_a_writable_marked_node_is_stopped() {
        for gate in GATES {
            for id in ALL_IDENTITIES {
                let stopped = marked_gate_action(gate, id) == MarkedGateAction::StopThenStandAside;
                let ineligible = matches!(
                    gate,
                    MarkedElectionGate::Behind { .. } | MarkedElectionGate::OwnTimelineUnknown
                );
                assert_eq!(
                    stopped,
                    ineligible && id == PgIdentity::Primary,
                    "{gate:?} {id:?}"
                );
            }
        }
    }

    #[test]
    fn peer_may_campaign_matches_self_gate() {
        for marker in [false, true] {
            for tl in TL {
                for recorded in TL {
                    for max_other in TL {
                        assert_eq!(
                            peer_may_campaign(marker, tl, recorded, max_other),
                            matches!(
                                marked_election_gate(marker, tl, recorded, max_other),
                                MarkedElectionGate::Allowed
                                    | MarkedElectionGate::AllowedIncompleteEvidence
                            ),
                            "{marker} {tl:?} {recorded:?} {max_other:?}"
                        );
                    }
                }
            }
        }
    }

    fn member_tl(name: &str, timeline: Option<u64>) -> crate::cluster::Member {
        crate::cluster::Member {
            name: name.to_string(),
            conn_url: String::new(),
            api_url: String::new(),
            state: MemberState::Running,
            role: MemberRole::Replica,
            wal_position: None,
            timeline,
            tags: Default::default(),
            version: None,
            rewind_check: false,
        }
    }

    #[test]
    fn max_timeline_excluding_skips_self_and_none() {
        let members = [
            member_tl("self", Some(9)),
            member_tl("a", Some(2)),
            member_tl("b", None),
            member_tl("c", Some(3)),
        ];
        assert_eq!(max_timeline_excluding(&members, "self"), Some(3));
        assert_eq!(max_timeline_excluding(&members, "c"), Some(9));
        assert_eq!(max_timeline_excluding(&members[..1], "self"), None);
        assert_eq!(
            max_timeline_excluding(&[member_tl("self", Some(1)), member_tl("b", None)], "self"),
            None
        );
        assert_eq!(max_timeline_excluding(&[], "self"), None);
    }

    #[test]
    fn honour_designated_candidate_table() {
        let table = [
            (true, None, true),
            (true, Some(true), true),
            (true, Some(false), true),
            (false, None, true),
            (false, Some(true), true),
            (false, Some(false), false),
        ];
        for (is_self, may, expected) in table {
            assert_eq!(
                honour_designated_candidate(is_self, may),
                expected,
                "{is_self} {may:?}"
            );
        }
    }

    #[test]
    fn planned_promote_timeline_table() {
        let table = [
            (None, None, None),
            (Some(2), None, Some(3)),
            (None, Some(2), Some(3)),
            (Some(1), Some(3), Some(4)),
            (Some(3), Some(1), Some(4)),
            (Some(2), Some(2), Some(3)),
        ];
        for (own, recorded, expected) in table {
            assert_eq!(
                planned_promote_timeline(own, recorded),
                expected,
                "{own:?} {recorded:?}"
            );
        }
    }

    #[test]
    fn timeline_record_action_table() {
        use TimelineRecord::*;
        let table = [
            (3, None, Write),
            (3, Some(2), Write),
            (3, Some(3), UpToDate),
            (3, Some(4), RecordedAhead),
        ];
        for (primary, recorded, expected) in table {
            assert_eq!(
                timeline_record_action(primary, recorded),
                expected,
                "{primary} {recorded:?}"
            );
        }
    }

    #[test]
    fn should_warn_timeline_ahead_table() {
        let table = [
            (None, 4, 3, true),
            (Some((4, 3)), 4, 3, false),
            (Some((4, 3)), 5, 3, true),
            (Some((4, 3)), 4, 4, true),
        ];
        for (last, k, tl, expected) in table {
            assert_eq!(
                should_warn_timeline_ahead(last, k, tl),
                expected,
                "{last:?} {k} {tl}"
            );
        }
    }

    // ─── D4 / D5: rewind check, source readiness, rewind hold ───

    fn dcs_member(state: MemberState, role: MemberRole) -> crate::cluster::Member {
        crate::cluster::Member {
            state,
            role,
            ..member_tl("node1", Some(1))
        }
    }

    /// The 7 DCS situations of the plan's readiness table, with whether the
    /// DCS part says ready.
    fn dcs_cases() -> Vec<(
        &'static str,
        Option<&'static str>,
        Option<crate::cluster::Member>,
        bool,
    )> {
        vec![
            (
                "no lock holder",
                None,
                Some(dcs_member(MemberState::Running, MemberRole::Primary)),
                false,
            ),
            (
                "lock holder is another node",
                Some("node3"),
                Some(dcs_member(MemberState::Running, MemberRole::Primary)),
                false,
            ),
            ("no member record", Some("node1"), None, false),
            (
                "running primary",
                Some("node1"),
                Some(dcs_member(MemberState::Running, MemberRole::Primary)),
                true,
            ),
            (
                "running replica",
                Some("node1"),
                Some(dcs_member(MemberState::Running, MemberRole::Replica)),
                false,
            ),
            (
                "stopped primary",
                Some("node1"),
                Some(dcs_member(MemberState::Stopped, MemberRole::Primary)),
                false,
            ),
            (
                "unknown primary",
                Some("node1"),
                Some(dcs_member(MemberState::Unknown, MemberRole::Primary)),
                false,
            ),
        ]
    }

    const PROBES: [Option<bool>; 3] = [None, Some(false), Some(true)];

    #[test]
    fn leader_ready_in_dcs_table() {
        let cases = dcs_cases();
        assert_eq!(cases.len(), 7);
        for (what, holder, member, expected) in cases {
            assert_eq!(
                leader_ready_in_dcs(holder, "node1", member.as_ref()),
                expected,
                "{what}"
            );
        }
    }

    #[test]
    fn leader_ready_for_rewind_table() {
        let mut cells = 0;
        for (what, holder, member, dcs_ready) in dcs_cases() {
            for probe in PROBES {
                let expected = dcs_ready && probe == Some(true);
                assert_eq!(
                    leader_ready_for_rewind(holder, "node1", member.as_ref(), probe),
                    expected,
                    "{what} {probe:?}"
                );
                cells += 1;
            }
        }
        assert_eq!(cells, 21);
        // The only ready cell.
        assert!(leader_ready_for_rewind(
            Some("node1"),
            "node1",
            Some(&dcs_member(MemberState::Running, MemberRole::Primary)),
            Some(true)
        ));
    }

    #[test]
    fn ready_for_rewind_implies_ready_in_dcs() {
        for (what, holder, member, _) in dcs_cases() {
            for probe in PROBES {
                if leader_ready_for_rewind(holder, "node1", member.as_ref(), probe) {
                    assert!(
                        leader_ready_in_dcs(holder, "node1", member.as_ref()),
                        "{what} {probe:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn rewind_source_not_ready_reason_table() {
        let table = [
            (
                false,
                None,
                "is not the leader-lock holder running as a primary yet",
            ),
            (
                false,
                Some(false),
                "is not the leader-lock holder running as a primary yet",
            ),
            (
                false,
                Some(true),
                "is not the leader-lock holder running as a primary yet",
            ),
            (
                true,
                Some(false),
                "does not answer as a primary yet (it is in recovery)",
            ),
            (
                true,
                None,
                "does not answer as a primary yet (connection or query failed)",
            ),
            (true, Some(true), ""),
        ];
        for (dcs_ready, probe, expected) in table {
            assert_eq!(
                rewind_source_not_ready_reason(dcs_ready, probe),
                expected,
                "{dcs_ready} {probe:?}"
            );
        }
    }

    #[test]
    fn after_rewind_start_table() {
        use AfterRewindStart::*;
        let table = [
            (true, true, Started),
            (true, false, Started),
            (false, true, StillStarting),
            (false, false, Failed),
        ];
        for (start_ok, running, expected) in table {
            assert_eq!(
                after_rewind_start(start_ok, running),
                expected,
                "{start_ok} {running}"
            );
        }
    }

    #[test]
    fn held_node_action_table() {
        use HeldNodeAction::*;
        let table = [
            (false, false, false, NotHeld),
            (false, false, true, NotHeld),
            (false, true, false, NotHeld),
            (false, true, true, NotHeld),
            (true, false, false, Hold),
            (true, false, true, Hold),
            (true, true, true, Rebuild),
            (true, true, false, WaitForLeader),
        ];
        for (hold, rebuild, ready, expected) in table {
            assert_eq!(
                held_node_action(hold, rebuild, ready),
                expected,
                "{hold} {rebuild} {ready}"
            );
        }
    }

    #[test]
    fn held_node_never_rebuilds_when_disabled() {
        for ready in [false, true] {
            assert_eq!(held_node_action(true, false, ready), HeldNodeAction::Hold);
        }
    }

    #[test]
    fn rewind_hold_state_table() {
        // (file_exists, in_memory, persisted) -> (held, operator_removed)
        let table = [
            (true, false, false, (true, false)),
            (true, false, true, (true, false)),
            (true, true, false, (true, false)),
            (true, true, true, (true, false)),
            (false, true, false, (true, false)),
            (false, true, true, (false, true)),
            (false, false, false, (false, false)),
            (false, false, true, (false, false)),
        ];
        for (file, mem, persisted, expected) in table {
            assert_eq!(
                rewind_hold_state(file, mem, persisted),
                expected,
                "{file} {mem} {persisted}"
            );
        }
    }
}
