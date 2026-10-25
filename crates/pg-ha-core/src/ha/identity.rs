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
pub(crate) fn follower_action(identity: PgIdentity, stop_pending: bool) -> FollowerAction {
    match identity {
        PgIdentity::Primary => FollowerAction::Rejoin,
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
        let table = [
            (PgIdentity::Primary, false, Rejoin),
            (PgIdentity::Primary, true, Rejoin),
            (PgIdentity::ManagedStandby, false, Follow),
            (PgIdentity::ManagedStandby, true, Follow),
            (PgIdentity::UnmanagedRecovery, false, HandOff),
            (PgIdentity::UnmanagedRecovery, true, HandOff),
            (UNKNOWN_NO_SIGNAL, false, Fence),
            (UNKNOWN_NO_SIGNAL, true, Fence),
            (UNKNOWN_SIGNAL, false, Wait),
            (UNKNOWN_SIGNAL, true, Fence),
        ];
        assert_eq!(table.len(), ALL_IDENTITIES.len() * 2);
        for (id, pending, expected) in table {
            assert_eq!(follower_action(id, pending), expected, "{id:?} {pending}");
        }
    }

    #[test]
    fn unmanaged_recovery_is_never_rewound_or_campaigned() {
        let id = PgIdentity::UnmanagedRecovery;
        for s in [false, true] {
            assert_ne!(follower_action(id, s), FollowerAction::Rejoin);
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
            let follower_changes = follower_action(id, false) != follower_action(id, true);
            assert_eq!(
                follower_changes,
                matches!(id, PgIdentity::Unknown { .. })
                    && follower_action(id, false) != FollowerAction::Fence,
                "{id:?}"
            );
            if follower_changes {
                assert_eq!(follower_action(id, true), FollowerAction::Fence, "{id:?}");
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
            assert_eq!(follower_action(id, true), FollowerAction::Fence, "{id:?}");
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
}
