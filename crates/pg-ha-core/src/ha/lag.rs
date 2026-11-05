//! SB-10: replication lag from the cluster view (pure, reused by SB-11).

use crate::cluster::MemberRole;

/// One `wal_status` reading taken by `touch_member`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct WalSample {
    pub position: u64,
    pub timeline: Option<u64>,
}

/// Bytes this node is behind the leader's last published `wal_position`.
///
/// - Primary with a known own position: `Some(0)` (it is the reference).
/// - Replica with both positions known: `leader - own`, floored at 0.
/// - Anything else (no leader record, a missing position, other roles,
///   PostgreSQL down): `None`, which is unknown, never 0.
///
/// The leader value is refreshed once per loop, so under write load the
/// result is understated by up to about one `loop_wait` of WAL. `own` is
/// `max(receive, replay)`: received, not applied.
pub(crate) fn replication_lag_bytes(
    role: &MemberRole,
    leader_wal: Option<u64>,
    own_wal: Option<u64>,
) -> Option<u64> {
    let own = own_wal?;
    match role {
        MemberRole::Primary => Some(0),
        MemberRole::Replica => Some(leader_wal?.saturating_sub(own)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replica_behind_leader() {
        assert_eq!(
            replication_lag_bytes(&MemberRole::Replica, Some(1500), Some(1000)),
            Some(500)
        );
    }

    #[test]
    fn leader_behind_replica_floors_at_zero() {
        assert_eq!(
            replication_lag_bytes(&MemberRole::Replica, Some(900), Some(1000)),
            Some(0)
        );
    }

    #[test]
    fn missing_side_is_unknown() {
        assert_eq!(
            replication_lag_bytes(&MemberRole::Replica, None, Some(1)),
            None
        );
        assert_eq!(
            replication_lag_bytes(&MemberRole::Replica, Some(1), None),
            None
        );
        assert_eq!(
            replication_lag_bytes(&MemberRole::Replica, None, None),
            None
        );
    }

    #[test]
    fn primary_is_zero_even_without_leader_record() {
        assert_eq!(
            replication_lag_bytes(&MemberRole::Primary, None, Some(7)),
            Some(0)
        );
        assert_eq!(
            replication_lag_bytes(&MemberRole::Primary, None, None),
            None
        );
    }
}
