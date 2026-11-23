//! SB-37: is the cluster leaderless and not healing?
//!
//! `/replica` must stay 200 during an ordinary failover (the lock is absent
//! for a few seconds) and turn 503 only when the cluster is stuck: no leader
//! for a whole lock ttl, two consecutive refused votes on this node, or a DCS
//! that could not be read for a whole ttl (fail closed, never on one read).
//! The decision is a pure function of times observed by the HA loop.
use std::time::{Duration, Instant};

/// Consecutive refused votes after which the election counts as stalled.
const STALLED_AFTER_REFUSALS: u32 = 2;

/// Equality at the threshold counts as stuck.
pub(crate) fn leaderless_stuck(
    now: Instant,
    leaderless_since: Option<Instant>,
    unreadable_since: Option<Instant>,
    stalled: bool,
    threshold: Duration,
) -> bool {
    let reached = |since: Option<Instant>| {
        since.is_some_and(|t| now.saturating_duration_since(t) >= threshold)
    };
    stalled || reached(leaderless_since) || reached(unreadable_since)
}

/// What the HA loop has seen, enough to evaluate [`leaderless_stuck`].
#[derive(Debug, Default, Clone)]
pub(crate) struct StuckTracker {
    leaderless_since: Option<Instant>,
    unreadable_since: Option<Instant>,
    refused_votes: u32,
}

impl StuckTracker {
    /// A successful DCS read: the read works again, and the leader lock is
    /// either present (everything resets) or absent (the clock starts once).
    pub(crate) fn observe_leader(&mut self, now: Instant, has_leader: bool) {
        self.unreadable_since = None;
        if has_leader {
            self.leaderless_since = None;
            self.refused_votes = 0;
        } else {
            self.leaderless_since.get_or_insert(now);
        }
    }

    /// A failed DCS read. The leaderless clock is left alone: an unreadable
    /// DCS says nothing about the lock.
    pub(crate) fn observe_read_failed(&mut self, now: Instant) {
        self.unreadable_since.get_or_insert(now);
    }

    /// The quorum vote of this node's own candidacy: refused (for a reason a
    /// later cycle will not fix by itself) or not.
    pub(crate) fn observe_vote(&mut self, refused: bool) {
        self.refused_votes = if refused {
            self.refused_votes.saturating_add(1)
        } else {
            0
        };
    }

    /// Two consecutive refused votes.
    pub(crate) fn election_stalled(&self) -> bool {
        self.refused_votes >= STALLED_AFTER_REFUSALS
    }

    pub(crate) fn is_stuck(&self, now: Instant, threshold: Duration) -> bool {
        leaderless_stuck(
            now,
            self.leaderless_since,
            self.unreadable_since,
            self.election_stalled(),
            threshold,
        )
    }
}

/// Does this vote refusal text name a refusal that does not heal by waiting
/// (not enough voters counted, or a voter on a newer or ahead position)?
/// `PrimaryAlive`, an unreadable own WAL and the lag ceiling are not.
pub(crate) fn refusal_is_stalling(refusal: &str) -> bool {
    ["InsufficientVotes", "HigherTimeline", "BehindPeer"]
        .iter()
        .any(|kind| refusal.contains(kind))
}

#[cfg(test)]
mod tests {
    use super::*;

    const TTL: Duration = Duration::from_secs(30);

    fn secs(base: Instant, s: u64) -> Instant {
        base + Duration::from_secs(s)
    }

    #[test]
    fn every_term_decides_on_both_sides() {
        let t0 = Instant::now();
        let now = secs(t0, 100);
        // (leaderless_since, unreadable_since, stalled, expected)
        let table = [
            (None, None, false, false),
            (Some(secs(t0, 71)), None, false, false), // 29 s
            (Some(secs(t0, 70)), None, false, true),  // exactly the ttl
            (Some(secs(t0, 10)), None, false, true),
            (None, Some(secs(t0, 71)), false, false),
            (None, Some(secs(t0, 70)), false, true),
            (None, None, true, true),
            (Some(secs(t0, 99)), Some(secs(t0, 99)), true, true),
            (Some(secs(t0, 99)), Some(secs(t0, 99)), false, false),
        ];
        for (leaderless, unreadable, stalled, expected) in table {
            assert_eq!(
                leaderless_stuck(now, leaderless, unreadable, stalled, TTL),
                expected,
                "{leaderless:?} {unreadable:?} {stalled}"
            );
        }
    }

    #[test]
    fn a_clock_that_is_ahead_of_now_is_not_stuck() {
        let t0 = Instant::now();
        assert!(!leaderless_stuck(t0, Some(secs(t0, 5)), None, false, TTL));
    }

    #[test]
    fn a_restarted_loop_starts_not_stuck() {
        let t = StuckTracker::default();
        assert!(!t.is_stuck(Instant::now() + Duration::from_secs(3600), TTL));
    }

    #[test]
    fn leader_appears_vanishes_and_reappears() {
        let t0 = Instant::now();
        let mut t = StuckTracker::default();
        t.observe_leader(t0, true);
        assert!(!t.is_stuck(secs(t0, 1000), TTL), "a leader is never stuck");
        // The leader vanishes: the clock starts at the first leaderless read.
        t.observe_leader(secs(t0, 1000), false);
        t.observe_leader(secs(t0, 1010), false);
        assert!(!t.is_stuck(secs(t0, 1029), TTL));
        assert!(
            t.is_stuck(secs(t0, 1030), TTL),
            "from the first read, not the last"
        );
        // A leader again: back to 200 at once.
        t.observe_leader(secs(t0, 1031), true);
        assert!(!t.is_stuck(secs(t0, 1031), TTL));
        // And the next failover starts a fresh clock.
        t.observe_leader(secs(t0, 2000), false);
        assert!(!t.is_stuck(secs(t0, 2029), TTL));
    }

    #[test]
    fn one_failed_read_is_nothing_a_ttl_of_them_is_stuck() {
        let t0 = Instant::now();
        let mut t = StuckTracker::default();
        t.observe_leader(t0, true);
        t.observe_read_failed(secs(t0, 10));
        assert!(!t.is_stuck(secs(t0, 11), TTL));
        t.observe_read_failed(secs(t0, 20));
        assert!(!t.is_stuck(secs(t0, 39), TTL));
        assert!(
            t.is_stuck(secs(t0, 40), TTL),
            "30 s after the first failure"
        );
        // One good read ends it.
        t.observe_leader(secs(t0, 41), true);
        assert!(!t.is_stuck(secs(t0, 41), TTL));
    }

    #[test]
    fn a_failed_read_does_not_touch_the_leaderless_clock() {
        let t0 = Instant::now();
        let mut t = StuckTracker::default();
        // No failed read yet and no leaderless read: nothing started.
        t.observe_read_failed(t0);
        assert!(t.leaderless_since.is_none());
        t.observe_leader(secs(t0, 5), false);
        let started = t.leaderless_since;
        t.observe_read_failed(secs(t0, 6));
        assert_eq!(t.leaderless_since, started);
        // A failed read while leader-holding does not start it either.
        let mut u = StuckTracker::default();
        u.observe_leader(t0, true);
        u.observe_read_failed(secs(t0, 1));
        assert!(u.leaderless_since.is_none());
    }

    #[test]
    fn two_consecutive_refused_votes_stall_one_does_not() {
        let t0 = Instant::now();
        let mut t = StuckTracker::default();
        t.observe_vote(true);
        assert!(!t.election_stalled());
        assert!(!t.is_stuck(t0, TTL));
        t.observe_vote(true);
        assert!(t.election_stalled());
        assert!(
            t.is_stuck(t0, TTL),
            "the candidate does not wait for the ttl"
        );
        // A vote that is not refused ends it; so does a leader.
        t.observe_vote(false);
        assert!(!t.election_stalled());
        t.observe_vote(true);
        t.observe_vote(false);
        t.observe_vote(true);
        assert!(!t.election_stalled(), "refusals must be consecutive");
        t.observe_vote(true);
        assert!(t.election_stalled());
        t.observe_leader(t0, true);
        assert!(!t.election_stalled());
    }

    #[test]
    fn only_refusals_that_waiting_does_not_fix_stall() {
        for stalling in [
            "quorum vote refused: InsufficientVotes { got: 1, need: 2 }",
            "quorum vote refused: HigherTimeline { peer: \"node2\" }",
            "quorum vote refused: BehindPeer { peer: \"node3\" }",
        ] {
            assert!(refusal_is_stalling(stalling), "{stalling}");
        }
        for transient in [
            "quorum vote refused: PrimaryAlive { peer: \"node2\" }",
            "quorum vote refused: NotVoter",
            "quorum vote refused: own WAL unreadable: timeout",
            "quorum vote refused: own timeline unknown",
            "quorum vote refused: live WAL lag 5 bytes exceeds maximum_lag_on_failover",
        ] {
            assert!(!refusal_is_stalling(transient), "{transient}");
        }
    }
}
