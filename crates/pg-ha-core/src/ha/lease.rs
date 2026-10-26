//! Local leader lease: the time bound of L5 (docs/invariants.md 1a,
//! "leader 租约").
//!
//! Both clocks are read right before `attempt_to_acquire_leader` /
//! `update_leader`. After `Ok(true)` the lease runs until anchor + the TTL
//! written into the lock (`dcs.ttl()`) − `watchdog.safety_margin`, on the
//! monotonic clock and on the wall clock. It has expired once either clock
//! reaches its deadline, whether or not any DCS call has returned.

use std::time::{Duration, SystemTime};

use tokio::sync::watch;
use tokio::time::Instant;

/// Longest sleep between two clock checks while waiting for expiry, so a wall
/// clock deadline (which no timer follows) is noticed within this time.
const LEASE_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Both clocks, read immediately before a leader-lock write.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LeaseAnchor {
    mono: Instant,
    wall: SystemTime,
}

impl LeaseAnchor {
    pub(crate) fn now() -> Self {
        Self {
            mono: Instant::now(),
            wall: SystemTime::now(),
        }
    }
}

/// Deadlines of the lease granted by one successful leader-lock write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LeaderLease {
    mono_deadline: Instant,
    wall_deadline: SystemTime,
}

impl LeaderLease {
    /// `lock_ttl_secs` must be the TTL written into the lock (`dcs.ttl()`).
    pub(crate) fn new(anchor: LeaseAnchor, lock_ttl_secs: u64, safety_margin_secs: u64) -> Self {
        let valid = Duration::from_secs(lock_ttl_secs.saturating_sub(safety_margin_secs));
        Self {
            mono_deadline: anchor.mono + valid,
            wall_deadline: anchor.wall + valid,
        }
    }

    pub(crate) fn expired_at(&self, mono_now: Instant, wall_now: SystemTime) -> bool {
        mono_now >= self.mono_deadline || wall_now >= self.wall_deadline
    }

    pub(crate) fn is_expired(&self) -> bool {
        self.expired_at(Instant::now(), SystemTime::now())
    }

    /// Time left until the lease expires on the earlier of both clocks
    /// (zero once expired).
    pub(crate) fn remaining(&self) -> Duration {
        let mono = self.mono_deadline.saturating_duration_since(Instant::now());
        let wall = self
            .wall_deadline
            .duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO);
        mono.min(wall)
    }

    fn until_next_check(&self) -> Duration {
        self.remaining().min(LEASE_POLL_INTERVAL)
    }
}

/// Completes when the published lease has expired. `None` (no lease, or
/// pause mode) never expires; a newly published lease replaces the one being
/// waited on.
pub(crate) async fn wait_for_expiry(mut rx: watch::Receiver<Option<LeaderLease>>) {
    loop {
        let lease = *rx.borrow_and_update();
        match lease {
            Some(lease) if lease.is_expired() => return,
            Some(lease) => {
                tokio::select! {
                    () = tokio::time::sleep(lease.until_next_check()) => {}
                    changed = rx.changed() => {
                        if changed.is_err() {
                            std::future::pending::<()>().await;
                        }
                    }
                }
            }
            None => {
                if rx.changed().await.is_err() {
                    std::future::pending::<()>().await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn anchor_at(mono: Instant, wall: SystemTime) -> LeaseAnchor {
        LeaseAnchor { mono, wall }
    }

    #[test]
    fn lease_runs_ttl_minus_safety_margin_from_the_anchor() {
        let (mono, wall) = (Instant::now(), SystemTime::now());
        let lease = LeaderLease::new(anchor_at(mono, wall), 30, 5);
        assert_eq!(lease.mono_deadline, mono + Duration::from_secs(25));
        assert_eq!(lease.wall_deadline, wall + Duration::from_secs(25));
        assert!(!lease.expired_at(mono + Duration::from_millis(24_999), wall));
        assert!(lease.expired_at(mono + Duration::from_secs(25), wall));
    }

    #[test]
    fn lease_expires_by_monotonic_or_wall_clock() {
        let (mono, wall) = (Instant::now(), SystemTime::now());
        let lease = LeaderLease::new(anchor_at(mono, wall), 15, 5);
        let ten = Duration::from_secs(10);
        assert!(!lease.expired_at(mono + Duration::from_secs(9), wall + Duration::from_secs(9)));
        // Monotonic clock past the deadline, wall clock set back.
        assert!(lease.expired_at(mono + ten, wall - Duration::from_secs(60)));
        // Monotonic clock stopped (suspend), wall clock moved on.
        assert!(lease.expired_at(mono, wall + ten));
    }

    #[test]
    fn margin_not_below_ttl_gives_an_expired_lease() {
        let (mono, wall) = (Instant::now(), SystemTime::now());
        assert!(LeaderLease::new(anchor_at(mono, wall), 5, 5).expired_at(mono, wall));
        assert!(LeaderLease::new(anchor_at(mono, wall), 3, 5).expired_at(mono, wall));
    }

    #[tokio::test(start_paused = true)]
    async fn wait_for_expiry_follows_the_latest_lease() {
        let start = Instant::now();
        let (tx, rx) = watch::channel(Some(LeaderLease::new(LeaseAnchor::now(), 15, 5)));
        let waiter = tokio::spawn(wait_for_expiry(rx));

        // Renewed after 4 s: the deadline moves to 4 + 10 s.
        tokio::time::sleep(Duration::from_secs(4)).await;
        tx.send_replace(Some(LeaderLease::new(LeaseAnchor::now(), 15, 5)));
        tokio::time::sleep(Duration::from_secs(9)).await;
        assert!(!waiter.is_finished(), "the renewed lease is still valid");

        waiter.await.unwrap();
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_secs(14) && elapsed < Duration::from_millis(14_100),
            "{elapsed:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn remaining_counts_down_on_both_clocks() {
        let lease = LeaderLease::new(LeaseAnchor::now(), 15, 5);
        let left = lease.remaining();
        assert!(
            left <= Duration::from_secs(10) && left > Duration::from_millis(9_900),
            "{left:?}"
        );
        tokio::time::sleep(Duration::from_secs(4)).await;
        let left = lease.remaining();
        assert!(
            left <= Duration::from_secs(6) && left > Duration::from_millis(5_900),
            "{left:?}"
        );
        // Wall clock ahead of the monotonic clock: the earlier deadline wins.
        let (mono, wall) = (Instant::now(), SystemTime::now());
        let lease = LeaderLease::new(anchor_at(mono, wall - Duration::from_secs(8)), 15, 5);
        assert!(lease.remaining() <= Duration::from_secs(2));
        tokio::time::sleep(Duration::from_secs(20)).await;
        assert_eq!(lease.remaining(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn wait_for_expiry_never_completes_without_a_lease() {
        let (tx, rx) = watch::channel(None);
        let r = tokio::time::timeout(Duration::from_secs(3600), wait_for_expiry(rx)).await;
        assert!(r.is_err());
        drop(tx);
    }
}
