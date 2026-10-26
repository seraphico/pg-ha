//! Graceful shutdown (SIGTERM / SIGINT): drain while renewing the leader
//! lock, then CHECKPOINT and stop PostgreSQL within the leader lease, release
//! the lock only after PostgreSQL stopped, and never let pg-ha exit while a
//! PostgreSQL that may be writable is still running (docs/invariants.md 1a,
//! "leader 租约").

use std::time::Duration;

use tracing::{error, info};

use super::{Ha, lease};

/// How the graceful shutdown ended. The caller (src/main.rs) exits on every
/// outcome except [`ShutdownOutcome::KeepRunning`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownOutcome {
    /// PostgreSQL is stopped; the leader lock was released (or this node did
    /// not hold it).
    Released,
    /// PostgreSQL is stopped, but deleting the lock failed or was rejected;
    /// it expires after its TTL.
    StoppedLockKept,
    /// The leader lease ran out (at the signal, during the drain, or before
    /// CHECKPOINT and the stop finished): PostgreSQL was fenced
    /// (standby.signal + rewind-check marker, `pg_ctl stop -m immediate`)
    /// and the lock was not released.
    Fenced,
    /// A standby pg-ha configured (in recovery with standby.signal) that does
    /// not hold the leader lock could not be stopped (fast and immediate
    /// failed). Nothing promotes it, so pg-ha exits with an ERROR. A lock
    /// holder in recovery or recovery pg-ha did not create gives
    /// [`ShutdownOutcome::KeepRunning`] instead.
    StandbyLeftRunning,
    /// PostgreSQL may be writable and could not be stopped: pg-ha must not
    /// exit. The caller keeps running the HA loop and calls
    /// [`Ha::continue_shutdown`] after every cycle.
    KeepRunning,
}

impl std::fmt::Display for ShutdownOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let text = match self {
            Self::Released => "PostgreSQL stopped, leader lock released (or not held by this node)",
            Self::StoppedLockKept => "PostgreSQL stopped, leader lock kept until its TTL expires",
            Self::Fenced => "fenced: leader lease expired, PostgreSQL stopped, lock not released",
            Self::StandbyLeftRunning => {
                "PostgreSQL (a standby without the leader lock) left running"
            }
            Self::KeepRunning => "PostgreSQL could not be stopped; pg-ha keeps running",
        };
        f.write_str(text)
    }
}

impl Ha {
    /// The whole graceful shutdown after the first SIGTERM / SIGINT. The HA
    /// loop is no longer running when this is called.
    ///
    /// 1. Mark the shutdown (`/primary` 503, no more starts, commands
    ///    rejected) and publish it to the REST API.
    /// 2. Lease already expired: fence at once (lock not released).
    /// 3. Drain for `drain`, renewing the lock every `loop_wait`; a failed
    ///    renewal fences (not in pause mode, W1).
    /// 4. CHECKPOINT and stop PostgreSQL (fast, then immediate once), raced
    ///    against the lease of the last renewal; the deadline first fences.
    /// 5. Release the lock only after PostgreSQL stopped; PostgreSQL that may
    ///    be writable and still runs gives [`ShutdownOutcome::KeepRunning`].
    pub async fn graceful_shutdown(&mut self, drain: Duration) -> ShutdownOutcome {
        self.begin_shutdown().await;

        if self.shutdown_fence_if_lease_expired().await {
            return self.outcome_after_fence().await;
        }

        info!(
            timeout_secs = drain.as_secs(),
            "Draining in-flight connections (the leader lock is renewed every loop_wait)"
        );
        if self.drain_renewing_leader_lock(drain).await {
            return self.outcome_after_fence().await;
        }

        if self.is_leader
            && !self.is_paused
            && let Some(lease) = self.leader_lease
        {
            info!(
                "Shutdown drain finished; leader lock renewed. CHECKPOINT and the PostgreSQL stop must finish within {:.1}s (leader lease), otherwise PostgreSQL is fenced",
                lease.remaining().as_secs_f64()
            );
        }

        if !self.stop_postgres_within_lease().await {
            return self.outcome_after_fence().await;
        }
        self.finish_shutdown().await
    }

    /// Called by src/main.rs after every HA cycle while the shutdown is in
    /// [`ShutdownOutcome::KeepRunning`]: same checks as the end of
    /// `graceful_shutdown`. Awaits a DCS write (the lock delete) only once
    /// PostgreSQL is stopped.
    pub async fn continue_shutdown(&mut self) -> ShutdownOutcome {
        self.finish_shutdown().await
    }

    async fn begin_shutdown(&mut self) {
        info!("Initiating graceful shutdown");
        self.shutting_down = true;
        self.postgresql.block_starts();
        if let Some(publish) = self.state_publisher.as_ref() {
            publish(self.api_snapshot()).await;
        }
    }

    /// CHECKPOINT and stop, raced against the leader lease like
    /// `run_cycle_bounded` (not raced in pause mode or without the lock).
    /// False: the lease expired first and PostgreSQL was fenced.
    async fn stop_postgres_within_lease(&mut self) -> bool {
        self.publish_fence_deadline();
        let expiry = lease::wait_for_expiry(self.fence_deadline_tx.subscribe());
        let finished = tokio::select! {
            biased;
            () = expiry => false,
            () = self.checkpoint_and_stop() => true,
        };
        if finished {
            return true;
        }
        error!(
            "Leader lease expired before CHECKPOINT and the PostgreSQL stop finished during shutdown; fencing PostgreSQL (immediate stop) and keeping the leader lock"
        );
        let _ = self.fence_for_lost_lease().await;
        false
    }

    async fn checkpoint_and_stop(&mut self) {
        if self.is_leader {
            info!("Leader shutdown: running CHECKPOINT");
            if let Err(e) = self.postgresql.checkpoint().await {
                error!("CHECKPOINT during shutdown failed: {e} (continuing)");
            }
        } else {
            info!("Not leader, skipping CHECKPOINT");
        }
        info!("Stopping PostgreSQL before releasing the leader lock");
        if let Err(e) = self.stop_postgres_for_shutdown().await {
            error!("Failed to stop PostgreSQL during shutdown: {e}");
        }
    }

    async fn finish_shutdown(&mut self) -> ShutdownOutcome {
        // Commands queued during the drain get their (rejected) answer.
        self.process_commands().await;
        if self.postgresql.is_running() {
            if self.stop_pending || self.is_leader {
                // A lock holder keeps stopping PostgreSQL in the HA cycle
                // (stop_pending) instead of promoting it.
                self.stop_pending = true;
                self.log_shutdown_stuck();
                return ShutdownOutcome::KeepRunning;
            }
            // Only a standby pg-ha configured, without the lock, gets here
            // (`identity::shutdown_may_leave_running`).
            error!(
                "PostgreSQL (a standby with standby.signal, not the leader lock holder) could not be stopped; exiting, nothing promotes it"
            );
            return ShutdownOutcome::StandbyLeftRunning;
        }
        if self.release_leader_lock_if_stopped().await {
            ShutdownOutcome::Released
        } else {
            error!("Leader lock kept: the delete failed or was rejected; it expires after its TTL");
            ShutdownOutcome::StoppedLockKept
        }
    }

    /// After a lease fence: the lock is never released here.
    async fn outcome_after_fence(&mut self) -> ShutdownOutcome {
        self.process_commands().await;
        if self.postgresql.is_running() {
            self.log_shutdown_stuck();
            ShutdownOutcome::KeepRunning
        } else {
            ShutdownOutcome::Fenced
        }
    }

    fn log_shutdown_stuck(&self) {
        let pg = &self.config.postgresql;
        error!(
            "Graceful shutdown could not stop PostgreSQL; pg-ha keeps running instead of exiting while PostgreSQL may be writable (it keeps renewing the leader lock while it holds it, so no other node promotes). Stop PostgreSQL manually as the PostgreSQL OS user: {} stop -D {} -m immediate; pg-ha then releases the lock and exits",
            pg.bin_dir.join("pg_ctl").display(),
            pg.data_dir.display()
        );
    }
}
