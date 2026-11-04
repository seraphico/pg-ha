//! Cluster history (`/history`): a failover or switchover is noted in memory
//! when this node acquires the leader lock, and appended to the DCS on a later
//! Hold cycle.
//!
//! The promote path races the leader lease, so it never writes the history.
//! The append is best effort: an entry is lost if the node crashes between
//! the acquisition and the append (docs/invariants.md S3). A failed append
//! keeps the pending entry and is retried next cycle; it never changes the
//! cycle result, demotes or fences.
use super::Ha;
use crate::cluster::{ClusterTimeline, Failover, TimelineId};
use crate::history::{History, HistoryEntry, HistoryEventType};
use crate::standby_cluster::StandbyCluster;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{info, warn};
/// Read-modify-write attempts per cycle when the key changes between the
/// read and the compare-and-set write.
const HISTORY_CAS_ATTEMPTS: usize = 3;
/// A history entry waiting for its append to the DCS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PendingHistory {
    pub(super) timestamp: u64,
    pub(super) event_type: HistoryEventType,
    pub(super) old_leader: Option<String>,
    pub(super) reason: String,
    /// Timeline of the last confirmed primary before this acquisition. When
    /// the node ends up on that same timeline no promotion happened (the
    /// same primary got the lock back), so nothing is recorded.
    pub(super) prev_timeline: Option<u64>,
}
/// What the append of one pending entry came to.
#[derive(Debug, PartialEq, Eq)]
enum HistoryAppend {
    Written,
    /// No new timeline was created: not a change of primary.
    NotAPromotion,
    Failed(String),
}
/// Event type, old leader and reason for an acquisition, from the
/// `/failover` intent that is about to be cleared and the `/timeline` record.
/// A switchover (leader named) is a Switchover; a manual failover and an
/// automatic election are Failovers.
pub(super) fn classify_acquisition(
    failover: Option<&Failover>,
    timeline: &ClusterTimeline,
) -> (HistoryEventType, Option<String>, String) {
    let previous_primary = timeline
        .record()
        .map(|r| r.primary.clone())
        .filter(|p| !p.is_empty());
    let named = |s: &Option<String>| s.clone().filter(|n| !n.is_empty());
    match failover {
        Some(f) if named(&f.leader).is_some() => {
            let leader = named(&f.leader);
            let reason = match named(&f.candidate) {
                Some(c) => format!(
                    "switchover requested from {} to {c}",
                    leader.as_deref().unwrap_or_default()
                ),
                None => format!(
                    "switchover requested from {}",
                    leader.as_deref().unwrap_or_default()
                ),
            };
            (HistoryEventType::Switchover, leader, reason)
        }
        Some(f) => {
            let reason = match named(&f.candidate) {
                Some(c) => format!("manual failover requested to {c}"),
                None => "manual failover requested".to_string(),
            };
            (HistoryEventType::Failover, previous_primary, reason)
        }
        None => (
            HistoryEventType::Failover,
            previous_primary,
            "automatic failover: the leader lock was free".to_string(),
        ),
    }
}
impl Ha {
    /// On a successful lock acquisition: remember what this election was, in
    /// memory only. No DCS access (the promote path must not gain a write).
    pub(super) fn note_pending_history(&mut self) {
        if StandbyCluster::is_standby_cluster(&self.dynamic_config_state.last_config) {
            // A standby leader never promotes; nothing to record.
            self.pending_history = None;
            return;
        }
        let (event_type, old_leader, reason) =
            classify_acquisition(self.cluster.failover.as_ref(), &self.cluster.timeline);
        self.pending_history = Some(PendingHistory {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            event_type,
            old_leader,
            reason,
            prev_timeline: self.cluster.timeline.record().map(|r| r.timeline),
        });
    }
    /// Hold cycle: append the pending entry to `/history`. Never returns an
    /// error and never alters the cycle: a failure is logged and the entry
    /// stays pending.
    pub(super) async fn append_pending_history(&mut self) {
        let Some(pending) = self.pending_history.clone() else {
            return;
        };
        if self.lease_lost() {
            return;
        }
        // The cycle is raced against the lease: never spend more than half of
        // what is left of it on history. Each DCS write is also bounded by
        // the DCS write timeout.
        let mut budget = Duration::from_secs(self.config.retry_timeout.max(1));
        if let Some(lease) = self.leader_lease {
            budget = budget.min(lease.remaining() / 2);
        }
        if budget < Duration::from_secs(1) {
            info!(
                "Not enough lease left to record the cluster history this cycle; retrying next cycle"
            );
            return;
        }
        let outcome = match tokio::time::timeout(budget, self.try_append_history(&pending)).await {
            Ok(outcome) => outcome,
            Err(_) => HistoryAppend::Failed(format!("timed out after {}s", budget.as_secs())),
        };
        match outcome {
            HistoryAppend::Written => self.pending_history = None,
            HistoryAppend::NotAPromotion => {
                info!("Lock acquired without a new timeline; not recording a history entry");
                self.pending_history = None;
            }
            HistoryAppend::Failed(why) => {
                warn!("Could not record the cluster history entry (will retry next cycle): {why}");
            }
        }
    }
    async fn try_append_history(&self, pending: &PendingHistory) -> HistoryAppend {
        let (id, authoritative) = self.history_timeline().await;
        if authoritative
            && let (Some(prev), Some(id)) = (pending.prev_timeline, id)
            && prev == id.timeline
        {
            return HistoryAppend::NotAPromotion;
        }
        let entry = HistoryEntry {
            timestamp: pending.timestamp,
            event_type: pending.event_type.clone(),
            old_leader: pending.old_leader.clone(),
            new_leader: Some(self.config.name.clone()),
            reason: pending.reason.clone(),
            timeline: id.map(|i| i.timeline),
            lsn: id.map(|i| i.begin),
        };
        for _ in 0..HISTORY_CAS_ATTEMPTS {
            let (entries, version) = match self.dcs.read_history().await {
                Ok(read) => read,
                Err(e) => return HistoryAppend::Failed(format!("read /history: {e}")),
            };
            let mut history = History::from_entries(entries);
            if !history.push(entry.clone()) {
                // Already there: an earlier write completed after we gave up on it.
                return HistoryAppend::Written;
            }
            match self.dcs.write_history(history.entries(), version).await {
                Ok(true) => return HistoryAppend::Written,
                Ok(false) => continue,
                Err(e) => return HistoryAppend::Failed(format!("write /history: {e}")),
            }
        }
        HistoryAppend::Failed("/history kept changing (version conflict)".into())
    }
    /// Timeline this primary created and the LSN it began at (the promote
    /// LSN). The local server is authoritative; the confirmed `/timeline`
    /// record naming this node is the fallback (not authoritative: it may
    /// still describe an earlier term).
    async fn history_timeline(&self) -> (Option<TimelineId>, bool) {
        if let Ok(id) = self.primary_timeline_id().await {
            return (Some(id), true);
        }
        let from_record = self
            .cluster
            .timeline
            .record()
            .filter(|r| r.primary == self.config.name && r.pending.is_none())
            .map(|r| r.id());
        (from_record, false)
    }
}
