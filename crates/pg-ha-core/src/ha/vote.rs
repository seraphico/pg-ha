//! SB-13: the live quorum vote a candidate must win before it asks for the
//! leader lock.
//!
//! It applies when `/sync` says `kind == Quorum` and either `synchronous_mode`
//! is on or `/sync` still names voters (the window after `quorum -> async`,
//! before the executor has taken `/sync` back). Otherwise [`Ha::quorum_vote`] returns `NotApplicable` before doing
//! any I/O, so the Priority election is unchanged.
//!
//! SB-40: asking a voter is `POST /vote`, not a plain read. The voter stops
//! streaming from its upstream, confirms it, and only then reports its WAL
//! position; a voter that cannot confirm answers 503, which is no vote. This
//! closes the window where a hung old primary keeps collecting acknowledgements
//! from voters that already answered. A fenced voter is pointed at the next
//! lock holder by the follow loop (a missing `primary_conninfo` is a changed
//! upstream). The watchdog stays optional (D12).
//!
//! Timing: the candidate's own live WAL read (`LIVE_WAL_READ_TIMEOUT`) and the
//! peer round (`VOTE_ROUND_TIMEOUT`) run concurrently, so the whole vote takes
//! at most `max(LIVE_WAL_READ_TIMEOUT, VOTE_ROUND_TIMEOUT)` (3 s), not their sum.
use tracing::{error, warn};

use super::Ha;
use super::lag;
use crate::cluster::{MemberState, SyncKind};
use crate::peer_http::align_api_scheme;
use crate::quorum::round::{
    PEER_REQUEST_TIMEOUT, PeerTarget, VOTE_ROUND_TIMEOUT, collect_wal_positions,
};
use crate::quorum::{Names, VoteDecision, WalPosition, quorum_vote_decision};

/// Is a rate-limited log line due? `last` is when it was last logged.
pub(super) fn log_due(
    last: Option<std::time::Instant>,
    now: std::time::Instant,
    interval: std::time::Duration,
) -> bool {
    last.is_none_or(|at| now.saturating_duration_since(at) >= interval)
}

/// Outcome of the vote step.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum VoteGate {
    /// Not a quorum-sync cluster: the election proceeds as before.
    NotApplicable,
    /// The vote was won.
    Pass,
    /// The candidate must not acquire the lock; the text is the reason.
    Refuse(String),
}

impl Ha {
    /// Read the voters' live WAL positions and apply the SI-2 rule (and, when
    /// configured, the SB-11 lag ceiling with the same live values).
    ///
    /// Every failure is a refusal ("no leader rather than a lost commit"),
    /// never a cycle error.
    pub(super) async fn quorum_vote(&self) -> VoteGate {
        let sync_on = self.dynamic_config_state.last_config.sync_mode();
        let Some(state) = self.cluster.sync_state.as_ref().filter(|s| {
            s.kind == SyncKind::Quorum
                && (sync_on.is_synchronous() || super::election::quorum_sync_in_force(Some(s)))
        }) else {
            return VoteGate::NotApplicable;
        };

        let voters: Names = state
            .sync_standby_names()
            .into_iter()
            .map(str::to_ascii_lowercase)
            .collect();
        let quorum = state.quorum as usize;
        let my_name = self.config.name.to_ascii_lowercase();

        let https = self.config.restapi.tls.is_some();
        let targets: Vec<PeerTarget> = voters
            .iter()
            .filter(|v| **v != my_name)
            .filter_map(|v| {
                let member = self.cluster.get_member(v)?;
                if member.state != MemberState::Running || member.api_url.is_empty() {
                    return None;
                }
                Some(PeerTarget {
                    name: member.name.clone(),
                    api_url: align_api_scheme(&member.api_url, https),
                })
            })
            .collect();

        // The own live read and the peer round are independent (the targets do
        // not depend on the own position), so they run concurrently: the whole
        // vote is bounded by max(LIVE_WAL_READ_TIMEOUT, VOTE_ROUND_TIMEOUT).
        let peer_probe = self.peer_probe.clone();
        let peer_round = async {
            match peer_probe {
                Some(probe) => Some(
                    collect_wal_positions(probe, targets, PEER_REQUEST_TIMEOUT, VOTE_ROUND_TIMEOUT)
                        .await,
                ),
                None => None,
            }
        };
        let (own, round) = tokio::join!(self.local_wal.probe(), peer_round);

        // Refusal precedence is the same as when the own read ran first: an
        // unreadable own WAL wins over everything else, and peer failures of
        // that round are not logged.
        let own = match own {
            Ok(status) => status,
            Err(e) => {
                return VoteGate::Refuse(format!("quorum vote refused: own WAL unreadable: {e}"));
            }
        };
        let Some(own_timeline) = own.timeline else {
            return VoteGate::Refuse("quorum vote refused: own timeline unknown".to_string());
        };
        let candidate = WalPosition {
            name: self.config.name.clone(),
            in_recovery: own.in_recovery,
            timeline: own_timeline,
            lsn: own.wal_position,
        };

        let Some(round) = round else {
            return VoteGate::Refuse("quorum vote refused: no peer probe installed".to_string());
        };

        if round.answers.is_empty()
            && let Some((peer, err)) = round.failures.first()
        {
            warn!(
                peer = %peer,
                error = %err,
                "Quorum vote: no voter could be contacted. Check restapi.tls, ca_cert and the client certificate (TLS or authentication) on every node; without answers this node cannot be elected"
            );
        }

        match quorum_vote_decision(&candidate, &voters, quorum, &round.answers) {
            VoteDecision::Refused(reason) => {
                VoteGate::Refuse(format!("quorum vote refused: {reason:?}"))
            }
            VoteDecision::Granted { .. } => {
                // SB-11 ceiling with the SAME live values as the vote (no
                // mixing of published and live readings).
                let max_lag = self
                    .dynamic_config_state
                    .last_config
                    .maximum_lag_on_failover;
                let max_wal = round
                    .answers
                    .iter()
                    .filter(|p| p.timeline == candidate.timeline)
                    .map(|p| p.lsn)
                    .fold(candidate.lsn, u64::max);
                if lag::exceeds_failover_lag(Some(candidate.lsn), max_wal, max_lag) {
                    VoteGate::Refuse(format!(
                        "quorum vote refused: live WAL lag {} bytes exceeds maximum_lag_on_failover",
                        max_wal.saturating_sub(candidate.lsn)
                    ))
                } else {
                    VoteGate::Pass
                }
            }
        }
    }

    /// `n` the executor last read from SHOW; only while this node leads and the
    /// executor applies (else it is stale or not ours).
    pub(super) fn quorum_observed_n(&self) -> Option<usize> {
        if self.is_leader && self.quorum_executor_applies() {
            self.quorum_exec.observed_n()
        } else {
            None
        }
    }

    /// Can `candidate` possibly win the quorum vote? `Some(reason)` when it
    /// cannot, so a switchover or failover is refused BEFORE the lock is given
    /// up (a refused vote after that leaves the cluster without a primary).
    ///
    /// Same inputs as [`Ha::quorum_vote`]: it applies when `/sync` is a quorum
    /// one with voters (`quorum_sync_in_force`), and the vote counts only the
    /// OTHER voters that answered. A voter can answer only if it is a Running
    /// member with an API URL (the vote's own targets), so the best case is
    /// that count. The candidate wins at best when `online >= q`; `q >= |V|`
    /// is the case `online <= |V| - 1 < q`, named separately because it is the
    /// "not relaxed yet" window. The leader (it stops, or is dead) is not
    /// counted. Nothing about LSNs or timelines is judged here: a candidate
    /// that passes can still lose the vote, which is the vote's call.
    pub(super) fn quorum_win_refusal(&self, candidate: &str) -> Option<String> {
        let state = self
            .cluster
            .sync_state
            .as_ref()
            .filter(|s| super::election::quorum_sync_in_force(Some(s)))?;
        let voters: Names = state
            .sync_standby_names()
            .into_iter()
            .map(str::to_ascii_lowercase)
            .collect();
        let me = candidate.to_ascii_lowercase();
        if !voters.contains(&me) {
            // Not a voter: the sync-eligibility check owns that refusal.
            return None;
        }
        let q = state.quorum as usize;
        let v = voters.len();
        if q >= v {
            let n = self
                .quorum_observed_n()
                .map(|n| format!(", n={n}"))
                .unwrap_or_default();
            return Some(format!(
                "quorum not settled: failover is impossible until q is relaxed (q={q}, voters={v}{n})"
            ));
        }
        let leader = self
            .cluster
            .leader
            .as_ref()
            .map(|l| l.name.to_ascii_lowercase());
        let own = self.config.name.to_ascii_lowercase();
        let online = voters
            .iter()
            .filter(|name| **name != me)
            .filter(|name| leader.as_deref() != Some(name.as_str()))
            .filter(|name| !(self.is_leader && **name == own))
            .filter(|name| {
                self.cluster
                    .get_member(name)
                    .is_some_and(|m| m.state == MemberState::Running && !m.api_url.is_empty())
            })
            .count();
        if online < q {
            return Some(format!(
                "not enough voters online for the candidate to win the vote (need q={q}, online={online})"
            ));
        }
        None
    }

    /// Log (rate limited) that a candidate is refused for lack of votes and
    /// the cluster has no leader: with `q >= |V|` at once (nobody can win,
    /// nothing relaxes `q` without a leader), otherwise from the second
    /// consecutive refusal (a single refusal during an ordinary failover is
    /// normal). True when the line was logged. `refusal` is the text of the
    /// refused vote and carries the counts.
    pub(super) fn note_unelectable_quorum(&mut self, refusal: &str) -> bool {
        let state = self
            .cluster
            .sync_state
            .as_ref()
            .filter(|s| super::election::quorum_sync_in_force(Some(s)));
        let unwinnable = state.is_some_and(|s| s.quorum as usize >= s.voter_count());
        let stalled = refusal.contains("InsufficientVotes")
            && state.is_some()
            && (unwinnable || self.stuck.election_stalled());
        if !stalled {
            self.quorum_stall_logged_at = None;
            return false;
        }
        let now = std::time::Instant::now();
        if !log_due(
            self.quorum_stall_logged_at,
            now,
            super::QUORUM_STALL_LOG_INTERVAL,
        ) {
            return false;
        }
        self.quorum_stall_logged_at = Some(now);
        let (q, voters) = state.map_or((0, Vec::new()), |s| {
            (
                s.quorum,
                s.sync_standby_names()
                    .into_iter()
                    .map(str::to_ascii_lowercase)
                    .collect::<Vec<_>>(),
            )
        });
        let me = self.config.name.to_ascii_lowercase();
        // Voters that are not a running member with an API address cannot
        // answer the vote; the others may still not have answered in time.
        let silent: Vec<&str> = voters
            .iter()
            .map(String::as_str)
            .filter(|v| *v != me)
            .filter(|v| {
                !self
                    .cluster
                    .get_member(v)
                    .is_some_and(|m| m.state == MemberState::Running && !m.api_url.is_empty())
            })
            .collect();
        let silent = if silent.is_empty() {
            "none (every other voter is a running member; they did not answer with enough votes)"
                .to_owned()
        } else {
            silent.join(",")
        };
        error!(
            "The cluster has no leader and this candidate cannot collect enough votes ({refusal}); /sync has q={q} over voters [{}]. Voters that cannot answer: {silent}. It stays without a primary until enough of those voters return. See the operations guide, section Quorum synchronous replication (known limitation)",
            voters.join(",")
        );
        true
    }
    /// Test seam: replace this node's own live WAL reader.
    #[cfg(test)]
    pub(super) fn set_local_wal_probe(
        &mut self,
        probe: std::sync::Arc<dyn crate::live_wal::LiveWalProbe>,
    ) {
        self.local_wal = probe;
    }
}
