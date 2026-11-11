//! SB-13: the live quorum vote a candidate must win before it asks for the
//! leader lock.
//!
//! It only applies when `/sync` says `kind == Quorum` (and `synchronous_mode`
//! is on). Otherwise [`Ha::quorum_vote`] returns `NotApplicable` before doing
//! any I/O, so the Priority election is unchanged.
use tracing::warn;

use super::Ha;
use super::lag;
use crate::cluster::{MemberState, SyncKind};
use crate::peer_http::align_api_scheme;
use crate::quorum::round::{
    PEER_REQUEST_TIMEOUT, PeerTarget, VOTE_ROUND_TIMEOUT, collect_wal_positions,
};
use crate::quorum::{Names, VoteDecision, WalPosition, quorum_vote_decision};

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
        let sync_on = self
            .dynamic_config_state
            .last_config
            .synchronous_mode
            .unwrap_or(false);
        let Some(state) = self
            .cluster
            .sync_state
            .as_ref()
            .filter(|s| sync_on && s.kind == SyncKind::Quorum)
        else {
            return VoteGate::NotApplicable;
        };

        let voters: Names = state
            .sync_standby_names()
            .into_iter()
            .map(str::to_ascii_lowercase)
            .collect();
        let quorum = state.quorum as usize;
        let my_name = self.config.name.to_ascii_lowercase();

        let own = match self.local_wal.probe().await {
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

        let Some(probe) = self.peer_probe.clone() else {
            return VoteGate::Refuse("quorum vote refused: no peer probe installed".to_string());
        };

        let round =
            collect_wal_positions(probe, targets, PEER_REQUEST_TIMEOUT, VOTE_ROUND_TIMEOUT).await;

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

    /// Test seam: replace this node's own live WAL reader.
    #[cfg(test)]
    pub(super) fn set_local_wal_probe(
        &mut self,
        probe: std::sync::Arc<dyn crate::live_wal::LiveWalProbe>,
    ) {
        self.local_wal = probe;
    }
}
