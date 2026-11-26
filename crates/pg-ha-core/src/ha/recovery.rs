//! SB-37: the HA side of quorum recovery (design 7.2 to 7.6).
//!
//! * `GET /recovery` ([`Ha::handle_recovery_explain`]): reads every member's
//!   live position with `GET /wal` (read-only; never `POST /vote`, which
//!   fences a voter), and runs [`assess`]. It writes nothing.
//! * `POST /recovery` ([`Ha::handle_recovery_grant`]): runs the same
//!   assessment again, and creates the one-shot `/recovery` grant only when it
//!   still needs acceptance with exactly the named voters and digest, and
//!   every answering node runs this pg-ha version.
//! * `DELETE /recovery` ([`Ha::handle_recovery_cancel`]).
//!
//! The grant is used only by the candidate it names, only while the live
//! `/sync` and `/timeline` equal its snapshot and no lock is held
//! ([`Ha::live_recovery_grant`]); the candidate then runs
//! `recovery_vote_decision` with a fresh `POST /vote` round (`ha::vote`). Any
//! lock acquisition deletes it (`ha::election`).
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tracing::{info, warn};

use super::Ha;
use super::lineage::Ineligible;
use crate::cluster::{MemberState, Recovery, RecoverySnapshot, SyncKind};
use crate::commands::CommandResponse;
use crate::peer_http::align_api_scheme;
use crate::quorum::recovery::{
    Assessment, Campaign, GrantView, Inputs, Outcome, VoterInfo, assess, is_digest,
};
use crate::quorum::round::{
    PEER_REQUEST_TIMEOUT, PeerProbeError, PeerTarget, PeerWalProbe, VOTE_ROUND_TIMEOUT,
    collect_wal_positions,
};
use crate::quorum::{AncestorEnd, WalPosition};

/// Records what each peer reports (version, own timeline history) while the
/// round reads it.
struct VersionRecorder {
    inner: Arc<dyn PeerWalProbe>,
    versions: Mutex<BTreeMap<String, Option<String>>>,
    histories: Mutex<BTreeMap<String, Vec<AncestorEnd>>>,
}

#[async_trait]
impl PeerWalProbe for VersionRecorder {
    async fn read(&self, target: &PeerTarget) -> Result<WalPosition, PeerProbeError> {
        let (p, meta) = self.inner.read_with_meta(target).await?;
        let name = target.name.to_ascii_lowercase();
        if let Some(h) = meta.timeline_history {
            self.histories
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(name.clone(), h);
        }
        self.versions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(name, meta.version);
        Ok(p)
    }
}

/// What the explain round read.
pub(super) struct RecoveryView {
    pub(super) assessment: Assessment,
    /// Version each answering peer reported (lower-cased names).
    versions: BTreeMap<String, Option<String>>,
}

impl Ha {
    /// The grant this node may use now: it names this node, no lock is held,
    /// the live `/sync` and `/timeline` equal its snapshot, and it was written
    /// by this pg-ha version. Otherwise None (a stale grant waits for its TTL).
    pub(super) fn live_recovery_grant(&self) -> Option<&Recovery> {
        let c = self.current_recovery_candidate()?;
        if !c.eq_ignore_ascii_case(&self.config.name) {
            return None;
        }
        self.cluster.recovery.as_ref()
    }

    /// The candidate of a grant that is current (no lock, snapshot equal to
    /// the live `/sync` and `/timeline`, this pg-ha version), whoever it names.
    pub(super) fn current_recovery_candidate(&self) -> Option<&str> {
        let r = self.cluster.recovery.as_ref()?;
        (self.cluster.leader.is_none()
            && r.pg_ha_version == crate::VERSION
            && r.snapshot
                .matches(self.cluster.sync_state.as_ref(), &self.cluster.timeline))
        .then_some(r.candidate.as_str())
    }

    /// The grant in force for this node: the one used for the lock it holds
    /// in this term, or a live one before the lock.
    pub(super) fn recovery_in_force(&self) -> Option<&Recovery> {
        if self.is_leader || self.is_lock_owner() {
            self.recovery_term.as_ref()
        } else {
            self.live_recovery_grant()
        }
    }

    /// May `name` be the recovery candidate (design 7.6)?
    fn recovery_campaign(&self, name: &str) -> VoterInfo {
        let me = name.eq_ignore_ascii_case(&self.config.name);
        let member = self
            .cluster
            .members
            .iter()
            .find(|m| m.name.eq_ignore_ascii_case(name));
        let (nofailover, priority, running) = if me {
            (
                self.config.tags.nofailover,
                self.config.tags.failover_priority,
                self.postgresql.is_running(),
            )
        } else {
            match member {
                Some(m) => (
                    m.is_nofailover(),
                    m.failover_priority(),
                    m.state == MemberState::Running,
                ),
                None => (false, 0, false),
            }
        };
        let campaign = if member.is_none() {
            Campaign::No("no member record".into())
        } else if nofailover {
            Campaign::No("nofailover".into())
        } else if priority == 0 {
            Campaign::No("failover_priority 0".into())
        } else if !running {
            Campaign::No("PostgreSQL not running".into())
        } else {
            match member.map(|m| self.member_eligibility(m)) {
                Some(Ok(())) => Campaign::Eligible,
                Some(Err(why @ Ineligible::Behind { .. })) => {
                    Campaign::TimelineLoss(why.to_string())
                }
                Some(Err(why)) => Campaign::No(why.to_string()),
                None => Campaign::No("no member record".into()),
            }
        };
        VoterInfo {
            campaign,
            failover_priority: priority,
        }
    }

    /// Read every member's live position (`GET /wal`) and assess.
    /// `Err` when the DCS view could not be refreshed.
    pub(super) async fn recovery_view(
        &self,
        dcs_error: Option<&str>,
    ) -> Result<RecoveryView, String> {
        if let Some(e) = dcs_error {
            return Err(format!("the DCS could not be read: {e}"));
        }
        let me = self.config.name.to_ascii_lowercase();
        let sync = self
            .cluster
            .sync_state
            .as_ref()
            .filter(|s| s.kind == SyncKind::Quorum);
        let voter_names: Vec<String> = sync
            .map(|s| {
                s.sync_standby_names()
                    .into_iter()
                    .map(str::to_ascii_lowercase)
                    .collect()
            })
            .unwrap_or_default();
        let https = self.config.restapi.tls.is_some();
        // Every member is read, voters or not: a writable primary anywhere
        // blocks the recovery.
        let targets: Vec<PeerTarget> = self
            .cluster
            .members
            .iter()
            .filter(|m| !m.name.eq_ignore_ascii_case(&me) && !m.api_url.is_empty())
            .map(|m| PeerTarget {
                name: m.name.clone(),
                api_url: align_api_scheme(&m.api_url, https),
            })
            .collect();
        let recorder = self.upstream_reader.clone().map(|inner| {
            Arc::new(VersionRecorder {
                inner,
                versions: Mutex::new(BTreeMap::new()),
                histories: Mutex::new(BTreeMap::new()),
            })
        });
        let round = async {
            match &recorder {
                Some(r) => Some(
                    collect_wal_positions(
                        r.clone() as Arc<dyn PeerWalProbe>,
                        targets,
                        PEER_REQUEST_TIMEOUT,
                        VOTE_ROUND_TIMEOUT,
                    )
                    .await,
                ),
                None => None,
            }
        };
        let (own, round) = tokio::join!(self.local_wal.probe(), round);
        let mut readings = Vec::new();
        let mut unreachable = BTreeMap::new();
        match own {
            Ok(s) => match s.timeline {
                Some(timeline) => readings.push(WalPosition {
                    name: self.config.name.clone(),
                    in_recovery: s.in_recovery,
                    timeline,
                    lsn: s.wal_position,
                }),
                None => {
                    unreachable.insert(me.clone(), "own timeline unknown".to_string());
                }
            },
            Err(e) => {
                unreachable.insert(me.clone(), format!("own WAL unreadable: {e}"));
            }
        }
        match round {
            Some(round) => {
                readings.extend(round.answers);
                for (name, err) in round.failures {
                    unreachable.insert(name.to_ascii_lowercase(), err.to_string());
                }
            }
            None => {
                for v in &voter_names {
                    if *v != me {
                        unreachable.insert(v.clone(), "no peer reader installed".into());
                    }
                }
            }
        }
        let voters: BTreeMap<String, VoterInfo> = voter_names
            .iter()
            .map(|v| (v.clone(), self.recovery_campaign(v)))
            .collect();
        // Each node's OWN history file for the timeline it reports: this
        // node's from its pg_wal, a peer's from its GET /wal answer. The
        // candidate's entry is what its own vote uses (prefix rule).
        let mut ancestors: BTreeMap<String, Vec<AncestorEnd>> = recorder
            .as_ref()
            .map(|r| {
                r.histories
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone()
            })
            .unwrap_or_default();
        ancestors.remove(&me);
        if let Some(own) = readings.iter().find(|p| p.name.eq_ignore_ascii_case(&me))
            && let Ok(Some(lines)) = crate::timeline_files::timeline_ancestors(
                &self.config.postgresql.data_dir,
                own.timeline,
            )
        {
            ancestors.insert(
                me.clone(),
                lines
                    .into_iter()
                    .map(|l| AncestorEnd {
                        timeline: l.parent,
                        switchpoint: l.switchpoint,
                    })
                    .collect(),
            );
        }
        let blocked = if self.is_paused {
            Some("pause mode is on: no election runs; resume the cluster first".to_string())
        } else if self.sync_config_unresolved() {
            Some(
                "the stored /config is invalid and no valid configuration is known (fix it with PUT /config)"
                    .to_string(),
            )
        } else if self.shutting_down {
            Some("graceful shutdown in progress".to_string())
        } else {
            None
        };
        let lock_holder = self.cluster.leader.as_ref().map(|l| l.name.as_str());
        let mut assessment = assess(&Inputs {
            lock_holder,
            sync: self.cluster.sync_state.as_ref(),
            timeline: &self.cluster.timeline,
            readings: &readings,
            unreachable: &unreachable,
            voters: &voters,
            ancestors: &ancestors,
            blocked,
        });
        assessment.grant = self.cluster.recovery.as_ref().map(|r| GrantView {
            candidate: r.candidate.clone(),
            lost: r.lost.clone(),
            expires_in_s: r.expire_at_ms.map(|at| {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| d.as_millis() as u64);
                at.saturating_sub(now) / 1000
            }),
            current: r
                .snapshot
                .matches(self.cluster.sync_state.as_ref(), &self.cluster.timeline),
        });
        let versions = recorder
            .map(|r| r.versions.lock().unwrap_or_else(|e| e.into_inner()).clone())
            .unwrap_or_default();
        Ok(RecoveryView {
            assessment,
            versions,
        })
    }

    /// `GET /recovery`: the assessment, nothing written.
    pub(super) async fn handle_recovery_explain(&self, dcs_error: Option<&str>) -> CommandResponse {
        match self.recovery_view(dcs_error).await {
            Ok(view) => {
                let message = match &view.assessment.outcome {
                    Outcome::NothingToDo { reason } => format!("nothing to do: {reason}"),
                    Outcome::Blocked { reason } => format!("refused: {reason}"),
                    Outcome::WillHeal { reason, .. } => format!("will heal by itself: {reason}"),
                    Outcome::NeedsAcceptance {
                        candidate,
                        lost,
                        risk,
                        ..
                    } if risk.named_online => format!(
                        "needs acceptance: every voter answers and the vote can never pass (q >= the voters); promoting {candidate} gives up no commit a voter holds, the online voter {} leaves the synchronous set until it follows the new primary",
                        lost.join(",")
                    ),
                    Outcome::NeedsAcceptance {
                        candidate, lost, ..
                    } => format!(
                        "needs acceptance: promoting {candidate} gives up the commits that only {} may hold",
                        lost.join(",")
                    ),
                };
                CommandResponse::accepted(message).with_data(
                    serde_json::to_value(&view.assessment).unwrap_or(serde_json::Value::Null),
                )
            }
            Err(e) => {
                warn!("Quorum recovery explain failed: {e}");
                CommandResponse::error(e)
            }
        }
    }

    /// `POST /recovery`: create the grant the explain proposed.
    pub(super) async fn handle_recovery_grant(
        &self,
        lost: Vec<String>,
        digest: String,
        dcs_error: Option<&str>,
    ) -> CommandResponse {
        let mut named: Vec<String> = lost.iter().map(|n| n.trim().to_ascii_lowercase()).collect();
        named.sort();
        named.dedup();
        if named.is_empty() || named.iter().any(String::is_empty) || named.len() != lost.len() {
            return CommandResponse::rejected(
                "lost must name one or more distinct voters".to_string(),
            );
        }
        let digest = digest.trim().to_ascii_lowercase();
        if !is_digest(&digest) {
            return CommandResponse::rejected("digest must be 64 hexadecimal characters");
        }
        if let Some(r) = &self.cluster.recovery {
            let expires = r
                .expire_at_ms
                .map(|at| {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_or(0, |d| d.as_millis() as u64);
                    format!(", expires in {} s", at.saturating_sub(now) / 1000)
                })
                .unwrap_or_default();
            return CommandResponse::rejected(format!(
                "a quorum recovery is already authorized for {}{expires}; DELETE /recovery (pg-ha-ctl recover --cancel) first",
                r.candidate
            ));
        }
        let view = match self.recovery_view(dcs_error).await {
            Ok(v) => v,
            Err(e) => {
                warn!("Quorum recovery grant failed: {e}");
                return CommandResponse::error(e);
            }
        };
        let (candidate, assessed_lost, risk, assessed_digest) = match &view.assessment.outcome {
            Outcome::NeedsAcceptance {
                candidate,
                lost,
                risk,
                digest,
            } => (candidate, lost, risk, digest),
            Outcome::NothingToDo { reason } => {
                return CommandResponse::rejected(format!("nothing to accept: {reason}"));
            }
            Outcome::WillHeal { reason, .. } => {
                return CommandResponse::rejected(format!(
                    "nothing to accept, the cluster heals by itself: {reason}"
                ));
            }
            Outcome::Blocked { reason } => {
                return CommandResponse::rejected(format!("refused: {reason}"));
            }
        };
        if *assessed_lost != named {
            return CommandResponse::rejected(format!(
                "the situation is not what was named: the voters that cannot be counted now are [{}], not [{}]; run pg-ha-ctl recover again",
                assessed_lost.join(","),
                named.join(",")
            ));
        }
        if *assessed_digest != digest {
            return CommandResponse::rejected(
                "the situation changed since the explain (digest differs); run pg-ha-ctl recover again",
            );
        }
        // Every node must run the same pg-ha version: the candidate's vote
        // and the voters' fencing must follow the same rules.
        // (Every node that answered; the others are named lost or not
        // reachable, and a node of another version ignores the grant.)
        for (name, v) in &view.versions {
            match v.as_deref() {
                Some(v) if v == crate::VERSION => {}
                Some(v) => {
                    return CommandResponse::rejected(format!(
                        "node {name} runs pg-ha {v}, this node {}: every node must run the same version",
                        crate::VERSION
                    ));
                }
                None => {
                    return CommandResponse::rejected(format!(
                        "node {name} does not report its pg-ha version (older than {}): every node must run the same version",
                        crate::VERSION
                    ));
                }
            }
        }
        let Some(sync) = self.cluster.sync_state.as_ref() else {
            return CommandResponse::rejected("no /sync");
        };
        let grant = Recovery {
            candidate: candidate.clone(),
            lost: named.clone(),
            snapshot: RecoverySnapshot::of(sync, &self.cluster.timeline),
            timeline_loss: risk.timeline_loss,
            named_online: risk.named_online,
            digest: digest.clone(),
            created_by: self.config.name.clone(),
            pg_ha_version: crate::VERSION.to_string(),
            expire_at_ms: None,
        };
        let value = match serde_json::to_string(&grant) {
            Ok(v) => v,
            Err(e) => return CommandResponse::error(format!("cannot encode the grant: {e}")),
        };
        match self.dcs.create_recovery(&value).await {
            Ok(true) => {
                let text = grant.loss_text();
                warn!(
                    candidate = %candidate,
                    lost = %named.join(","),
                    timeline_loss = risk.timeline_loss,
                    named_online = risk.named_online,
                    "Quorum recovery authorized by the operator: {candidate} may be promoted without counting {}; accepted: {text}. The grant expires in {} s",
                    named.join(","),
                    self.dcs.ttl()
                );
                CommandResponse::accepted(format!(
                    "quorum recovery authorized: {candidate} may be promoted, accepting the {text}; the grant expires in {} s",
                    self.dcs.ttl()
                ))
                .with_data(serde_json::to_value(&view.assessment).unwrap_or_default())
            }
            Ok(false) => CommandResponse::rejected(
                "a quorum recovery was authorized at the same time; run pg-ha-ctl recover to see it",
            ),
            Err(e) => {
                warn!("Quorum recovery grant could not be written: {e}");
                CommandResponse::error(format!(
                    "the grant could not be written (nothing authorized): {e}"
                ))
            }
        }
    }

    /// `DELETE /recovery`.
    pub(super) async fn handle_recovery_cancel(&self) -> CommandResponse {
        match self.dcs.delete_recovery().await {
            Ok(true) => {
                info!("Quorum recovery grant deleted by the operator");
                CommandResponse::accepted("quorum recovery grant deleted")
            }
            Ok(false) => CommandResponse::rejected("no quorum recovery grant to delete"),
            Err(e) => CommandResponse::error(format!("could not delete the grant: {e}")),
        }
    }
}
