//! SB-37: the read-only assessment behind `GET /recovery` and
//! `pg-ha-ctl recover` (design 7.2, 7.6), and the digest the operator
//! confirms with.
//!
//! Pure: the caller (`ha::commands`) reads the voters' live positions with
//! `GET /wal` (never `POST /vote`, which fences a voter) and passes them in.
//! The function never decides safety for the election: the candidate's HA
//! loop applies [`super::recovery_vote_decision`] again with fresh votes. Its
//! job is to tell the operator, from the same rule, whether there is anything
//! to accept at all, and if so exactly what.
//!
//! The order of the answers:
//!
//! | State | Outcome | `pg-ha-ctl` exit |
//! |---|---|---|
//! | lock held, or `/sync` is not a quorum one with voters | `NothingToDo` | 0 |
//! | pause, `/config` unresolved, shutdown (caller's `blocked`) | `Blocked` | 4 |
//! | a node answers `in_recovery = false` | `Blocked` | 4 |
//! | no voter may campaign | `Blocked` | 4 |
//! | the normal vote passes for the best candidate | `WillHeal` | 3 |
//! | every voter answers and the vote still fails (`q >= |V|`) | `Blocked` | 4 |
//! | the recovery rule refuses (a voter ahead that may not campaign) | `Blocked` | 4 |
//! | otherwise | `NeedsAcceptance` with the digest | 4 |
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::{
    AncestorEnd, Names, VoteDecision, VoteRefusal, WalPosition, quorum_vote_decision_with_history,
    recovery_vote_decision,
};
use crate::cluster::{ClusterTimeline, RecoverySnapshot, SyncKind, SyncState};

/// May this voter be the recovery candidate?
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Campaign {
    /// It may campaign today (D6 eligible, failover allowed, running).
    Eligible,
    /// Only by abandoning the newer recorded history (D6 `Behind`): the grant
    /// then carries `timeline_loss`.
    TimelineLoss(String),
    /// Never (nofailover, priority 0, not running, other D6 refusals).
    No(String),
}

/// What the caller knows about one voter besides its live reading.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoterInfo {
    pub campaign: Campaign,
    pub failover_priority: u32,
}

/// Everything [`assess`] looks at.
#[derive(Debug, Clone)]
pub struct Inputs<'a> {
    /// Current leader lock holder.
    pub lock_holder: Option<&'a str>,
    pub sync: Option<&'a SyncState>,
    pub timeline: &'a ClusterTimeline,
    /// Live `GET /wal` answers, the serving node's own reading included.
    pub readings: &'a [WalPosition],
    /// Voters that gave no answer, with the reason (lower-cased names).
    pub unreachable: &'a BTreeMap<String, String>,
    /// Per voter in `/sync` (lower-cased names).
    pub voters: &'a BTreeMap<String, VoterInfo>,
    /// Timeline history files as the serving node knows them, by timeline
    /// (D6: one file per timeline number), for the prefix rule of the
    /// normal vote. A missing entry: the plain rule.
    pub ancestors: &'a BTreeMap<u64, Vec<AncestorEnd>>,
    /// A condition of the serving node that stops every recovery (pause,
    /// unresolved `/config`, shutdown).
    pub blocked: Option<String>,
}

/// The loss an acceptance may cause.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Risk {
    /// Commits held only by the named voters may exist (`k >= |V| - q`).
    /// False means nothing acknowledged under quorum can be lost.
    pub possible: bool,
    /// The candidate is on the recorded timeline at or past its begin, so
    /// everything acknowledged before that timeline began is on it.
    pub pre_timeline_safe: bool,
    /// The candidate is below the recorded timeline: the newer history is
    /// abandoned.
    pub timeline_loss: bool,
}

/// The verdict.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum Outcome {
    NothingToDo {
        reason: String,
    },
    Blocked {
        reason: String,
    },
    WillHeal {
        candidate: String,
        reason: String,
    },
    NeedsAcceptance {
        candidate: String,
        lost: Vec<String>,
        risk: Risk,
        digest: String,
    },
}

impl Outcome {
    /// `pg-ha-ctl recover` exit code: 0 nothing to do, 3 will heal by
    /// itself, 4 refused or needs acceptance.
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::NothingToDo { .. } => 0,
            Self::WillHeal { .. } => 3,
            Self::Blocked { .. } | Self::NeedsAcceptance { .. } => 4,
        }
    }
}

/// One voter as the explain shows it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoterReport {
    pub name: String,
    pub timeline: Option<u64>,
    pub lsn: Option<u64>,
    /// What this voter is for the decision.
    pub status: String,
}

/// An existing grant, as the explain shows it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrantView {
    pub candidate: String,
    pub lost: Vec<String>,
    pub expires_in_s: Option<u64>,
    /// The live `/sync` and `/timeline` still equal the grant's snapshot
    /// (false: the grant is ignored and only waits for its TTL).
    pub current: bool,
}

/// [`assess`]'s answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Assessment {
    #[serde(flatten)]
    pub outcome: Outcome,
    pub voters: Vec<VoterReport>,
    pub quorum: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant: Option<GrantView>,
}

/// The digest an operator confirms: SHA-256 over canonical JSON of the
/// snapshot, the candidate, the named voters and the loss classes. LSNs are
/// deliberately left out (they move while standbys replay; the vote reads
/// them again).
pub fn digest(
    snapshot: &RecoverySnapshot,
    candidate: &str,
    lost: &[String],
    risk: &Risk,
) -> String {
    // Fields in alphabetical order; serde writes them in declaration order.
    #[derive(Serialize)]
    struct Canonical<'a> {
        candidate: String,
        lost: Vec<String>,
        possible: bool,
        pre_timeline_safe: bool,
        quorum: u32,
        sync_leader: &'a str,
        timeline: Option<(u64, u64)>,
        timeline_loss: bool,
        timeline_version: Option<u64>,
        voters: Vec<String>,
    }
    let mut lost: Vec<String> = lost.iter().map(|n| n.to_ascii_lowercase()).collect();
    lost.sort();
    lost.dedup();
    let mut voters = snapshot.voters.clone();
    voters.sort();
    let canonical = Canonical {
        candidate: candidate.to_ascii_lowercase(),
        lost,
        possible: risk.possible,
        pre_timeline_safe: risk.pre_timeline_safe,
        quorum: snapshot.quorum,
        sync_leader: &snapshot.sync_leader,
        timeline: snapshot.timeline.map(|t| (t.timeline, t.begin)),
        timeline_loss: risk.timeline_loss,
        timeline_version: snapshot.timeline_version,
        voters,
    };
    let bytes = serde_json::to_vec(&canonical).expect("plain data serializes");
    let hash = ring::digest::digest(&ring::digest::SHA256, &bytes);
    hash.as_ref().iter().map(|b| format!("{b:02x}")).collect()
}

/// Is `digest` a well-formed digest (64 hex characters)?
pub fn is_digest(digest: &str) -> bool {
    digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit())
}

fn quorum_sync(sync: Option<&SyncState>) -> Option<&SyncState> {
    sync.filter(|s| s.kind == SyncKind::Quorum && !s.sync_standby_names().is_empty())
}

/// The assessment (module docs for the order).
pub fn assess(input: &Inputs<'_>) -> Assessment {
    let done = |outcome: Outcome, voters: Vec<VoterReport>, quorum: Option<u32>| Assessment {
        outcome,
        voters,
        quorum,
        grant: None,
    };
    if let Some(holder) = input.lock_holder {
        return done(
            Outcome::NothingToDo {
                reason: format!("the leader lock is held by {holder}"),
            },
            Vec::new(),
            None,
        );
    }
    let Some(sync) = quorum_sync(input.sync) else {
        return done(
            Outcome::NothingToDo {
                reason: "/sync is not a quorum one with voters: quorum recovery does not apply"
                    .into(),
            },
            Vec::new(),
            None,
        );
    };
    let voters: Names = sync
        .sync_standby_names()
        .into_iter()
        .map(str::to_ascii_lowercase)
        .collect();
    let q = sync.quorum;
    let reading = |name: &str| {
        input
            .readings
            .iter()
            .find(|p| p.name.eq_ignore_ascii_case(name))
    };
    let info = |name: &str| input.voters.get(name);
    let reports = |candidate: Option<&WalPosition>| -> Vec<VoterReport> {
        voters
            .iter()
            .map(|v| {
                let r = reading(v);
                let mut status = match (r, candidate) {
                    (None, _) => format!(
                        "unreachable: {}",
                        input
                            .unreachable
                            .get(v)
                            .map(String::as_str)
                            .unwrap_or("no member record with an API address")
                    ),
                    (Some(p), _) if !p.in_recovery => "writable primary".to_string(),
                    (Some(p), Some(c)) if p.name.eq_ignore_ascii_case(&c.name) => {
                        "candidate".to_string()
                    }
                    (Some(p), Some(c)) if p.timeline < c.timeline => format!(
                        "on timeline {} below the candidate's {}: not counted",
                        p.timeline, c.timeline
                    ),
                    (Some(p), Some(c)) if p.timeline > c.timeline || p.lsn > c.lsn => {
                        "ahead of the candidate".to_string()
                    }
                    (Some(_), Some(_)) => "counted".to_string(),
                    (Some(_), None) => "answered".to_string(),
                };
                match info(v).map(|i| &i.campaign) {
                    Some(Campaign::No(why)) => {
                        status.push_str(&format!("; may not campaign: {why}"))
                    }
                    Some(Campaign::TimelineLoss(why)) => status.push_str(&format!(
                        "; may campaign only by abandoning newer history: {why}"
                    )),
                    _ => {}
                }
                VoterReport {
                    name: v.clone(),
                    timeline: r.map(|p| p.timeline),
                    lsn: r.map(|p| p.lsn),
                    status,
                }
            })
            .collect()
    };
    if let Some(why) = &input.blocked {
        return done(
            Outcome::Blocked {
                reason: why.clone(),
            },
            reports(None),
            Some(q),
        );
    }
    if let Some(p) = input.readings.iter().find(|p| !p.in_recovery) {
        return done(
            Outcome::Blocked {
                reason: format!(
                    "node {} is a writable primary without the leader lock; it must be stopped or take the lock, nothing is accepted while it runs",
                    p.name
                ),
            },
            reports(None),
            Some(q),
        );
    }
    // The candidate: the best answering voter that may campaign; one that
    // abandons history only when no voter may campaign without that.
    let pick = |want_loss: bool| {
        voters
            .iter()
            .filter_map(|v| {
                let i = info(v)?;
                let ok = match i.campaign {
                    Campaign::Eligible => !want_loss,
                    Campaign::TimelineLoss(_) => want_loss,
                    Campaign::No(_) => false,
                };
                ok.then(|| reading(v).map(|p| (p, i.failover_priority)))
                    .flatten()
            })
            .max_by(|(a, pa), (b, pb)| {
                (a.timeline, a.lsn, *pa)
                    .cmp(&(b.timeline, b.lsn, *pb))
                    .then_with(|| b.name.cmp(&a.name))
            })
            .map(|(p, _)| p.clone())
    };
    let (candidate, timeline_loss) = match pick(false) {
        Some(c) => (c, false),
        None => match pick(true) {
            Some(c) => (c, true),
            None => {
                return done(
                    Outcome::Blocked {
                        reason: "no answering voter may be promoted (see the voters)".into(),
                    },
                    reports(None),
                    Some(q),
                );
            }
        },
    };
    let report = reports(Some(&candidate));
    let cname = candidate.name.to_ascii_lowercase();
    // The vote passes no history to a candidate under the timeline-loss
    // override; neither does this.
    let ancestors = if timeline_loss {
        &[][..]
    } else {
        input
            .ancestors
            .get(&candidate.timeline)
            .map_or(&[][..], Vec::as_slice)
    };
    if let VoteDecision::Granted { votes } = quorum_vote_decision_with_history(
        &candidate,
        &voters,
        q as usize,
        input.readings,
        ancestors,
    ) {
        return done(
            Outcome::WillHeal {
                candidate: cname.clone(),
                reason: format!(
                    "the normal quorum vote can pass now ({votes} of q={q} votes): {cname} takes the leader lock by itself; nothing to accept"
                ),
            },
            report,
            Some(q),
        );
    }
    let lost: Names = voters
        .iter()
        .filter(|v| **v != cname)
        .filter(|v| reading(v).is_none_or(|p| p.timeline < candidate.timeline))
        .cloned()
        .collect();
    if lost.is_empty() {
        return done(
            Outcome::Blocked {
                reason: format!(
                    "every voter answers and the vote still fails (q={q} of {} voters): wait for the leader that relaxes q; naming no voter accepts nothing",
                    voters.len()
                ),
            },
            report,
            Some(q),
        );
    }
    if let VoteDecision::Refused(why) =
        recovery_vote_decision(&candidate, &voters, &lost, input.readings)
    {
        let reason = match why {
            VoteRefusal::BehindPeer { peer } | VoteRefusal::HigherTimeline { peer } => format!(
                "voter {peer} is ahead of the best candidate {cname} but may not campaign; promoting {cname} would discard WAL that is still reachable"
            ),
            other => format!("the recovery vote would refuse {cname}: {other:?}"),
        };
        return done(Outcome::Blocked { reason }, report, Some(q));
    }
    let k = lost.len();
    let risk = Risk {
        possible: k >= voters.len().saturating_sub(q as usize),
        pre_timeline_safe: input
            .timeline
            .record()
            .is_some_and(|r| r.timeline == candidate.timeline && candidate.lsn >= r.begin),
        timeline_loss,
    };
    let lost: Vec<String> = lost.into_iter().collect();
    let snapshot = RecoverySnapshot::of(sync, input.timeline);
    let digest = digest(&snapshot, &cname, &lost, &risk);
    done(
        Outcome::NeedsAcceptance {
            candidate: cname,
            lost,
            risk,
            digest,
        },
        report,
        Some(q),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{TimelineId, TimelineRecord};

    fn sync(voters: &str, q: u32) -> SyncState {
        SyncState {
            leader: "node1".into(),
            sync_standby: Some(voters.into()),
            quorum: q,
            kind: SyncKind::Quorum,
        }
    }
    fn tl(timeline: u64, begin: u64) -> ClusterTimeline {
        ClusterTimeline::Record {
            record: TimelineRecord {
                timeline,
                begin,
                primary: "node1".into(),
                generation: 0,
                pending: None,
            },
            version: 7,
        }
    }
    fn pos(name: &str, timeline: u64, lsn: u64) -> WalPosition {
        WalPosition {
            name: name.into(),
            in_recovery: true,
            timeline,
            lsn,
        }
    }
    fn eligible(names: &[&str]) -> BTreeMap<String, VoterInfo> {
        names
            .iter()
            .map(|n| {
                (
                    n.to_string(),
                    VoterInfo {
                        campaign: Campaign::Eligible,
                        failover_priority: 1,
                    },
                )
            })
            .collect()
    }
    struct Case {
        lock: Option<&'static str>,
        sync: Option<SyncState>,
        timeline: ClusterTimeline,
        readings: Vec<WalPosition>,
        voters: BTreeMap<String, VoterInfo>,
        ancestors: BTreeMap<u64, Vec<AncestorEnd>>,
        blocked: Option<String>,
    }
    impl Case {
        /// 5 voters node2..node6, q = 2 (n = 3): node1 (the leader) is dead.
        fn five() -> Case {
            Case {
                lock: None,
                sync: Some(sync("node2,node3,node4,node5,node6", 2)),
                timeline: tl(2, 100),
                readings: vec![pos("node2", 2, 500), pos("node3", 2, 400)],
                voters: eligible(&["node2", "node3", "node4", "node5", "node6"]),
                ancestors: BTreeMap::new(),
                blocked: None,
            }
        }
        fn run(&self) -> Assessment {
            let unreachable = BTreeMap::new();
            let ancestors = self.ancestors.clone();
            assess(&Inputs {
                lock_holder: self.lock,
                sync: self.sync.as_ref(),
                timeline: &self.timeline,
                readings: &self.readings,
                unreachable: &unreachable,
                voters: &self.voters,
                ancestors: &ancestors,
                blocked: self.blocked.clone(),
            })
        }
    }
    fn blocked(a: &Assessment) -> &str {
        match &a.outcome {
            Outcome::Blocked { reason } => reason,
            other => panic!("expected Blocked, got {other:?}"),
        }
    }

    #[test]
    fn lock_held_or_not_quorum_is_nothing_to_do() {
        let mut c = Case::five();
        c.lock = Some("node2");
        assert!(matches!(c.run().outcome, Outcome::NothingToDo { .. }));
        assert_eq!(c.run().outcome.exit_code(), 0);
        let mut c = Case::five();
        c.sync = Some(SyncState {
            kind: SyncKind::Priority,
            ..sync("node2,node3", 1)
        });
        assert!(matches!(c.run().outcome, Outcome::NothingToDo { .. }));
        c.sync = None;
        assert!(matches!(c.run().outcome, Outcome::NothingToDo { .. }));
        c.sync = Some(sync("", 0));
        assert!(matches!(c.run().outcome, Outcome::NothingToDo { .. }));
    }

    #[test]
    fn caller_conditions_block() {
        for why in [
            "pause mode",
            "invalid /config",
            "graceful shutdown in progress",
        ] {
            let mut c = Case::five();
            c.blocked = Some(why.into());
            let a = c.run();
            assert_eq!(blocked(&a), why);
            assert_eq!(a.outcome.exit_code(), 4);
        }
    }

    #[test]
    fn writable_primary_blocks() {
        let mut c = Case::five();
        c.readings.push(WalPosition {
            in_recovery: false,
            ..pos("node4", 2, 900)
        });
        assert!(blocked(&c.run()).contains("node4 is a writable primary"));
        // A non-voter too.
        let mut c = Case::five();
        c.readings.push(WalPosition {
            in_recovery: false,
            ..pos("node9", 1, 1)
        });
        assert!(blocked(&c.run()).contains("node9"));
    }

    #[test]
    fn normal_vote_passing_will_heal() {
        let mut c = Case::five();
        c.readings.push(pos("node4", 2, 300));
        let a = c.run();
        match &a.outcome {
            Outcome::WillHeal { candidate, .. } => assert_eq!(candidate, "node2"),
            other => panic!("{other:?}"),
        }
        assert_eq!(a.outcome.exit_code(), 3);
    }

    /// A voter on the parent timeline at or below the switchpoint counts in
    /// the normal vote (prefix rule), so the cluster heals by itself.
    #[test]
    fn prefix_voter_makes_it_heal() {
        let mut c = Case::five();
        c.readings.push(pos("node4", 1, 90));
        assert!(matches!(c.run().outcome, Outcome::NeedsAcceptance { .. }));
        c.ancestors.insert(
            2,
            vec![AncestorEnd {
                timeline: 1,
                switchpoint: 100,
            }],
        );
        assert!(matches!(c.run().outcome, Outcome::WillHeal { .. }));
    }

    #[test]
    fn needs_acceptance_names_every_uncounted_voter() {
        let mut c = Case::five();
        // node4 answers on an older timeline (X2 residual): not counted.
        c.readings.push(pos("node4", 1, 999));
        let a = c.run();
        match &a.outcome {
            Outcome::NeedsAcceptance {
                candidate,
                lost,
                risk,
                digest,
            } => {
                assert_eq!(candidate, "node2");
                assert_eq!(lost, &["node4", "node5", "node6"]);
                // k = 3 >= |V| - q = 3
                assert!(risk.possible);
                assert!(risk.pre_timeline_safe);
                assert!(!risk.timeline_loss);
                assert!(is_digest(digest));
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(a.outcome.exit_code(), 4);
        let status: Vec<&str> = a.voters.iter().map(|v| v.status.as_str()).collect();
        assert_eq!(status[0], "candidate");
        assert_eq!(status[1], "counted");
        assert!(status[2].contains("below the candidate's 2"), "{status:?}");
        assert!(status[3].starts_with("unreachable"), "{status:?}");
    }

    /// When the normal vote fails for lack of votes, `k` uncounted voters
    /// satisfy `|V| - 1 - k < q`, i.e. `k >= |V| - q`: a loss is always
    /// possible. (`k <= |V| - q - 1` is exactly the case the vote passes.)
    #[test]
    fn a_needed_acceptance_always_carries_a_possible_loss() {
        for (q, answering) in [(1u32, 0usize), (2, 1), (3, 2), (4, 3)] {
            let mut c = Case::five();
            c.sync = Some(sync("node2,node3,node4,node5,node6", q));
            c.readings = std::iter::once(pos("node2", 2, 500))
                .chain(
                    ["node3", "node4", "node5"]
                        .iter()
                        .take(answering)
                        .map(|n| pos(n, 2, 400)),
                )
                .collect();
            match c.run().outcome {
                Outcome::NeedsAcceptance { lost, risk, .. } => {
                    assert_eq!(lost.len(), 4 - answering, "q={q}");
                    assert!(risk.possible, "q={q}");
                }
                other => panic!("q={q}: {other:?}"),
            }
        }
    }

    #[test]
    fn candidate_is_the_best_answering_eligible_voter() {
        let mut c = Case::five();
        c.readings = vec![pos("node3", 2, 500), pos("node2", 2, 500)];
        c.voters.get_mut("node3").unwrap().failover_priority = 2;
        match c.run().outcome {
            Outcome::NeedsAcceptance { candidate, .. } => assert_eq!(candidate, "node3"),
            other => panic!("{other:?}"),
        }
        // Equal priority: the lower name.
        c.voters.get_mut("node3").unwrap().failover_priority = 1;
        match c.run().outcome {
            Outcome::NeedsAcceptance { candidate, .. } => assert_eq!(candidate, "node2"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn voter_ahead_that_may_not_campaign_blocks() {
        let mut c = Case::five();
        c.voters.get_mut("node3").unwrap().campaign = Campaign::No("nofailover".into());
        c.readings = vec![pos("node2", 2, 500), pos("node3", 2, 600)];
        let a = c.run();
        assert!(blocked(&a).contains("node3 is ahead"), "{a:?}");
        assert!(a.voters[1].status.contains("may not campaign: nofailover"));
    }

    #[test]
    fn nobody_may_campaign_blocks() {
        let mut c = Case::five();
        for v in c.voters.values_mut() {
            v.campaign = Campaign::No("failover_priority 0".into());
        }
        assert!(blocked(&c.run()).contains("no answering voter"));
        // Eligible voters exist but none answers.
        let mut c = Case::five();
        c.readings.clear();
        assert!(blocked(&c.run()).contains("no answering voter"));
    }

    #[test]
    fn timeline_loss_only_without_an_eligible_candidate() {
        let mut c = Case::five();
        c.voters.get_mut("node2").unwrap().campaign =
            Campaign::TimelineLoss("on timeline 1, latest is 2".into());
        c.readings = vec![pos("node2", 1, 900), pos("node3", 2, 400)];
        match c.run().outcome {
            Outcome::NeedsAcceptance {
                candidate, risk, ..
            } => {
                assert_eq!(candidate, "node3", "an eligible voter wins");
                assert!(!risk.timeline_loss);
            }
            other => panic!("{other:?}"),
        }
        let mut c = Case::five();
        for v in c.voters.values_mut() {
            v.campaign = Campaign::TimelineLoss("behind".into());
        }
        c.readings = vec![pos("node2", 1, 900)];
        match c.run().outcome {
            Outcome::NeedsAcceptance {
                candidate, risk, ..
            } => {
                assert_eq!(candidate, "node2");
                assert!(risk.timeline_loss);
                assert!(!risk.pre_timeline_safe);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn everyone_answers_and_q_too_high_blocks() {
        let mut c = Case::five();
        c.sync = Some(sync("node2,node3", 2));
        c.voters = eligible(&["node2", "node3"]);
        c.readings = vec![pos("node2", 2, 500), pos("node3", 2, 400)];
        assert!(blocked(&c.run()).contains("every voter answers"));
    }

    #[test]
    fn digest_is_stable_and_covers_the_decision() {
        let s = RecoverySnapshot::of(&sync("node3,node2", 1), &tl(2, 100));
        let s2 = RecoverySnapshot::of(&sync("NODE2,node3", 1), &tl(2, 100));
        assert_eq!(s, s2);
        let risk = Risk {
            possible: true,
            pre_timeline_safe: true,
            timeline_loss: false,
        };
        let d = digest(&s, "node2", &["node3".into()], &risk);
        assert!(is_digest(&d));
        assert_eq!(d, digest(&s2, "NODE2", &["Node3".into()], &risk));
        assert_ne!(d, digest(&s, "node2", &["node4".into()], &risk));
        assert_ne!(d, digest(&s, "node3", &["node3".into()], &risk));
        assert_ne!(
            d,
            digest(
                &s,
                "node2",
                &["node3".into()],
                &Risk {
                    timeline_loss: true,
                    ..risk.clone()
                }
            )
        );
        let other_tl = RecoverySnapshot::of(&sync("node3,node2", 1), &tl(3, 100));
        assert_ne!(d, digest(&other_tl, "node2", &["node3".into()], &risk));
        let other_q = RecoverySnapshot::of(&sync("node3,node2", 2), &tl(2, 100));
        assert_ne!(d, digest(&other_q, "node2", &["node3".into()], &risk));
        // Known answer: the canonical bytes are pinned.
        assert_eq!(
            d,
            digest(
                &RecoverySnapshot {
                    sync_leader: "node1".into(),
                    voters: vec!["node2".into(), "node3".into()],
                    quorum: 1,
                    timeline: Some(TimelineId {
                        timeline: 2,
                        begin: 100
                    }),
                    timeline_version: Some(7),
                },
                "node2",
                &["node3".into()],
                &risk
            )
        );
    }

    #[test]
    fn is_digest_checks_length_and_hex() {
        assert!(is_digest(&"a".repeat(64)));
        assert!(is_digest(&"0F".repeat(32)));
        assert!(!is_digest(&"a".repeat(63)));
        assert!(!is_digest(&"g".repeat(64)));
        assert!(!is_digest(""));
    }
}
