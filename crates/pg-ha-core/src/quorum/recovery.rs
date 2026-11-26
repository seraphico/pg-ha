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
//! | every voter answers, the vote fails (`q >= |V|`) | `NeedsAcceptance` naming the lowest ONLINE voter (`risk.named_online`) | 4 |
//! | every voter answers, `q < |V|`, a voter ahead or no one to name | `Blocked` | 4 |
//! | the recovery rule refuses (a voter ahead that may not campaign) | `Blocked` | 4 |
//! | otherwise | `NeedsAcceptance` with the digest | 4 |
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::{
    AncestorEnd, Names, PrefixTest, Standing, VoteDecision, VoteRefusal, WalPosition, decide,
    holds_a_prefix, recovery_decide, standing,
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
    /// Each answering node's own history file for the timeline it reports
    /// (lower-cased names): the serving node's from its pg_wal, a peer's from
    /// its `GET /wal` answer. The candidate's entry is exactly what its own
    /// vote uses for the prefix rule, so the explain does not depend on
    /// which node serves it. A missing entry: the plain rule (fail closed).
    pub ancestors: &'a BTreeMap<String, Vec<AncestorEnd>>,
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
    /// The only voter(s) named are ONLINE ones: every voter answers, `q >=
    /// |V|` and the vote can never pass. The named voter answers on the
    /// candidate's timeline and is not ahead (the vote checks that again), so
    /// no acknowledged commit that a voter holds is given up; what the grant
    /// gives away is only the voter's place in the new synchronous set.
    #[serde(default)]
    pub named_online: bool,
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
        named_online: bool,
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
        named_online: risk.named_online,
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

fn refusal_reason(cname: &str, why: VoteRefusal) -> String {
    match why {
        VoteRefusal::BehindPeer { peer } | VoteRefusal::HigherTimeline { peer } => format!(
            "voter {peer} is ahead of the best candidate {cname} but may not campaign; promoting {cname} would discard WAL that is still reachable"
        ),
        other => format!("the recovery vote would refuse {cname}: {other:?}"),
    }
}

fn quorum_sync(sync: Option<&SyncState>) -> Option<&SyncState> {
    sync.filter(|s| s.kind == SyncKind::Quorum && !s.sync_standby_names().is_empty())
}

/// The assessment (module docs for the order).
pub fn assess(input: &Inputs<'_>) -> Assessment {
    assess_with(input, holds_a_prefix)
}

/// [`assess`] with the older-timeline test as a parameter: the vote's own
/// rule in production; mutated versions in the tests. Every count below goes
/// through [`standing`] / [`decide`] / [`recovery_decide`] with it, so the
/// explain counts exactly the voters the vote counts.
pub(crate) fn assess_with(input: &Inputs<'_>, prefix: PrefixTest) -> Assessment {
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
    let reports = |candidate: Option<&WalPosition>,
                   ancestors: &[AncestorEnd]|
     -> Vec<VoterReport> {
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
                    (Some(p), Some(c)) if p.timeline < c.timeline => {
                        // The vote's own rule (`standing`): counted when its
                        // WAL is a prefix of the candidate's.
                        if standing(c, &voters, p, ancestors, prefix) == Standing::Counts {
                            format!(
                                "counted: on timeline {} below the candidate's {}, at or before the point where the candidate's history left it",
                                p.timeline, c.timeline
                            )
                        } else {
                            format!(
                                "on timeline {} below the candidate's {}: not counted",
                                p.timeline, c.timeline
                            )
                        }
                    }
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
            reports(None, &[]),
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
            reports(None, &[]),
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
                    reports(None, &[]),
                    Some(q),
                );
            }
        },
    };
    let cname = candidate.name.to_ascii_lowercase();
    // The vote passes no history to a candidate under the timeline-loss
    // override; neither does this.
    let ancestors = if timeline_loss {
        &[][..]
    } else {
        input
            .ancestors
            .get(&candidate.name.to_ascii_lowercase())
            .map_or(&[][..], Vec::as_slice)
    };
    let report = reports(Some(&candidate), ancestors);
    if let VoteDecision::Granted { votes } = decide(
        &candidate,
        &voters,
        q as usize,
        input.readings,
        ancestors,
        prefix,
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
    // Not counted by the vote's own rule (`standing`, prefix rule included):
    // no answer, or an older timeline the candidate's history does not
    // cover. A voter that refuses is left to the vote check below.
    let mut lost: Names = voters
        .iter()
        .filter(|v| **v != cname)
        .filter(|v| {
            reading(v).is_none_or(|p| {
                standing(&candidate, &voters, p, ancestors, prefix) == Standing::NotCounted
            })
        })
        .cloned()
        .collect();
    // Every other voter answers on the candidate's timeline (or ahead) and
    // the vote still fails. The count `q` can then never be met (`q >= |V|`:
    // the candidate needs `q` OTHER voters), whatever the voters hold: the
    // dead leader is the only one that can relax `q`. The operator may give
    // up one ONLINE voter to get out (maintainer decision SB-37 b). It is the
    // lowest one, so the claim is the smallest true one: the vote below
    // refuses when any voter is ahead of the candidate, named or not, and a
    // voter level with or behind the candidate holds nothing the candidate
    // lacks.
    let mut named_online = false;
    if lost.is_empty() {
        let stuck = (q as usize) >= voters.len();
        let given_up = voters
            .iter()
            .filter(|v| **v != cname)
            .filter_map(|v| reading(v))
            .min_by(|a, b| {
                (a.timeline, a.lsn)
                    .cmp(&(b.timeline, b.lsn))
                    .then_with(|| b.name.cmp(&a.name))
            })
            .map(|p| p.name.to_ascii_lowercase());
        match given_up.filter(|_| stuck) {
            Some(name) => {
                named_online = true;
                lost.insert(name);
            }
            None => {
                // `q < |V|`: only a voter ahead that may not campaign can
                // refuse the vote (the check below says which); a single
                // voter has nobody to name.
                if let VoteDecision::Refused(why) = recovery_decide(
                    &candidate,
                    &voters,
                    &lost,
                    input.readings,
                    ancestors,
                    prefix,
                ) {
                    return done(
                        Outcome::Blocked {
                            reason: refusal_reason(&cname, why),
                        },
                        report,
                        Some(q),
                    );
                }
                return done(
                    Outcome::Blocked {
                        reason: format!(
                            "the vote fails although every voter answers (q={q} of {} voters) and there is no other voter to name",
                            voters.len()
                        ),
                    },
                    report,
                    Some(q),
                );
            }
        }
    }
    if let VoteDecision::Refused(why) = recovery_decide(
        &candidate,
        &voters,
        &lost,
        input.readings,
        ancestors,
        prefix,
    ) {
        return done(
            Outcome::Blocked {
                reason: refusal_reason(&cname, why),
            },
            report,
            Some(q),
        );
    }
    let k = lost.len();
    let risk = Risk {
        named_online,
        possible: k >= voters.len().saturating_sub(q as usize),
        pre_timeline_safe: input
            .timeline
            .record()
            .is_some_and(|r| r.timeline == candidate.timeline && candidate.lsn >= r.begin),
        timeline_loss,
    };
    let lost: Vec<String> = lost.into_iter().collect();
    let report = if named_online {
        report
            .into_iter()
            .map(|mut r| {
                if lost.contains(&r.name.to_ascii_lowercase()) {
                    r.status.push_str(
                        "; named as given up while online (holds nothing the candidate lacks)",
                    );
                }
                r
            })
            .collect()
    } else {
        report
    };
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
        ancestors: BTreeMap<String, Vec<AncestorEnd>>,
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
        /// Through the public `assess` (the production rule).
        fn run(&self) -> Assessment {
            self.run_inner(None)
        }
        /// Through `assess_with` and a mutated older-timeline test.
        fn run_with(&self, prefix: PrefixTest) -> Assessment {
            self.run_inner(Some(prefix))
        }
        fn run_inner(&self, prefix: Option<PrefixTest>) -> Assessment {
            let unreachable = BTreeMap::new();
            let ancestors = self.ancestors.clone();
            let inputs = Inputs {
                lock_holder: self.lock,
                sync: self.sync.as_ref(),
                timeline: &self.timeline,
                readings: &self.readings,
                unreachable: &unreachable,
                voters: &self.voters,
                ancestors: &ancestors,
                blocked: self.blocked.clone(),
            };
            match prefix {
                None => assess(&inputs),
                Some(p) => assess_with(&inputs, p),
            }
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
        // Another node's history does not stand in for the candidate's.
        c.ancestors.insert(
            "node3".into(),
            vec![AncestorEnd {
                timeline: 1,
                switchpoint: 100,
            }],
        );
        assert!(matches!(c.run().outcome, Outcome::NeedsAcceptance { .. }));
        c.ancestors.insert(
            "node2".into(),
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

    /// SB-37 decision (b): every voter answers and `q >= |V|`, the vote can
    /// never pass. The lowest ONLINE voter is proposed as given up.
    #[test]
    fn everyone_answers_and_q_too_high_names_the_lowest_online_voter() {
        let mut c = Case::five();
        c.sync = Some(sync("node2,node3,node4", 3));
        c.voters = eligible(&["node2", "node3", "node4"]);
        c.readings = vec![
            pos("node2", 2, 500),
            pos("node3", 2, 400),
            pos("node4", 2, 450),
        ];
        let a = c.run();
        match &a.outcome {
            Outcome::NeedsAcceptance {
                candidate,
                lost,
                risk,
                digest,
            } => {
                assert_eq!(candidate, "node2");
                assert_eq!(lost, &["node3"], "the lowest voter, online");
                assert!(risk.named_online);
                assert!(risk.possible, "q=|V|: k >= |V|-q");
                assert!(risk.pre_timeline_safe && !risk.timeline_loss);
                assert!(is_digest(digest));
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(a.outcome.exit_code(), 4);
        assert!(a.voters[1].status.contains("given up while online"));
        assert!(!a.voters[2].status.contains("given up"));
        // The digest covers the class: the same names with named_online off
        // (voters really lost) is another digest.
        let (d, snap) = match &a.outcome {
            Outcome::NeedsAcceptance { digest, risk, .. } => (
                digest.clone(),
                (
                    risk.clone(),
                    RecoverySnapshot::of(c.sync.as_ref().unwrap(), &c.timeline),
                ),
            ),
            _ => unreachable!(),
        };
        let off = Risk {
            named_online: false,
            ..snap.0
        };
        assert_ne!(d, digest(&snap.1, "node2", &["node3".into()], &off));
        // Ties: the lower position first, then the greater name (the mirror
        // of the candidate order).
        c.readings = vec![
            pos("node2", 2, 500),
            pos("node3", 2, 400),
            pos("node4", 2, 400),
        ];
        match c.run().outcome {
            Outcome::NeedsAcceptance { lost, .. } => assert_eq!(lost, &["node4"]),
            other => panic!("{other:?}"),
        }
        // Two voters (the candidate and one more), q = 2.
        let mut c = Case::five();
        c.sync = Some(sync("node2,node3", 2));
        c.voters = eligible(&["node2", "node3"]);
        c.readings = vec![pos("node2", 2, 500), pos("node3", 2, 400)];
        match c.run().outcome {
            Outcome::NeedsAcceptance { lost, risk, .. } => {
                assert_eq!(lost, &["node3"]);
                assert!(risk.named_online);
            }
            other => panic!("{other:?}"),
        }
    }

    /// The same state, but a voter is ahead of the best campaigner: nothing
    /// to accept, whoever is named.
    #[test]
    fn stuck_q_with_a_voter_ahead_that_may_not_campaign_blocks() {
        let mut c = Case::five();
        c.sync = Some(sync("node2,node3,node4", 3));
        c.voters = eligible(&["node2", "node3", "node4"]);
        c.voters.get_mut("node4").unwrap().campaign = Campaign::No("nofailover".into());
        c.readings = vec![
            pos("node2", 2, 500),
            pos("node3", 2, 400),
            pos("node4", 2, 600),
        ];
        assert!(
            blocked(&c.run()).contains("node4 is ahead"),
            "{:?}",
            c.run()
        );
        // On a newer timeline too.
        c.readings[2] = pos("node4", 3, 1);
        assert!(
            blocked(&c.run()).contains("node4 is ahead"),
            "{:?}",
            c.run()
        );
    }

    /// `q < |V|` with every voter answering and the vote failing can only be
    /// a voter ahead; a lone voter has nobody to name.
    #[test]
    fn online_voter_is_named_only_when_q_can_never_be_met() {
        let mut c = Case::five();
        c.sync = Some(sync("node2,node3,node4", 2));
        c.voters = eligible(&["node2", "node3", "node4"]);
        c.voters.get_mut("node4").unwrap().campaign = Campaign::No("nofailover".into());
        c.readings = vec![
            pos("node2", 2, 500),
            pos("node3", 2, 400),
            pos("node4", 2, 600),
        ];
        assert!(blocked(&c.run()).contains("node4 is ahead"));
        c.sync = Some(sync("node2", 1));
        c.voters = eligible(&["node2"]);
        c.readings = vec![pos("node2", 2, 500)];
        assert!(blocked(&c.run()).contains("no other voter to name"));
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
            named_online: false,
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

    /// The PG 18.7 state of evidence/pr-b2-accepted-loss-online-*: a
    /// switchover node2 -> node1, node1 killed while /sync was V={node2..5},
    /// q=4. node2 (the demoted old primary) answers on timeline 2 at
    /// 117671168; node3..5 answer on timeline 1 at 0/70383C0 = 117670848,
    /// exactly where node2's 00000002.history says timeline 2 began.
    fn online_stuck_q() -> Case {
        let s = 117_670_848;
        Case {
            lock: None,
            sync: Some(SyncState {
                leader: "node1".into(),
                ..sync("node2,node3,node4,node5", 4)
            }),
            timeline: tl(2, s),
            readings: vec![
                pos("node2", 2, 117_671_168),
                pos("node3", 1, s),
                pos("node4", 1, s),
                pos("node5", 1, s),
            ],
            voters: eligible(&["node2", "node3", "node4", "node5"]),
            ancestors: BTreeMap::from([(
                "node2".to_string(),
                vec![AncestorEnd {
                    timeline: 1,
                    switchpoint: s,
                }],
            )]),
            blocked: None,
        }
    }

    #[test]
    fn online_stuck_q_with_prefix_voters_names_one_online_voter() {
        let a = online_stuck_q().run();
        match &a.outcome {
            Outcome::NeedsAcceptance {
                candidate,
                lost,
                risk,
                ..
            } => {
                assert_eq!(candidate, "node2");
                // Equal positions: the tie rule takes the greater name.
                assert_eq!(lost, &["node5"]);
                assert!(risk.named_online);
            }
            other => panic!("{other:?}"),
        }
        let status: Vec<&str> = a.voters.iter().map(|v| v.status.as_str()).collect();
        assert!(
            status[1].starts_with("counted: on timeline 1"),
            "{status:?}"
        );
        assert!(
            status[3].contains("named as given up while online"),
            "{status:?}"
        );
        // Without the history file (unreadable, D6 override) they are not
        // counted and the old answer stands: all three named.
        let mut c = online_stuck_q();
        c.ancestors.clear();
        match c.run().outcome {
            Outcome::NeedsAcceptance { lost, risk, .. } => {
                assert_eq!(lost, ["node3", "node4", "node5"]);
                assert!(!risk.named_online);
            }
            other => panic!("{other:?}"),
        }
        // One byte past the switchpoint: forked off, not counted.
        let mut c = online_stuck_q();
        c.readings[1].lsn += 1;
        match c.run().outcome {
            Outcome::NeedsAcceptance { lost, risk, .. } => {
                assert_eq!(lost, ["node3"]);
                assert!(!risk.named_online);
            }
            other => panic!("{other:?}"),
        }
    }

    /// Mutation: an assessment that ignores the prefix rule is caught.
    #[test]
    fn assess_ignoring_the_prefix_rule_is_caught() {
        fn never(_: &WalPosition, _: &[AncestorEnd]) -> bool {
            false
        }
        let a = online_stuck_q().run_with(never);
        assert!(
            !matches!(&a.outcome, Outcome::NeedsAcceptance { lost, risk, .. } if lost == &["node5"] && risk.named_online),
            "the mutant gives the same answer: {:?}",
            a.outcome
        );
    }

    /// The explain and the vote count the same voters: over the prefix
    /// cases, a voter is reported "counted" exactly when `standing` (the
    /// vote's rule) counts it, it is in `lost` exactly when it is not, and a
    /// grant on that `lost` passes the recovery vote with the same history.
    #[test]
    fn assess_and_vote_agree_on_which_voters_count() {
        let s = 1000u64;
        let anc = vec![AncestorEnd {
            timeline: 1,
            switchpoint: s,
        }];
        let cases: Vec<(u64, u64)> =
            vec![(1, s - 1), (1, s), (1, s + 1), (0, s), (2, 900), (2, 1200)];
        for (tl_v, lsn_v) in cases {
            for history in [true, false] {
                let mut c = Case {
                    lock: None,
                    sync: Some(sync("node2,node3,node4", 2)),
                    timeline: tl(2, s),
                    readings: vec![
                        pos("node2", 2, 1200),
                        pos("node3", 2, 1100),
                        pos("node4", tl_v, lsn_v),
                    ],
                    voters: eligible(&["node2", "node3", "node4"]),
                    ancestors: BTreeMap::new(),
                    blocked: None,
                };
                if history {
                    c.ancestors.insert("node2".into(), anc.clone());
                }
                let used: &[AncestorEnd] = if history { &anc } else { &[] };
                let voters: Names = ["node2", "node3", "node4"]
                    .iter()
                    .map(|v| v.to_string())
                    .collect();
                let counts = standing(
                    &c.readings[0],
                    &voters,
                    &c.readings[2],
                    used,
                    holds_a_prefix,
                ) == Standing::Counts;
                let a = c.run();
                let label = format!("node4 tl {tl_v} lsn {lsn_v} history {history}");
                let status = &a.voters[2].status;
                assert_eq!(status.starts_with("counted"), counts, "{label}: {status}");
                let vote = decide(
                    &c.readings[0],
                    &voters,
                    2,
                    &c.readings,
                    used,
                    holds_a_prefix,
                );
                match (&a.outcome, &vote) {
                    (Outcome::WillHeal { .. }, VoteDecision::Granted { .. }) => {
                        assert!(counts, "{label}")
                    }
                    (Outcome::NeedsAcceptance { lost, .. }, VoteDecision::Refused(_)) => {
                        assert_eq!(lost.contains(&"node4".to_string()), !counts, "{label}");
                        let lost: Names = lost.iter().cloned().collect();
                        assert!(
                            matches!(
                                recovery_decide(
                                    &c.readings[0],
                                    &voters,
                                    &lost,
                                    &c.readings,
                                    used,
                                    holds_a_prefix
                                ),
                                VoteDecision::Granted { .. }
                            ),
                            "{label}: the grant must pass the recovery vote"
                        );
                    }
                    (o, v) => panic!("{label}: explain {o:?} vs vote {v:?}"),
                }
            }
        }
    }
}
