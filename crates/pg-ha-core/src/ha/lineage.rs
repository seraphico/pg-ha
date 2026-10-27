//! D6: which timeline is the latest, and who may campaign on it.
//!
//! Invariants (docs/invariants.md 1a "D6"):
//! - I1: each timeline number pg-ha manages names one history.
//! - I2: `/timeline` records the identity (number + begin LSN) of the latest
//!   confirmed timeline, plus a `pending` promote while one may be creating a
//!   newer timeline. Every timeline that exists is at or below the record, or
//!   is the one that pending promote created.
//! - I3: only a node on the latest timeline may campaign, marked or not.
//!
//! I1 follows: a node on the latest timeline L has L as its newest history
//! file, so PostgreSQL names its next timeline L + 1, which no node has while
//! no promote is pending.
//!
//! Every function here is pure and uses only what members published, so all
//! nodes reach the same verdict about the same member (A9).

use crate::cluster::{
    ClusterTimeline, HistoryEvidence, Member, MemberState, PendingPromote, TimelineId,
    TimelineRecord,
};

/// How the latest timeline was established.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LatestVia {
    /// The record, no promote pending.
    Record,
    /// A pending promote by `node` created it (members publish it).
    PendingCreated { node: String },
    /// The pending promote by `node` cannot have created a timeline.
    PendingVoid { node: String },
    /// No current-format record; every node published its newest history.
    Inferred,
}

/// Why the latest timeline cannot be established.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UnknownReason {
    /// Two members publish different histories for the newest timeline.
    Conflict { a: TimelineId, b: TimelineId },
    /// No current-format record and not every node published evidence.
    Incomplete {
        reported: usize,
        voters: Option<usize>,
    },
    /// No current-format record and members are in different generations.
    MixedGenerations,
}

/// The latest timeline (I2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Latest {
    Known {
        id: TimelineId,
        generation: u64,
        via: LatestVia,
    },
    /// `node`'s promote may have created a newer timeline and nobody
    /// publishes it: only `node` may campaign.
    PendingUnresolved { pending: PendingPromote },
    /// Nobody may campaign until this changes or an operator acts.
    Unknown(UnknownReason),
}

impl std::fmt::Display for TimelineId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "timeline {} (begins at {:X}/{:X})",
            self.timeline,
            self.begin >> 32,
            self.begin & 0xFFFF_FFFF
        )
    }
}

impl std::fmt::Display for UnknownReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Conflict { a, b } => write!(
                f,
                "members publish two different histories for the newest timeline: {a} and {b}"
            ),
            Self::Incomplete { reported, voters } => match voters {
                Some(v) => write!(
                    f,
                    "no /timeline record and only {reported} of {v} nodes published their timeline history"
                ),
                None => write!(f, "no /timeline record and the number of nodes is unknown"),
            },
            Self::MixedGenerations => write!(
                f,
                "no /timeline record and members are in different history generations"
            ),
        }
    }
}

fn history_of(m: &Member) -> Option<&HistoryEvidence> {
    m.lineage.as_ref().and_then(|l| l.history.as_ref())
}

/// The latest timeline from the record and what members published.
pub(crate) fn resolve_latest(
    timeline: &ClusterTimeline,
    members: &[Member],
    voters: Option<usize>,
) -> Latest {
    match timeline {
        ClusterTimeline::Record { record, .. } => match &record.pending {
            None => Latest::Known {
                id: record.id(),
                generation: record.generation,
                via: LatestVia::Record,
            },
            Some(p) => resolve_pending(record, p, members),
        },
        ClusterTimeline::Absent
        | ClusterTimeline::Legacy { .. }
        | ClusterTimeline::Unreadable { .. } => infer_latest(members, voters),
    }
}

fn resolve_pending(record: &TimelineRecord, p: &PendingPromote, members: &[Member]) -> Latest {
    // Created: a member of the promote's generation knows a timeline above
    // its base. Only the lock holder that wrote `p` can have created one.
    let mut created: Option<TimelineId> = None;
    for m in members {
        let Some(l) = &m.lineage else { continue };
        if l.generation != p.generation {
            continue;
        }
        let Some(h) = &l.history else { continue };
        if h.timeline <= p.base_timeline {
            continue;
        }
        match created {
            None => created = Some(h.id()),
            Some(c) if c == h.id() => {}
            Some(c) => return Latest::Unknown(UnknownReason::Conflict { a: c, b: h.id() }),
        }
    }
    if let Some(id) = created {
        return Latest::Known {
            id,
            generation: p.generation,
            via: LatestVia::PendingCreated {
                node: p.node.clone(),
            },
        };
    }

    // Void: the promoter has no newer history and its PostgreSQL is stopped
    // or has restarted since (postmaster removes promote signal files at
    // startup), so the promote cannot still complete.
    let void = members.iter().find(|m| m.name == p.node).is_some_and(|m| {
        let Some(l) = &m.lineage else { return false };
        let no_newer = l.history.is_some_and(|h| h.timeline <= p.base_timeline);
        let stopped = matches!(m.state, MemberState::Stopped | MemberState::Crashed);
        let restarted = l.postmaster_start.is_some() && l.postmaster_start != p.postmaster_start;
        no_newer && (stopped || restarted)
    });
    if void {
        return Latest::Known {
            id: record.id(),
            generation: record.generation,
            via: LatestVia::PendingVoid {
                node: p.node.clone(),
            },
        };
    }
    Latest::PendingUnresolved { pending: p.clone() }
}

/// No current-format record (upgrade, or a legacy number that may exceed
/// every real timeline). Safe only when every node reported: then the
/// highest history any node knows is the highest that exists.
fn infer_latest(members: &[Member], voters: Option<usize>) -> Latest {
    let histories: Vec<(&Member, &HistoryEvidence)> = members
        .iter()
        .filter_map(|m| history_of(m).map(|h| (m, h)))
        .collect();
    let complete = voters.is_some_and(|v| histories.len() >= v && histories.len() == members.len());
    if !complete {
        return Latest::Unknown(UnknownReason::Incomplete {
            reported: histories.len(),
            voters,
        });
    }
    let generation = histories[0].0.lineage.as_ref().map_or(0, |l| l.generation);
    if histories
        .iter()
        .any(|(m, _)| m.lineage.as_ref().map_or(0, |l| l.generation) != generation)
    {
        return Latest::Unknown(UnknownReason::MixedGenerations);
    }
    let newest = histories.iter().map(|(_, h)| h.timeline).max().unwrap_or(1);
    let mut id: Option<TimelineId> = None;
    for (_, h) in histories.iter().filter(|(_, h)| h.timeline == newest) {
        match id {
            None => id = Some(h.id()),
            Some(i) if i == h.id() => {}
            Some(i) => return Latest::Unknown(UnknownReason::Conflict { a: i, b: h.id() }),
        }
    }
    match id {
        Some(id) => Latest::Known {
            id,
            generation,
            via: LatestVia::Inferred,
        },
        None => Latest::Unknown(UnknownReason::Incomplete {
            reported: histories.len(),
            voters,
        }),
    }
}

/// Why a member may not campaign.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Ineligible {
    /// Older version, or nothing published yet.
    NoEvidence,
    /// Data of another history generation: must be cloned again.
    Generation {
        own: u64,
        cluster: u64,
    },
    /// Not shown to be on the latest timeline.
    Behind {
        own: Option<u64>,
        latest: TimelineId,
    },
    /// Same timeline number, different history (created outside I1).
    DifferentHistory {
        own: TimelineId,
        latest: TimelineId,
    },
    /// Knows a timeline above the latest that no record covers.
    UnrecordedNewer {
        own: TimelineId,
        latest: TimelineId,
    },
    /// A promote by another node is unresolved.
    Pending {
        node: String,
    },
    LatestUnknown(UnknownReason),
}

impl std::fmt::Display for Ineligible {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoEvidence => write!(f, "no timeline evidence published"),
            Self::Generation { own, cluster } => write!(
                f,
                "its data is history generation {own}, the cluster is in generation {cluster}; it must be reinitialized"
            ),
            Self::Behind { own, latest } => match own {
                Some(t) => write!(f, "timeline {t} is not shown to be the latest {latest}"),
                None => write!(f, "timeline unknown, the latest is {latest}"),
            },
            Self::DifferentHistory { own, latest } => write!(
                f,
                "its {own} is a different history than the cluster's {latest}"
            ),
            Self::UnrecordedNewer { own, latest } => {
                write!(f, "it knows {own}, above the recorded latest {latest}")
            }
            Self::Pending { node } => write!(
                f,
                "a promote by {node} may have created a newer timeline and is not resolved"
            ),
            Self::LatestUnknown(r) => write!(f, "the latest timeline is unknown: {r}"),
        }
    }
}

/// I3: may `m` campaign? Judged only from what `m` published.
pub(crate) fn eligibility(m: &Member, latest: &Latest) -> Result<(), Ineligible> {
    let (id, generation) = match latest {
        Latest::Known { id, generation, .. } => (*id, *generation),
        Latest::PendingUnresolved { pending } => {
            return if m.name == pending.node {
                Ok(())
            } else {
                Err(Ineligible::Pending {
                    node: pending.node.clone(),
                })
            };
        }
        Latest::Unknown(r) => return Err(Ineligible::LatestUnknown(r.clone())),
    };
    let Some(l) = &m.lineage else {
        return Err(Ineligible::NoEvidence);
    };
    if l.generation != generation {
        return Err(Ineligible::Generation {
            own: l.generation,
            cluster: generation,
        });
    }
    if let Some(h) = &l.history
        && h.timeline > id.timeline
    {
        return Err(Ineligible::UnrecordedNewer {
            own: h.id(),
            latest: id,
        });
    }
    // The published timeline (exact, or a lower bound: a lower bound equal
    // to L means the true one is >= L, and nothing above L exists) ...
    let mut on_latest = false;
    if m.timeline == Some(id.timeline) {
        match l.timeline_begin {
            Some(b) if b == id.begin => on_latest = true,
            Some(b) => {
                return Err(Ineligible::DifferentHistory {
                    own: TimelineId {
                        timeline: id.timeline,
                        begin: b,
                    },
                    latest: id,
                });
            }
            None => {}
        }
    }
    // ... or WAL on L's newest history (a primary fenced before its first
    // checkpoint on L still has a checkpoint timeline below L).
    if let Some(h) = &l.history
        && h.timeline == id.timeline
        && h.wal
    {
        if h.begin != id.begin {
            return Err(Ineligible::DifferentHistory {
                own: h.id(),
                latest: id,
            });
        }
        on_latest = true;
    }
    if on_latest {
        Ok(())
    } else if m.timeline == Some(id.timeline) {
        Err(Ineligible::NoEvidence)
    } else {
        Err(Ineligible::Behind {
            own: m.timeline,
            latest: id,
        })
    }
}

/// What the lock holder records before `pg_ctl promote`.
pub(crate) fn plan_promote(
    me: &str,
    latest: &Latest,
    record_generation: u64,
    local: HistoryEvidence,
    accept_loss: bool,
    postmaster_start: Option<String>,
) -> Result<PendingPromote, String> {
    let pending = |base: TimelineId, generation: u64| PendingPromote {
        node: me.to_string(),
        base_timeline: base.timeline,
        base_begin: base.begin,
        generation,
        postmaster_start: postmaster_start.clone(),
    };
    if accept_loss {
        // Override: whatever this node has becomes the new history, in a new
        // generation, so older data cannot be rewound onto it.
        return Ok(pending(local.id(), record_generation + 1));
    }
    match latest {
        Latest::Known { id, generation, .. } if local.id() == *id => Ok(pending(*id, *generation)),
        Latest::Known { id, .. } => Err(format!(
            "this node's newest history is {}, not the latest {id}; promoting would reuse a timeline number",
            local.id()
        )),
        Latest::PendingUnresolved { pending: p } if p.node == me => {
            if local.timeline < p.base_timeline {
                return Err(format!(
                    "this node's newest history {} is below the base of its own pending promote (timeline {})",
                    local.id(),
                    p.base_timeline
                ));
            }
            Ok(pending(local.id(), p.generation))
        }
        Latest::PendingUnresolved { pending: p } => {
            Err(format!("a promote by {} is not resolved", p.node))
        }
        Latest::Unknown(r) => Err(format!("the latest timeline is unknown: {r}")),
    }
}

/// The record to write after a confirmed promote.
pub(crate) fn confirmed_record(
    me: &str,
    pending: &PendingPromote,
    new: TimelineId,
) -> Result<TimelineRecord, String> {
    if new.timeline <= pending.base_timeline {
        return Err(format!(
            "PostgreSQL reports {new} after promote, not above the base timeline {}",
            pending.base_timeline
        ));
    }
    Ok(TimelineRecord {
        timeline: new.timeline,
        begin: new.begin,
        primary: me.to_string(),
        generation: pending.generation,
        pending: None,
    })
}

/// What a lock-holding primary does with `/timeline` each cycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HoldAction {
    UpToDate,
    Write(TimelineRecord),
    /// Do not write; log this as an error.
    Conflict(String),
}

pub(crate) fn hold_action(
    me: &str,
    timeline: &ClusterTimeline,
    members: &[Member],
    voters: Option<usize>,
    own: TimelineId,
    own_generation: u64,
) -> HoldAction {
    let write = || {
        HoldAction::Write(TimelineRecord {
            timeline: own.timeline,
            begin: own.begin,
            primary: me.to_string(),
            generation: own_generation,
            pending: None,
        })
    };
    let Some(r) = timeline.record() else {
        // Absent / legacy / unreadable: the lock-holding primary is the
        // current writer, so its timeline is the latest.
        return write();
    };
    // A generation changes only through this node's own override promote,
    // and only once its data directory is in the new generation.
    if let Some(p) = &r.pending
        && p.node == me
        && p.generation != own_generation
    {
        return HoldAction::Conflict(format!(
            "this node's pending promote is in history generation {} but its data directory in {own_generation}",
            p.generation
        ));
    }
    let own_override = r
        .pending
        .as_ref()
        .is_some_and(|p| p.node == me && p.generation == own_generation);
    if r.generation != own_generation && !own_override {
        return HoldAction::Conflict(format!(
            "/timeline is in history generation {} and this primary's data in {own_generation}",
            r.generation
        ));
    }
    if let Some(p) = &r.pending
        && p.node != me
        && let Latest::PendingUnresolved { .. } = resolve_latest(timeline, members, voters)
    {
        return HoldAction::Conflict(format!(
            "a promote by {} is not resolved; not clearing it",
            p.node
        ));
    }
    if r.pending.is_none() && r.generation == own_generation {
        if r.id() == own {
            return HoldAction::UpToDate;
        }
        if r.timeline > own.timeline {
            return HoldAction::Conflict(format!(
                "/timeline records {}, above this primary's {own}; never lowered",
                r.id()
            ));
        }
        if r.timeline == own.timeline {
            return HoldAction::Conflict(format!(
                "/timeline records {}, a different history than this primary's {own}",
                r.id()
            ));
        }
    }
    write()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{Lineage, MemberRole};

    const B6: u64 = 0x600;
    const B7: u64 = 0x700;

    fn id(timeline: u64, begin: u64) -> TimelineId {
        TimelineId { timeline, begin }
    }

    fn record(timeline: u64, begin: u64, pending: Option<PendingPromote>) -> ClusterTimeline {
        ClusterTimeline::Record {
            record: TimelineRecord {
                timeline,
                begin,
                primary: "p".into(),
                generation: 0,
                pending,
            },
            version: 1,
        }
    }

    fn pending(node: &str, base: TimelineId, start: &str) -> PendingPromote {
        PendingPromote {
            node: node.into(),
            base_timeline: base.timeline,
            base_begin: base.begin,
            generation: 0,
            postmaster_start: Some(start.into()),
        }
    }

    /// Member on `tl` (published timeline, begin) whose newest history is
    /// `hist` (with WAL).
    fn member(name: &str, tl: Option<u64>, tl_begin: Option<u64>, hist: TimelineId) -> Member {
        Member {
            name: name.into(),
            conn_url: String::new(),
            api_url: String::new(),
            state: MemberState::Running,
            role: MemberRole::Replica,
            wal_position: Some(100),
            timeline: tl,
            tags: Default::default(),
            version: None,
            rewind_check: false,
            lineage: Some(Lineage {
                timeline_begin: tl_begin,
                history: Some(HistoryEvidence {
                    timeline: hist.timeline,
                    begin: hist.begin,
                    wal: true,
                }),
                postmaster_start: Some("run1".into()),
                generation: 0,
            }),
        }
    }

    fn on(name: &str, t: TimelineId) -> Member {
        member(name, Some(t.timeline), Some(t.begin), t)
    }

    fn known(t: TimelineId) -> Latest {
        Latest::Known {
            id: t,
            generation: 0,
            via: LatestVia::Record,
        }
    }

    // ── resolve_latest ──

    #[test]
    fn record_without_pending_is_the_latest() {
        let l = resolve_latest(&record(6, B6, None), &[], None);
        assert_eq!(l, known(id(6, B6)));
    }

    #[test]
    fn pending_promote_seen_by_members_establishes_the_new_timeline() {
        let p = pending("r", id(6, B6), "run1");
        let members = [on("r", id(7, B7)), on("f", id(7, B7)), on("x", id(6, B6))];
        let l = resolve_latest(&record(6, B6, Some(p)), &members, Some(3));
        assert_eq!(
            l,
            Latest::Known {
                id: id(7, B7),
                generation: 0,
                via: LatestVia::PendingCreated { node: "r".into() }
            }
        );
    }

    #[test]
    fn history_file_alone_is_evidence_that_the_timeline_was_created() {
        // PostgreSQL writes the history file before switching: the promoter
        // restarted as a standby still has it.
        let p = pending("r", id(6, B6), "run1");
        let r = member("r", Some(6), Some(B6), id(7, B7));
        let l = resolve_latest(&record(6, B6, Some(p)), &[r], None);
        assert!(matches!(l, Latest::Known { id: i, .. } if i == id(7, B7)));
    }

    #[test]
    fn two_histories_above_the_base_conflict() {
        let p = pending("r", id(6, B6), "run1");
        let members = [on("r", id(7, B7)), on("f", id(7, 0x999))];
        let l = resolve_latest(&record(6, B6, Some(p)), &members, None);
        assert!(matches!(l, Latest::Unknown(UnknownReason::Conflict { .. })));
    }

    #[test]
    fn pending_is_void_after_the_promoter_restarted_without_a_new_timeline() {
        let p = pending("r", id(6, B6), "run0");
        let r = on("r", id(6, B6)); // postmaster_start run1 != run0
        let l = resolve_latest(&record(6, B6, Some(p)), &[r], None);
        assert_eq!(
            l,
            Latest::Known {
                id: id(6, B6),
                generation: 0,
                via: LatestVia::PendingVoid { node: "r".into() }
            }
        );
    }

    #[test]
    fn pending_is_void_while_the_promoter_postgres_is_stopped() {
        let p = pending("r", id(6, B6), "run1");
        let mut r = on("r", id(6, B6));
        r.state = MemberState::Stopped;
        r.timeline = None;
        let l = resolve_latest(&record(6, B6, Some(p)), &[r], None);
        assert!(matches!(
            l,
            Latest::Known {
                via: LatestVia::PendingVoid { .. },
                ..
            }
        ));
    }

    #[test]
    fn pending_stays_unresolved_while_a_promote_may_still_complete() {
        let p = pending("r", id(6, B6), "run1");
        // Same server run, still running: the promote may be in flight.
        let r = on("r", id(6, B6));
        let l = resolve_latest(
            &record(6, B6, Some(p.clone())),
            std::slice::from_ref(&r),
            None,
        );
        assert_eq!(l, Latest::PendingUnresolved { pending: p.clone() });

        // State Unknown (a query failed) is not "stopped".
        let mut unknown = r;
        unknown.state = MemberState::Unknown;
        let l = resolve_latest(&record(6, B6, Some(p.clone())), &[unknown], None);
        assert!(matches!(l, Latest::PendingUnresolved { .. }));

        // Promoter gone: unresolved.
        let l = resolve_latest(&record(6, B6, Some(p)), &[on("x", id(6, B6))], None);
        assert!(matches!(l, Latest::PendingUnresolved { .. }));
    }

    #[test]
    fn override_pending_only_counts_members_of_the_new_generation() {
        // Override by x (behind, on 4) in generation 1; old-generation
        // members on 6 are not evidence that x created anything.
        let mut p = pending("x", id(4, 0x400), "run1");
        p.generation = 1;
        let old = on("old", id(6, B6));
        let x = on("x", id(4, 0x400));
        let l = resolve_latest(&record(6, B6, Some(p.clone())), &[old.clone(), x], None);
        assert!(matches!(l, Latest::PendingUnresolved { .. }));

        let mut x5 = on("x", id(5, 0x450));
        x5.lineage.as_mut().unwrap().generation = 1;
        let l = resolve_latest(&record(6, B6, Some(p)), &[old, x5], None);
        assert_eq!(
            l,
            Latest::Known {
                id: id(5, 0x450),
                generation: 1,
                via: LatestVia::PendingCreated { node: "x".into() }
            }
        );
    }

    #[test]
    fn legacy_key_is_inferred_only_when_every_node_reported() {
        // The 2026-10-26 docker cluster: key 17, histories 14, 14, 13.
        let legacy = ClusterTimeline::Legacy {
            value: 17,
            version: 9,
        };
        let t14 = id(14, 0x404E4B0);
        let members = [
            on("node1", t14),
            on("node3", t14),
            on("node2", id(13, 0x404E398)),
        ];
        assert_eq!(
            resolve_latest(&legacy, &members, Some(3)),
            Latest::Known {
                id: t14,
                generation: 0,
                via: LatestVia::Inferred
            }
        );
        assert!(matches!(
            resolve_latest(&legacy, &members[..2], Some(3)),
            Latest::Unknown(UnknownReason::Incomplete { reported: 2, .. })
        ));
        assert!(matches!(
            resolve_latest(&legacy, &members, None),
            Latest::Unknown(UnknownReason::Incomplete { .. })
        ));
        let mut old = members.to_vec();
        old[2].lineage = None; // older version
        assert!(matches!(
            resolve_latest(&ClusterTimeline::Absent, &old, Some(3)),
            Latest::Unknown(UnknownReason::Incomplete { .. })
        ));
    }

    #[test]
    fn legacy_conflicting_histories_or_generations_are_unknown() {
        let members = [on("a", id(14, 1)), on("b", id(14, 2))];
        assert!(matches!(
            resolve_latest(&ClusterTimeline::Absent, &members, Some(2)),
            Latest::Unknown(UnknownReason::Conflict { .. })
        ));
        let mut members = [on("a", id(14, 1)), on("b", id(14, 1))];
        members[1].lineage.as_mut().unwrap().generation = 1;
        assert!(matches!(
            resolve_latest(&ClusterTimeline::Absent, &members, Some(2)),
            Latest::Unknown(UnknownReason::MixedGenerations)
        ));
    }

    // ── eligibility ──

    #[test]
    fn member_on_the_latest_timeline_is_eligible_marked_or_not() {
        let l = known(id(6, B6));
        assert_eq!(eligibility(&on("a", id(6, B6)), &l), Ok(()));
        let mut marked = on("a", id(6, B6));
        marked.rewind_check = true;
        assert_eq!(eligibility(&marked, &l), Ok(()));
    }

    #[test]
    fn behind_member_is_not_eligible_even_without_a_marker() {
        let l = known(id(6, B6));
        assert_eq!(
            eligibility(&on("a", id(5, 0x500)), &l),
            Err(Ineligible::Behind {
                own: Some(5),
                latest: id(6, B6)
            })
        );
    }

    #[test]
    fn fetched_history_without_wal_does_not_make_a_member_eligible() {
        // A diverged node fetched 6.history while trying to follow.
        let mut m = member("d", Some(5), Some(0x500), id(6, B6));
        m.lineage.as_mut().unwrap().history.as_mut().unwrap().wal = false;
        assert!(matches!(
            eligibility(&m, &known(id(6, B6))),
            Err(Ineligible::Behind { .. })
        ));
    }

    #[test]
    fn wal_on_the_latest_history_counts_with_a_lagging_checkpoint_timeline() {
        // Primary fenced before its first checkpoint on 6.
        let m = member("p", Some(5), Some(0x500), id(6, B6));
        assert_eq!(eligibility(&m, &known(id(6, B6))), Ok(()));
    }

    #[test]
    fn same_number_with_another_begin_is_a_different_history() {
        let l = known(id(11, 0x1100));
        let m = on("a", id(11, 0x2200));
        assert!(matches!(
            eligibility(&m, &l),
            Err(Ineligible::DifferentHistory { .. })
        ));
    }

    #[test]
    fn unrecorded_newer_history_is_not_eligible() {
        let m = member("a", Some(6), Some(B6), id(7, B7));
        assert!(matches!(
            eligibility(&m, &known(id(6, B6))),
            Err(Ineligible::UnrecordedNewer { .. })
        ));
    }

    #[test]
    fn missing_begin_or_lineage_is_no_evidence() {
        let l = known(id(6, B6));
        let mut m = on("a", id(6, B6));
        m.lineage.as_mut().unwrap().timeline_begin = None;
        m.lineage.as_mut().unwrap().history.as_mut().unwrap().wal = false;
        assert_eq!(eligibility(&m, &l), Err(Ineligible::NoEvidence));
        m.lineage = None;
        assert_eq!(eligibility(&m, &l), Err(Ineligible::NoEvidence));
    }

    #[test]
    fn older_generation_is_never_eligible() {
        let l = Latest::Known {
            id: id(6, B6),
            generation: 1,
            via: LatestVia::Record,
        };
        assert_eq!(
            eligibility(&on("a", id(6, B6)), &l),
            Err(Ineligible::Generation { own: 0, cluster: 1 })
        );
    }

    #[test]
    fn unresolved_pending_admits_only_the_promoter() {
        let l = Latest::PendingUnresolved {
            pending: pending("r", id(6, B6), "run1"),
        };
        assert_eq!(eligibility(&on("r", id(6, B6)), &l), Ok(()));
        assert!(matches!(
            eligibility(&on("x", id(6, B6)), &l),
            Err(Ineligible::Pending { .. })
        ));
        assert!(matches!(
            eligibility(
                &on("x", id(6, B6)),
                &Latest::Unknown(UnknownReason::MixedGenerations)
            ),
            Err(Ineligible::LatestUnknown(_))
        ));
    }

    // ── promote planning, confirmation, Hold ──

    fn local(t: TimelineId) -> HistoryEvidence {
        HistoryEvidence {
            timeline: t.timeline,
            begin: t.begin,
            wal: true,
        }
    }

    #[test]
    fn promote_from_the_latest_records_it_as_the_base() {
        let p = plan_promote("r", &known(id(6, B6)), 0, local(id(6, B6)), false, None).unwrap();
        assert_eq!(
            (p.node.as_str(), p.base(), p.generation),
            ("r", id(6, B6), 0)
        );
    }

    #[test]
    fn promote_with_another_newest_history_is_refused() {
        // Would reuse a timeline number (I1).
        for l in [id(5, 0x500), id(7, B7), id(6, 0x999)] {
            assert!(plan_promote("r", &known(id(6, B6)), 0, local(l), false, None).is_err());
        }
    }

    #[test]
    fn promoter_retries_its_own_unresolved_pending() {
        let p = pending("r", id(6, B6), "run1");
        let l = Latest::PendingUnresolved { pending: p };
        let again = plan_promote("r", &l, 0, local(id(7, B7)), false, None).unwrap();
        assert_eq!(again.base(), id(7, B7));
        assert!(plan_promote("x", &l, 0, local(id(6, B6)), false, None).is_err());
        assert!(
            plan_promote(
                "r",
                &Latest::Unknown(UnknownReason::MixedGenerations),
                0,
                local(id(6, B6)),
                false,
                None
            )
            .is_err()
        );
    }

    #[test]
    fn override_promote_starts_a_new_generation_from_local_history() {
        let p = plan_promote(
            "x",
            &Latest::Unknown(UnknownReason::MixedGenerations),
            2,
            local(id(4, 0x400)),
            true,
            Some("run1".into()),
        )
        .unwrap();
        assert_eq!((p.base(), p.generation), (id(4, 0x400), 3));
    }

    #[test]
    fn confirmed_record_requires_a_timeline_above_the_base() {
        let p = pending("r", id(6, B6), "run1");
        let r = confirmed_record("r", &p, id(7, B7)).unwrap();
        assert_eq!(
            (r.id(), r.pending.is_none(), r.primary.as_str()),
            (id(7, B7), true, "r")
        );
        assert!(confirmed_record("r", &p, id(6, B6)).is_err());
    }

    #[test]
    fn hold_writes_missing_legacy_or_lower_records() {
        for tl in [
            ClusterTimeline::Absent,
            ClusterTimeline::Legacy {
                value: 17,
                version: 1,
            },
            ClusterTimeline::Unreadable { version: 1 },
            record(5, 0x500, None),
        ] {
            assert!(matches!(
                hold_action("r", &tl, &[], None, id(6, B6), 0),
                HoldAction::Write(_)
            ));
        }
        assert_eq!(
            hold_action("r", &record(6, B6, None), &[], None, id(6, B6), 0),
            HoldAction::UpToDate
        );
    }

    #[test]
    fn hold_clears_its_own_pending_and_a_resolved_one() {
        let own = record(6, B6, Some(pending("r", id(6, B6), "run1")));
        assert!(matches!(
            hold_action("r", &own, &[], None, id(7, B7), 0),
            HoldAction::Write(_)
        ));
        // Another node's void pending.
        let mut x = on("x", id(6, B6));
        x.state = MemberState::Stopped;
        let other = record(6, B6, Some(pending("x", id(6, B6), "run1")));
        assert!(matches!(
            hold_action("r", &other, &[x], None, id(7, B7), 0),
            HoldAction::Write(_)
        ));
    }

    #[test]
    fn hold_never_lowers_or_overwrites_another_history() {
        assert!(matches!(
            hold_action("r", &record(7, B7, None), &[], None, id(6, B6), 0),
            HoldAction::Conflict(_)
        ));
        assert!(matches!(
            hold_action("r", &record(6, 0x999, None), &[], None, id(6, B6), 0),
            HoldAction::Conflict(_)
        ));
        let unresolved = record(6, B6, Some(pending("x", id(6, B6), "run1")));
        assert!(matches!(
            hold_action("r", &unresolved, &[on("x", id(6, B6))], None, id(6, B6), 0),
            HoldAction::Conflict(_)
        ));
        let mut gen1 = record(6, B6, None);
        if let ClusterTimeline::Record { record, .. } = &mut gen1 {
            record.generation = 1;
        }
        assert!(matches!(
            hold_action("r", &gen1, &[], None, id(6, B6), 0),
            HoldAction::Conflict(_)
        ));
        // A primary in a newer generation without its own override pending.
        assert!(matches!(
            hold_action("r", &record(6, B6, None), &[], None, id(7, B7), 1),
            HoldAction::Conflict(_)
        ));
    }

    #[test]
    fn hold_does_not_record_an_override_without_the_new_generation_on_disk() {
        let mut p = pending("x", id(4, 0x400), "run1");
        p.generation = 1;
        let tl = record(6, B6, Some(p));
        assert!(matches!(
            hold_action("x", &tl, &[], None, id(5, 0x450), 0),
            HoldAction::Conflict(_)
        ));
    }

    #[test]
    fn hold_completes_its_own_override() {
        let mut p = pending("x", id(4, 0x400), "run1");
        p.generation = 1;
        let tl = record(6, B6, Some(p));
        assert_eq!(
            hold_action("x", &tl, &[], None, id(5, 0x450), 1),
            HoldAction::Write(TimelineRecord {
                timeline: 5,
                begin: 0x450,
                primary: "x".into(),
                generation: 1,
                pending: None,
            })
        );
    }

    #[test]
    fn legacy_values_parse_as_upper_bounds_not_identities() {
        assert_eq!(
            ClusterTimeline::parse(" 17\n", 4),
            ClusterTimeline::Legacy {
                value: 17,
                version: 4
            }
        );
        assert_eq!(
            ClusterTimeline::parse("junk", 4),
            ClusterTimeline::Unreadable { version: 4 }
        );
        let r = TimelineRecord {
            timeline: 6,
            begin: B6,
            primary: "p".into(),
            generation: 0,
            pending: Some(pending("r", id(6, B6), "run1")),
        };
        let json = serde_json::to_string(&r).unwrap();
        assert_eq!(
            ClusterTimeline::parse(&json, 2),
            ClusterTimeline::Record {
                record: r,
                version: 2
            }
        );
    }
}
