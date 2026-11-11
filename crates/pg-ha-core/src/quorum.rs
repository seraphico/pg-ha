//! Pure quorum synchronous-replication logic (SB-13, PR1).
//!
//! Nothing here is wired into the HA loop: no I/O, no async, no network.
//! Every function takes plain values and returns plain values, so the
//! safety argument can be machine-checked by the model in `model_tests`.
//!
//! Design: `.agents/tasks/sb-13-quorum-design/design.md` (sections 1, 3, 4).
//!
//! # Vocabulary
//!
//! * `V` — the voter set published in DCS `/sync` (standbys only, never the
//!   leader) and `q` — the DCS `quorum`.
//! * `S` — the names in PostgreSQL's `synchronous_standby_names` and `n` — the
//!   `ANY n` number. An empty `S` means asynchronous, so `n` counts as 0.
//!
//! # Safety invariants (design section 1.2)
//!
//! * **SI-1** `q + n >= |V ∪ S|` (the leader is excluded from both sets). Every
//!   state the planner can produce, and every single step, satisfies it.
//! * **SI-2** the election rule ([`quorum_vote_decision`]).
//! * **SI-3** the votes are read after the old leader can no longer acknowledge
//!   (a caller obligation, not checkable here).
//! * **SI-4** lowering `q` is only allowed after enough voters are known to
//!   hold everything acknowledged under an older configuration.
//!
//! # How SI-4 is expressed here
//!
//! The caller supplies `caught_up`: the standbys whose flush position has
//! reached the `mark`, the primary's flush LSN recorded when
//! `synchronous_standby_names` last changed. Every commit acknowledged under an
//! older configuration is at or below the mark, so each of those standbys holds
//! it. `q` may drop to `q'` only if `|V ∖ caught_up| <= q'`; a commit
//! acknowledged after the mark is covered by SI-1 on the current `(S, n)`.
//! This is the same condition as the design's "at least `n` voters are past the
//! mark" in steady state, but it also stays correct while `V` and `S` differ.
//! With no known mark the caller must pass an empty `caught_up` (or take a fresh
//! mark at the current primary flush LSN, which is always safe).

use std::collections::BTreeSet;

use crate::sync::SyncSelection;

/// A case-folded set of member names (as DCS `/sync` sets are compared).
pub type Names = BTreeSet<String>;

fn fold<I, S>(names: I) -> Names
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    names
        .into_iter()
        .map(|n| n.as_ref().to_ascii_lowercase())
        .collect()
}

/// The configuration actually in force: DCS `(V, q)` plus PostgreSQL `(S, n)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuorumState {
    /// DCS `/sync.sync_standby` as a set (voters, leader excluded).
    pub voters: Names,
    /// DCS `/sync.quorum`.
    pub quorum: usize,
    /// Names in the effective `synchronous_standby_names`.
    pub pg_names: Names,
    /// The `n` of the effective `ANY n (...)`.
    pub pg_n: usize,
}

impl QuorumState {
    pub fn new<A, B, S1, S2>(voters: A, quorum: usize, pg_names: B, pg_n: usize) -> Self
    where
        A: IntoIterator<Item = S1>,
        B: IntoIterator<Item = S2>,
        S1: AsRef<str>,
        S2: AsRef<str>,
    {
        Self {
            voters: fold(voters),
            quorum,
            pg_names: fold(pg_names),
            pg_n,
        }
    }

    /// `n` as PostgreSQL enforces it: an empty list is asynchronous.
    pub fn effective_n(&self) -> usize {
        if self.pg_names.is_empty() {
            0
        } else {
            self.pg_n
        }
    }

    /// SI-1: `q + n >= |V ∪ S|`.
    pub fn si1_holds(&self) -> bool {
        self.quorum + self.effective_n() >= self.voters.union(&self.pg_names).count()
    }

    /// SI-1 with one node that can never be read as a voter removed from the
    /// union. Used for the short window around a promotion, where the new
    /// leader is still listed in the old `V` but is the node doing the writing.
    pub fn si1_holds_excluding(&self, leader: &str) -> bool {
        let leader = leader.to_ascii_lowercase();
        let u = self
            .voters
            .union(&self.pg_names)
            .filter(|n| **n != leader)
            .count();
        self.quorum + self.effective_n() >= u
    }

    /// Apply one step to this state.
    pub fn apply(&mut self, step: &Step) {
        match step {
            Step::SetDcs { voters, quorum } => {
                self.voters = voters.clone();
                self.quorum = *quorum;
            }
            Step::SetPg { names, n } => {
                self.pg_names = names.clone();
                self.pg_n = *n;
            }
        }
    }
}

/// What the cluster should converge to, derived from the standby selection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuorumTarget {
    /// Desired voter set `V*`.
    pub voters: Names,
    /// Desired `n*` (0 only when `voters` is empty).
    pub n: usize,
}

impl QuorumTarget {
    pub fn new<A, S>(voters: A, n: usize) -> Self
    where
        A: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self {
            voters: fold(voters),
            n,
        }
    }

    /// Build the target from a [`SyncSelection`] made in `Quorum` mode.
    pub fn from_selection(selection: &SyncSelection) -> Self {
        Self::new(selection.voters(), selection.confirmations())
    }

    /// `q* = |V*| - n*`, or 0 when the list cannot satisfy `n*`.
    pub fn quorum(&self) -> usize {
        self.voters.len().saturating_sub(self.n)
    }
}

/// One atomic write. A cycle may run several; a crash may stop between any two.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// One CAS write of DCS `/sync` `(sync_standby = voters, quorum)`.
    SetDcs { voters: Names, quorum: usize },
    /// One `ALTER SYSTEM synchronous_standby_names` + reload to `ANY n (names)`
    /// (an empty `names` means asynchronous).
    SetPg { names: Names, n: usize },
}

/// The next single step toward `target`, or `None` when converged or waiting
/// on the SI-4 gate.
///
/// Order (design section 3.2): grow `V` (and `q` with it) first, then grow `S`,
/// then tighten `n`; when `n` must drop, raise `q` first; shrink `S` before
/// `V`; relax `q` last and only behind the SI-4 gate. Every step keeps SI-1.
pub fn next_step(state: &QuorumState, target: &QuorumTarget, caught_up: &Names) -> Option<Step> {
    let union_len = state.voters.union(&state.pg_names).count();

    if target.voters.is_empty() {
        // Nobody eligible: drop to asynchronous with no voters. While `S` is
        // non-empty make `q >= |U|` first so nobody can be elected during the
        // async window, then empty `S`, then empty `V`.
        if !state.pg_names.is_empty() {
            if state.quorum < union_len {
                return Some(Step::SetDcs {
                    voters: state.voters.clone(),
                    quorum: union_len,
                });
            }
            return Some(Step::SetPg {
                names: Names::new(),
                n: 0,
            });
        }
        if !state.voters.is_empty() || state.quorum != 0 {
            return Some(Step::SetDcs {
                voters: Names::new(),
                quorum: 0,
            });
        }
        return None;
    }

    // A. Add voters: V grows and q grows with it (T1: DCS first).
    let to_add: Vec<&String> = target.voters.difference(&state.voters).collect();
    if !to_add.is_empty() {
        let mut voters = state.voters.clone();
        voters.extend(target.voters.iter().cloned());
        return Some(Step::SetDcs {
            voters,
            quorum: state.quorum + to_add.len(),
        });
    }

    // B. Add to S (every new name is already in V, so |U| is unchanged).
    if !target.voters.is_subset(&state.pg_names) {
        let mut names = state.pg_names.clone();
        names.extend(target.voters.iter().cloned());
        let n = if state.pg_names.is_empty() {
            target.n
        } else {
            state.pg_n
        };
        return Some(Step::SetPg { names, n });
    }

    // C. n too small: tighten.
    if state.pg_n < target.n {
        return Some(Step::SetPg {
            names: state.pg_names.clone(),
            n: target.n,
        });
    }

    // D. n too large: raise q first (T5), then lower n.
    if state.pg_n > target.n {
        let needed_q = union_len.saturating_sub(target.n);
        if state.quorum < needed_q {
            return Some(Step::SetDcs {
                voters: state.voters.clone(),
                quorum: needed_q,
            });
        }
        return Some(Step::SetPg {
            names: state.pg_names.clone(),
            n: target.n,
        });
    }

    // E. Remove from S before V (T2).
    if !state.pg_names.is_subset(&target.voters) {
        return Some(Step::SetPg {
            names: state
                .pg_names
                .intersection(&target.voters)
                .cloned()
                .collect(),
            n: state.pg_n,
        });
    }

    // F. Remove from V with q unchanged (this never weakens any guarantee).
    if !state.voters.is_subset(&target.voters) {
        return Some(Step::SetDcs {
            voters: state.voters.intersection(&target.voters).cloned().collect(),
            quorum: state.quorum,
        });
    }

    // G. Relax q, last, behind the SI-4 gate: `|V ∖ caught_up| <= q'`.
    let target_q = target.quorum();
    if state.quorum > target_q {
        let missing = state.voters.difference(caught_up).count();
        let new_q = target_q.max(missing);
        if new_q < state.quorum {
            return Some(Step::SetDcs {
                voters: state.voters.clone(),
                quorum: new_q,
            });
        }
    }
    None
}

/// Steps to run in one HA cycle, in order. Stops at a gate (nothing more
/// to do until standbys catch up) or after `max_steps`.
///
/// A [`Step::SetPg`] starts a new mark, so the remaining plan assumes nobody
/// has caught up to it yet; any relaxation that needs it waits for a later
/// cycle.
pub fn plan(
    state: &QuorumState,
    target: &QuorumTarget,
    caught_up: &Names,
    max_steps: usize,
) -> Vec<Step> {
    let mut state = state.clone();
    let mut caught_up = caught_up.clone();
    let mut steps = Vec::new();
    while steps.len() < max_steps {
        let Some(step) = next_step(&state, target, &caught_up) else {
            break;
        };
        state.apply(&step);
        if matches!(step, Step::SetPg { .. }) {
            caught_up.clear();
        }
        steps.push(step);
    }
    steps
}

/// The state a node must install BEFORE `pg_ctl promote` (design T10, section
/// 1.3): the old leader takes the promoted node's seat, and `q` is kept.
///
/// `V' = (V ∖ {new_leader}) ∪ {old_leader}`, `S' = V'`, `n' = |V| - q`. Lowering
/// `q` here would be unsafe, so it is not done. Returns `None` when
/// `new_leader` is not a voter, or when `q >= |V|` (then no election could have
/// granted the vote, so there is nothing to install).
pub fn post_promotion_state(
    state: &QuorumState,
    new_leader: &str,
    old_leader: &str,
) -> Option<QuorumState> {
    let new_leader = new_leader.to_ascii_lowercase();
    let old_leader = old_leader.to_ascii_lowercase();
    if !state.voters.contains(&new_leader) || state.quorum >= state.voters.len() {
        return None;
    }
    let mut voters = state.voters.clone();
    voters.remove(&new_leader);
    voters.insert(old_leader);
    Some(QuorumState {
        pg_names: voters.clone(),
        pg_n: state.voters.len() - state.quorum,
        voters,
        quorum: state.quorum,
    })
}

/// A node's live WAL position as read at election time (design section 5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalPosition {
    pub name: String,
    /// `false` means this node is a writable primary.
    pub in_recovery: bool,
    pub timeline: u64,
    pub lsn: u64,
}

/// Why a candidate was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VoteRefusal {
    /// The candidate is not in `V`.
    NotVoter,
    /// A peer reports `in_recovery = false`: a writable primary exists.
    PrimaryAlive { peer: String },
    /// A voter is on a newer timeline.
    HigherTimeline { peer: String },
    /// A voter on the same timeline is ahead of the candidate.
    BehindPeer { peer: String },
    /// Fewer than `quorum` voters could be counted.
    InsufficientVotes { got: usize, need: usize },
}

/// Outcome of [`quorum_vote_decision`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VoteDecision {
    Granted { votes: usize },
    Refused(VoteRefusal),
}

/// SI-2: may `candidate` be promoted?
///
/// * the candidate must be a voter;
/// * any responder that is not in recovery means a primary is alive: refuse;
/// * a voter on a newer timeline: refuse; on an older one: not counted (LSNs
///   of different timelines are not comparable);
/// * a same-timeline voter with a higher LSN: refuse;
/// * otherwise that voter counts as one vote. At least `quorum` voters other
///   than the candidate must count. Fewer means refuse (no leader rather than
///   a lost commit).
///
/// `peers` holds the live answers actually received (unreachable nodes are
/// simply absent). Answers from non-voters and from the candidate itself are
/// ignored, except that a non-voter reporting a writable primary still refuses.
pub fn quorum_vote_decision(
    candidate: &WalPosition,
    voters: &Names,
    quorum: usize,
    peers: &[WalPosition],
) -> VoteDecision {
    let me = candidate.name.to_ascii_lowercase();
    if !voters.contains(&me) {
        return VoteDecision::Refused(VoteRefusal::NotVoter);
    }
    let mut votes = Names::new();
    for p in peers {
        let name = p.name.to_ascii_lowercase();
        if name == me {
            continue;
        }
        if !p.in_recovery {
            return VoteDecision::Refused(VoteRefusal::PrimaryAlive {
                peer: p.name.clone(),
            });
        }
        if !voters.contains(&name) {
            continue;
        }
        if p.timeline > candidate.timeline {
            return VoteDecision::Refused(VoteRefusal::HigherTimeline {
                peer: p.name.clone(),
            });
        }
        if p.timeline < candidate.timeline {
            continue;
        }
        if p.lsn > candidate.lsn {
            return VoteDecision::Refused(VoteRefusal::BehindPeer {
                peer: p.name.clone(),
            });
        }
        votes.insert(name);
    }
    if votes.len() < quorum {
        return VoteDecision::Refused(VoteRefusal::InsufficientVotes {
            got: votes.len(),
            need: quorum,
        });
    }
    VoteDecision::Granted { votes: votes.len() }
}

#[cfg(test)]
mod model_tests;

#[cfg(test)]
mod tests {
    use super::*;

    fn n(names: &[&str]) -> Names {
        fold(names.iter())
    }

    fn pos(name: &str, tl: u64, lsn: u64) -> WalPosition {
        WalPosition {
            name: name.into(),
            in_recovery: true,
            timeline: tl,
            lsn,
        }
    }

    /// Run `plan` to a fixed point, assuming every standby is caught up after
    /// each batch (the steady case).
    fn converge(mut st: QuorumState, tgt: &QuorumTarget) -> (QuorumState, Vec<Step>) {
        let mut all = Vec::new();
        for _ in 0..8 {
            let caught = st.voters.union(&st.pg_names).cloned().collect();
            let steps = plan(&st, tgt, &caught, 16);
            if steps.is_empty() {
                break;
            }
            for s in &steps {
                assert!(
                    {
                        let mut next = st.clone();
                        next.apply(s);
                        next.si1_holds()
                    },
                    "SI-1 broken by {s:?} from {st:?}"
                );
                st.apply(s);
            }
            all.extend(steps);
        }
        (st, all)
    }

    // ---- planner: one test per transition in design 3.2 -------------------

    #[test]
    fn t1_add_voter_dcs_first_then_pg() {
        let st = QuorumState::new(["a", "b"], 1, ["a", "b"], 1);
        let tgt = QuorumTarget::new(["a", "b", "c"], 1);
        let steps = plan(&st, &tgt, &n(&["a", "b"]), 16);
        assert_eq!(
            steps,
            vec![
                Step::SetDcs {
                    voters: n(&["a", "b", "c"]),
                    quorum: 2
                },
                Step::SetPg {
                    names: n(&["a", "b", "c"]),
                    n: 1
                },
            ]
        );
        // Adding a voter raises q* by one (|V| grows, n stays), so q needs no
        // relaxing afterwards.
        let mut st2 = st.clone();
        for s in &steps {
            st2.apply(s);
        }
        assert!(plan(&st2, &tgt, &n(&["a", "b", "c"]), 16).is_empty());
    }

    #[test]
    fn t2_remove_voter_pg_first_then_dcs_then_gated_relax() {
        let st = QuorumState::new(["a", "b", "c"], 2, ["a", "b", "c"], 1);
        let tgt = QuorumTarget::new(["a", "b"], 1);
        let steps = plan(&st, &tgt, &n(&["a", "b", "c"]), 16);
        assert_eq!(
            steps,
            vec![
                Step::SetPg {
                    names: n(&["a", "b"]),
                    n: 1
                },
                Step::SetDcs {
                    voters: n(&["a", "b"]),
                    quorum: 2
                },
            ]
        );
    }

    #[test]
    fn t2_relax_waits_until_voters_pass_mark() {
        // Design 1.2 counter-example: S={a,b,c} n=2 -> drop c. Relaxing q to 0
        // before a and b are both past the mark would let `a` win without
        // having a commit that only {b,c} acknowledged.
        let st = QuorumState::new(["a", "b"], 1, ["a", "b"], 2);
        let tgt = QuorumTarget::new(["a", "b"], 2); // q* = 0
        // Only b is past the mark: |V ∖ C| = 1, so q may not go below 1.
        assert!(plan(&st, &tgt, &n(&["b"]), 4).is_empty());
        assert!(plan(&st, &tgt, &Names::new(), 4).is_empty());
        assert_eq!(
            plan(&st, &tgt, &n(&["a", "b"]), 4),
            vec![Step::SetDcs {
                voters: n(&["a", "b"]),
                quorum: 0
            }]
        );
    }

    #[test]
    fn t4_n_increase_pg_first_then_relax_q() {
        let st = QuorumState::new(["a", "b", "c", "d"], 3, ["a", "b", "c", "d"], 1);
        let tgt = QuorumTarget::new(["a", "b", "c", "d"], 2);
        let (end, _) = converge(st, &tgt);
        assert_eq!(
            end,
            QuorumState::new(["a", "b", "c", "d"], 2, ["a", "b", "c", "d"], 2)
        );
    }

    #[test]
    fn t5_n_decrease_raises_q_first() {
        let st = QuorumState::new(["a", "b", "c", "d"], 2, ["a", "b", "c", "d"], 2);
        let tgt = QuorumTarget::new(["a", "b", "c", "d"], 1);
        let first = plan(&st, &tgt, &n(&["a", "b", "c", "d"]), 1);
        assert_eq!(
            first,
            vec![Step::SetDcs {
                voters: n(&["a", "b", "c", "d"]),
                quorum: 3
            }]
        );
        let (end, _) = converge(st, &tgt);
        assert_eq!(end.quorum, 3);
        assert_eq!(end.pg_n, 1);
    }

    #[test]
    fn t6_off_to_quorum() {
        let st = QuorumState::new::<[&str; 0], [&str; 0], _, _>([], 0, [], 0);
        let tgt = QuorumTarget::new(["a", "b"], 1);
        let steps = plan(&st, &tgt, &Names::new(), 16);
        // Voters are published with q = |V| (nobody electable), then PG, then
        // q relaxes in a later cycle behind the gate.
        assert_eq!(
            steps,
            vec![
                Step::SetDcs {
                    voters: n(&["a", "b"]),
                    quorum: 2
                },
                Step::SetPg {
                    names: n(&["a", "b"]),
                    n: 1
                },
            ]
        );
    }

    #[test]
    fn t7_quorum_to_off_blocks_elections_before_async() {
        let st = QuorumState::new(["a", "b"], 1, ["a", "b"], 1);
        let tgt = QuorumTarget::new::<[&str; 0], _>([], 0);
        let (end, steps) = converge(st, &tgt);
        assert!(end.voters.is_empty() && end.pg_names.is_empty() && end.quorum == 0);
        // The very first step must not leave an electable async window.
        assert_eq!(
            steps[0],
            Step::SetDcs {
                voters: n(&["a", "b"]),
                quorum: 2
            }
        );
    }

    #[test]
    fn t8_priority_special_case_converges_to_quorum_target() {
        // Priority FIRST 2 (a,b) == V={a,b}, q=0, n=2. Add c, keep n=2.
        let st = QuorumState::new(["a", "b"], 0, ["a", "b"], 2);
        let tgt = QuorumTarget::new(["a", "b", "c"], 2);
        let (end, _) = converge(st, &tgt);
        assert_eq!(
            end,
            QuorumState::new(["a", "b", "c"], 1, ["a", "b", "c"], 2)
        );
    }

    #[test]
    fn t11_strict_unsatisfiable_target() {
        // strict, M=2 < N=3: list of 2, n=3, q*=0.
        let st = QuorumState::new(["a", "b"], 0, ["a", "b"], 2);
        let tgt = QuorumTarget::new(["a", "b"], 3);
        assert_eq!(tgt.quorum(), 0);
        let (end, _) = converge(st, &tgt);
        assert_eq!(end.pg_n, 3);
        assert_eq!(end.quorum, 0);
    }

    #[test]
    fn converged_state_has_no_steps() {
        let st = QuorumState::new(["a", "b", "c"], 1, ["a", "b", "c"], 2);
        let tgt = QuorumTarget::new(["a", "b", "c"], 2);
        assert!(plan(&st, &tgt, &n(&["a", "b", "c"]), 16).is_empty());
    }

    #[test]
    fn target_from_selection() {
        let sel = SyncSelection::Voters {
            names: vec!["R2".into(), "r1".into(), "r3".into()],
            count: 2,
        };
        let t = QuorumTarget::from_selection(&sel);
        assert_eq!(t.voters, n(&["r1", "r2", "r3"]));
        assert_eq!(t.quorum(), 1);
        let none = QuorumTarget::from_selection(&SyncSelection::NoEligible { strict: true });
        assert!(none.voters.is_empty() && none.n == 0 && none.quorum() == 0);
    }

    // ---- promotion state (T10) --------------------------------------------

    #[test]
    fn promotion_swaps_seats_and_keeps_q() {
        let st = QuorumState::new(["b", "c"], 1, ["b", "c"], 1);
        let post = post_promotion_state(&st, "b", "a").unwrap();
        assert_eq!(post, QuorumState::new(["a", "c"], 1, ["a", "c"], 1));
        assert!(post.si1_holds());
        // Pre-promote window: new PG list, old DCS, leader b still in V.
        let window = QuorumState {
            voters: st.voters.clone(),
            quorum: 1,
            ..post.clone()
        };
        assert!(window.si1_holds_excluding("b"));
        assert!(!window.si1_holds());
    }

    #[test]
    fn promotion_refused_for_non_voter_or_unelectable_q() {
        let st = QuorumState::new(["b", "c"], 1, ["b", "c"], 1);
        assert!(post_promotion_state(&st, "x", "a").is_none());
        let st = QuorumState::new(["b", "c"], 2, ["b", "c"], 1);
        assert!(post_promotion_state(&st, "b", "a").is_none());
    }

    // ---- vote decision -----------------------------------------------------

    #[test]
    fn vote_granted_with_enough_equal_or_lower_voters() {
        let v = n(&["a", "b", "c"]);
        let d = quorum_vote_decision(
            &pos("a", 1, 100),
            &v,
            2,
            &[pos("b", 1, 100), pos("c", 1, 90)],
        );
        assert_eq!(d, VoteDecision::Granted { votes: 2 });
    }

    #[test]
    fn vote_refused_when_not_a_voter() {
        let v = n(&["b", "c"]);
        let d = quorum_vote_decision(&pos("a", 1, 100), &v, 0, &[]);
        assert_eq!(d, VoteDecision::Refused(VoteRefusal::NotVoter));
    }

    #[test]
    fn vote_refused_when_not_enough_votes() {
        let v = n(&["a", "b", "c"]);
        let d = quorum_vote_decision(&pos("a", 1, 100), &v, 2, &[pos("b", 1, 100)]);
        assert_eq!(
            d,
            VoteDecision::Refused(VoteRefusal::InsufficientVotes { got: 1, need: 2 })
        );
    }

    #[test]
    fn vote_refused_when_behind_any_voter() {
        let v = n(&["a", "b", "c"]);
        let d = quorum_vote_decision(
            &pos("a", 1, 100),
            &v,
            1,
            &[pos("b", 1, 100), pos("c", 1, 101)],
        );
        assert_eq!(
            d,
            VoteDecision::Refused(VoteRefusal::BehindPeer { peer: "c".into() })
        );
    }

    #[test]
    fn vote_timeline_rules() {
        let v = n(&["a", "b", "c"]);
        // Higher timeline: refuse.
        assert_eq!(
            quorum_vote_decision(&pos("a", 1, 100), &v, 1, &[pos("b", 2, 5)]),
            VoteDecision::Refused(VoteRefusal::HigherTimeline { peer: "b".into() })
        );
        // Lower timeline: not counted, even with a bigger LSN.
        assert_eq!(
            quorum_vote_decision(&pos("a", 2, 100), &v, 1, &[pos("b", 1, 999)]),
            VoteDecision::Refused(VoteRefusal::InsufficientVotes { got: 0, need: 1 })
        );
    }

    #[test]
    fn vote_refused_when_a_primary_is_alive() {
        let v = n(&["a", "b"]);
        let mut p = pos("b", 1, 100);
        p.in_recovery = false;
        assert_eq!(
            quorum_vote_decision(&pos("a", 1, 100), &v, 0, &[p.clone()]),
            VoteDecision::Refused(VoteRefusal::PrimaryAlive { peer: "b".into() })
        );
        // A non-voter reporting a writable primary refuses too.
        p.name = "z".into();
        assert!(matches!(
            quorum_vote_decision(&pos("a", 1, 100), &v, 0, &[p]),
            VoteDecision::Refused(VoteRefusal::PrimaryAlive { .. })
        ));
    }

    #[test]
    fn vote_ignores_non_voters_self_and_duplicates() {
        let v = n(&["a", "b", "c"]);
        let d = quorum_vote_decision(
            &pos("a", 1, 100),
            &v,
            2,
            &[
                pos("A", 1, 5000), // the candidate itself (case-folded): ignored
                pos("z", 1, 5000), // non-voter: ignored
                pos("b", 1, 100),
                pos("B", 1, 100), // duplicate answer counts once
            ],
        );
        assert_eq!(
            d,
            VoteDecision::Refused(VoteRefusal::InsufficientVotes { got: 1, need: 2 })
        );
    }

    #[test]
    fn vote_with_zero_quorum_needs_no_peers() {
        let v = n(&["a", "b"]);
        assert_eq!(
            quorum_vote_decision(&pos("a", 1, 1), &v, 0, &[]),
            VoteDecision::Granted { votes: 0 }
        );
    }
}
