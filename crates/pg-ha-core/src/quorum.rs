//! Pure quorum synchronous-replication logic (SB-13, PR1).
//!
//! Nothing here is wired into the HA loop: no I/O, no async, no network.
//! Every function takes plain values and returns plain values, so the
//! safety argument can be machine-checked by the model in `model_tests`.
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
    next_step_with(state, target, caught_up, true, false)
}
/// [`next_step`] for `synchronous_mode` leaving quorum (SB-38): no step
/// publishes `q >= |V|`.
///
/// * Nobody eligible: PostgreSQL goes asynchronous first with `/sync` as it is
///   (`q = |V| - n < |V|`, the vote still passes); the hand-off write
///   `(V = {}, q = 0)` follows, only once every voter is in `caught_up`
///   (`None` until then). `q` is not raised to `|U|` in between.
/// * Removing voters: steps F and G are one write
///   `(V*, min(q, max(q*, |V* ∖ caught_up|)))`, made only when that `q` is
///   below `|V*|`; otherwise it waits with `/sync` unchanged.
pub fn next_step_leaving(
    state: &QuorumState,
    target: &QuorumTarget,
    caught_up: &Names,
) -> Option<Step> {
    next_step_with(state, target, caught_up, true, true)
}
/// [`next_step_leaving`] without the enabling rule (see
/// [`plan_without_enabling`]).
pub(crate) fn next_step_leaving_legacy(
    state: &QuorumState,
    target: &QuorumTarget,
    caught_up: &Names,
) -> Option<Step> {
    next_step_with(state, target, caught_up, false, true)
}
/// The planner without the enabling rule (the behavior before SB-36); see
/// [`plan_without_enabling`].
pub(crate) fn next_step_legacy(
    state: &QuorumState,
    target: &QuorumTarget,
    caught_up: &Names,
) -> Option<Step> {
    next_step_with(state, target, caught_up, false, false)
}
/// Enabling rule (SB-36 fix 1). From an empty `S` (asynchronous) with voters to
/// enable: write DCS `(V*, q*)` first, with `q* = |V*| - n*`, and only then
/// `ANY n* (V*)` in PostgreSQL, so a primary lost right after the switch
/// still leaves a quorum that can win the vote. After a crash between the two
/// writes only the PG step is left.
pub fn enabling_step(state: &QuorumState, target: &QuorumTarget) -> Option<Step> {
    if !state.pg_names.is_empty() || target.voters.is_empty() {
        return None;
    }
    let quorum = target.quorum();
    if state.voters != target.voters || state.quorum != quorum {
        return Some(Step::SetDcs {
            voters: target.voters.clone(),
            quorum,
        });
    }
    Some(Step::SetPg {
        names: target.voters.clone(),
        n: target.n,
    })
}
/// `S` empty, `/sync` already `(V*, q*)` with `q* < |V|`: the state between the
/// two enabling writes. SI-1 does not hold here by design (nothing in PG yet).
pub fn in_enabling_window(state: &QuorumState, target: &QuorumTarget) -> bool {
    state.pg_names.is_empty()
        && !target.voters.is_empty()
        && state.voters == target.voters
        && state.quorum == target.quorum()
        && state.quorum < state.voters.len()
}
fn next_step_with(
    state: &QuorumState,
    target: &QuorumTarget,
    caught_up: &Names,
    enabling: bool,
    leaving: bool,
) -> Option<Step> {
    let union_len = state.voters.union(&state.pg_names).count();

    if target.voters.is_empty() {
        // Nobody eligible: drop to asynchronous with no voters. While `S` is
        // non-empty make `q >= |U|` first so nobody can be elected during the
        // async window, then empty `S`, then empty `V`.
        if !state.pg_names.is_empty() {
            if !leaving && state.quorum < union_len {
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
            // Leaving quorum: the hand-off ends the vote, so it waits until
            // every voter has reached the mark taken after PG went async.
            // Acknowledged commits are then on all of them (SB-38).
            if leaving && !state.voters.is_subset(caught_up) {
                return None;
            }
            return Some(Step::SetDcs {
                voters: Names::new(),
                quorum: 0,
            });
        }
        return None;
    }

    if enabling && let Some(step) = enabling_step(state, target) {
        return Some(step);
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
        if leaving {
            // F and G in one write: with q unchanged, `q >= |V*|` is possible
            // (`|V| >= 2n`) and nobody could win the vote.
            let voters: Names = state.voters.intersection(&target.voters).cloned().collect();
            let missing = voters.difference(caught_up).count();
            let quorum = state.quorum.min(target.quorum().max(missing));
            return (quorum < voters.len()).then_some(Step::SetDcs { voters, quorum });
        }
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
    plan_with(state, target, caught_up, max_steps, next_step)
}
fn plan_with(
    state: &QuorumState,
    target: &QuorumTarget,
    caught_up: &Names,
    max_steps: usize,
    next: fn(&QuorumState, &QuorumTarget, &Names) -> Option<Step>,
) -> Vec<Step> {
    let mut state = state.clone();
    let mut caught_up = caught_up.clone();
    let mut steps = Vec::new();
    while steps.len() < max_steps {
        let Some(step) = next(&state, target, &caught_up) else {
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

/// [`plan`] for `synchronous_mode` leaving quorum; see [`next_step_leaving`].
/// `enabling` is false for a PostgreSQL value that is not really asynchronous.
pub fn plan_leaving(
    state: &QuorumState,
    target: &QuorumTarget,
    caught_up: &Names,
    max_steps: usize,
    enabling: bool,
) -> Vec<Step> {
    let next = if enabling {
        next_step_leaving
    } else {
        next_step_leaving_legacy
    };
    plan_with(state, target, caught_up, max_steps, next)
}

/// [`plan`] without the enabling rule: used for a PostgreSQL value that is
/// not really asynchronous although it reads as `S = {}` (a wildcard), and by
/// the model tests to pin the old world.
pub(crate) fn plan_without_enabling(
    state: &QuorumState,
    target: &QuorumTarget,
    caught_up: &Names,
    max_steps: usize,
) -> Vec<Step> {
    plan_with(state, target, caught_up, max_steps, next_step_legacy)
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

/// SB-37: one line of the candidate's own timeline history file: the
/// ancestor `timeline` ended at `switchpoint` (the next timeline of the
/// candidate's history began there).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AncestorEnd {
    pub timeline: u64,
    pub switchpoint: u64,
}

/// SI-2: may `candidate` be promoted?
///
/// * the candidate must be a voter;
/// * any responder that is not in recovery means a primary is alive: refuse;
/// * a voter on a newer timeline: refuse;
/// * a same-timeline voter with a higher LSN: refuse; otherwise it counts as
///   one vote;
/// * a voter on an older timeline counts only under the prefix rule below;
///   otherwise it is not counted (LSNs of different timelines are not
///   comparable);
/// * at least `quorum` voters other than the candidate must count. Fewer
///   means refuse (no leader rather than a lost commit).
///
/// Equivalent to [`quorum_vote_decision_with_history`] with no history.
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
    quorum_vote_decision_with_history(candidate, voters, quorum, peers, &[])
}

/// [`quorum_vote_decision`] plus the SB-37 prefix rule. `ancestors` is the
/// candidate's own history file for `candidate.timeline` (empty: the rule
/// does not apply, which is also what a caller passes when the file cannot be
/// read: fail closed).
///
/// Prefix rule: a voter that reports timeline `T_v < candidate.timeline`
/// counts as a vote when `ancestors` names `T_v` with switchpoint `S` and the
/// voter's LSN is `<= S`. Its WAL is then a prefix of the candidate's: the
/// candidate has all of timeline `T_v` up to `S`. A voter beyond `S`, or on a
/// timeline that is not in the candidate's history, is not counted.
///
/// A published timeline is a lower bound (a voter may under-report). That is
/// still safe while every candidate is on the latest recorded timeline (D6,
/// so timelines form one chain and each one began exactly at its parent's
/// switchpoint): a voter that really is on a later timeline than `T_v` has
/// WAL at or past `S` (where `T_v + 1` began), so its LSN is `> S`. Callers
/// pass no history when the candidate campaigns under the D6 override.
pub fn quorum_vote_decision_with_history(
    candidate: &WalPosition,
    voters: &Names,
    quorum: usize,
    peers: &[WalPosition],
    ancestors: &[AncestorEnd],
) -> VoteDecision {
    decide(candidate, voters, quorum, peers, ancestors, holds_a_prefix)
}

/// The rule with the older-timeline test as a parameter (the tests run
/// mutated versions of it through the same code).
pub(crate) type PrefixTest = fn(&WalPosition, &[AncestorEnd]) -> bool;

pub(crate) fn decide(
    candidate: &WalPosition,
    voters: &Names,
    quorum: usize,
    peers: &[WalPosition],
    ancestors: &[AncestorEnd],
    prefix: PrefixTest,
) -> VoteDecision {
    let me = candidate.name.to_ascii_lowercase();
    if !voters.contains(&me) {
        return VoteDecision::Refused(VoteRefusal::NotVoter);
    }
    let votes = match counted_voters(candidate, voters, peers, ancestors, prefix) {
        Ok(v) => v,
        Err(refusal) => return VoteDecision::Refused(refusal),
    };
    if votes.len() < quorum {
        return VoteDecision::Refused(VoteRefusal::InsufficientVotes {
            got: votes.len(),
            need: quorum,
        });
    }
    VoteDecision::Granted { votes: votes.len() }
}

/// SB-37: how one answer stands against the candidate. The only place the
/// counting rule lives; the vote, the recovery vote and the recovery
/// assessment all use it (through [`counted_voters`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Standing {
    /// The answer refuses the vote.
    Refuses(VoteRefusal),
    /// A voter whose WAL is a prefix of the candidate's: same timeline and
    /// not ahead, or an older timeline under the prefix rule.
    Counts,
    /// Not counted: a non-voter, the candidate itself, or an older-timeline
    /// voter the prefix rule does not cover (LSNs not comparable).
    NotCounted,
}

pub(crate) fn standing(
    candidate: &WalPosition,
    voters: &Names,
    peer: &WalPosition,
    ancestors: &[AncestorEnd],
    prefix: PrefixTest,
) -> Standing {
    let name = peer.name.to_ascii_lowercase();
    if name == candidate.name.to_ascii_lowercase() {
        return Standing::NotCounted;
    }
    if !peer.in_recovery {
        return Standing::Refuses(VoteRefusal::PrimaryAlive {
            peer: peer.name.clone(),
        });
    }
    if !voters.contains(&name) {
        return Standing::NotCounted;
    }
    if peer.timeline > candidate.timeline {
        return Standing::Refuses(VoteRefusal::HigherTimeline {
            peer: peer.name.clone(),
        });
    }
    if peer.timeline < candidate.timeline {
        return if prefix(peer, ancestors) {
            Standing::Counts
        } else {
            Standing::NotCounted
        };
    }
    if peer.lsn > candidate.lsn {
        return Standing::Refuses(VoteRefusal::BehindPeer {
            peer: peer.name.clone(),
        });
    }
    Standing::Counts
}

/// The voters (lower-case) that count for `candidate`, or the first answer
/// that refuses, in `peers` order.
pub(crate) fn counted_voters(
    candidate: &WalPosition,
    voters: &Names,
    peers: &[WalPosition],
    ancestors: &[AncestorEnd],
    prefix: PrefixTest,
) -> Result<Names, VoteRefusal> {
    let mut votes = Names::new();
    for p in peers {
        match standing(candidate, voters, p, ancestors, prefix) {
            Standing::Refuses(r) => return Err(r),
            Standing::Counts => {
                votes.insert(p.name.to_ascii_lowercase());
            }
            Standing::NotCounted => {}
        }
    }
    Ok(votes)
}

/// SB-37 prefix rule for one older-timeline voter.
pub(crate) fn holds_a_prefix(peer: &WalPosition, ancestors: &[AncestorEnd]) -> bool {
    ancestors
        .iter()
        .any(|a| a.timeline == peer.timeline && peer.lsn <= a.switchpoint)
}
/// SB-37: may `candidate` be promoted under an operator's recovery grant that
/// names the voters `lost`?
///
/// The strict rule of [`quorum_vote_decision`] needs `q` other voters, which a
/// cluster that lost voters for good cannot reach. The grant replaces that
/// count by a named loss: the operator accepts that commits held only by
/// `lost` may be gone, and in exchange every OTHER voter must answer.
///
/// * the candidate must be a voter, and `lost` may name only voters other
///   than the candidate (so `|lost| <= |V| - 1`);
/// * the answers are judged by [`standing`], the rule of the normal vote:
///   any responder that is not in recovery refuses (`PrimaryAlive`), any
///   voter, named or not, on a newer timeline refuses (`HigherTimeline`),
///   one on the same timeline with a higher LSN refuses (`BehindPeer`): a
///   named voter that came back ahead must not be discarded silently;
/// * a voter counts when [`standing`] counts it: same timeline and not ahead,
///   or an older timeline under the prefix rule with `ancestors` (the
///   candidate's own history file; empty under the D6 override or when it is
///   unreadable: fail closed). SB-37 PR B2: before, an older-timeline voter
///   was never counted here, unlike the normal vote;
/// * a named voter that counts or not never stands in for a missing voter;
/// * every voter in `V ∖ lost ∖ {candidate}` must count, otherwise
///   `InsufficientVotes { got, need }` with `need = |V ∖ lost ∖ {candidate}|`.
///
/// `quorum` plays no part: with `k` named voters the commits that can be lost
/// are exactly those held only by `lost`, and none when `k <= |V| - q - 1`
/// (then every acknowledged commit has a holder among the counted voters).
pub fn recovery_vote_decision(
    candidate: &WalPosition,
    voters: &Names,
    lost: &Names,
    peers: &[WalPosition],
    ancestors: &[AncestorEnd],
) -> VoteDecision {
    recovery_decide(candidate, voters, lost, peers, ancestors, holds_a_prefix)
}

pub(crate) fn recovery_decide(
    candidate: &WalPosition,
    voters: &Names,
    lost: &Names,
    peers: &[WalPosition],
    ancestors: &[AncestorEnd],
    prefix: PrefixTest,
) -> VoteDecision {
    let me = candidate.name.to_ascii_lowercase();
    if !voters.contains(&me) || lost.contains(&me) || !lost.is_subset(voters) {
        return VoteDecision::Refused(VoteRefusal::NotVoter);
    }
    let counted = match counted_voters(candidate, voters, peers, ancestors, prefix) {
        Ok(v) => v,
        Err(refusal) => return VoteDecision::Refused(refusal),
    };
    let present = counted.difference(lost).count();
    let need = voters
        .iter()
        .filter(|v| **v != me && !lost.contains(*v))
        .count();
    if present < need {
        return VoteDecision::Refused(VoteRefusal::InsufficientVotes { got: present, need });
    }
    VoteDecision::Granted { votes: present }
}
pub mod derive;
pub mod mark;
pub mod recovery;
pub mod round;

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
                let enabling = in_enabling_window(
                    &{
                        let mut next = st.clone();
                        next.apply(s);
                        next
                    },
                    tgt,
                );
                assert!(
                    enabling || {
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
        // SB-36: `/sync` is written with the settled q* = |V| - n first, then PG.
        assert_eq!(
            steps,
            vec![
                Step::SetDcs {
                    voters: n(&["a", "b"]),
                    quorum: 1
                },
                Step::SetPg {
                    names: n(&["a", "b"]),
                    n: 1
                },
            ]
        );
    }

    #[test]
    fn enabling_writes_dcs_before_pg_and_resumes_after_a_crash() {
        let st = QuorumState::new::<[&str; 0], [&str; 0], _, _>([], 0, [], 0);
        let tgt = QuorumTarget::new(["a", "b", "c"], 2);
        let steps = plan(&st, &tgt, &Names::new(), 2);
        assert_eq!(
            steps,
            vec![
                Step::SetDcs {
                    voters: n(&["a", "b", "c"]),
                    quorum: 1
                },
                Step::SetPg {
                    names: n(&["a", "b", "c"]),
                    n: 2
                },
            ]
        );
        // Crash between the two writes: only the PG step is left.
        let mut mid = st.clone();
        mid.apply(&steps[0]);
        assert!(in_enabling_window(&mid, &tgt));
        assert_eq!(plan(&mid, &tgt, &Names::new(), 4), vec![steps[1].clone()]);
        // Never PG first, and the window closes once PG is set.
        mid.apply(&steps[1]);
        assert!(!in_enabling_window(&mid, &tgt));
        assert!(plan(&mid, &tgt, &n(&["a", "b", "c"]), 4).is_empty());
        // Not in the window: S non-empty, q = |V|, or no voters.
        assert!(!in_enabling_window(&st, &tgt));
        let stuck = QuorumState::new(["a", "b", "c"], 3, [] as [&str; 0], 0);
        assert!(!in_enabling_window(&stuck, &tgt));
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
    fn sb38_async_leave_empties_pg_first_and_never_raises_q() {
        let st = QuorumState::new(["a", "b"], 1, ["a", "b"], 1);
        let tgt = QuorumTarget::new::<[&str; 0], _>([], 0);
        let caught = n(&["a", "b"]);
        let steps = plan_leaving(&st, &tgt, &caught, 8, true);
        // The new PG value starts a new mark: the hand-off waits for the next
        // cycle even though everyone was caught up to the old one.
        assert_eq!(
            steps,
            vec![Step::SetPg {
                names: Names::new(),
                n: 0
            }]
        );
        let mut s = st.clone();
        s.apply(&steps[0]);
        let hand_off = Step::SetDcs {
            voters: Names::new(),
            quorum: 0,
        };
        // Gate: every voter must be caught up to that mark.
        assert_eq!(next_step_leaving(&s, &tgt, &n(&["a"])), None);
        assert_eq!(next_step_leaving(&s, &tgt, &Names::new()), None);
        assert_eq!(next_step_leaving(&s, &tgt, &caught), Some(hand_off.clone()));
        let steps = vec![steps[0].clone(), hand_off];
        let mut s = st;
        for step in &steps {
            if let Step::SetDcs { voters, quorum } = step
                && !voters.is_empty()
            {
                assert!(quorum < &voters.len());
            }
            s.apply(step);
            assert!(s.voters.is_empty() || s.quorum < s.voters.len());
        }
    }

    #[test]
    fn sb38_sync_leave_fuses_voter_removal_with_relax() {
        // V={a,b,c,d} q=2 (n=2); target V*={a,b} n=1 (q*=1). Removing c,d with
        // q unchanged would give q=2=|V*|.
        let st = QuorumState::new(["a", "b", "c", "d"], 2, ["a", "b"], 1);
        let tgt = QuorumTarget::new(["a", "b"], 1);
        let caught = n(&["a", "b"]);
        let step = next_step_leaving(&st, &tgt, &caught);
        assert_eq!(
            step,
            Some(Step::SetDcs {
                voters: n(&["a", "b"]),
                quorum: 1
            })
        );
        // Nobody caught up: q' = 2 = |V*|, so wait.
        assert_eq!(next_step_leaving(&st, &tgt, &Names::new()), None);
        // Steady quorum keeps the old two writes.
        assert_eq!(
            next_step(&st, &tgt, &caught),
            Some(Step::SetDcs {
                voters: n(&["a", "b"]),
                quorum: 2
            })
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

    // ── SB-37 prefix rule ──
    /// The PG 18 3-node scenario 8 values (evidence pr-a-pg18-3n-r2.md):
    /// candidate node2 on timeline 3, `00000003.history` = 1 0/5000060,
    /// 2 0/5037050; voter node3 on timeline 2 at exactly 0/5037050.
    const S1: u64 = 0x5000060;
    const S2: u64 = 0x5037050;
    fn r2_history() -> Vec<AncestorEnd> {
        vec![
            AncestorEnd {
                timeline: 1,
                switchpoint: S1,
            },
            AncestorEnd {
                timeline: 2,
                switchpoint: S2,
            },
        ]
    }
    fn gap_history() -> Vec<AncestorEnd> {
        // Timeline 4 whose history skips 2 (2 was abandoned).
        vec![
            AncestorEnd {
                timeline: 1,
                switchpoint: 0x100,
            },
            AncestorEnd {
                timeline: 3,
                switchpoint: 0x300,
            },
        ]
    }
    fn no_switchpoint_check(p: &WalPosition, a: &[AncestorEnd]) -> bool {
        a.iter().any(|a| a.timeline == p.timeline)
    }
    fn one_past_switchpoint(p: &WalPosition, a: &[AncestorEnd]) -> bool {
        a.iter()
            .any(|a| a.timeline == p.timeline && p.lsn <= a.switchpoint + 1)
    }
    fn any_older_timeline(p: &WalPosition, a: &[AncestorEnd]) -> bool {
        a.iter().any(|a| p.lsn <= a.switchpoint)
    }
    /// (label, candidate, quorum, peers, history, granted)
    type PrefixRow = (
        &'static str,
        WalPosition,
        usize,
        Vec<WalPosition>,
        Vec<AncestorEnd>,
        bool,
    );
    fn prefix_table() -> Vec<PrefixRow> {
        let c3 = pos("node2", 3, 0x50370E0);
        vec![
            (
                "r2: voter at the switchpoint counts",
                c3.clone(),
                1,
                vec![pos("node3", 2, S2)],
                r2_history(),
                true,
            ),
            (
                "voter below the switchpoint counts",
                c3.clone(),
                1,
                vec![pos("node3", 2, S2 - 0x10)],
                r2_history(),
                true,
            ),
            (
                "grandparent timeline below its end counts",
                c3.clone(),
                1,
                vec![pos("node3", 1, S1)],
                r2_history(),
                true,
            ),
            (
                "one byte past the switchpoint is not counted",
                c3.clone(),
                1,
                vec![pos("node3", 2, S2 + 1)],
                r2_history(),
                false,
            ),
            (
                "far past the switchpoint is not counted",
                c3.clone(),
                1,
                vec![pos("node3", 2, S2 + 0x1000)],
                r2_history(),
                false,
            ),
            (
                "grandparent past its end is not counted",
                c3.clone(),
                1,
                vec![pos("node3", 1, S1 + 1)],
                r2_history(),
                false,
            ),
            (
                "under-reported (really on 3, publishes 2) past S",
                c3.clone(),
                1,
                vec![pos("node3", 2, 0x5037100)],
                r2_history(),
                false,
            ),
            (
                "no history: fail closed",
                c3.clone(),
                1,
                vec![pos("node3", 2, S2)],
                vec![],
                false,
            ),
            (
                "timeline not in the history",
                pos("c", 4, 0x400),
                1,
                vec![pos("node3", 2, 0x200)],
                gap_history(),
                false,
            ),
            (
                "both kinds of votes add up",
                c3.clone(),
                2,
                vec![pos("node3", 2, S2), pos("node4", 3, 0x5037090)],
                r2_history(),
                true,
            ),
        ]
    }
    fn voters_of(c: &WalPosition, peers: &[WalPosition]) -> Names {
        let mut names: Vec<&str> = peers.iter().map(|p| p.name.as_str()).collect();
        names.push(c.name.as_str());
        n(&names)
    }
    #[test]
    fn prefix_rule_table() {
        for (label, c, q, peers, hist, granted) in prefix_table() {
            let d = quorum_vote_decision_with_history(&c, &voters_of(&c, &peers), q, &peers, &hist);
            assert_eq!(
                matches!(d, VoteDecision::Granted { .. }),
                granted,
                "{label}: {d:?}"
            );
        }
    }
    #[test]
    fn prefix_rule_keeps_every_other_refusal() {
        let c3 = pos("node2", 3, 0x50370E0);
        let v = n(&["node2", "node3", "node4"]);
        let hist = r2_history();
        // A higher timeline still refuses, even next to a prefix voter.
        let d = quorum_vote_decision_with_history(
            &c3,
            &v,
            1,
            &[pos("node3", 2, S2), pos("node4", 4, 1)],
            &hist,
        );
        assert_eq!(
            d,
            VoteDecision::Refused(VoteRefusal::HigherTimeline {
                peer: "node4".into()
            })
        );
        // A same-timeline voter ahead still refuses.
        let d = quorum_vote_decision_with_history(
            &c3,
            &v,
            1,
            &[pos("node3", 2, S2), pos("node4", 3, 0x5037200)],
            &hist,
        );
        assert_eq!(
            d,
            VoteDecision::Refused(VoteRefusal::BehindPeer {
                peer: "node4".into()
            })
        );
        // A writable primary still refuses.
        let mut primary = pos("node4", 2, S2);
        primary.in_recovery = false;
        let d =
            quorum_vote_decision_with_history(&c3, &v, 1, &[pos("node3", 2, S2), primary], &hist);
        assert!(matches!(
            d,
            VoteDecision::Refused(VoteRefusal::PrimaryAlive { .. })
        ));
        // A non-voter on an ancestor timeline is not counted.
        let d = quorum_vote_decision_with_history(
            &c3,
            &n(&["node2", "node4"]),
            1,
            &[pos("node3", 2, S2)],
            &hist,
        );
        assert_eq!(
            d,
            VoteDecision::Refused(VoteRefusal::InsufficientVotes { got: 0, need: 1 })
        );
        // Without history the old rule is unchanged.
        assert_eq!(
            quorum_vote_decision(&c3, &v, 1, &[pos("node3", 2, S2)]),
            VoteDecision::Refused(VoteRefusal::InsufficientVotes { got: 0, need: 1 })
        );
    }
    /// Each mutation of the older-timeline test grants a row the real rule
    /// refuses: the table would catch it.
    #[test]
    fn prefix_rule_mutations_are_caught() {
        let mutants: [(&str, PrefixTest); 3] = [
            ("no switchpoint check", no_switchpoint_check),
            ("LSN past the switchpoint counted", one_past_switchpoint),
            ("non-ancestor timeline counted", any_older_timeline),
        ];
        for (what, m) in mutants {
            let caught = prefix_table()
                .into_iter()
                .any(|(_, c, q, peers, hist, granted)| {
                    let d = decide(&c, &voters_of(&c, &peers), q, &peers, &hist, m);
                    matches!(d, VoteDecision::Granted { .. }) != granted
                });
            assert!(caught, "mutation not caught: {what}");
        }
        // And the real test passes the same table through the same seam.
        for (label, c, q, peers, hist, granted) in prefix_table() {
            let d = decide(&c, &voters_of(&c, &peers), q, &peers, &hist, holds_a_prefix);
            assert_eq!(
                matches!(d, VoteDecision::Granted { .. }),
                granted,
                "{label}"
            );
        }
    }
    #[test]
    fn recovery_vote_table() {
        use VoteRefusal::*;
        let v = n(&["a", "b", "c", "d", "e"]);
        let lost = n(&["d", "e"]);
        let a = pos("a", 2, 100);
        let ok = [pos("b", 2, 100), pos("c", 2, 90)];
        assert_eq!(
            recovery_vote_decision(&a, &v, &lost, &ok, &[]),
            VoteDecision::Granted { votes: 2 }
        );
        // candidate named lost, or not a voter
        assert_eq!(
            recovery_vote_decision(&a, &v, &n(&["a"]), &ok, &[]),
            VoteDecision::Refused(NotVoter)
        );
        assert_eq!(
            recovery_vote_decision(&pos("z", 2, 1), &v, &lost, &ok, &[]),
            VoteDecision::Refused(NotVoter)
        );
        // `lost` names a non-voter
        assert_eq!(
            recovery_vote_decision(&a, &v, &n(&["d", "x"]), &ok, &[]),
            VoteDecision::Refused(NotVoter)
        );
        // a writable primary refuses, named or not, voter or not
        for who in ["b", "d", "z"] {
            let mut p = ok.to_vec();
            p.push(WalPosition {
                in_recovery: false,
                ..pos(who, 2, 100)
            });
            assert_eq!(
                recovery_vote_decision(&a, &v, &lost, &p, &[]),
                VoteDecision::Refused(PrimaryAlive { peer: who.into() }),
                "{who}"
            );
        }
        // higher timeline refuses, named or not
        for who in ["b", "d"] {
            let mut p = ok.to_vec();
            p.push(pos(who, 3, 1));
            assert_eq!(
                recovery_vote_decision(&a, &v, &lost, &p, &[]),
                VoteDecision::Refused(HigherTimeline { peer: who.into() }),
                "{who}"
            );
        }
        // ahead on the same timeline refuses, also a returned named voter
        for who in ["c", "e"] {
            let mut p = ok.to_vec();
            p.push(pos(who, 2, 101));
            assert_eq!(
                recovery_vote_decision(&a, &v, &lost, &p, &[]),
                VoteDecision::Refused(BehindPeer { peer: who.into() }),
                "{who}"
            );
        }
        // a named voter that answers level or lower is ignored
        let mut p = ok.to_vec();
        p.extend([pos("d", 2, 100), pos("e", 1, 5)]);
        assert_eq!(
            recovery_vote_decision(&a, &v, &lost, &p, &[]),
            VoteDecision::Granted { votes: 2 }
        );
        // a missing non-named voter refuses and nothing from the named
        // voters stands in for it
        assert_eq!(
            recovery_vote_decision(&a, &v, &lost, &[pos("b", 2, 100), pos("d", 2, 100)], &[]),
            VoteDecision::Refused(InsufficientVotes { got: 1, need: 2 })
        );
        // a non-named voter on an older timeline is not counted
        assert_eq!(
            recovery_vote_decision(&a, &v, &lost, &[pos("b", 2, 100), pos("c", 1, 90)], &[]),
            VoteDecision::Refused(InsufficientVotes { got: 1, need: 2 })
        );
        // the candidate's own answer is ignored, answers from non-voters too
        let mut p = ok.to_vec();
        p.extend([pos("a", 9, 9), pos("z", 9, 999)]);
        assert_eq!(
            recovery_vote_decision(&a, &v, &lost, &p, &[]),
            VoteDecision::Granted { votes: 2 }
        );
    }
    #[test]
    fn recovery_vote_with_empty_lost_needs_every_other_voter() {
        let v = n(&["a", "b", "c"]);
        let a = pos("a", 1, 10);
        assert_eq!(
            recovery_vote_decision(&a, &v, &n(&[]), &[pos("b", 1, 10)], &[]),
            VoteDecision::Refused(VoteRefusal::InsufficientVotes { got: 1, need: 2 })
        );
        assert_eq!(
            recovery_vote_decision(&a, &v, &n(&[]), &[pos("b", 1, 10), pos("c", 1, 1)], &[]),
            VoteDecision::Granted { votes: 2 }
        );
    }
    #[test]
    fn recovery_vote_with_everyone_else_lost_needs_no_peer() {
        let v = n(&["a", "b", "c"]);
        assert_eq!(
            recovery_vote_decision(&pos("a", 1, 10), &v, &n(&["b", "c"]), &[], &[]),
            VoteDecision::Granted { votes: 0 }
        );
    }
    /// SB-37 PR B2: the recovery vote counts an older-timeline voter by the
    /// same prefix rule as the normal vote (before it never counted one).
    #[test]
    fn recovery_vote_applies_the_prefix_rule_like_the_vote() {
        let v = n(&["a", "b", "c", "d"]);
        let anc = [AncestorEnd {
            timeline: 1,
            switchpoint: 100,
        }];
        let a = pos("a", 2, 150);
        let lost = n(&["d"]);
        for (lsn, counts) in [(99, true), (100, true), (101, false)] {
            let peers = [pos("b", 2, 120), pos("c", 1, lsn), pos("d", 1, 100)];
            let granted = matches!(
                recovery_vote_decision(&a, &v, &lost, &peers, &anc),
                VoteDecision::Granted { votes: 2 }
            );
            assert_eq!(granted, counts, "c at {lsn}");
            assert_eq!(
                standing(&a, &v, &peers[1], &anc, holds_a_prefix) == Standing::Counts,
                counts
            );
            // Without the history file: not counted, fail closed.
            assert!(!matches!(
                recovery_vote_decision(&a, &v, &lost, &peers, &[]),
                VoteDecision::Granted { .. }
            ));
        }
        // A named voter that is a prefix neither blocks nor counts.
        let peers = [pos("b", 2, 120), pos("c", 2, 1), pos("d", 1, 50)];
        assert_eq!(
            recovery_vote_decision(&a, &v, &lost, &peers, &anc),
            VoteDecision::Granted { votes: 2 }
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
