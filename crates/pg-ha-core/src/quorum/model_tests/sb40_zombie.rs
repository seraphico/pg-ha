//! SB-40 model: a zombie primary that keeps committing, and the voter fence.
//!
//! The main model (`model_tests.rs`) assumes the old leader stops acknowledging
//! before the votes are read (SI-3). A primary whose pg-ha hangs while
//! PostgreSQL stays writable (watchdog off) breaks that: it keeps collecting
//! acks from every standby that still streams from it, also after another node
//! was promoted. This module checks that case exhaustively (breadth-first over
//! every interleaving, no random sampling) on small clusters.
//!
//! # Abstraction
//!
//! * One zombie `Z` (its pg-ha is hung, its PostgreSQL always accepts commits)
//!   and `m` standbys, `m = nodes - 1` (3 nodes: 2, 5 nodes: 4). `V` = `S` = all
//!   standbys, `n` = the `ANY n` number, `q` the DCS quorum, `q + n >= m`
//!   (SI-1) and `q <= m - 1` (the candidate is a voter and not its own vote).
//! * `Z`'s log is a counter. Standby `i` holds a prefix `lsn[i]` of it and
//!   receives it one entry at a time, only while it streams from `Z`.
//! * Commit `k` is acknowledged once `n` standbys that stream from `Z` right
//!   now (alive, not fenced) hold entry `k`. `acked` is the highest such `k`
//!   ever reached. Acknowledgement is never undone.
//! * Standby modes: `Streaming` (from `Z`), `Fenced` (walreceiver stopped and
//!   `primary_conninfo` no longer points at `Z`; durable, it survives a restart),
//!   `Following` (the new primary) and `Primary`.
//! * An election has a candidate; each other voter performs `Fence`, `ReadLsn`
//!   and `Send` as separate steps, so any order and any interleaving with
//!   commits is explored. `Grant` runs the REAL `quorum_vote_decision` on the
//!   answers sent so far, `Promote` freezes the candidate's log
//!   (`promo_lsn`). After promotion `Repoint(i)` moves a standby to the new
//!   primary (the follow loop). Standbys die and revive (a revived standby
//!   keeps its mode: a fence is a configuration change, not a connection).
//!
//! # Property checked after every step
//!
//! **No acknowledged commit is lost:** once a node is promoted,
//! `acked <= promo_lsn`, forever (also while the zombie keeps committing).
//!
//! # Protocols
//!
//! * `Current`: the voter reads its LSN and answers, nothing else.
//! * `Fix`: the voter first fences, only when the fence is confirmed reads its
//!   LSN, then answers. No confirmation, no answer.
//! * Mutations, each must produce a counterexample: `VoteBeforeFence`,
//!   `ReadBeforeFence` (LSN read, then fence, stale LSN sent),
//!   `FenceUnconfirmed` (a failed fence is ignored), `FenceSome` (only some
//!   voters fence: at most `q - 1`), `TransientFence` (the walreceiver comes back by itself).
//!
//! # Liveness (the fix must not make a normal failover impossible)
//!
//! Checked with the fence assumed to succeed (the fence-failure case is its own
//! test). From EVERY reachable state before a promotion (including states left
//! by abandoned elections: candidate died, vote refused, vote granted to a
//! candidate that never promoted), if the candidate plus `q` voters are alive,
//! a greedy election (the live node with the highest LSN, everyone answers) is
//! granted, promotes, and every live standby ends up following it. If the old
//! leader comes back (`LeaderBack`), every live fenced voter can stream from it
//! again. If voters cannot fence, a promotion is reachable exactly when
//! `m - |F| >= q` (`F` = voters that cannot fence, the candidate may be one of
//! them), and the safety property still holds: no leader rather than a lost
//! commit.
use super::*;
use std::collections::{HashMap, VecDeque};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
enum Mode {
    Streaming,
    Fenced,
    Following,
    Primary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Proto {
    Current,
    Fix,
    VoteBeforeFence,
    ReadBeforeFence,
    FenceUnconfirmed,
    FenceSome,
    TransientFence,
}

const MUTANTS: [Proto; 5] = [
    Proto::VoteBeforeFence,
    Proto::ReadBeforeFence,
    Proto::FenceUnconfirmed,
    Proto::FenceSome,
    Proto::TransientFence,
];

#[derive(Debug, Clone, Copy)]
struct Cfg {
    m: usize,
    n: usize,
    q: usize,
    proto: Proto,
    max_commits: u8,
    /// Voters whose fence attempt always fails (bit i = standby i). Mapped
    /// into the state (`St::ff`) so symmetric states can be merged.
    fence_fail: u8,
    /// The old leader may come back and the lock expire again.
    leader_back: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct St {
    zl: u8,
    lsn: Vec<u8>,
    mode: Vec<Mode>,
    alive: Vec<bool>,
    /// A fence attempt was made in this round (success or silent failure).
    tried: Vec<bool>,
    /// LSN read for this round.
    rep: Vec<Option<u8>>,
    /// Answer delivered to the candidate.
    sent: Vec<bool>,
    cand: Option<usize>,
    granted: bool,
    lock_free: bool,
    /// (candidate, its log length when it promoted)
    promoted: Option<(usize, u8)>,
    acked: u8,
    /// Voters whose fence attempt always fails (constant).
    ff: Vec<bool>,
}

impl St {
    fn new(m: usize, fence_fail: u8) -> Self {
        St {
            zl: 0,
            lsn: vec![0; m],
            mode: vec![Mode::Streaming; m],
            alive: vec![true; m],
            tried: vec![false; m],
            rep: vec![None; m],
            sent: vec![false; m],
            cand: None,
            granted: false,
            lock_free: true,
            promoted: None,
            acked: 0,
            ff: (0..m).map(|i| fence_fail & (1 << i) != 0).collect(),
        }
    }
    /// Standbys are interchangeable: sort them to merge symmetric states.
    fn canon(&self) -> St {
        let m = self.lsn.len();
        let key = |i: usize| {
            (
                self.lsn[i],
                self.mode[i] as u8,
                self.alive[i],
                self.tried[i],
                self.rep[i],
                self.sent[i],
                self.ff[i],
            )
        };
        let mut order: Vec<usize> = (0..m).collect();
        order.sort_by_key(|&i| key(i));
        let pos = |old: usize| order.iter().position(|&o| o == old).unwrap();
        St {
            zl: self.zl,
            lsn: order.iter().map(|&i| self.lsn[i]).collect(),
            mode: order.iter().map(|&i| self.mode[i]).collect(),
            alive: order.iter().map(|&i| self.alive[i]).collect(),
            tried: order.iter().map(|&i| self.tried[i]).collect(),
            rep: order.iter().map(|&i| self.rep[i]).collect(),
            sent: order.iter().map(|&i| self.sent[i]).collect(),
            cand: self.cand.map(pos),
            granted: self.granted,
            lock_free: self.lock_free,
            promoted: self.promoted.map(|(c, l)| (pos(c), l)),
            acked: self.acked,
            ff: order.iter().map(|&i| self.ff[i]).collect(),
        }
    }
    fn reset_round(&mut self) {
        let m = self.lsn.len();
        self.tried = vec![false; m];
        self.rep = vec![None; m];
        self.sent = vec![false; m];
        self.granted = false;
    }
}

fn voter_names(m: usize) -> Names {
    (0..m).map(|i| format!("n{i}")).collect()
}

fn refresh(s: &mut St, n: usize) {
    for k in (1..=s.zl).rev() {
        let holders = (0..s.lsn.len())
            .filter(|&i| s.alive[i] && s.mode[i] == Mode::Streaming && s.lsn[i] >= k)
            .count();
        if holders >= n {
            s.acked = s.acked.max(k);
            break;
        }
    }
}

/// `FenceSome` is an off-by-one: at most `q - 1` voters fence, the others answer
/// without fencing. (The number of fenced voters is read from the state, which
/// keeps the rule symmetric between voters.)
fn fences(cfg: &Cfg, s: &St, _v: usize) -> bool {
    match cfg.proto {
        Proto::Current => false,
        Proto::FenceSome => {
            s.mode.iter().filter(|m| **m == Mode::Fenced).count() < cfg.q.saturating_sub(1)
        }
        _ => true,
    }
}

fn may_read(cfg: &Cfg, s: &St, v: usize) -> bool {
    match cfg.proto {
        Proto::Current | Proto::VoteBeforeFence | Proto::ReadBeforeFence => true,
        Proto::FenceUnconfirmed => s.tried[v],
        // Voters that were not asked to fence answer anyway.
        Proto::FenceSome => true,
        Proto::Fix | Proto::TransientFence => s.mode[v] == Mode::Fenced,
    }
}

fn may_send(cfg: &Cfg, s: &St, v: usize) -> bool {
    match cfg.proto {
        Proto::ReadBeforeFence => s.mode[v] == Mode::Fenced,
        Proto::Fix | Proto::TransientFence => s.mode[v] == Mode::Fenced,
        _ => true,
    }
}

fn succ(cfg: &Cfg, s: &St) -> Vec<(String, St)> {
    let mut out: Vec<(String, St)> = Vec::new();
    let mut push = |label: String, mut t: St| {
        refresh(&mut t, cfg.n);
        out.push((label, t));
    };
    // The zombie keeps committing.
    if s.zl < cfg.max_commits {
        let mut t = s.clone();
        t.zl += 1;
        push("Commit".into(), t);
    }
    for i in 0..cfg.m {
        if s.alive[i] && s.mode[i] == Mode::Streaming && s.lsn[i] < s.zl {
            let mut t = s.clone();
            t.lsn[i] += 1;
            push(format!("Advance({i})"), t);
        }
        if s.mode[i] != Mode::Primary {
            let mut t = s.clone();
            t.alive[i] = !t.alive[i];
            let l = if s.alive[i] { "Kill" } else { "Revive" };
            push(format!("{l}({i})"), t);
        }
        if cfg.proto == Proto::TransientFence && s.alive[i] && s.mode[i] == Mode::Fenced {
            let mut t = s.clone();
            t.mode[i] = Mode::Streaming;
            push(format!("Reconnect({i})"), t);
        }
        // After a promotion the follow loop re-points every standby.
        if let Some((c, _)) = s.promoted
            && i != c
            && s.alive[i]
            && matches!(s.mode[i], Mode::Streaming | Mode::Fenced)
        {
            let mut t = s.clone();
            t.mode[i] = Mode::Following;
            push(format!("Repoint({i})"), t);
        }
        // The old leader holds the lock again: fenced voters stream from it.
        if s.promoted.is_none() && !s.lock_free && s.alive[i] && s.mode[i] == Mode::Fenced {
            let mut t = s.clone();
            t.mode[i] = Mode::Streaming;
            push(format!("Reattach({i})"), t);
        }
    }
    if cfg.leader_back && s.promoted.is_none() {
        let mut t = s.clone();
        if s.lock_free {
            t.lock_free = false;
            t.cand = None;
            t.reset_round();
            push("LeaderBack".into(), t);
        } else {
            t.lock_free = true;
            push("LockExpire".into(), t);
        }
    }
    if s.lock_free && s.promoted.is_none() {
        for c in 0..cfg.m {
            if s.alive[c] && matches!(s.mode[c], Mode::Streaming | Mode::Fenced) {
                let mut t = s.clone();
                t.cand = Some(c);
                t.reset_round();
                push(format!("StartElection({c})"), t);
            }
        }
    }
    if let Some(c) = s.cand {
        for v in 0..cfg.m {
            if v == c || !s.alive[v] {
                continue;
            }
            if s.mode[v] == Mode::Streaming
                && fences(cfg, s, v)
                && (s.lock_free || s.promoted.is_some())
                && !s.tried[v]
            {
                // Fence attempt. Success flips the mode; a failure is silent.
                if !s.ff[v] {
                    let mut t = s.clone();
                    t.mode[v] = Mode::Fenced;
                    t.tried[v] = true;
                    push(format!("Fence({v})"), t);
                }
                if cfg.proto == Proto::FenceUnconfirmed {
                    let mut t = s.clone();
                    t.tried[v] = true;
                    push(format!("FenceFailsSilently({v})"), t);
                }
            }
            // Mutations that fence after the answer was read or sent.
            if matches!(cfg.proto, Proto::VoteBeforeFence | Proto::ReadBeforeFence)
                && s.mode[v] == Mode::Streaming
                && s.rep[v].is_some()
                && (s.lock_free || s.promoted.is_some())
                && !s.tried[v]
                && !s.ff[v]
            {
                let mut t = s.clone();
                t.mode[v] = Mode::Fenced;
                t.tried[v] = true;
                push(format!("LateFence({v})"), t);
            }
            if s.promoted.is_none() && s.lock_free {
                if s.rep[v].is_none() && may_read(cfg, s, v) {
                    let mut t = s.clone();
                    t.rep[v] = Some(s.lsn[v]);
                    push(format!("ReadLsn({v})"), t);
                }
                if s.rep[v].is_some() && !s.sent[v] && may_send(cfg, s, v) {
                    let mut t = s.clone();
                    t.sent[v] = true;
                    push(format!("Send({v})"), t);
                }
            }
        }
        if s.alive[c] && s.lock_free && s.promoted.is_none() {
            if !s.granted {
                let candidate = WalPosition {
                    name: format!("n{c}"),
                    in_recovery: true,
                    timeline: 1,
                    lsn: s.lsn[c] as u64,
                };
                let peers: Vec<WalPosition> = (0..cfg.m)
                    .filter(|&v| v != c && s.sent[v])
                    .map(|v| WalPosition {
                        name: format!("n{v}"),
                        in_recovery: true,
                        timeline: 1,
                        lsn: s.rep[v].unwrap() as u64,
                    })
                    .collect();
                let d = quorum_vote_decision(&candidate, &voter_names(cfg.m), cfg.q, &peers);
                if matches!(d, VoteDecision::Granted { .. }) {
                    let mut t = s.clone();
                    t.granted = true;
                    push(format!("Grant({c})"), t);
                }
            } else {
                let mut t = s.clone();
                t.promoted = Some((c, s.lsn[c]));
                t.mode[c] = Mode::Primary;
                push(format!("Promote({c})"), t);
            }
        }
    }
    out
}

fn violated(s: &St) -> Option<String> {
    let (c, pl) = s.promoted?;
    (s.acked > pl).then(|| {
        format!(
            "acknowledged commit {} is not on the new primary n{c} (log length {pl})",
            s.acked
        )
    })
}

struct Outcome {
    states: usize,
    trace: Option<Vec<String>>,
    promoted_reached: bool,
    all: Vec<St>,
}

fn explore(cfg: &Cfg) -> Outcome {
    let init = St::new(cfg.m, cfg.fence_fail);
    let mut all = vec![init.clone()];
    let mut parent: Vec<(usize, String)> = vec![(usize::MAX, String::new())];
    let mut seen: HashMap<St, usize> = HashMap::new();
    seen.insert(init.canon(), 0);
    let mut queue = VecDeque::from([0usize]);
    let mut promoted_reached = false;
    while let Some(i) = queue.pop_front() {
        let s = all[i].clone();
        if s.promoted.is_some() {
            promoted_reached = true;
        }
        if let Some(why) = violated(&s) {
            let mut labels = vec![format!("VIOLATION: {why}")];
            let mut j = i;
            while parent[j].0 != usize::MAX {
                labels.push(parent[j].1.clone());
                j = parent[j].0;
            }
            labels.reverse();
            return Outcome {
                states: all.len(),
                trace: Some(labels),
                promoted_reached,
                all,
            };
        }
        for (label, t) in succ(cfg, &s) {
            let key = t.canon();
            if let std::collections::hash_map::Entry::Vacant(e) = seen.entry(key) {
                e.insert(all.len());
                all.push(t);
                parent.push((i, label));
                queue.push_back(all.len() - 1);
            }
        }
    }
    Outcome {
        states: all.len(),
        trace: None,
        promoted_reached,
        all,
    }
}

/// All `(n, q)` with `q + n >= m` and `q <= m - 1`. Without `full`, 5 nodes
/// only run the settled value `q = m - n` (what the planner publishes); the
/// larger `q` of the full matrix are in the `#[ignore]`d `full_matrix`.
fn configs(m: usize, full: bool) -> Vec<(usize, usize)> {
    let mut v = Vec::new();
    for n in 1..=m {
        for q in (m - n)..m {
            if full || m == 2 || q == m - n {
                v.push((n, q));
            }
        }
    }
    v
}

fn cfg(m: usize, n: usize, q: usize, proto: Proto) -> Cfg {
    Cfg {
        m,
        n,
        q,
        proto,
        // One commit is enough on 5 nodes (the loss is a commit acknowledged
        // after the promotion); `five_nodes_two_commits` runs 2 (slow).
        max_commits: if m == 2 { 3 } else { 1 },
        fence_fail: 0,
        leader_back: false,
    }
}

fn loses(m: usize, n: usize, q: usize, proto: Proto) -> Option<Vec<String>> {
    explore(&cfg(m, n, q, proto)).trace
}

/// The repro of SB-40 in the model: with the current protocol an acknowledged
/// commit is lost exactly when `n < m`, the zombie can still get `n` acks from
/// the voters that answered and kept streaming. With `n = m` the candidate's own
/// departure already stops the acks.
#[test]
fn current_protocol_loses_acked_commits_with_a_zombie() {
    current_protocol_loses_acked_commits_with_a_zombie_impl(false);
}

fn current_protocol_loses_acked_commits_with_a_zombie_impl(full: bool) {
    for m in [2usize, 4] {
        for (n, q) in configs(m, full) {
            let t = loses(m, n, q, Proto::Current);
            if n < m {
                assert!(
                    t.is_some(),
                    "nodes={} n={n} q={q}: the model must reproduce the loss",
                    m + 1
                );
            } else {
                assert!(
                    t.is_none(),
                    "nodes={} n={n} q={q}: n = m cannot lose: {t:?}",
                    m + 1
                );
            }
        }
    }
}

#[test]
fn fix_never_loses_an_acked_commit_with_a_zombie() {
    fix_never_loses_an_acked_commit_with_a_zombie_impl(false);
}

fn fix_never_loses_an_acked_commit_with_a_zombie_impl(full: bool) {
    for m in [2usize, 4] {
        for (n, q) in configs(m, full) {
            let o = explore(&cfg(m, n, q, Proto::Fix));
            assert!(
                o.trace.is_none(),
                "nodes={} n={n} q={q}: {:?}",
                m + 1,
                o.trace
            );
            assert!(o.promoted_reached, "nodes={} n={n} q={q}", m + 1);
        }
    }
}

#[test]
fn every_mutation_of_the_fix_is_caught() {
    every_mutation_of_the_fix_is_caught_impl(false);
}

fn every_mutation_of_the_fix_is_caught_impl(full: bool) {
    for proto in MUTANTS {
        for m in [2usize, 4] {
            for (n, q) in configs(m, full) {
                if n == m {
                    continue;
                }
                assert!(
                    loses(m, n, q, proto).is_some(),
                    "{proto:?} nodes={} n={n} q={q} was not caught",
                    m + 1
                );
            }
        }
    }
}

/// Liveness 1 and 2: the fix keeps a normal failover possible from every
/// reachable pre-promotion state, and the old leader coming back is survivable.
#[test]
fn fix_failover_stays_possible_from_every_state() {
    fix_failover_stays_possible_from_every_state_impl(false);
}

fn fix_failover_stays_possible_from_every_state_impl(full: bool) {
    for m in [2usize, 4] {
        for (n, q) in configs(m, full) {
            let mut c = cfg(m, n, q, Proto::Fix);
            c.leader_back = m == 2;
            let o = explore(&c);
            assert!(o.trace.is_none(), "m={m} n={n} q={q}: {:?}", o.trace);
            let (mut elected, mut skipped, mut fenced_states, mut back) = (0, 0, 0, 0);
            for s in &o.all {
                if s.promoted.is_some() {
                    continue;
                }
                if !s.lock_free {
                    // The old leader is back: every live fenced voter can
                    // stream from it again, and then acks resume.
                    let mut t = s.clone();
                    for i in 0..m {
                        if t.alive[i] && t.mode[i] == Mode::Fenced {
                            t = step(&c, &t, &format!("Reattach({i})"));
                        }
                    }
                    assert!(
                        (0..m).all(|i| !t.alive[i] || t.mode[i] == Mode::Streaming),
                        "m={m} n={n} q={q}: a live voter is stuck fenced"
                    );
                    back += 1;
                    continue;
                }
                let live: Vec<usize> = (0..m).filter(|&i| s.alive[i]).collect();
                if live.len() < q + 1 {
                    skipped += 1;
                    continue;
                }
                fenced_states += s.mode.contains(&Mode::Fenced) as usize;
                let t = greedy_election(&c, s, &live);
                let (win, _) = t.promoted.expect("promoted");
                assert!(
                    (0..m).all(
                        |i| !t.alive[i] || matches!(t.mode[i], Mode::Following | Mode::Primary)
                    ),
                    "m={m} n={n} q={q}: a live standby does not follow n{win}"
                );
                assert!(violated(&t).is_none());
                elected += 1;
            }
            println!(
                "liveness nodes={} n={n} q={q}: states={} elected={elected} (with fenced voters: {fenced_states}) leader-back={back} skipped(fewer than q+1 alive)={skipped}",
                m + 1,
                o.all.len()
            );
            assert!(elected > 0);
        }
    }
}

fn step(cfg: &Cfg, s: &St, label: &str) -> St {
    succ(cfg, s)
        .into_iter()
        .find(|(l, _)| l == label)
        .unwrap_or_else(|| panic!("step {label} not enabled in {s:?}"))
        .1
}

fn greedy_election(cfg: &Cfg, s: &St, live: &[usize]) -> St {
    let c = *live
        .iter()
        .filter(|&&i| matches!(s.mode[i], Mode::Streaming | Mode::Fenced))
        .max_by_key(|&&i| (s.lsn[i], std::cmp::Reverse(i)))
        .expect("a live candidate");
    let mut t = step(cfg, s, &format!("StartElection({c})"));
    for &v in live.iter().filter(|&&v| v != c) {
        if t.mode[v] == Mode::Streaming {
            t = step(cfg, &t, &format!("Fence({v})"));
        }
        t = step(cfg, &t, &format!("ReadLsn({v})"));
        t = step(cfg, &t, &format!("Send({v})"));
    }
    t = step(cfg, &t, &format!("Grant({c})"));
    t = step(cfg, &t, &format!("Promote({c})"));
    for &v in live.iter().filter(|&&v| v != c) {
        t = step(cfg, &t, &format!("Repoint({v})"));
    }
    t
}

/// Liveness 3: voters whose fence cannot be confirmed are not counted. A
/// promotion is reachable exactly when `m - |F| >= q` (for non-empty `F` the
/// candidate is one of the failing voters), and safety holds either way.
#[test]
fn fix_with_unfenceable_voters_is_safe_and_live_exactly_when_enough_can_fence() {
    fix_with_unfenceable_voters_is_safe_and_live_exactly_when_enough_can_fence_impl(false);
}

fn fix_with_unfenceable_voters_is_safe_and_live_exactly_when_enough_can_fence_impl(full: bool) {
    for m in [2usize, 4] {
        for (n, q) in configs(m, full) {
            for k in 0..=m {
                let mut c = cfg(m, n, q, Proto::Fix);
                c.fence_fail = ((1u16 << k) - 1) as u8;
                let o = explore(&c);
                assert!(o.trace.is_none(), "m={m} n={n} q={q} F={k}: {:?}", o.trace);
                let expect = if k == 0 { true } else { m - k >= q };
                assert_eq!(
                    o.promoted_reached, expect,
                    "m={m} n={n} q={q} failing fences={k}"
                );
            }
        }
    }
}

/// Prints the shortest counterexamples quoted in `evidence/model.md`
/// (`cargo test -p pg-ha-core sb40_print -- --nocapture --ignored`).
/// Slow: every `(n, q)` of the matrix on 2 and 5 nodes.
/// `cargo test --release -p pg-ha-core full_matrix -- --ignored`
#[test]
#[ignore]
fn full_matrix() {
    current_protocol_loses_acked_commits_with_a_zombie_impl(true);
    fix_never_loses_an_acked_commit_with_a_zombie_impl(true);
    every_mutation_of_the_fix_is_caught_impl(true);
    fix_failover_stays_possible_from_every_state_impl(true);
    fix_with_unfenceable_voters_is_safe_and_live_exactly_when_enough_can_fence_impl(true);
}

/// Slow (minutes in debug): the 5-node safety runs with 2 commits.
/// `cargo test --release -p pg-ha-core five_nodes_two_commits -- --ignored`
#[test]
#[ignore]
fn five_nodes_two_commits() {
    for (n, q) in configs(4, true) {
        let mut c = cfg(4, n, q, Proto::Fix);
        c.max_commits = 2;
        let o = explore(&c);
        println!("n={n} q={q} states={}", o.states);
        assert!(o.trace.is_none(), "n={n} q={q}: {:?}", o.trace);
        for p in MUTANTS {
            let mut c = cfg(4, n, q, p);
            c.max_commits = 2;
            assert!(n == 4 || explore(&c).trace.is_some(), "{p:?} n={n} q={q}");
        }
    }
}

#[test]
#[ignore]
fn sb40_print_counterexamples() {
    for (m, n, q, p) in [
        (2, 1, 1, Proto::Current),
        (4, 2, 2, Proto::Current),
        (2, 1, 1, Proto::VoteBeforeFence),
        (2, 1, 1, Proto::ReadBeforeFence),
        (2, 1, 1, Proto::FenceUnconfirmed),
        (2, 1, 1, Proto::FenceSome),
        (2, 1, 1, Proto::TransientFence),
    ] {
        let o = explore(&cfg(m, n, q, p));
        println!(
            "== {p:?} nodes={} n={n} q={q} states={}\n  {}",
            m + 1,
            o.states,
            o.trace.map(|t| t.join("\n  ")).unwrap_or("none".into())
        );
    }
    for m in [2usize, 4] {
        for (n, q) in configs(m, true) {
            for p in [Proto::Current, Proto::Fix] {
                let o = explore(&cfg(m, n, q, p));
                println!(
                    "nodes={} n={n} q={q} {p:?}: states={} lost={}",
                    m + 1,
                    o.states,
                    o.trace.is_some()
                );
            }
        }
    }
}
