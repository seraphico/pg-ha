//! Machine check of the SB-13 safety argument (design sections 1.2, 3, 4, 9.1).
//!
//! The model drives the REAL `quorum::{plan, post_promotion_state,
//! quorum_vote_decision}` and the REAL `SyncManager::select_sync_standbys`
//! through random histories, and checks against ground truth that an
//! acknowledged commit is never lost by an election that the vote rule grants.
//!
//! # What is modelled
//!
//! * 2..=5 standbys + a leader. Each node has a real log of `(id, timeline)`
//!   entries; its LSN is the log length and its timeline the last entry's.
//!   Standbys receive a prefix of the leader's log in random-sized chunks.
//!   A (re)attaching standby first rewinds to the common prefix with the
//!   leader (pg_rewind), so unacknowledged tails are discarded.
//! * A commit is *acknowledged* once at least `n` live, attached members of the
//!   effective `S` hold it (`n = 0` while `S` is empty, i.e. asynchronous). An
//!   unsatisfiable `n` (strict mode, `n > |S|`) never acknowledges.
//! * Standbys die and come back (they keep their log); the leader dies.
//! * Config changes: `synchronous_node_count`, strict, `maximum_lag_on_syncnode`
//!   and an excluded (`nosync`) set, applied through the real `SyncManager`.
//! * The planner runs on every cycle with the ground-truth `caught_up` set; a
//!   random PREFIX of its steps executes (a crash between any two steps).
//! * Elections: the dead leader's successor is a random live standby that
//!   asks the live nodes through the REAL `round::collect_wal_positions` with a
//!   fake `PeerWalProbe` (paused tokio time, no real sleeping). Per voter the
//!   world scripts: unreachable, answering at once, answering after the 2 s
//!   per-request limit (a late answer), or failing with HTTP 503. Late and
//!   failed voters must be absent from the peers list. The real vote rule
//!   then decides. A granted
//!   candidate is promoted through `post_promotion_state` (pre-promote
//!   `ALTER SYSTEM` first, DCS update later as a separate action), starting a
//!   new timeline. Many generations are chained.
//! * Re-marking at the current primary LSN after a pg-ha restart.
//!
//! # What is checked after EVERY action
//!
//! * **SI-1** on the state in force (leader excluded from the union).
//! * **Invariant I** (the truth behind SI-1..SI-4): for every acknowledged
//!   commit X, `|V ∖ holders(X)| <= q`, with `V` minus the sitting leader. This
//!   is exactly "any `q+1` voters contain a holder of X".
//! * **End to end**: every candidate the vote rule grants holds every
//!   acknowledged commit.
//!
//! # What is NOT modelled
//!
//! * `ALTER SYSTEM` and `reload` are one atomic step (the design's file-vs-
//!   effective split is covered only by reasoning that each step's pre- and
//!   post-state satisfy SI-1, checked here per step).
//! * Network: reads are instantaneous and consistent; there is no staleness, so
//!   SI-3 (read after the old leader stopped acknowledging) is true by
//!   construction. A zombie primary that keeps acknowledging is NOT modelled
//!   here; `model_tests/sb40_zombie.rs` models it (SB-40).
//! * Real PostgreSQL semantics (`ANY` vs `FIRST` equivalence, sync-rep waiting
//!   details), real LSN byte arithmetic, REST, DCS CAS races, two leaders.
//! * Voter-set sizes above 5, and multiple simultaneous failures of the DCS.

use super::*;
use crate::cluster::{Member, MemberRole, MemberState};
use crate::postgresql::{ReplicaRow, ReplicationSnapshot};
use crate::sync::{SyncManager, SyncMode};
use async_trait::async_trait;
use proptest::prelude::*;
use round::{
    PEER_REQUEST_TIMEOUT, PeerProbeError, PeerTarget, PeerWalProbe, VOTE_ROUND_TIMEOUT,
    collect_wal_positions,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

mod executor_model;
mod sb36_fixes;
mod sb37_prefix;
mod sb37_recovery;
mod sb38_leave;
mod sb40_zombie;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Entry {
    id: u64,
    tl: u64,
}

#[derive(Debug, Clone)]
struct Node {
    alive: bool,
    /// Streaming from the sitting leader (its log is a prefix of the leader's).
    attached: bool,
    log: Vec<Entry>,
}

impl Node {
    fn lsn(&self) -> u64 {
        self.log.len() as u64
    }
    fn tl(&self) -> u64 {
        self.log.last().map(|e| e.tl).unwrap_or(0)
    }
    fn holds(&self, id: u64) -> bool {
        self.log.iter().any(|e| e.id == id)
    }
}

#[derive(Debug, Clone)]
enum Action {
    Commit(usize),
    Advance(usize, usize),
    Plan(usize),
    Finalize,
    Restart,
    Kill(usize),
    Revive(usize),
    KillLeader,
    /// Candidate, `reach` (voters the candidate can reach), `late` (reachable
    /// voters that answer after the per-request limit), `fail` (reachable
    /// voters answering HTTP 503). `late` wins over `fail`.
    Elect(usize, u8, u8, u8),
    Reconfig {
        n: usize,
        strict: bool,
        lag: Option<u64>,
        excluded: u8,
    },
}

#[derive(Debug, Clone, Default)]
struct Stats {
    commits: usize,
    acked: usize,
    plan_steps: usize,
    relax_steps: usize,
    elections_granted: usize,
    elections_refused: usize,
    refused_insufficient: usize,
    refused_behind: usize,
    refused_other: usize,
    refused_higher_timeline: usize,
    elections_with_missing_answers: usize,
    refused_only_for_missing_answers: usize,
    refused_with_acked_missing: usize,
    generations: usize,
    /// Promotions where `/sync.leader` differed from the real previous leader.
    stale_promotions: usize,
    /// SB-36 fix 1: enabling DCS writes (V, |V|-n) made while `S` was empty.
    enable_writes: usize,
    /// SB-36 fix 1: granted elections while the enabling window was open.
    granted_in_enable_window: usize,
    /// SB-36 fix 2: planner runs where an offline voter was kept in the target.
    kept_offline: usize,
    /// SB-38: planner steps that left `/sync` at `q >= |V|` (nobody electable)
    /// while quorum was being switched off and the hand-off had not happened.
    leave_vote_impossible: usize,
    /// SB-38: hand-offs to the Priority/async code that completed.
    handed_off: usize,
    /// SB-38: elections by the async rule (highest WAL) after the hand-off.
    async_elections: usize,
}

/// SB-36 switches. All off = the model of the code before the fixes, so every
/// existing test is unchanged. The mutation tests flip them the wrong way.
#[derive(Debug, Clone, Copy, Default)]
struct Fixes {
    /// Fix 1: from an empty `S`, write DCS `(V*, |V*|-n*)` first, then PG.
    enable_dcs_first: bool,
    /// Mutation (a) of fix 1: PG `ANY n* (V*)` first, then DCS.
    enable_pg_first: bool,
    /// Mutation of fix 1: the enabling `q` is one too low.
    enable_q_too_low: bool,
    /// Fix 2: an offline voter whose member record has not expired stays in `V*`.
    keep_offline: bool,
    /// SB-38: order of the writes when `synchronous_mode` leaves quorum.
    /// false = the order of the code today (planner nobody-eligible branch and
    /// steps F then G); true = the proposed order (see `sb38_leave.rs`).
    leave_new: bool,
    /// SB-38 mutation of the new order (sync): remove from `V` without the
    /// SI-4 gate (`q` straight to the target value).
    leave_no_gate: bool,
    /// SB-38 mutation of the new order (async): empty `/sync` before PG.
    leave_dcs_first: bool,
    /// SB-38 (maintainer decision): the async hand-off `(V = {}, q = 0)` waits
    /// until every voter is caught up to the mark taken after PG went
    /// asynchronous (`|V \ caught_up| == 0`). false = hand-off ungated (the
    /// code before this decision), which can lose an acknowledged commit.
    leave_gate: bool,
}

/// The `synchronous_mode` the planner is driving toward (SB-38).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum Tm {
    #[default]
    Quorum,
    Async,
    Sync,
}

impl Fixes {
    fn enabling(&self) -> bool {
        self.enable_dcs_first || self.enable_pg_first
    }
}

impl Stats {
    fn add(&mut self, o: &Stats) {
        self.commits += o.commits;
        self.acked += o.acked;
        self.plan_steps += o.plan_steps;
        self.relax_steps += o.relax_steps;
        self.elections_granted += o.elections_granted;
        self.elections_refused += o.elections_refused;
        self.refused_insufficient += o.refused_insufficient;
        self.refused_behind += o.refused_behind;
        self.refused_other += o.refused_other;
        self.refused_higher_timeline += o.refused_higher_timeline;
        self.elections_with_missing_answers += o.elections_with_missing_answers;
        self.refused_only_for_missing_answers += o.refused_only_for_missing_answers;
        self.refused_with_acked_missing += o.refused_with_acked_missing;
        self.generations += o.generations;
        self.stale_promotions += o.stale_promotions;
        self.enable_writes += o.enable_writes;
        self.granted_in_enable_window += o.granted_in_enable_window;
        self.kept_offline += o.kept_offline;
        self.leave_vote_impossible += o.leave_vote_impossible;
        self.handed_off += o.handed_off;
        self.async_elections += o.async_elections;
    }
}

/// How one voter behaves when the candidate asks for its live position.
#[derive(Clone)]
enum Script {
    Immediate,
    /// Answers after the per-request limit, so the round counts it as a timeout.
    Late,
    Fail(PeerProbeError),
}

/// Fake network for the vote round: answers come from the model's ground truth.
struct ModelProbe {
    voters: HashMap<String, (Script, WalPosition)>,
}

#[async_trait]
impl PeerWalProbe for ModelProbe {
    async fn read(&self, target: &PeerTarget) -> Result<WalPosition, PeerProbeError> {
        let (script, position) = self.voters.get(&target.name).cloned().expect("scripted");
        match script {
            Script::Immediate => Ok(position),
            Script::Late => {
                tokio::time::sleep(PEER_REQUEST_TIMEOUT + Duration::from_millis(500)).await;
                Ok(position)
            }
            Script::Fail(e) => Err(e),
        }
    }
}

struct World {
    /// Paused-clock runtime that drives the async vote round, one per run.
    rt: tokio::runtime::Runtime,
    nodes: Vec<Node>,
    leader: usize,
    leader_dead: bool,
    /// DCS `(V, q)` and the effective PG `(S, n)`.
    st: QuorumState,
    /// DCS update still owed after a promotion (`V'`, `q`).
    pending_dcs: Option<(Names, usize)>,
    /// Leader log length when `synchronous_standby_names` last changed.
    mark: Option<usize>,
    /// Commits on the sitting leader not yet acknowledged: (position, id).
    waiting: Vec<(usize, u64)>,
    /// Ground truth: acknowledged commit ids.
    acked: Vec<u64>,
    next_id: u64,
    next_tl: u64,
    // config
    cfg_n: usize,
    cfg_strict: bool,
    cfg_lag: Option<u64>,
    cfg_excluded: u8,
    /// DCS `/sync.leader`: the last node that finalised its own `/sync`.
    sync_leader: usize,
    /// Production only knows `/sync.leader`: pass it, not the real previous
    /// leader, as `old_leader` of `post_promotion_state`.
    stale_old_leader: bool,
    /// A granted election stops before `promote` and records the candidate in
    /// `granted` (the executor-in-model drives the pre-promote write itself).
    hold_promotion: bool,
    granted: Option<usize>,
    stats: Stats,
    /// SB-36 switches (all off outside `sb36_fixes`).
    fix: Fixes,
    /// Bit `i % 8`: node `i` is offline and its member record has expired.
    expired: u8,
    /// SB-38: the mode being driven toward (default quorum: nothing changes).
    mode: Tm,
    /// SB-38: the hand-off to the Priority/async code happened. The quorum
    /// guarantee ends there (commits under the new mode follow its rules).
    handed_off: bool,
    /// SB-38: the asynchronous election after the hand-off already happened.
    async_elected: bool,
}

fn name(i: usize) -> String {
    format!("n{i}")
}

impl World {
    fn new(standbys: usize, n0: usize, steady: bool) -> World {
        let total = standbys + 1;
        let base = vec![Entry { id: 0, tl: 1 }];
        let nodes = (0..total)
            .map(|_| Node {
                alive: true,
                attached: true,
                log: base.clone(),
            })
            .collect();
        let names: Vec<String> = (1..total).map(name).collect();
        let st = if steady {
            let n = n0.min(standbys).max(1);
            QuorumState::new(names.clone(), standbys - n, names, n)
        } else {
            QuorumState::new::<[&str; 0], [&str; 0], _, _>([], 0, [], 0)
        };
        World {
            rt: tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .start_paused(true)
                .build()
                .expect("paused runtime"),
            nodes,
            leader: 0,
            leader_dead: false,
            st,
            pending_dcs: None,
            mark: Some(1),
            waiting: Vec::new(),
            acked: vec![0],
            next_id: 1,
            next_tl: 2,
            cfg_n: n0,
            cfg_strict: false,
            cfg_lag: None,
            cfg_excluded: 0,
            sync_leader: 0,
            stale_old_leader: false,
            hold_promotion: false,
            granted: None,
            stats: Stats::default(),
            fix: Fixes::default(),
            expired: 0,
            mode: Tm::Quorum,
            handed_off: false,
            async_elected: false,
        }
    }

    /// SB-38: `/sync` and PG have reached the state the Priority/async code
    /// takes over from (T7: `V = S = {}`, `q = 0`; T9: `q = 0`, `V = S`,
    /// `|S| = n`).
    fn leave_done(&self) -> bool {
        let st = &self.st;
        match self.mode {
            Tm::Quorum => false,
            Tm::Async => st.voters.is_empty() && st.pg_names.is_empty() && st.quorum == 0,
            Tm::Sync => {
                st.quorum == 0
                    && !st.pg_names.is_empty()
                    && st.voters == st.pg_names
                    && st.pg_names.len() == st.effective_n()
            }
        }
    }

    /// SB-38: PG is asynchronous while `/sync` still holds `V` with `q < |V|`,
    /// on the way out of quorum. Commits here are not covered; the commits
    /// acknowledged earlier still are (invariant I stays checked).
    fn leaving_window(&self) -> bool {
        self.mode != Tm::Quorum
            && self.fix.enabling()
            && self.st.pg_names.is_empty()
            && !self.st.voters.is_empty()
            && self.st.quorum < self.st.voters.len()
    }

    /// SB-36: `S` is empty while `V` is non-empty with `q < |V|`. The cluster
    /// is asynchronous, quorum is not in force in PG yet, and commits here are
    /// not covered (maintainer decision).
    fn enabling_window(&self) -> bool {
        self.fix.enabling()
            && self.mode == Tm::Quorum
            && self.st.pg_names.is_empty()
            && !self.st.voters.is_empty()
            && self.st.quorum < self.st.voters.len()
    }

    fn idx(&self, x: usize) -> usize {
        x % self.nodes.len()
    }

    fn leader_alive(&self) -> bool {
        !self.leader_dead && self.nodes[self.leader].alive
    }

    // ---- derived inputs for the code under test --------------------------

    fn target(&self) -> QuorumTarget {
        let members: Vec<Member> = (0..self.nodes.len())
            .filter(|&i| i != self.leader)
            .map(|i| {
                let mut tags = HashMap::new();
                if self.cfg_excluded & (1 << (i % 8)) != 0 {
                    tags.insert("nosync".to_string(), serde_json::json!(true));
                }
                Member {
                    name: name(i),
                    conn_url: String::new(),
                    api_url: String::new(),
                    state: MemberState::Running,
                    role: MemberRole::Replica,
                    wal_position: Some(self.nodes[i].lsn()),
                    timeline: Some(self.nodes[i].tl()),
                    tags,
                    version: None,
                    rewind_check: false,
                    lineage: None,
                    watchdog_failed: false,
                }
            })
            .collect();
        let snapshot = ReplicationSnapshot {
            primary_flush_lsn: self.nodes[self.leader].lsn(),
            replicas: (0..self.nodes.len())
                .filter(|&i| i != self.leader && self.nodes[i].alive && self.nodes[i].attached)
                .map(|i| ReplicaRow {
                    application_name: name(i),
                    flush_lsn: self.nodes[i].lsn(),
                })
                .collect(),
        };
        if self.mode == Tm::Async {
            return QuorumTarget::new::<[&str; 0], _>([], 0);
        }
        let sm = if self.mode == Tm::Sync {
            SyncMode::Priority
        } else {
            SyncMode::Quorum
        };
        let mgr = SyncManager::new(sm, self.cfg_n, self.cfg_strict);
        let voters: Vec<String> = self.st.voters.iter().cloned().collect();
        let sel = mgr
            .select_sync_standbys(
                &members,
                &name(self.leader),
                &snapshot,
                self.cfg_lag,
                &voters,
            )
            .expect("quorum mode is never Off");
        let mut target = QuorumTarget::from_selection(&sel);
        if self.fix.keep_offline && !target.voters.is_empty() {
            // Fix 2: a voter that is offline but whose member record is still
            // there stays in V*; only expiry (or being alive) lets it go.
            for v in &self.st.voters {
                let i: usize = v[1..].parse().unwrap();
                let offline = !(self.nodes[i].alive && self.nodes[i].attached);
                if i != self.leader && offline && self.expired & (1 << (i % 8)) == 0 {
                    target.voters.insert(v.clone());
                }
            }
        }
        target
    }

    fn caught_up(&self) -> Names {
        let Some(mark) = self.mark else {
            return Names::new();
        };
        (0..self.nodes.len())
            .filter(|&i| i != self.leader)
            .filter(|&i| {
                self.nodes[i].alive && self.nodes[i].attached && self.nodes[i].log.len() >= mark
            })
            .map(name)
            .collect()
    }

    // ---- acknowledgement and invariants ----------------------------------

    fn refresh_acks(&mut self) {
        if !self.leader_alive() {
            return;
        }
        if self.fix.enabling() && self.st.pg_names.is_empty() {
            // Asynchronous: nothing committed now is covered by the guarantee,
            // which starts when quorum is in force in PG.
            self.waiting.clear();
            return;
        }
        let n = self.st.effective_n();
        let waiting = std::mem::take(&mut self.waiting);
        for (pos, id) in waiting {
            let holders = (0..self.nodes.len())
                .filter(|&i| i != self.leader)
                .filter(|&i| {
                    let nd = &self.nodes[i];
                    nd.alive
                        && nd.attached
                        && self.st.pg_names.contains(&name(i))
                        && nd.log.get(pos).map(|e| e.id) == Some(id)
                })
                .count();
            if holders >= n {
                self.acked.push(id);
                self.stats.acked += 1;
            } else {
                self.waiting.push((pos, id));
            }
        }
    }

    fn check(&self, ctx: &str) -> Result<(), String> {
        if self.handed_off || self.enabling_window() {
            return Ok(());
        }
        if self.fix.enabling() && self.st.effective_n() > 0 && self.st.voters.is_empty() {
            return Err(format!(
                "quorum in force in PG while DCS /sync has no voters {ctx}: {:?}",
                self.st
            ));
        }
        let leader = name(self.leader);
        if !self.leaving_window() && !self.st.si1_holds_excluding(&leader) {
            return Err(format!(
                "SI-1 violated {ctx}: {:?} (leader {leader})",
                self.st
            ));
        }
        let readable: Vec<&String> = self.st.voters.iter().filter(|v| **v != leader).collect();
        for &x in &self.acked {
            let missing = readable
                .iter()
                .filter(|v| {
                    let i: usize = v[1..].parse().unwrap();
                    !self.nodes[i].holds(x)
                })
                .count();
            if missing > self.st.quorum {
                return Err(format!(
                    "invariant I violated {ctx}: acked commit {x} is missing on {missing} voters of {:?} > q={} (state {:?})",
                    readable, self.st.quorum, self.st
                ));
            }
        }
        Ok(())
    }

    // ---- actions ----------------------------------------------------------

    fn finalize_dcs(&mut self) -> Result<(), String> {
        if let Some((v, q)) = self.pending_dcs.take() {
            self.sync_leader = self.leader;
            self.st.voters = v;
            self.st.quorum = q;
            self.check("after promotion DCS update")?;
        }
        Ok(())
    }

    fn apply(&mut self, a: &Action) -> Result<(), String> {
        match a {
            Action::Commit(k) => {
                if self.leader_alive() {
                    for _ in 0..*k {
                        let e = Entry {
                            id: self.next_id,
                            tl: self.nodes[self.leader].tl(),
                        };
                        self.next_id += 1;
                        let l = self.leader;
                        self.nodes[l].log.push(e);
                        self.waiting.push((self.nodes[l].log.len() - 1, e.id));
                        self.stats.commits += 1;
                    }
                }
            }
            Action::Advance(i, k) => {
                let i = self.idx(*i);
                if self.leader_alive() && i != self.leader && self.nodes[i].alive {
                    let (lead_log, _) = (self.nodes[self.leader].log.clone(), ());
                    let node = &mut self.nodes[i];
                    if !node.attached {
                        let common = node
                            .log
                            .iter()
                            .zip(lead_log.iter())
                            .take_while(|(a, b)| a == b)
                            .count();
                        node.log.truncate(common);
                        node.attached = true;
                    }
                    let have = node.log.len();
                    let take = (*k).min(lead_log.len() - have);
                    node.log.extend_from_slice(&lead_log[have..have + take]);
                }
            }
            Action::Plan(limit) => self.run_planner(*limit)?,
            Action::Finalize => {
                if self.leader_alive() {
                    self.finalize_dcs()?;
                }
            }
            Action::Restart => {
                if self.leader_alive() {
                    // pg-ha restarted: no remembered mark, take a fresh one at
                    // the current primary flush LSN (always safe).
                    self.mark = Some(self.nodes[self.leader].log.len());
                }
            }
            Action::Kill(i) => {
                let i = self.idx(*i);
                if i != self.leader {
                    self.nodes[i].alive = false;
                    self.nodes[i].attached = false;
                }
            }
            Action::Revive(i) => {
                let i = self.idx(*i);
                if i != self.leader {
                    self.nodes[i].alive = true;
                    self.expired &= !(1 << (i % 8));
                }
            }
            Action::KillLeader => {
                if self.leader_alive() {
                    self.leader_dead = true;
                    self.nodes[self.leader].alive = false;
                    self.waiting.clear();
                }
            }
            Action::Elect(c, reach, late, fail) => self.elect(*c, *reach, *late, *fail)?,
            Action::Reconfig {
                n,
                strict,
                lag,
                excluded,
            } => {
                self.cfg_n = *n;
                self.cfg_strict = *strict;
                self.cfg_lag = *lag;
                self.cfg_excluded = *excluded;
            }
        }
        self.refresh_acks();
        self.check(&format!("after {a:?}"))
    }

    fn run_planner(&mut self, limit: usize) -> Result<(), String> {
        if !self.leader_alive() {
            return Ok(());
        }
        if self.handed_off {
            return Ok(());
        }
        self.finalize_dcs()?;
        let target = self.target();
        if self.fix.keep_offline
            && target.voters.iter().any(|v| {
                let i: usize = v[1..].parse().unwrap();
                !(self.nodes[i].alive && self.nodes[i].attached)
            })
        {
            self.stats.kept_offline += 1;
        }
        let caught = self.caught_up();
        let steps = self.plan_steps(&target, &caught);
        for step in steps.into_iter().take(limit) {
            let before = self.st.clone();
            if self.fix.enabling()
                && before.pg_names.is_empty()
                // The async hand-off is not an enabling write: `acked` stays.
                && self.mode != Tm::Async
                && matches!(step, Step::SetDcs { .. })
            {
                // The guarantee restarts when quorum is in force in PG.
                self.acked.clear();
                self.stats.enable_writes += 1;
            }
            self.st.apply(&step);
            self.stats.plan_steps += 1;
            match &step {
                Step::SetPg { .. } => self.mark = Some(self.nodes[self.leader].log.len()),
                Step::SetDcs { quorum, .. } if *quorum < before.quorum => {
                    self.stats.relax_steps += 1;
                }
                _ => {}
            }
            self.refresh_acks();
            self.check(&format!("after step {step:?} from {before:?}"))?;
            if self.mode != Tm::Quorum
                && !self.st.voters.is_empty()
                && self.st.quorum >= self.st.voters.len()
            {
                self.stats.leave_vote_impossible += 1;
            }
            if self.leave_done() {
                // The Priority/async code takes over; the quorum guarantee ends.
                // `acked` is kept: the async election below checks it.
                self.handed_off = true;
                self.stats.handed_off += 1;
                break;
            }
        }
        Ok(())
    }

    /// The planner, plus the SB-36 enabling rule when a fix switch is on.
    /// From an empty `S` (asynchronous) with something to enable, the first two
    /// steps are `SetDcs (V*, q*)` and `SetPg (V*, n*)` where `q*` is the
    /// settled `|V*| - n*` of `QuorumTarget::quorum`; everything after that is
    /// the unchanged planner.
    fn plan_steps(&mut self, target: &QuorumTarget, caught: &Names) -> Vec<Step> {
        if self.mode != Tm::Quorum {
            return if self.fix.leave_new {
                plan_leave(&self.st, target, caught, 16, self.fix)
            } else {
                plan(&self.st, target, caught, 16)
            };
        }
        let mutated = self.fix.enable_pg_first || self.fix.enable_q_too_low;
        if mutated && self.st.pg_names.is_empty() && !target.voters.is_empty() {
            let q = target
                .quorum()
                .saturating_sub(usize::from(self.fix.enable_q_too_low));
            let dcs = Step::SetDcs {
                voters: target.voters.clone(),
                quorum: q,
            };
            let pg = Step::SetPg {
                names: target.voters.clone(),
                n: target.n,
            };
            let dcs_done = self.st.voters == target.voters && self.st.quorum == q;
            let mut steps = Vec::new();
            if self.fix.enable_pg_first {
                steps.push(pg);
                steps.push(dcs);
            } else {
                if !dcs_done {
                    steps.push(dcs);
                }
                steps.push(pg);
            }
            return steps;
        }
        if self.fix.enable_dcs_first {
            plan(&self.st, target, caught, 16)
        } else {
            plan_without_enabling(&self.st, target, caught, 16)
        }
    }

    /// SB-38: after an async hand-off (`/sync` empty, no vote) the candidate
    /// that holds the highest WAL among the live, eligible nodes wins. Every
    /// commit acknowledged while quorum was in force must be on it. Sync
    /// hand-offs keep the Priority rules and are not checked here.
    fn elect_async(&mut self, c: usize) -> Result<(), String> {
        if self.mode != Tm::Async {
            return Ok(());
        }
        let c = self.idx(c);
        let eligible = |w: &World, i: usize| {
            i != w.leader && w.nodes[i].alive && w.cfg_excluded & (1 << (i % 8)) == 0
        };
        if !eligible(self, c) {
            return Ok(());
        }
        let key = |w: &World, i: usize| (w.nodes[i].tl(), w.nodes[i].lsn());
        let best = (0..self.nodes.len())
            .filter(|&i| eligible(self, i))
            .map(|i| key(self, i))
            .max();
        if best != Some(key(self, c)) {
            return Ok(());
        }
        self.stats.async_elections += 1;
        if let Some(&lost) = self.acked.iter().find(|&&x| !self.nodes[c].holds(x)) {
            return Err(format!(
                "LOST ACKNOWLEDGED COMMIT {lost} after the async hand-off: {} wins by highest WAL \
                 (state {:?}, acked {:?})",
                name(c),
                self.st,
                self.acked
            ));
        }
        self.async_elected = true;
        Ok(())
    }

    fn elect(&mut self, c: usize, reach: u8, late: u8, fail: u8) -> Result<(), String> {
        if !self.leader_dead || self.async_elected {
            return Ok(());
        }
        if self.handed_off {
            return self.elect_async(c);
        }
        let c = self.idx(c);
        if c == self.leader || !self.nodes[c].alive {
            return Ok(());
        }
        let wal = |i: usize| WalPosition {
            name: name(i),
            in_recovery: true,
            timeline: self.nodes[i].tl(),
            lsn: self.nodes[i].lsn(),
        };
        // Every live node but the candidate is asked, as the real vote does for
        // each voter that has a running member record.
        let mut voters_net = HashMap::new();
        let mut targets = Vec::new();
        let mut expected: Vec<String> = Vec::new();
        let mut would_answer: Vec<WalPosition> = Vec::new();
        let mut missing_answers = false;
        for j in 0..self.nodes.len() {
            if j == c || j == self.leader || !self.nodes[j].alive {
                continue;
            }
            let bit = 1u8 << (j % 8);
            let script = if reach & bit == 0 {
                Script::Fail(PeerProbeError::Transport("unreachable".into()))
            } else if late & bit != 0 {
                missing_answers = true;
                would_answer.push(wal(j));
                Script::Late
            } else if fail & bit != 0 {
                missing_answers = true;
                would_answer.push(wal(j));
                Script::Fail(PeerProbeError::Status(503))
            } else {
                expected.push(name(j));
                would_answer.push(wal(j));
                Script::Immediate
            };
            voters_net.insert(name(j), (script, wal(j)));
            targets.push(PeerTarget {
                name: name(j),
                api_url: format!("http://{}:8008", name(j)),
            });
        }
        let asked = targets.len();
        let probe: Arc<dyn PeerWalProbe> = Arc::new(ModelProbe { voters: voters_net });
        let round = self.rt.block_on(collect_wal_positions(
            probe,
            targets,
            PEER_REQUEST_TIMEOUT,
            VOTE_ROUND_TIMEOUT,
        ));
        let mut got: Vec<String> = round.answers.iter().map(|p| p.name.clone()).collect();
        got.sort();
        expected.sort();
        if got != expected || round.failures.len() != asked - expected.len() {
            return Err(format!(
                "vote round returned answers {got:?} (failures {:?}), expected exactly {expected:?} of {asked} asked",
                round.failures
            ));
        }
        if missing_answers {
            self.stats.elections_with_missing_answers += 1;
        }
        let peers = round.answers;
        let candidate = wal(c);
        let decision = quorum_vote_decision(&candidate, &self.st.voters, self.st.quorum, &peers);
        match decision {
            VoteDecision::Refused(reason) => {
                self.stats.elections_refused += 1;
                match reason {
                    VoteRefusal::InsufficientVotes { .. } => self.stats.refused_insufficient += 1,
                    VoteRefusal::BehindPeer { .. } => self.stats.refused_behind += 1,
                    VoteRefusal::HigherTimeline { .. } => self.stats.refused_higher_timeline += 1,
                    _ => self.stats.refused_other += 1,
                }
                if matches!(reason, VoteRefusal::InsufficientVotes { .. })
                    && missing_answers
                    && matches!(
                        quorum_vote_decision(
                            &candidate,
                            &self.st.voters,
                            self.st.quorum,
                            &would_answer
                        ),
                        VoteDecision::Granted { .. }
                    )
                {
                    // Only the late or failed voters kept this election from passing.
                    self.stats.refused_only_for_missing_answers += 1;
                }
                if self.acked.iter().any(|&x| !self.nodes[c].holds(x)) {
                    // The rule refused a candidate that would have lost data.
                    self.stats.refused_with_acked_missing += 1;
                }
                Ok(())
            }
            VoteDecision::Granted { .. } => {
                self.stats.elections_granted += 1;
                if self.enabling_window() {
                    self.stats.granted_in_enable_window += 1;
                }
                if let Some(&lost) = self.acked.iter().find(|&&x| !self.nodes[c].holds(x)) {
                    return Err(format!(
                        "LOST ACKNOWLEDGED COMMIT {lost}: candidate {} granted by the vote rule \
                         (state {:?}, peers read {:?}, candidate log len {})",
                        name(c),
                        self.st,
                        peers,
                        self.nodes[c].log.len()
                    ));
                }
                if self.hold_promotion {
                    self.granted = Some(c);
                    return Ok(());
                }
                self.promote(c)
            }
        }
    }

    fn promote(&mut self, c: usize) -> Result<(), String> {
        let old = self.leader;
        let old_for_state = if self.stale_old_leader {
            self.sync_leader
        } else {
            old
        };
        if old_for_state != old {
            self.stats.stale_promotions += 1;
        }
        let post = post_promotion_state(&self.st, &name(c), &name(old_for_state))
            .ok_or_else(|| format!("granted candidate got no promotion state: {:?}", self.st))?;
        let tl = self.next_tl;
        self.next_tl += 1;
        let id = self.next_id;
        self.next_id += 1;
        self.nodes[c].log.push(Entry { id, tl });
        self.mark = Some(self.nodes[c].log.len());
        // Pre-promote: PG side now, DCS side later (separate action).
        self.st = QuorumState {
            voters: self.st.voters.clone(),
            quorum: self.st.quorum,
            pg_names: post.pg_names.clone(),
            pg_n: post.pg_n,
        };
        self.pending_dcs = Some((post.voters, post.quorum));
        self.leader = c;
        self.leader_dead = false;
        self.waiting.clear();
        for (i, n) in self.nodes.iter_mut().enumerate() {
            if i != c {
                n.attached = false;
            }
        }
        self.nodes[c].attached = true;
        self.stats.generations += 1;
        Ok(())
    }
}

fn action_strategy() -> impl Strategy<Value = Action> {
    prop_oneof![
        6 => (1usize..4).prop_map(Action::Commit),
        10 => (any::<usize>(), 1usize..5).prop_map(|(i, k)| Action::Advance(i, k)),
        8 => (0usize..8).prop_map(Action::Plan),
        2 => Just(Action::Finalize),
        1 => Just(Action::Restart),
        2 => any::<usize>().prop_map(Action::Kill),
        3 => any::<usize>().prop_map(Action::Revive),
        2 => Just(Action::KillLeader),
        5 => (any::<usize>(), any::<u8>(), prop_oneof![3 => Just(0u8), 1 => any::<u8>()], prop_oneof![3 => Just(0u8), 1 => any::<u8>()])
            .prop_map(|(c, m, late, fail)| Action::Elect(c, m, late, fail)),
        2 => (1usize..5, any::<bool>(), prop_oneof![Just(None), (0u64..4).prop_map(Some)], prop_oneof![3 => Just(0u8), 1 => any::<u8>()])
            .prop_map(|(n, strict, lag, excluded)| Action::Reconfig { n, strict, lag, excluded }),
    ]
}

fn run_world(
    standbys: usize,
    n0: usize,
    steady: bool,
    actions: &[Action],
) -> Result<Stats, String> {
    run_world_with(false, standbys, n0, steady, actions)
}

fn run_world_with(
    stale_old_leader: bool,
    standbys: usize,
    n0: usize,
    steady: bool,
    actions: &[Action],
) -> Result<Stats, String> {
    let mut w = World::new(standbys, n0, steady);
    w.stale_old_leader = stale_old_leader;
    for (k, a) in actions.iter().enumerate() {
        w.apply(a).map_err(|e| format!("step {k}: {e}"))?;
    }
    Ok(w.stats)
}

const CASES: u32 = 2500;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(CASES))]

    /// SI-1..SI-4: no acknowledged commit is ever lost by a granted election,
    /// and invariant I holds after every action, across promotions, crashes
    /// between planner steps, config changes and node failures.
    #[test]
    fn acknowledged_commits_survive_every_history(
        standbys in 2usize..6,
        n0 in 1usize..4,
        steady in any::<bool>(),
        actions in prop::collection::vec(action_strategy(), 10..140),
    ) {
        run_world(standbys, n0, steady, &actions).map_err(TestCaseError::fail)?;
    }
}

/// The same property run under a manually driven runner so the test can assert
/// that the generated histories exercise the interesting paths (an
/// invariant-checking model that never elects or relaxes would prove nothing).
#[test]
fn model_actually_exercises_elections_and_relaxation() {
    use proptest::test_runner::{Config, TestRng, TestRunner};
    let total = Mutex::new(Stats::default());
    // Fixed seed: this test asserts coverage MINIMUMS, which are a property of
    // the generator, not of the system. With a random seed a run can land just
    // below a minimum (CI on 2026-11-15 hit pre_idempotent = 5 with a minimum of > 5). The
    // invariant-checking property tests keep their random seeds.
    let config = Config {
        cases: CASES,
        ..Config::default()
    };
    let mut runner = TestRunner::new_with_rng(
        config.clone(),
        TestRng::deterministic_rng(config.rng_algorithm),
    );

    let strat = (
        2usize..6,
        1usize..4,
        any::<bool>(),
        prop::collection::vec(action_strategy(), 10..140),
    );
    runner
        .run(&strat, |(s, n0, steady, actions)| {
            let st = run_world(s, n0, steady, &actions).map_err(TestCaseError::fail)?;
            total.lock().unwrap().add(&st);
            Ok(())
        })
        .unwrap();
    let t = total.lock().unwrap().clone();
    eprintln!("quorum model coverage over {CASES} histories: {t:#?}");
    assert!(t.acked > 5_000, "too few acknowledged commits: {t:?}");
    assert!(t.plan_steps > 5_000, "planner barely ran: {t:?}");
    assert!(t.relax_steps > 500, "q was rarely relaxed: {t:?}");
    assert!(
        t.elections_granted > 500,
        "too few granted elections: {t:?}"
    );
    assert!(t.generations > 500, "too few promotions: {t:?}");
    assert!(t.elections_refused > 1_000, "too few refusals: {t:?}");
    assert!(
        t.refused_higher_timeline > 5,
        "no 'newer timeline' refusals: {t:?}"
    );
    assert!(
        t.elections_with_missing_answers > 200,
        "late or failed voters were rarely exercised: {t:?}"
    );
    assert!(
        t.refused_only_for_missing_answers > 20,
        "no election was refused only because voters answered late: {t:?}"
    );
    assert!(
        t.refused_behind > 100,
        "no 'behind a voter' refusals: {t:?}"
    );
    assert!(
        t.refused_insufficient > 100,
        "no 'not enough votes' refusals: {t:?}"
    );
    // The vote rule must have actually mattered: it refused candidates that
    // were missing an acknowledged commit.
    assert!(
        t.refused_with_acked_missing > 100,
        "the rule never refused a data-losing candidate, the model is too weak: {t:?}"
    );
}

// ---- the stale long-tail candidate (higher-timeline rule) ------------------

/// Four standbys, `ANY 2`, so `V = {n1..n4}`, `q = 2`.
const TAIL_STANDBYS: usize = 4;
const TAIL_N: usize = 2;

/// The history that only the "a voter on a newer timeline refuses" rule can
/// stop. `tail_a + tail_b` commits go to the old leader and ONLY n4 receives
/// them, so they are never acknowledged (`n = 2`) and n4 holds a long tail on
/// timeline 1. The leader dies, n4 dies too, n1 is elected on a NEW timeline
/// (shorter than n4's log), a commit is acknowledged by n2 and n3, that leader
/// dies, and n4 comes back WITHOUT rewind and stands for election with every
/// voter reachable. Its LSN is at least as high as everyone's, so without the
/// timeline rule n2 and n3 would both count as votes and n4 would be granted
/// although it misses the acknowledged commit.
fn stale_tail_actions(tail_a: usize, tail_b: usize) -> Vec<Action> {
    vec![
        Action::Commit(tail_a),
        Action::Commit(tail_b),
        // n4 alone receives the unacknowledged tail.
        Action::Advance(4, 64),
        Action::KillLeader,
        Action::Kill(4),
        // n1 wins on a new timeline with n2 and n3 as its two votes.
        Action::Elect(1, 0xFF, 0, 0),
        Action::Finalize,
        Action::Commit(1),
        // n2 and n3 rewind to the new leader and acknowledge the commit.
        Action::Advance(2, 64),
        Action::Advance(3, 64),
        Action::KillLeader,
        // The stale node returns with its tail intact and asks for the lead.
        Action::Revive(4),
        Action::Elect(4, 0xFF, 0, 0),
    ]
}

/// Targeted prefix with random tail sizes; the normal random suffix follows.
fn stale_tail_prefix() -> impl Strategy<Value = Vec<Action>> {
    (2usize..5, 2usize..5).prop_map(|(a, b)| stale_tail_actions(a, b))
}

#[test]
fn stale_long_tail_candidate_is_refused_by_higher_timeline() {
    let mut w = World::new(TAIL_STANDBYS, TAIL_N, true);
    for (k, a) in stale_tail_actions(3, 3).iter().enumerate() {
        w.apply(a).unwrap_or_else(|e| panic!("step {k}: {e}"));
    }
    // The stale node's log is longer than the new leader's, and it missed the
    // acknowledged commit: the rule is the only thing that stopped it.
    assert!(w.nodes[4].lsn() > w.nodes[1].lsn());
    assert!(w.nodes[4].tl() < w.nodes[1].tl());
    assert_eq!(w.acked.len(), 2, "genesis and the new-timeline commit");
    assert!(w.acked.iter().any(|&x| !w.nodes[4].holds(x)));
    assert_eq!(w.stats.generations, 1, "n4 must not have been promoted");
    assert_eq!(w.stats.elections_granted, 1);
    assert_eq!(w.stats.refused_higher_timeline, 1);
    assert_eq!(w.stats.elections_refused, 1);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(600))]

    /// Same property as the main model, behind the targeted stale-tail prefix.
    #[test]
    fn stale_tail_histories_never_lose_acknowledged_commits(
        prefix in stale_tail_prefix(),
        suffix in prop::collection::vec(action_strategy(), 0..60),
    ) {
        let mut actions = prefix;
        actions.extend(suffix);
        let stats = run_world(TAIL_STANDBYS, TAIL_N, true, &actions).map_err(TestCaseError::fail)?;
        prop_assert!(stats.refused_higher_timeline >= 1, "prefix missed its target: {:?}", stats);
    }
}

// ---- stale `/sync.leader` as `old_leader` (PR3 plan 4.2) -------------------

/// Double failover inside one window: the first new leader dies before it
/// finalises `/sync`, so the second promotion only sees the older
/// `/sync.leader`.
fn double_failover_prefix() -> impl Strategy<Value = Vec<Action>> {
    (1usize..4, 1usize..5, 1usize..5, any::<u8>(), any::<u8>()).prop_map(
        |(commits, c1, c2, adv, reach)| {
            vec![
                Action::Commit(commits),
                Action::Advance(1, adv as usize % 4 + 1),
                Action::Advance(2, adv as usize % 3 + 1),
                Action::Advance(3, adv as usize % 5 + 1),
                Action::KillLeader,
                Action::Elect(c1, 0xFF, 0, 0),
                // no Finalize: the new leader dies before it writes /sync
                Action::KillLeader,
                Action::Elect(c2, reach | 1, 0, 0),
            ]
        },
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(CASES))]

    /// Same property as the main model, with `old_leader` taken from the stale
    /// `/sync.leader` like production does.
    #[test]
    fn stale_sync_leader_random_histories_keep_invariant_i(
        standbys in 2usize..6,
        n0 in 1usize..4,
        steady in any::<bool>(),
        actions in prop::collection::vec(action_strategy(), 10..140),
    ) {
        run_world_with(true, standbys, n0, steady, &actions).map_err(TestCaseError::fail)?;
    }

    /// Targeted: a double failover with no finalisation in between, then the
    /// normal random suffix.
    #[test]
    fn stale_sync_leader_double_failover_keeps_invariant_i(
        standbys in 3usize..6,
        n0 in 1usize..4,
        prefix in double_failover_prefix(),
        suffix in prop::collection::vec(action_strategy(), 0..80),
    ) {
        let mut actions = prefix;
        actions.extend(suffix);
        run_world_with(true, standbys, n0, true, &actions).map_err(TestCaseError::fail)?;
    }
}

/// The stale-leader scenarios must really promote with a stale `old_leader`.
#[test]
fn stale_sync_leader_scenario_is_exercised() {
    use proptest::test_runner::{Config, TestRng, TestRunner};
    let total = Mutex::new(Stats::default());
    // Fixed seed: this test asserts coverage MINIMUMS, which are a property of
    // the generator, not of the system. With a random seed a run can land just
    // below a minimum (CI on 2026-11-15 hit pre_idempotent = 5 with a minimum of > 5). The
    // invariant-checking property tests keep their random seeds.
    let config = Config {
        cases: CASES,
        ..Config::default()
    };
    let mut runner = TestRunner::new_with_rng(
        config.clone(),
        TestRng::deterministic_rng(config.rng_algorithm),
    );

    let strat = (
        3usize..6,
        1usize..4,
        double_failover_prefix(),
        prop::collection::vec(action_strategy(), 0..80),
    );
    runner
        .run(&strat, |(s, n0, prefix, suffix)| {
            let mut actions = prefix;
            actions.extend(suffix);
            let st = run_world_with(true, s, n0, true, &actions).map_err(TestCaseError::fail)?;
            total.lock().unwrap().add(&st);
            Ok(())
        })
        .unwrap();
    let t = total.lock().unwrap().clone();
    eprintln!("stale /sync.leader coverage over {CASES} histories: {t:#?}");
    assert!(
        t.stale_promotions > 100,
        "stale old_leader rarely used: {t:?}"
    );
    assert!(t.generations > 300, "too few promotions: {t:?}");
}

// ---- planner-only properties ---------------------------------------------

fn names_strategy() -> impl Strategy<Value = Names> {
    prop::collection::btree_set(0usize..6, 0..6).prop_map(|s| s.into_iter().map(name).collect())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(4000))]

    /// From any state satisfying SI-1, with any target, repeated cycles
    /// (everyone caught up after each batch) reach the target, every step keeps
    /// SI-1, and `q` only goes down behind the SI-4 gate.
    #[test]
    fn planner_converges_and_every_step_is_safe(
        v in names_strategy(),
        s in names_strategy(),
        n in 0usize..6,
        extra_q in 0usize..3,
        tv in names_strategy(),
        tn in 1usize..6,
    ) {
        let n_eff = if s.is_empty() { 0 } else { n };
        let need = v.union(&s).count().saturating_sub(n_eff);
        let mut st = QuorumState { voters: v, quorum: need + extra_q, pg_names: s, pg_n: n };
        prop_assert!(st.si1_holds());
        let target = if tv.is_empty() { QuorumTarget { voters: tv, n: 0 } } else { QuorumTarget { voters: tv, n: tn } };
        for _ in 0..10 {
            // Everyone listed anywhere is caught up, except that the gate is
            // still checked for the reduced sets below.
            let caught: Names = st.voters.union(&st.pg_names).chain(target.voters.iter()).cloned().collect();
            let steps = plan_without_enabling(&st, &target, &caught, 16);
            if steps.is_empty() { break; }
            for step in &steps {
                let before = st.clone();
                st.apply(step);
                prop_assert!(st.si1_holds(), "SI-1 broken by {:?} from {:?}", step, before);
            }
        }
        prop_assert_eq!(&st.voters, &target.voters);
        prop_assert_eq!(&st.pg_names, &target.voters);
        prop_assert_eq!(st.quorum, target.quorum());
        if !target.voters.is_empty() { prop_assert_eq!(st.pg_n, target.n); }
    }

    /// `q` never drops below what the SI-4 gate allows.
    #[test]
    fn planner_never_relaxes_q_past_the_gate(
        v in names_strategy(),
        s in names_strategy(),
        n in 0usize..6,
        extra_q in 0usize..3,
        tv in names_strategy(),
        tn in 1usize..6,
        caught in names_strategy(),
    ) {
        let n_eff = if s.is_empty() { 0 } else { n };
        let need = v.union(&s).count().saturating_sub(n_eff);
        let st = QuorumState { voters: v, quorum: need + extra_q, pg_names: s, pg_n: n };
        let target = if tv.is_empty() { QuorumTarget { voters: tv, n: 0 } } else { QuorumTarget { voters: tv, n: tn } };
        let mut cur = st.clone();
        let mut gate_set = caught.clone();
        for step in plan_without_enabling(&st, &target, &caught, 16) {
            let before = cur.clone();
            cur.apply(&step);
            match &step {
                Step::SetPg { .. } => gate_set.clear(),
                Step::SetDcs { voters, quorum } if *quorum < before.quorum => {
                    let missing = voters.difference(&gate_set).count();
                    prop_assert!(missing <= *quorum, "relaxed q {} -> {} with {} voters not past the mark", before.quorum, quorum, missing);
                }
                _ => {}
            }
        }
    }

    /// Vote rule: a grant implies every stated condition; refusals are
    /// independent of the order the answers arrived in.
    #[test]
    fn vote_rule_is_characterised_and_order_independent(
        voters in names_strategy(),
        q in 0usize..5,
        my in 0usize..6,
        my_tl in 1u64..4,
        my_lsn in 0u64..8,
        peers in prop::collection::vec((0usize..6, any::<bool>(), 1u64..4, 0u64..8), 0..8),
    ) {
        let me = WalPosition { name: name(my), in_recovery: true, timeline: my_tl, lsn: my_lsn };
        let peers: Vec<WalPosition> = peers.into_iter().map(|(i, rec, tl, lsn)| WalPosition { name: name(i), in_recovery: rec, timeline: tl, lsn }).collect();
        let d = quorum_vote_decision(&me, &voters, q, &peers);
        let mut rev = peers.clone();
        rev.reverse();
        let d2 = quorum_vote_decision(&me, &voters, q, &rev);
        prop_assert_eq!(matches!(d, VoteDecision::Granted { .. }), matches!(d2, VoteDecision::Granted { .. }));
        if let VoteDecision::Granted { votes } = d {
            prop_assert!(voters.contains(&me.name));
            prop_assert!(votes >= q);
            let others: Vec<&WalPosition> = peers.iter().filter(|p| p.name != me.name).collect();
            prop_assert!(others.iter().all(|p| p.in_recovery));
            for p in others.iter().filter(|p| voters.contains(&p.name)) {
                prop_assert!(p.timeline <= me.timeline);
                if p.timeline == me.timeline { prop_assert!(p.lsn <= me.lsn); }
            }
            let counted: Names = others.iter().filter(|p| voters.contains(&p.name) && p.timeline == me.timeline).map(|p| p.name.clone()).collect();
            prop_assert_eq!(counted.len(), votes);
        }
    }

    /// Selection: in Quorum mode (non-strict) the list is every eligible
    /// standby and `q + n = |V|`; Priority lists a prefix of the same set.
    /// This is the property the old `take(count)` truncation broke.
    #[test]
    fn quorum_selection_is_not_truncated(
        lsns in prop::collection::vec(prop::option::of(0u64..50), 1..7),
        nosync in prop::collection::vec(any::<bool>(), 7),
        n in 1usize..5,
    ) {
        let members: Vec<Member> = lsns.iter().enumerate().map(|(i, _)| {
            let mut tags = HashMap::new();
            if nosync[i] { tags.insert("nosync".to_string(), serde_json::json!(true)); }
            Member {
                name: name(i + 1), conn_url: String::new(), api_url: String::new(),
                state: MemberState::Running, role: MemberRole::Replica,
                wal_position: Some(1), timeline: Some(1), tags, version: None,
                rewind_check: false, lineage: None, watchdog_failed: false,
            }
        }).collect();
        let snapshot = ReplicationSnapshot {
            primary_flush_lsn: 100,
            replicas: lsns.iter().enumerate().filter_map(|(i, l)| l.map(|l| ReplicaRow { application_name: name(i + 1), flush_lsn: l })).collect(),
        };
        let eligible: Names = lsns.iter().enumerate().filter(|(i, l)| l.is_some() && !nosync[*i]).map(|(i, _)| name(i + 1)).collect();
        let quorum = SyncManager::new(SyncMode::Quorum, n, false)
            .select_sync_standbys(&members, "n0", &snapshot, None, &[]).unwrap();
        let priority = SyncManager::new(SyncMode::Priority, n, false)
            .select_sync_standbys(&members, "n0", &snapshot, None, &[]).unwrap();
        let qset: Names = quorum.voters().iter().cloned().collect();
        prop_assert_eq!(&qset, &eligible);
        if !eligible.is_empty() {
            prop_assert_eq!(quorum.confirmations(), n.min(eligible.len()));
            prop_assert_eq!(quorum.quorum() + quorum.confirmations(), eligible.len());
            prop_assert_eq!(priority.voters().len(), n.min(eligible.len()));
            prop_assert!(priority.voters().iter().all(|p| eligible.contains(p)));
        }
    }
}

/// SB-38: the planner when `synchronous_mode` leaves quorum, in the proposed
/// order. Runs the real planner's `next_step` for everything except the two
/// places that publish `q >= |V|`:
///
/// * nobody eligible (async): PG goes to async FIRST with `(V, q)` unchanged
///   (`q = |V| - n < |V|`: the vote still passes and invariant I still holds
///   for every commit acknowledged under quorum), then the hand-off write
///   `(V = {}, q = 0)`; no `q = |U|` raise in between.
/// * removing voters (sync): the planner's F (`V*`, `q` unchanged) followed by
///   G (relax behind SI-4) is one write `(V*, min(q, max(q*, |V* \ caught|)))`,
///   and only when that `q'` is `< |V*|`; otherwise wait (`/sync` stays as it
///   is, `q < |V|`).
fn plan_leave(
    state: &QuorumState,
    target: &QuorumTarget,
    caught_up: &Names,
    max_steps: usize,
    fix: Fixes,
) -> Vec<Step> {
    let mut state = state.clone();
    let mut caught_up = caught_up.clone();
    let mut steps = Vec::new();
    while steps.len() < max_steps {
        let Some(step) = next_leave(&state, target, &caught_up, fix) else {
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

fn next_leave(
    state: &QuorumState,
    target: &QuorumTarget,
    caught_up: &Names,
    fix: Fixes,
) -> Option<Step> {
    if !fix.leave_dcs_first && !fix.leave_no_gate {
        if !fix.leave_gate
            && target.voters.is_empty()
            && state.pg_names.is_empty()
            && (!state.voters.is_empty() || state.quorum != 0)
        {
            // Mutation: the hand-off before the gate existed.
            return Some(Step::SetDcs {
                voters: Names::new(),
                quorum: 0,
            });
        }
        // The production planner, including the hand-off gate of the
        // maintainer decision (`|V ∖ caught_up| == 0`).
        return next_step_leaving(state, target, caught_up);
    }
    if target.voters.is_empty() {
        let empty_pg = Step::SetPg {
            names: Names::new(),
            n: 0,
        };
        let hand_off = Step::SetDcs {
            voters: Names::new(),
            quorum: 0,
        };
        if fix.leave_dcs_first {
            // Mutation: `/sync` emptied while PG still has quorum.
            if !state.voters.is_empty() || state.quorum != 0 {
                return Some(hand_off);
            }
            return (!state.pg_names.is_empty()).then_some(empty_pg);
        }
        if !state.pg_names.is_empty() {
            return Some(empty_pg);
        }
        if !state.voters.is_empty() || state.quorum != 0 {
            return Some(hand_off);
        }
        return None;
    }
    let step = next_step(state, target, caught_up)?;
    if let Step::SetDcs { voters, quorum } = &step
        && voters.is_subset(&state.voters)
        && voters.len() < state.voters.len()
        && *quorum == state.quorum
    {
        // The planner's F. Fuse it with G.
        let missing = voters.difference(caught_up).count();
        let q = if fix.leave_no_gate {
            target.quorum().min(state.quorum)
        } else {
            state.quorum.min(target.quorum().max(missing))
        };
        if q >= voters.len() {
            return None;
        }
        return Some(Step::SetDcs {
            voters: voters.clone(),
            quorum: q,
        });
    }
    Some(step)
}
