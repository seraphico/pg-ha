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
//!   construction. A zombie primary that keeps acknowledging is NOT modelled.
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
    stats: Stats,
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
            stats: Stats::default(),
        }
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
        let mgr = SyncManager::new(SyncMode::Quorum, self.cfg_n, self.cfg_strict);
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
        QuorumTarget::from_selection(&sel)
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
        let leader = name(self.leader);
        if !self.st.si1_holds_excluding(&leader) {
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
        self.finalize_dcs()?;
        let target = self.target();
        let caught = self.caught_up();
        let steps = plan(&self.st, &target, &caught, 16);
        for step in steps.into_iter().take(limit) {
            let before = self.st.clone();
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
        }
        Ok(())
    }

    fn elect(&mut self, c: usize, reach: u8, late: u8, fail: u8) -> Result<(), String> {
        if !self.leader_dead {
            return Ok(());
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
                self.promote(c)
            }
        }
    }

    fn promote(&mut self, c: usize) -> Result<(), String> {
        let old = self.leader;
        let post = post_promotion_state(&self.st, &name(c), &name(old))
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
    let mut w = World::new(standbys, n0, steady);
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
    use proptest::test_runner::{Config, TestRunner};
    let total = Mutex::new(Stats::default());
    let mut runner = TestRunner::new(Config {
        cases: CASES,
        ..Config::default()
    });
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
            let steps = plan(&st, &target, &caught, 16);
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
        for step in plan(&st, &target, &caught, 16) {
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
