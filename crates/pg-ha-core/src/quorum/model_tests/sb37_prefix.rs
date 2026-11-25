//! SB-37: machine check of the prefix rule of
//! [`quorum_vote_decision_with_history`] under UNDER-REPORTED timelines.
//!
//! # What is modelled
//!
//! * 3..=5 nodes, every node a voter (`V` = all nodes), `n` in `1..m-1`,
//!   `q = m - 1 - n`. A commit is acknowledged once the live leader and at
//!   least `n` other nodes hold it, so any `q + 1` voters contain a holder
//!   (invariant I of the main model, here by construction).
//! * Each node has a log of `(id, timeline)` entries; its LSN is the log
//!   length. Standbys stream a prefix of the leader's log after a rewind to
//!   the common prefix. Nodes die and come back, the dead leader included (as
//!   a standby with its unreplicated tail, never rewound until it streams).
//! * Promotion is PostgreSQL's: the new timeline begins at the candidate's
//!   LSN `S` and its history file is the parent's plus `(parent, S)`.
//! * D6: only a node whose WAL is on the latest timeline campaigns (pg-ha
//!   checks it before the vote; the override path passes no history).
//! * Every answering voter may UNDER-REPORT its timeline: it publishes the
//!   timeline of any entry at or below its LSN (what the checkpoint, the
//!   receiver memory or `pg_control_recovery()` can show), never a higher one.
//!   That includes a voter really on the candidate's timeline, or really on a
//!   branch of an older timeline past its switchpoint (the dead leader's
//!   tail), that publishes an older timeline.
//! * The REAL `decide` (the vote rule) with the REAL prefix test, or with a
//!   mutated one.
//!
//! # What is checked
//!
//! * **P1**: every granted candidate holds every acknowledged commit.
//! * **P2**: every older-timeline voter the prefix test counts holds a log that
//!   is a prefix of the candidate's log (ground truth), granted or not.
//!
//! # What is NOT modelled
//!
//! The D6 override (a candidate not on the latest timeline; pg-ha then passes
//! no history, so the plain rule applies), network timing (covered by the
//! main model), the executor and `/sync` changes (covered by the main model).
use super::*;
use crate::quorum::{AncestorEnd, PrefixTest, decide, holds_a_prefix};

#[derive(Debug, Clone)]
enum PAction {
    Commit(usize),
    Advance(usize, usize),
    Kill(usize),
    Revive(usize),
    KillLeader,
    /// Candidate, reachable mask, under-report mask, pick.
    Elect(usize, u8, u8, u8),
}

#[derive(Debug, Default, Clone)]
struct PStats {
    acked: u64,
    elections: u64,
    granted: u64,
    /// Granted only because of prefix-rule votes (the plain rule refuses).
    granted_by_prefix: u64,
    /// Older-timeline voters the prefix test counted.
    prefix_votes: u64,
    /// Under-reported answers.
    under_reported: u64,
    /// Under-reported answers of a voter whose log is NOT a prefix of the
    /// candidate's (the dangerous kind) that published an ancestor timeline.
    dangerous_under_reports: u64,
    refused_with_acked_missing: u64,
}

impl PStats {
    fn add(&mut self, o: &PStats) {
        self.acked += o.acked;
        self.elections += o.elections;
        self.granted += o.granted;
        self.granted_by_prefix += o.granted_by_prefix;
        self.prefix_votes += o.prefix_votes;
        self.under_reported += o.under_reported;
        self.dangerous_under_reports += o.dangerous_under_reports;
        self.refused_with_acked_missing += o.refused_with_acked_missing;
    }
}

struct PWorld {
    nodes: Vec<Node>,
    leader: usize,
    leader_dead: bool,
    n: usize,
    q: usize,
    latest_tl: u64,
    histories: HashMap<u64, Vec<AncestorEnd>>,
    acked: Vec<u64>,
    next_id: u64,
    prefix: PrefixTest,
    /// Check P2 (off: only an actual lost commit fails the run).
    check_p2: bool,
    stats: PStats,
}

fn is_prefix(a: &[Entry], b: &[Entry]) -> bool {
    a.len() <= b.len() && a.iter().zip(b).all(|(x, y)| x == y)
}

impl PWorld {
    fn new(m: usize, n: usize, prefix: PrefixTest) -> PWorld {
        let base = vec![Entry { id: 0, tl: 1 }];
        let nodes = (0..m)
            .map(|_| Node {
                alive: true,
                attached: true,
                log: base.clone(),
            })
            .collect();
        let mut histories = HashMap::new();
        histories.insert(1, Vec::new());
        PWorld {
            nodes,
            leader: 0,
            leader_dead: false,
            n,
            q: m - 1 - n,
            latest_tl: 1,
            histories,
            acked: vec![0],
            next_id: 1,
            prefix,
            check_p2: true,
            stats: PStats::default(),
        }
    }

    fn idx(&self, i: usize) -> usize {
        i % self.nodes.len()
    }

    fn voters(&self) -> Names {
        (0..self.nodes.len()).map(name).collect()
    }

    fn refresh_acks(&mut self) {
        if self.leader_dead {
            return;
        }
        let log = self.nodes[self.leader].log.clone();
        for e in log {
            if self.acked.contains(&e.id) {
                continue;
            }
            let holders = (0..self.nodes.len())
                .filter(|&j| j != self.leader && self.nodes[j].holds(e.id))
                .count();
            if holders >= self.n {
                self.acked.push(e.id);
                self.stats.acked += 1;
            }
        }
    }

    fn apply(&mut self, a: &PAction) -> Result<(), String> {
        match a {
            PAction::Commit(k) => {
                if !self.leader_dead {
                    let tl = self.nodes[self.leader].tl();
                    for _ in 0..*k {
                        let id = self.next_id;
                        self.next_id += 1;
                        let l = self.leader;
                        self.nodes[l].log.push(Entry { id, tl });
                    }
                }
            }
            PAction::Advance(i, k) => {
                let i = self.idx(*i);
                if !self.leader_dead && i != self.leader && self.nodes[i].alive {
                    let lead = self.nodes[self.leader].log.clone();
                    let node = &mut self.nodes[i];
                    if !node.attached {
                        let common = node
                            .log
                            .iter()
                            .zip(lead.iter())
                            .take_while(|(a, b)| a == b)
                            .count();
                        node.log.truncate(common);
                        node.attached = true;
                    }
                    let have = node.log.len();
                    let take = (*k).min(lead.len() - have);
                    node.log.extend_from_slice(&lead[have..have + take]);
                }
            }
            PAction::Kill(i) => {
                let i = self.idx(*i);
                if i != self.leader || self.leader_dead {
                    self.nodes[i].alive = false;
                    self.nodes[i].attached = false;
                }
            }
            PAction::Revive(i) => {
                let i = self.idx(*i);
                self.nodes[i].alive = true;
            }
            PAction::KillLeader => {
                if !self.leader_dead {
                    self.leader_dead = true;
                    let l = self.leader;
                    self.nodes[l].alive = false;
                    self.nodes[l].attached = false;
                }
            }
            PAction::Elect(c, reach, under, pick) => self.elect(*c, *reach, *under, *pick)?,
        }
        self.refresh_acks();
        Ok(())
    }

    fn elect(&mut self, c: usize, reach: u8, under: u8, pick: u8) -> Result<(), String> {
        if !self.leader_dead {
            return Ok(());
        }
        let c = self.idx(c);
        if !self.nodes[c].alive || self.nodes[c].tl() != self.latest_tl {
            // Not running, or D6: not on the latest timeline.
            return Ok(());
        }
        self.stats.elections += 1;
        let cand_log = self.nodes[c].log.clone();
        let candidate = WalPosition {
            name: name(c),
            in_recovery: true,
            timeline: self.nodes[c].tl(),
            lsn: self.nodes[c].lsn(),
        };
        let hist = self.histories[&candidate.timeline].clone();
        let mut peers = Vec::new();
        for j in 0..self.nodes.len() {
            if j == c || !self.nodes[j].alive || reach & (1 << (j % 8)) == 0 {
                continue;
            }
            let log = &self.nodes[j].log;
            let timeline = if under & (1 << (j % 8)) != 0 {
                self.stats.under_reported += 1;
                // Any timeline its WAL had at or below its position.
                let k = (pick as usize).wrapping_mul(j + 1) % log.len();
                log[k].tl
            } else {
                self.nodes[j].tl()
            };
            let p = WalPosition {
                name: name(j),
                in_recovery: true,
                timeline,
                lsn: log.len() as u64,
            };
            let prefix_of_candidate = is_prefix(log, &cand_log);
            if timeline < candidate.timeline {
                if timeline != self.nodes[j].tl()
                    && !prefix_of_candidate
                    && hist.iter().any(|a| a.timeline == timeline)
                {
                    self.stats.dangerous_under_reports += 1;
                }
                if (self.prefix)(&p, &hist) {
                    self.stats.prefix_votes += 1;
                    if self.check_p2 && !prefix_of_candidate {
                        return Err(format!(
                            "P2: {} counted by the prefix test (published {p:?}, history {hist:?}) \
                             but its log {log:?} is not a prefix of candidate {}'s {cand_log:?}",
                            name(j),
                            name(c)
                        ));
                    }
                }
            }
            peers.push(p);
        }
        let voters = self.voters();
        let d = decide(&candidate, &voters, self.q, &peers, &hist, self.prefix);
        match d {
            VoteDecision::Refused(_) => {
                if self.acked.iter().any(|&x| !self.nodes[c].holds(x)) {
                    self.stats.refused_with_acked_missing += 1;
                }
                Ok(())
            }
            VoteDecision::Granted { .. } => {
                self.stats.granted += 1;
                if !matches!(
                    quorum_vote_decision(&candidate, &voters, self.q, &peers),
                    VoteDecision::Granted { .. }
                ) {
                    self.stats.granted_by_prefix += 1;
                }
                if let Some(&lost) = self.acked.iter().find(|&&x| !self.nodes[c].holds(x)) {
                    return Err(format!(
                        "P1: LOST ACKNOWLEDGED COMMIT {lost}: {} granted (peers {peers:?}, \
                         history {hist:?}, candidate log {cand_log:?}, nodes {:?})",
                        name(c),
                        self.nodes.iter().map(|n| &n.log).collect::<Vec<_>>()
                    ));
                }
                self.promote(c);
                Ok(())
            }
        }
    }

    fn promote(&mut self, c: usize) {
        let parent = self.nodes[c].tl();
        let s = self.nodes[c].lsn();
        let tl = self.latest_tl + 1;
        self.latest_tl = tl;
        let mut h = self.histories[&parent].clone();
        h.push(AncestorEnd {
            timeline: parent,
            switchpoint: s,
        });
        self.histories.insert(tl, h);
        let id = self.next_id;
        self.next_id += 1;
        self.nodes[c].log.push(Entry { id, tl });
        for (i, node) in self.nodes.iter_mut().enumerate() {
            node.attached = i == c;
        }
        self.leader = c;
        self.leader_dead = false;
    }
}

fn paction_strategy() -> impl Strategy<Value = PAction> {
    prop_oneof![
        6 => (1usize..4).prop_map(PAction::Commit),
        10 => (any::<usize>(), 1usize..5).prop_map(|(i, k)| PAction::Advance(i, k)),
        2 => any::<usize>().prop_map(PAction::Kill),
        3 => any::<usize>().prop_map(PAction::Revive),
        3 => Just(PAction::KillLeader),
        6 => (any::<usize>(), any::<u8>(), prop_oneof![1 => Just(0u8), 2 => any::<u8>(), 1 => Just(0xffu8)], any::<u8>())
            .prop_map(|(c, r, u, p)| PAction::Elect(c, r, u, p)),
    ]
}

fn run_p(m: usize, n: usize, prefix: PrefixTest, actions: &[PAction]) -> Result<PStats, String> {
    run_p_with(true, m, n, prefix, actions)
}

fn run_p_with(
    check_p2: bool,
    m: usize,
    n: usize,
    prefix: PrefixTest,
    actions: &[PAction],
) -> Result<PStats, String> {
    let mut w = PWorld::new(m, n.min(m - 1), prefix);
    w.check_p2 = check_p2;
    for (k, a) in actions.iter().enumerate() {
        w.apply(a).map_err(|e| format!("step {k}: {e}"))?;
    }
    Ok(w.stats)
}

fn histories() -> impl Strategy<Value = (usize, usize, Vec<PAction>)> {
    (
        3usize..6,
        1usize..4,
        prop::collection::vec(paction_strategy(), 20..140),
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(CASES))]
    /// P1 and P2 with the real prefix test, under-reported timelines included.
    #[test]
    fn prefix_rule_never_loses_an_acknowledged_commit(h in histories()) {
        run_p(h.0, h.1, holds_a_prefix, &h.2).map_err(TestCaseError::fail)?;
    }
}

/// Deterministic run over `CASES` histories: the real rule holds and the
/// interesting paths are exercised (a rule that never counts anything, or a
/// model that never under-reports, would prove nothing).
#[test]
fn prefix_model_exercises_prefix_votes_and_under_reporting() {
    use proptest::test_runner::{Config, TestRng, TestRunner};
    let config = Config {
        cases: CASES,
        ..Config::default()
    };
    let mut runner = TestRunner::new_with_rng(
        config.clone(),
        TestRng::deterministic_rng(config.rng_algorithm),
    );
    let total = Mutex::new(PStats::default());
    runner
        .run(&histories(), |(m, n, actions)| {
            let st = run_p(m, n, holds_a_prefix, &actions).map_err(TestCaseError::fail)?;
            total.lock().unwrap().add(&st);
            Ok(())
        })
        .unwrap();
    let t = total.lock().unwrap().clone();
    eprintln!("SB-37 prefix model coverage over {CASES} histories: {t:#?}");
    assert!(t.granted > 1_000, "{t:?}");
    assert!(
        t.granted_by_prefix > 50,
        "the prefix rule rarely mattered: {t:?}"
    );
    assert!(t.prefix_votes > 200, "{t:?}");
    assert!(t.under_reported > 3_000, "{t:?}");
    assert!(
        t.dangerous_under_reports > 30,
        "no voter off the candidate's WAL published an ancestor timeline: {t:?}"
    );
    assert!(t.refused_with_acked_missing > 100, "{t:?}");
}

/// The r2 case of the PG 18 container run: node2 promoted timeline 3 and died
/// before node3 streamed anything of it; both return. Plain rule: refused.
/// Prefix rule: granted, nothing lost.
#[test]
fn r2_scenario_heals_with_the_prefix_rule_only() {
    use PAction::*;
    // node0 leader on tl1; quorum 3 nodes, n = 1, q = 1.
    let actions = vec![
        Commit(3),
        Advance(1, 10),
        Advance(2, 10),
        KillLeader,
        // node1 promotes timeline 2 (node2 votes on timeline 1).
        Elect(1, 0xff, 0, 0),
        Revive(0),
        Commit(2),
        Advance(2, 10),
        Advance(0, 10),
        KillLeader,
        // node2 promotes timeline 3 at the end of timeline 2 (node0 votes).
        Elect(2, 0xff, 0, 0),
        // ...and dies before anybody streams timeline 3.
        KillLeader,
        Revive(2),
        // node2 (timeline 3) asks node0 (timeline 2, exactly at the switchpoint).
        Elect(2, 0xff, 0, 0),
    ];
    let mut w = PWorld::new(3, 1, holds_a_prefix);
    for a in &actions {
        w.apply(a).unwrap();
    }
    assert!(!w.leader_dead && w.leader == 2, "healed: node2 leads");
    assert_eq!(w.stats.granted_by_prefix, 1);
    let mut plain = PWorld::new(3, 1, |_, _| false);
    for a in &actions {
        plain.apply(a).unwrap();
    }
    assert!(
        plain.leader_dead,
        "without the rule the cluster stays leaderless"
    );
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

/// Search the same generator for a history the mutant breaks.
fn mutant_is_caught(prefix: PrefixTest) -> Option<String> {
    mutant_breaks(true, prefix)
}

fn mutant_breaks(check_p2: bool, prefix: PrefixTest) -> Option<String> {
    use proptest::test_runner::{Config, TestRng, TestRunner};
    let config = Config {
        cases: 4 * CASES,
        failure_persistence: None,
        ..Config::default()
    };
    let mut runner = TestRunner::new_with_rng(
        config.clone(),
        TestRng::deterministic_rng(config.rng_algorithm),
    );
    runner
        .run(&histories(), |(m, n, actions)| {
            run_p_with(check_p2, m, n, prefix, &actions).map_err(TestCaseError::fail)?;
            Ok(())
        })
        .err()
        .map(|e| e.to_string())
}

#[test]
fn mutation_no_switchpoint_check_is_caught() {
    let e = mutant_is_caught(no_switchpoint_check).expect("mutant survived");
    eprintln!("{e}");
    assert!(e.contains("P1") || e.contains("P2"), "{e}");
}

#[test]
fn mutation_lsn_past_the_switchpoint_counted_is_caught() {
    let e = mutant_is_caught(one_past_switchpoint).expect("mutant survived");
    eprintln!("{e}");
    assert!(e.contains("P1") || e.contains("P2"), "{e}");
}

#[test]
fn mutation_non_ancestor_timeline_counted_is_caught() {
    let e = mutant_is_caught(any_older_timeline).expect("mutant survived");
    eprintln!("{e}");
    assert!(e.contains("P1") || e.contains("P2"), "{e}");
}

/// Without the switchpoint check the model finds an actual lost commit (P1),
/// not only a miscounted voter: a voter really on the candidate's timeline,
/// ahead of it, publishes an older timeline and is counted.
#[test]
fn mutation_no_switchpoint_check_loses_a_commit() {
    let e = mutant_breaks(false, no_switchpoint_check).expect("no lost commit found");
    eprintln!("{e}");
    assert!(e.contains("P1"), "{e}");
}
