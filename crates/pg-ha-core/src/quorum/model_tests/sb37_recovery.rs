//! SB-37: machine check of the operator recovery grant
//! ([`recovery_vote_decision`]) against real log contents.
//!
//! # What is modelled
//!
//! * 3..=5 nodes, every node a voter (`V` = all nodes), `n` in `1..m-1`,
//!   `q = m - 1 - n`. A commit is acknowledged once the live leader and at
//!   least `n` other nodes hold it (so `|V ∖ holders| <= q`, invariant I).
//! * Per-node logs of `(id, timeline)`, PostgreSQL promotion (a new timeline
//!   begins at the candidate's LSN), standbys that rewind to the common prefix
//!   when they attach, nodes that die and return, and the dead leader that
//!   returns either as a standby or as a WRITABLE primary without the lock.
//! * The grant (`/recovery`): `Grant(candidate, lost)` is create-only and keeps
//!   the timeline id it was made on (its snapshot); `Expire` and `Cancel` end
//!   it; a grant whose snapshot is not the current timeline is ignored.
//! * `Elect(candidate, reach, crash)` runs the REAL `recovery_vote_decision`
//!   when a live grant names the candidate, else the REAL
//!   `quorum_vote_decision`, on the positions of the reachable nodes. Crash
//!   points: after the vote (voters fenced, no lock), after the lock (grant
//!   deleted, no promote), after the promote (state is the same as no crash:
//!   what is not finalized is the DCS `/sync`, which this model does not
//!   carry, see the main model).
//! * A node may carry a stale `pg_control` pair `(timeline, lsn)` that the
//!   published timeline may use only when its replay LSN has reached `lsn`
//!   (the rule of `postgresql::resolve_replica_wal`).
//!
//! # What is checked
//!
//! * **P1** a granted candidate holds every acknowledged commit, except those
//!   whose holders ALL are named in the grant and none was reachable; with
//!   `|lost| <= |V| - q - 1 = n` nothing may be lost. Accepted losses leave the
//!   acknowledged set (they were given up on purpose).
//! * **P2** no election is granted while a reachable node is a writable
//!   primary, and one grant authorizes at most one lock acquisition.
//! * **P3** a grant whose snapshot is stale is ignored (the plain rule decides).
//! * **P4** every counted non-named voter holds a log that is a prefix of the
//!   candidate's (ground truth).
//!
//! # What is NOT modelled
//!
//! The TTL as a clock (`Expire` is an action), the DCS, REST, the digest and
//! the assessment (`quorum::recovery`), the D6 override, zombie primaries
//! that keep acknowledging (SB-40, own model), and `/sync` changes.
use super::*;
use crate::quorum::{AncestorEnd, recovery_vote_decision};
use std::collections::{BTreeSet, HashSet, VecDeque};

#[derive(Debug, Clone)]
enum RAction {
    Commit(usize),
    Advance(usize, usize),
    Kill(usize),
    Revive(usize),
    KillLeader,
    /// The dead leader restarts as a writable primary that holds no lock.
    ReviveWritable,
    /// Leave a stale `pg_control` pair on a node.
    StalePair(usize),
    /// Candidate, lost mask, add the dead leader to `lost`.
    Grant(usize, u8, bool),
    Expire,
    Cancel,
    /// Candidate, reachable mask, crash point (0 none, 1 after vote, 2 after
    /// lock, 3 after promote).
    Elect(usize, u8, u8),
}

/// Switches that turn the real protocol into a defective one; every one of
/// them must be caught.
#[derive(Debug, Clone, Copy, Default)]
struct Protocol {
    /// M-a: the vote does not look at the voters' WAL positions.
    skip_wal_check: bool,
    /// M-b: answers of named voters are dropped (a returned one ahead too).
    ignore_named: bool,
    /// M-c: a writable primary does not refuse.
    ignore_primary: bool,
    /// M-d: a missing non-named voter is counted as present.
    count_missing: bool,
    /// M-e: the grant is deleted only after the promote.
    consume_after_promote: bool,
    /// M-f: the `pg_control` lower bound without `replay >= lsn`.
    bound_without_replay_check: bool,
}

#[derive(Debug, Default, Clone)]
struct RStats {
    acked: u64,
    elections: u64,
    plain_granted: u64,
    grants_made: u64,
    grant_elections: u64,
    grant_granted: u64,
    /// Granted by the grant while the strict rule refuses the same positions.
    grant_only: u64,
    refused_primary_alive: u64,
    refused_behind_or_higher: u64,
    refused_insufficient: u64,
    accepted_loss: u64,
    stale_ignored: u64,
    crash_after_vote: u64,
    crash_after_lock: u64,
    crash_after_promote: u64,
    stale_pairs_ignored_by_guard: u64,
}

impl RStats {
    fn add(&mut self, o: &RStats) {
        self.acked += o.acked;
        self.elections += o.elections;
        self.plain_granted += o.plain_granted;
        self.grants_made += o.grants_made;
        self.grant_elections += o.grant_elections;
        self.grant_granted += o.grant_granted;
        self.grant_only += o.grant_only;
        self.refused_primary_alive += o.refused_primary_alive;
        self.refused_behind_or_higher += o.refused_behind_or_higher;
        self.refused_insufficient += o.refused_insufficient;
        self.accepted_loss += o.accepted_loss;
        self.stale_ignored += o.stale_ignored;
        self.crash_after_vote += o.crash_after_vote;
        self.crash_after_lock += o.crash_after_lock;
        self.crash_after_promote += o.crash_after_promote;
        self.stale_pairs_ignored_by_guard += o.stale_pairs_ignored_by_guard;
    }
}

#[derive(Debug, Clone)]
struct Grant {
    id: u32,
    candidate: usize,
    lost: BTreeSet<usize>,
    /// The timeline id when the grant was made (the `/timeline` snapshot).
    snap_tl: u64,
}

#[derive(Clone)]
struct RWorld {
    nodes: Vec<Node>,
    writable: Vec<bool>,
    pair: Vec<Option<(u64, u64)>>,
    leader: usize,
    leader_dead: bool,
    n: usize,
    q: usize,
    latest_tl: u64,
    histories: HashMap<u64, Vec<AncestorEnd>>,
    acked: Vec<u64>,
    next_id: u64,
    grant: Option<Grant>,
    next_grant: u32,
    uses: HashMap<u32, u32>,
    proto: Protocol,
    stats: RStats,
}

fn is_prefix(a: &[Entry], b: &[Entry]) -> bool {
    a.len() <= b.len() && a.iter().zip(b).all(|(x, y)| x == y)
}

impl RWorld {
    fn new(m: usize, n: usize, proto: Protocol) -> RWorld {
        let base = vec![Entry { id: 0, tl: 1 }];
        let mut histories = HashMap::new();
        histories.insert(1, Vec::new());
        RWorld {
            nodes: (0..m)
                .map(|_| Node {
                    alive: true,
                    attached: true,
                    log: base.clone(),
                })
                .collect(),
            writable: vec![false; m],
            pair: vec![None; m],
            leader: 0,
            leader_dead: false,
            n,
            q: m - 1 - n,
            latest_tl: 1,
            histories,
            acked: vec![0],
            next_id: 1,
            grant: None,
            next_grant: 1,
            uses: HashMap::new(),
            proto,
            stats: RStats::default(),
        }
    }

    fn idx(&self, i: usize) -> usize {
        i % self.nodes.len()
    }

    fn voters(&self) -> Names {
        (0..self.nodes.len()).map(name).collect()
    }

    fn names(&self, set: &BTreeSet<usize>) -> Names {
        set.iter().map(|&i| name(i)).collect()
    }

    /// The timeline a node publishes: its own, or the `pg_control` pair's when
    /// the replay position has reached the pair's LSN (M-f drops that test).
    fn published_tl(&mut self, j: usize) -> u64 {
        let real = self.nodes[j].tl();
        match self.pair[j] {
            Some((t, lsn)) if t > real => {
                if self.proto.bound_without_replay_check || self.nodes[j].lsn() >= lsn {
                    t
                } else {
                    self.stats.stale_pairs_ignored_by_guard += 1;
                    real
                }
            }
            _ => real,
        }
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

    fn apply(&mut self, a: &RAction) -> Result<(), String> {
        match a {
            RAction::Commit(k) => {
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
            RAction::Advance(i, k) => {
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
            RAction::Kill(i) => {
                let i = self.idx(*i);
                if i != self.leader || self.leader_dead {
                    self.nodes[i].alive = false;
                    self.nodes[i].attached = false;
                    self.writable[i] = false;
                }
            }
            RAction::Revive(i) => {
                let i = self.idx(*i);
                self.nodes[i].alive = true;
            }
            RAction::KillLeader => {
                if !self.leader_dead {
                    self.leader_dead = true;
                    let l = self.leader;
                    self.nodes[l].alive = false;
                    self.nodes[l].attached = false;
                }
            }
            RAction::ReviveWritable => {
                if self.leader_dead {
                    let l = self.leader;
                    self.nodes[l].alive = true;
                    self.writable[l] = true;
                }
            }
            RAction::StalePair(i) => {
                let i = self.idx(*i);
                // A pair naming the latest timeline at a point this node has
                // NOT replayed (its WAL may be another lineage): the guard
                // `replay >= lsn` must reject it.
                if self.nodes[i].tl() < self.latest_tl {
                    self.pair[i] = Some((self.latest_tl, self.nodes[i].lsn() + 1));
                }
            }
            RAction::Grant(c, mask, with_leader) => {
                let c = self.idx(*c);
                if self.leader_dead && self.grant.is_none() && self.nodes[c].alive {
                    let mut lost: BTreeSet<usize> = (0..self.nodes.len())
                        .filter(|&j| j != c && mask & (1 << (j % 8)) != 0)
                        .collect();
                    if *with_leader && self.leader != c {
                        lost.insert(self.leader);
                    }
                    self.grant = Some(Grant {
                        id: self.next_grant,
                        candidate: c,
                        lost,
                        snap_tl: self.latest_tl,
                    });
                    self.next_grant += 1;
                    self.stats.grants_made += 1;
                }
            }
            RAction::Expire | RAction::Cancel => self.grant = None,
            RAction::Elect(c, reach, crash) => self.elect(*c, *reach, *crash)?,
        }
        self.refresh_acks();
        Ok(())
    }

    fn mutate(
        &self,
        candidate: &WalPosition,
        lost: &BTreeSet<usize>,
        mut peers: Vec<WalPosition>,
    ) -> Vec<WalPosition> {
        if self.proto.skip_wal_check {
            for p in &mut peers {
                p.timeline = candidate.timeline;
                p.lsn = 0;
            }
        }
        if self.proto.ignore_named {
            let names = self.names(lost);
            peers.retain(|p| !names.contains(&p.name));
        }
        if self.proto.ignore_primary {
            for p in &mut peers {
                p.in_recovery = true;
            }
        }
        if self.proto.count_missing {
            let names = self.names(lost);
            for v in self.voters() {
                if v != candidate.name && !names.contains(&v) && !peers.iter().any(|p| p.name == v)
                {
                    peers.push(WalPosition {
                        name: v,
                        in_recovery: true,
                        timeline: candidate.timeline,
                        lsn: candidate.lsn,
                    });
                }
            }
        }
        peers
    }

    fn elect(&mut self, c: usize, reach: u8, crash: u8) -> Result<(), String> {
        if !self.leader_dead {
            return Ok(());
        }
        let c = self.idx(c);
        if !self.nodes[c].alive || self.writable[c] || self.nodes[c].tl() != self.latest_tl {
            // Not running, a writable primary itself, or D6: not on the
            // latest timeline.
            return Ok(());
        }
        self.stats.elections += 1;
        let candidate = WalPosition {
            name: name(c),
            in_recovery: true,
            timeline: self.nodes[c].tl(),
            lsn: self.nodes[c].lsn(),
        };
        let mut reached = Vec::new();
        let mut peers = Vec::new();
        for j in 0..self.nodes.len() {
            if j == c || !self.nodes[j].alive || reach & (1 << (j % 8)) == 0 {
                continue;
            }
            reached.push(j);
            let timeline = self.published_tl(j);
            peers.push(WalPosition {
                name: name(j),
                in_recovery: !self.writable[j],
                timeline,
                lsn: self.nodes[j].lsn(),
            });
        }
        let grant = self.grant.clone().filter(|g| g.candidate == c);
        let live = match grant {
            Some(g) if g.snap_tl == self.latest_tl => Some(g),
            Some(_) => {
                // P3: a stale grant is ignored, the plain rule decides.
                self.stats.stale_ignored += 1;
                None
            }
            None => None,
        };
        match live {
            Some(g) => self.elect_with_grant(&g, &candidate, &reached, peers, crash),
            None => self.elect_plain(c, &candidate, &reached, &peers, crash),
        }
    }

    fn crash_stat(&mut self, crash: u8) {
        match crash {
            1 => self.stats.crash_after_vote += 1,
            2 => self.stats.crash_after_lock += 1,
            3 => self.stats.crash_after_promote += 1,
            _ => {}
        }
    }

    fn elect_plain(
        &mut self,
        c: usize,
        candidate: &WalPosition,
        reached: &[usize],
        peers: &[WalPosition],
        crash: u8,
    ) -> Result<(), String> {
        let d = quorum_vote_decision(candidate, &self.voters(), self.q, peers);
        if let VoteDecision::Refused(_) = d {
            return Ok(());
        }
        if let Some(&j) = reached.iter().find(|&&j| self.writable[j]) {
            return Err(format!("P2: plain vote granted with writable {}", name(j)));
        }
        if let Some(&x) = self.acked.iter().find(|&&x| !self.nodes[c].holds(x)) {
            return Err(format!("P1: plain vote lost acknowledged commit {x}"));
        }
        self.stats.plain_granted += 1;
        self.crash_stat(crash);
        if crash == 1 || crash == 2 {
            return Ok(());
        }
        self.promote(c);
        Ok(())
    }

    fn elect_with_grant(
        &mut self,
        g: &Grant,
        candidate: &WalPosition,
        reached: &[usize],
        peers: Vec<WalPosition>,
        crash: u8,
    ) -> Result<(), String> {
        let c = g.candidate;
        self.stats.grant_elections += 1;
        let lost_names = self.names(&g.lost);
        let voters = self.voters();
        let fed = self.mutate(candidate, &g.lost, peers.clone());
        match recovery_vote_decision(candidate, &voters, &lost_names, &fed) {
            VoteDecision::Refused(r) => {
                match r {
                    VoteRefusal::PrimaryAlive { .. } => self.stats.refused_primary_alive += 1,
                    VoteRefusal::InsufficientVotes { .. } => self.stats.refused_insufficient += 1,
                    _ => self.stats.refused_behind_or_higher += 1,
                }
                Ok(())
            }
            VoteDecision::Granted { .. } => {
                self.stats.grant_granted += 1;
                if !matches!(
                    quorum_vote_decision(candidate, &voters, self.q, &peers),
                    VoteDecision::Granted { .. }
                ) {
                    self.stats.grant_only += 1;
                }
                // P2: nobody reachable may be a writable primary.
                if let Some(&j) = reached.iter().find(|&&j| self.writable[j]) {
                    return Err(format!(
                        "P2: grant {} granted while {} is a writable primary",
                        g.id,
                        name(j)
                    ));
                }
                // P4: every counted non-named voter holds a prefix of the
                // candidate's log.
                let cand_log = self.nodes[c].log.clone();
                for &j in reached {
                    if !g.lost.contains(&j) && !is_prefix(&self.nodes[j].log, &cand_log) {
                        return Err(format!(
                            "P4: counted voter {} holds a log that is not a prefix of {}'s \
                             (published {:?})",
                            name(j),
                            name(c),
                            peers.iter().find(|p| p.name == name(j))
                        ));
                    }
                }
                // P1.
                let missing: Vec<u64> = self
                    .acked
                    .iter()
                    .copied()
                    .filter(|&x| !self.nodes[c].holds(x))
                    .collect();
                for &x in &missing {
                    let holders: Vec<usize> = (0..self.nodes.len())
                        .filter(|&j| self.nodes[j].holds(x))
                        .collect();
                    let named_only = holders.iter().all(|j| g.lost.contains(j));
                    let seen = holders.iter().any(|j| reached.contains(j));
                    if g.lost.len() <= self.n {
                        return Err(format!(
                            "P1: commit {x} lost with |lost| = {} <= |V|-q-1 = {} (grant {})",
                            g.lost.len(),
                            self.n,
                            g.id
                        ));
                    }
                    if !named_only || seen {
                        return Err(format!(
                            "P1: LOST ACKNOWLEDGED COMMIT {x}: holders {holders:?}, named {:?}, \
                             reachable {reached:?}, grant {}",
                            g.lost, g.id
                        ));
                    }
                }
                self.crash_stat(crash);
                if crash == 1 {
                    // Voters fenced, no lock: the grant is still there.
                    return Ok(());
                }
                // The lock: the grant authorizes one acquisition.
                let uses = self.uses.entry(g.id).or_insert(0);
                *uses += 1;
                if *uses > 1 {
                    return Err(format!(
                        "P2: grant {} authorized a second lock acquisition",
                        g.id
                    ));
                }
                let delete_now = !self.proto.consume_after_promote || crash != 2;
                if delete_now {
                    self.grant = None;
                }
                if crash == 2 {
                    return Ok(());
                }
                if !missing.is_empty() {
                    self.stats.accepted_loss += 1;
                    self.acked.retain(|x| !missing.contains(x));
                }
                self.promote(c);
                self.grant = None;
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
        // The other primaries are fenced or rewound in pg-ha; only what the
        // vote could see is checked (P2).
        self.writable.iter_mut().for_each(|w| *w = false);
        self.leader = c;
        self.leader_dead = false;
    }

    fn key(&self) -> String {
        format!(
            "{:?}|{:?}|{:?}|{}|{}|{}|{:?}|{:?}|{:?}|{:?}",
            self.nodes
                .iter()
                .map(|n| (n.alive, n.attached, &n.log))
                .collect::<Vec<_>>(),
            self.writable,
            self.pair,
            self.leader,
            self.leader_dead,
            self.latest_tl,
            self.acked,
            self.grant,
            {
                let mut u: Vec<_> = self.uses.iter().collect();
                u.sort();
                u
            },
            (self.next_id, self.next_grant)
        )
    }
}

fn raction_strategy() -> impl Strategy<Value = RAction> {
    prop_oneof![
        6 => (1usize..4).prop_map(RAction::Commit),
        10 => (any::<usize>(), 1usize..5).prop_map(|(i, k)| RAction::Advance(i, k)),
        2 => any::<usize>().prop_map(RAction::Kill),
        3 => any::<usize>().prop_map(RAction::Revive),
        3 => Just(RAction::KillLeader),
        2 => Just(RAction::ReviveWritable),
        2 => any::<usize>().prop_map(RAction::StalePair),
        5 => (any::<usize>(), any::<u8>(), any::<bool>())
            .prop_map(|(c, m, l)| RAction::Grant(c, m, l)),
        1 => Just(RAction::Expire),
        1 => Just(RAction::Cancel),
        8 => (any::<usize>(), prop_oneof![1 => Just(0xffu8), 2 => any::<u8>()], 0u8..4)
            .prop_map(|(c, r, k)| RAction::Elect(c, r, k)),
    ]
}

fn run_r(proto: Protocol, m: usize, n: usize, actions: &[RAction]) -> Result<RStats, String> {
    let mut w = RWorld::new(m, n.min(m - 1), proto);
    for (k, a) in actions.iter().enumerate() {
        w.apply(a).map_err(|e| format!("step {k}: {e}"))?;
    }
    Ok(w.stats)
}

fn histories() -> impl Strategy<Value = (usize, usize, Vec<RAction>)> {
    (
        3usize..6,
        1usize..4,
        prop::collection::vec(raction_strategy(), 20..140),
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(CASES))]
    /// P1 to P4 under random histories, grants and crash points.
    #[test]
    fn recovery_grant_never_loses_more_than_it_names(h in histories()) {
        run_r(Protocol::default(), h.0, h.1, &h.2).map_err(TestCaseError::fail)?;
    }
}

/// Deterministic run over `CASES` histories: the properties hold and every
/// interesting path is exercised (a model that never grants, never crashes at a
/// point or never accepts a loss would prove nothing).
#[test]
fn recovery_model_exercises_grants_refusals_and_each_crash_point() {
    use proptest::test_runner::{Config, TestRng, TestRunner};
    let config = Config {
        cases: CASES,
        ..Config::default()
    };
    let mut runner = TestRunner::new_with_rng(
        config.clone(),
        TestRng::deterministic_rng(config.rng_algorithm),
    );
    let total = Mutex::new(RStats::default());
    runner
        .run(&histories(), |(m, n, actions)| {
            let st = run_r(Protocol::default(), m, n, &actions).map_err(TestCaseError::fail)?;
            total.lock().unwrap().add(&st);
            Ok(())
        })
        .unwrap();
    let t = total.lock().unwrap().clone();
    eprintln!("SB-37 recovery model coverage over {CASES} histories: {t:#?}");
    assert!(t.grants_made > 1_000, "{t:?}");
    assert!(t.grant_granted > 150, "{t:?}");
    assert!(t.grant_only > 40, "the grant rarely mattered: {t:?}");
    assert!(t.accepted_loss > 3, "no loss was ever accepted: {t:?}");
    assert!(t.refused_primary_alive > 20, "{t:?}");
    assert!(t.refused_behind_or_higher > 50, "{t:?}");
    assert!(t.refused_insufficient > 100, "{t:?}");
    assert!(t.stale_ignored > 5, "{t:?}");
    assert!(t.crash_after_vote > 20, "{t:?}");
    assert!(t.crash_after_lock > 20, "{t:?}");
    assert!(t.crash_after_promote > 20, "{t:?}");
    assert!(t.stale_pairs_ignored_by_guard > 20, "{t:?}");
}

fn all_actions(m: usize) -> Vec<RAction> {
    let mut v = vec![
        RAction::Commit(1),
        RAction::KillLeader,
        RAction::ReviveWritable,
    ];
    v.push(RAction::Expire);
    for i in 0..m {
        v.push(RAction::Advance(i, 1));
        v.push(RAction::Kill(i));
        v.push(RAction::Revive(i));
        v.push(RAction::StalePair(i));
        // Named sets: the dead leader alone, and every other single node
        // together with the dead leader.
        v.push(RAction::Grant(i, 0, true));
        for j in 0..m {
            if j != i {
                v.push(RAction::Grant(i, 1 << j, true));
            }
        }
        for crash in 0..4 {
            v.push(RAction::Elect(i, 0xff, crash));
            for j in 0..m {
                if j != i {
                    v.push(RAction::Elect(i, !(1u8 << j), crash));
                }
            }
        }
    }
    v
}

/// Breadth-first over every action sequence up to `depth`, with state
/// de-duplication, capped at `cap` distinct states. Returns the number of
/// states, whether the cap cut the search, and the stats summed over every
/// explored transition.
fn exhaustive(
    m: usize,
    n: usize,
    depth: usize,
    cap: usize,
) -> Result<(usize, bool, RStats), String> {
    let actions = all_actions(m);
    let start = RWorld::new(m, n, Protocol::default());
    let mut seen: HashSet<String> = HashSet::new();
    seen.insert(start.key());
    let mut total = RStats::default();
    let mut frontier: VecDeque<(RWorld, usize)> = VecDeque::new();
    frontier.push_back((start, 0));
    while let Some((w, d)) = frontier.pop_front() {
        if d == depth {
            continue;
        }
        for a in &actions {
            let mut next = w.clone();
            next.stats = RStats::default();
            next.apply(a)
                .map_err(|e| format!("depth {} after {a:?}: {e}", d + 1))?;
            if seen.len() >= cap {
                return Ok((seen.len(), true, total));
            }
            total.add(&next.stats);
            if seen.insert(next.key()) {
                frontier.push_back((next, d + 1));
            }
        }
    }
    Ok((seen.len(), false, total))
}

#[test]
fn recovery_exhaustive_three_nodes() {
    let (states, capped, t) = exhaustive(3, 1, 9, 400_000).unwrap();
    eprintln!("SB-37 recovery exhaustive 3 nodes: {states} states, capped: {capped}, {t:?}");
    assert!(states > 50_000, "{states}");
    assert!(t.grant_granted > 0 && t.accepted_loss > 0, "{t:?}");
    assert!(
        t.refused_primary_alive > 0 && t.crash_after_lock > 0,
        "{t:?}"
    );
}

#[test]
fn recovery_exhaustive_five_nodes() {
    for n in [1, 2] {
        let (states, capped, t) = exhaustive(5, n, 6, 400_000).unwrap();
        eprintln!(
            "SB-37 recovery exhaustive 5 nodes n={n}: {states} states, capped: {capped}, {t:?}"
        );
        assert!(states > 50_000, "{states}");
        assert!(t.grant_granted > 0, "{t:?}");
    }
}

/// An accepted loss is exactly what the named voters alone held: with n1
/// unreachable and named, n2 is promoted without n1's commits; naming only the
/// dead leader (|lost| = 1 <= n) gives nothing away and is refused.
#[test]
fn accepted_loss_is_only_what_the_named_voters_held() {
    use RAction::*;
    let mut w = RWorld::new(3, 1, Protocol::default());
    for a in [Commit(2), Advance(1, 9), KillLeader, Grant(2, 0, true)] {
        w.apply(&a).unwrap();
    }
    // Only the dead leader named: n1 must answer, and it is ahead.
    w.apply(&Elect(2, 0xff, 0)).unwrap();
    assert!(w.leader_dead);
    assert_eq!(w.stats.refused_behind_or_higher, 1);
    // n1 unreachable: the strict count of n1 is missing.
    w.apply(&Elect(2, 0b100, 0)).unwrap();
    assert!(w.leader_dead);
    assert_eq!(w.stats.refused_insufficient, 1);
    w.apply(&Cancel).unwrap();
    let acked_before = w.acked.clone();
    for a in [Grant(2, 0b010, true), Elect(2, 0b100, 0)] {
        w.apply(&a).unwrap();
    }
    assert!(!w.leader_dead && w.leader == 2, "granted");
    assert_eq!(w.stats.accepted_loss, 1);
    let lost: Vec<u64> = acked_before
        .iter()
        .copied()
        .filter(|x| !w.nodes[2].holds(*x))
        .collect();
    assert_eq!(lost, vec![1, 2], "the two commits only n0 and n1 held");
    assert!(lost.iter().all(|x| !w.acked.contains(x)));
}

/// P3: after another node promoted, a grant made earlier is ignored: the plain
/// rule decides, and a cluster that cannot reach `q` stays leaderless.
#[test]
fn stale_grant_is_ignored() {
    use RAction::*;
    // 3 nodes, n = 1, q = 1. node0 dies; node1 promotes (plain); node1 dies;
    // a grant made before the promote must not help node2.
    let mut w = RWorld::new(3, 1, Protocol::default());
    for a in [
        Commit(2),
        Advance(1, 9),
        Advance(2, 9),
        KillLeader,
        Grant(2, 0, true),
        // A plain election by node1 first (timeline 2), with node2 reached.
        Elect(1, 0xff, 0),
    ] {
        w.apply(&a).unwrap();
    }
    assert_eq!(w.latest_tl, 2);
    assert!(w.grant.is_some(), "the grant was made for node2, not used");
    let before = w.stats.grant_elections;
    for a in [Advance(2, 9), KillLeader, Elect(2, 0xff, 0)] {
        w.apply(&a).unwrap();
    }
    assert_eq!(w.stats.grant_elections, before, "stale grant was used");
    assert!(w.stats.stale_ignored >= 1);
}

/// The grant is consumed at the lock: after a crash after the lock it is
/// gone; a second election needs a new grant.
#[test]
fn grant_is_consumed_at_the_lock() {
    use RAction::*;
    let mut w = RWorld::new(3, 1, Protocol::default());
    for a in [
        Commit(2),
        Advance(1, 9),
        Advance(2, 9),
        KillLeader,
        Kill(2),
        Grant(1, 0b100, true),
        Grant(1, 0b100, true),
        Elect(1, 0xff, 1),
    ] {
        w.apply(&a).unwrap();
    }
    assert_eq!(w.stats.grants_made, 1, "create-only: second grant refused");
    assert!(w.grant.is_some(), "crash after the vote keeps the grant");
    w.apply(&Elect(1, 0xff, 2)).unwrap();
    assert!(w.grant.is_none(), "the lock consumed the grant");
    assert!(w.leader_dead, "crash after the lock: nobody promoted");
    w.apply(&Elect(1, 0xff, 0)).unwrap();
    assert!(w.leader_dead, "no second election without a new grant");
}

/// Runs `actions` under the real protocol (must hold) and under `proto` (must
/// break); returns the mutant's error.
fn witness(proto: Protocol, m: usize, n: usize, actions: &[RAction]) -> String {
    run_r(Protocol::default(), m, n, actions).expect("the real protocol must hold");
    run_r(proto, m, n, actions).expect_err("mutant survived the witness")
}

/// 3 nodes, n = 1, q = 1: n1 holds acknowledged commits, n0 (the leader) dies,
/// the operator names n0 and n1 lost, n2 (behind) is the candidate.
fn named_voter_returns_ahead() -> Vec<RAction> {
    use RAction::*;
    vec![
        Commit(2),
        Advance(1, 9),
        KillLeader,
        Grant(2, 0b010, true),
        Elect(2, 0xff, 0),
    ]
}

/// 3 nodes, n = 1: n0 leads tl1 and keeps an unreplicated tail; n1 promotes
/// tl2; n0 returns diverged and carries a pair for tl2 it has not replayed;
/// n1 dies; n2 (tl2) has a grant naming n1.
fn diverged_node_with_unreplayed_pair() -> Vec<RAction> {
    use RAction::*;
    vec![
        Commit(1),
        Advance(1, 9),
        Advance(2, 9),
        Commit(1),
        KillLeader,
        Elect(1, 0b110, 0),
        Commit(2),
        Advance(2, 9),
        Revive(0),
        StalePair(0),
        KillLeader,
        Grant(2, 0b010, false),
        Elect(2, 0xff, 0),
    ]
}

/// Search the generator for a history that the defective protocol breaks.
fn mutant_caught(proto: Protocol) -> Option<String> {
    use proptest::test_runner::{Config, TestRng, TestRunner};
    let config = Config {
        cases: 8 * CASES,
        failure_persistence: None,
        ..Config::default()
    };
    let mut runner = TestRunner::new_with_rng(
        config.clone(),
        TestRng::deterministic_rng(config.rng_algorithm),
    );
    runner
        .run(&histories(), |(m, n, actions)| {
            run_r(proto, m, n, &actions).map_err(TestCaseError::fail)?;
            Ok(())
        })
        .err()
        .map(|e| e.to_string())
}

#[test]
fn mutation_skip_the_wal_check_is_caught() {
    let e = mutant_caught(Protocol {
        skip_wal_check: true,
        ..Protocol::default()
    })
    .expect("mutant survived");
    eprintln!("{e}");
    assert!(e.contains("P1") || e.contains("P4"), "{e}");
}

#[test]
fn mutation_ignore_a_reachable_named_voter_is_caught() {
    let proto = Protocol {
        ignore_named: true,
        ..Protocol::default()
    };
    let e = witness(proto, 3, 1, &named_voter_returns_ahead());
    eprintln!("{e}");
    assert!(e.contains("P1: LOST ACKNOWLEDGED COMMIT"), "{e}");
}

#[test]
fn mutation_act_while_a_primary_exists_is_caught() {
    let e = mutant_caught(Protocol {
        ignore_primary: true,
        ..Protocol::default()
    })
    .expect("mutant survived");
    eprintln!("{e}");
    assert!(e.contains("P2") && e.contains("writable"), "{e}");
}

#[test]
fn mutation_count_a_missing_voter_is_caught() {
    let e = mutant_caught(Protocol {
        count_missing: true,
        ..Protocol::default()
    })
    .expect("mutant survived");
    eprintln!("{e}");
    assert!(e.contains("P1"), "{e}");
}

#[test]
fn mutation_consume_the_grant_after_the_promote_is_caught() {
    let e = mutant_caught(Protocol {
        consume_after_promote: true,
        ..Protocol::default()
    })
    .expect("mutant survived");
    eprintln!("{e}");
    assert!(e.contains("second lock acquisition"), "{e}");
}

#[test]
fn mutation_pg_control_bound_without_the_replay_check_is_caught() {
    let proto = Protocol {
        bound_without_replay_check: true,
        ..Protocol::default()
    };
    let e = witness(proto, 3, 1, &diverged_node_with_unreplayed_pair());
    eprintln!("{e}");
    assert!(e.contains("P4") || e.contains("P1"), "{e}");
}
