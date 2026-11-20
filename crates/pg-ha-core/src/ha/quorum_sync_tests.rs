//! Unit tests of the quorum-sync executor against a scripted `SyncIo`.
//!
//! `FakeIo` logs every call. `drive` feeds the written `/sync` back as the
//! next cycle's input, so multi-cycle transitions (T7, T8, T9) run end to end.

use super::*;
use crate::cluster::{MemberRole, MemberState};
use crate::postgresql::ReplicaRow;
use crate::sync_value::parse_sync_value;
use std::cell::Cell;
use std::collections::VecDeque;

// ───────────────────────────── scripted IO ─────────────────────────────

/// A scripted `SyncIo`. `calls` is the full ordered log: `reload`, `show`,
/// `repl`, `set:<text>`, `dcs:<leader>:<names>:<q>:<kind>`.
struct FakeIo {
    show: String,
    show_err: bool,
    repl: ReplicationSnapshot,
    repl_err: bool,
    reload_err: bool,
    set_pg_err: bool,
    /// A successful `set_pg` changes what SHOW returns.
    set_pg_applies: bool,
    /// Scripted answers of `write_dcs`; `Ok(true)` when empty.
    dcs_script: VecDeque<std::result::Result<bool, String>>,
    /// `lease_ok()` is false from this (0-based) check on.
    lease_false_from: Option<usize>,
    lease_checks: Cell<usize>,
    calls: Vec<String>,
    /// The `/sync` value after the successful writes.
    dcs_state: Option<SyncState>,
    dcs_writes: Vec<SyncState>,
    pg_sets: Vec<String>,
    /// `set_pg` never returns (the 5 s bound must cut it).
    hang_set_pg: bool,
    /// After a `set_pg` that applies, SHOW returns the old value this many
    /// more times (a reload that lands late).
    stale_shows: usize,
    landing: Option<(String, usize)>,
}

impl FakeIo {
    fn new(show: &str, rows: &[(&str, u64)]) -> Self {
        FakeIo {
            show: show.to_string(),
            show_err: false,
            repl: snapshot(1000, rows),
            repl_err: false,
            reload_err: false,
            set_pg_err: false,
            set_pg_applies: false,
            dcs_script: VecDeque::new(),
            lease_false_from: None,
            lease_checks: Cell::new(0),
            calls: Vec::new(),
            dcs_state: None,
            dcs_writes: Vec::new(),
            pg_sets: Vec::new(),
            hang_set_pg: false,
            stale_shows: 0,
            landing: None,
        }
    }

    /// Calls that change something.
    fn writes(&self) -> Vec<String> {
        self.calls
            .iter()
            .filter(|c| !matches!(c.as_str(), "show" | "repl"))
            .cloned()
            .collect()
    }
}

fn snapshot(primary: u64, rows: &[(&str, u64)]) -> ReplicationSnapshot {
    ReplicationSnapshot {
        primary_flush_lsn: primary,
        replicas: rows
            .iter()
            .map(|(n, l)| ReplicaRow {
                application_name: n.to_string(),
                flush_lsn: *l,
            })
            .collect(),
    }
}

fn dcs_desc(s: &SyncState) -> String {
    format!(
        "dcs:{}:{}:{}:{:?}",
        s.leader,
        s.sync_standby.clone().unwrap_or_default(),
        s.quorum,
        s.kind
    )
}

#[async_trait]
impl SyncIo for FakeIo {
    async fn show_sync_names(&mut self) -> Result<String> {
        self.calls.push("show".into());
        if self.show_err {
            return Err(Error::Postgres("show failed".into()));
        }
        if let Some((v, left)) = self.landing.take() {
            if left == 0 {
                self.show = v;
            } else {
                self.landing = Some((v, left - 1));
            }
        }
        Ok(self.show.clone())
    }

    async fn replication(&mut self) -> Result<ReplicationSnapshot> {
        self.calls.push("repl".into());
        if self.repl_err {
            return Err(Error::Postgres("replication failed".into()));
        }
        Ok(self.repl.clone())
    }

    async fn set_pg(&mut self, value: &str) -> Result<()> {
        self.calls.push(format!("set:{value}"));
        if self.hang_set_pg {
            std::future::pending::<()>().await;
        }
        if self.set_pg_err {
            return Err(Error::Postgres("set failed".into()));
        }
        self.pg_sets.push(value.to_string());
        if self.set_pg_applies {
            if self.stale_shows == 0 {
                self.show = value.to_string();
            } else {
                self.landing = Some((value.to_string(), self.stale_shows));
            }
        }
        Ok(())
    }

    async fn reload(&mut self) -> Result<()> {
        self.calls.push("reload".into());
        if self.reload_err {
            return Err(Error::Postgres("reload failed".into()));
        }
        Ok(())
    }

    async fn write_dcs(&mut self, state: &SyncState) -> Result<bool> {
        self.calls.push(dcs_desc(state));
        self.dcs_writes.push(state.clone());
        match self.dcs_script.pop_front() {
            Some(Err(e)) => Err(Error::Dcs(e)),
            Some(Ok(false)) => Ok(false),
            _ => {
                self.dcs_state = Some(state.clone());
                Ok(true)
            }
        }
    }

    fn lease_ok(&self) -> bool {
        let n = self.lease_checks.get();
        self.lease_checks.set(n + 1);
        !self.lease_false_from.is_some_and(|from| n >= from)
    }
}

fn member(name: &str, nosync: bool) -> Member {
    let mut tags = std::collections::HashMap::new();
    if nosync {
        tags.insert("nosync".to_string(), serde_json::json!(true));
    }
    Member {
        name: name.to_string(),
        conn_url: String::new(),
        api_url: String::new(),
        state: MemberState::Running,
        role: MemberRole::Replica,
        wal_position: Some(1000),
        timeline: Some(1),
        tags,
        version: None,
        rewind_check: false,
        lineage: None,
        watchdog_failed: false,
    }
}

fn sync(leader: &str, voters: &str, quorum: u32, kind: SyncKind) -> SyncState {
    SyncState {
        leader: leader.to_string(),
        sync_standby: if voters.is_empty() {
            None
        } else {
            Some(voters.to_string())
        },
        quorum,
        kind,
    }
}

fn input(mode: SynchronousMode, count: usize, names: &[&str], s: Option<SyncState>) -> ExecInput {
    ExecInput {
        me: "n0".into(),
        mode,
        count,
        strict: false,
        lag_threshold: None,
        members: names.iter().map(|n| member(n, false)).collect(),
        sync: s,
        watchdog_on: true,
    }
}

const ALL: [(&str, u64); 3] = [("n1", 1000), ("n2", 1000), ("n3", 1000)];
const Q: SynchronousMode = SynchronousMode::Quorum;

fn ready() -> QuorumExec {
    QuorumExec {
        reloaded_since_start: true,
        ..Default::default()
    }
}

fn ready_with_mark(show: &str, lsn: u64) -> QuorumExec {
    QuorumExec {
        reloaded_since_start: true,
        mark: Some(Mark {
            shape_key: parse_sync_value(show).unwrap().key(),
            lsn,
        }),
        pending: None,
        ..Default::default()
    }
}

fn cycle(exec: &mut QuorumExec, io: &mut FakeIo, input: &ExecInput) -> ExecOutcome {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(exec.cycle(io, input))
}

/// Run cycles, feeding the written `/sync` back as the next input.
fn drive(
    exec: &mut QuorumExec,
    io: &mut FakeIo,
    input: &mut ExecInput,
    max: usize,
) -> Vec<ExecOutcome> {
    let mut out = Vec::new();
    for _ in 0..max {
        let o = cycle(exec, io, input);
        if let Some(s) = io.dcs_state.clone() {
            input.sync = Some(s);
        }
        let done = o == ExecOutcome::HandedOff;
        out.push(o);
        if done {
            break;
        }
    }
    out
}

fn strs(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

// ───────────────────────────── restart rule ─────────────────────────────

#[test]
fn first_cycle_only_reloads_and_takes_no_mark() {
    let mut exec = QuorumExec::default();
    let mut io = FakeIo::new("", &ALL);
    let inp = input(Q, 1, &["n1", "n2"], None);
    assert_eq!(cycle(&mut exec, &mut io, &inp), ExecOutcome::Progressed);
    assert_eq!(io.calls, vec!["reload"]);
    assert!(exec.reloaded_since_start);
    assert!(exec.mark().is_none());
    // The next cycle observes (and plans).
    cycle(&mut exec, &mut io, &inp);
    assert!(io.calls.iter().any(|c| c == "show"));
    assert_eq!(io.calls.iter().filter(|c| *c == "reload").count(), 1);
    assert!(exec.mark().is_some());
}

#[test]
fn failed_reload_keeps_the_restart_rule_pending() {
    let mut exec = QuorumExec::default();
    let mut io = FakeIo::new("", &ALL);
    io.reload_err = true;
    let inp = input(Q, 1, &["n1", "n2"], None);
    assert!(matches!(
        cycle(&mut exec, &mut io, &inp),
        ExecOutcome::Failed(_)
    ));
    assert!(!exec.reloaded_since_start);
    assert_eq!(io.calls, vec!["reload"]);
    io.reload_err = false;
    cycle(&mut exec, &mut io, &inp);
    assert!(exec.reloaded_since_start);
}

#[test]
fn restart_then_relaxation_needs_two_more_cycles() {
    // V = S = {n1,n2}, q = 1, n = 2: the target q is 0, everyone is caught up.
    let show = "ANY 2 (\"n1\",\"n2\")";
    let mut exec = QuorumExec::default();
    let mut io = FakeIo::new(show, &ALL);
    let mut inp = input(
        Q,
        2,
        &["n1", "n2"],
        Some(sync("n0", "n1,n2", 1, SyncKind::Quorum)),
    );
    let outcomes = drive(&mut exec, &mut io, &mut inp, 4);
    assert_eq!(outcomes[0], ExecOutcome::Progressed); // reload
    assert_eq!(outcomes[1], ExecOutcome::NoChange); // fresh mark: q stays
    assert_eq!(outcomes[2], ExecOutcome::Progressed); // relaxed
    assert_eq!(io.writes(), strs(&["reload", "dcs:n0:n1,n2:0:Quorum"]));
}

// ───────────────────────────── observation errors ─────────────────────────────

#[test]
fn show_and_replication_errors_change_nothing() {
    let inp = input(Q, 1, &["n1", "n2"], None);
    let mut io = FakeIo::new("", &ALL);
    io.show_err = true;
    assert_eq!(cycle(&mut ready(), &mut io, &inp), ExecOutcome::NoChange);
    assert!(io.writes().is_empty());
    let mut io = FakeIo::new("", &ALL);
    io.repl_err = true;
    assert_eq!(cycle(&mut ready(), &mut io, &inp), ExecOutcome::NoChange);
    assert!(io.writes().is_empty());
}

// ───────────────────────────── finalization ─────────────────────────────

/// `/sync.leader = n9` (the dead leader), V = {n0,n2,n3}, q = 1, so the
/// pre-promote value for `n0` is `ANY 2 (n2,n3,n9)` and V' = {n2,n3,n9}.
fn window_input() -> ExecInput {
    input(
        Q,
        2,
        &["n2", "n3", "n9"],
        Some(sync("n9", "n0,n2,n3", 1, SyncKind::Quorum)),
    )
}

const WINDOW_ROWS: [(&str, u64); 3] = [("n2", 1000), ("n3", 1000), ("n9", 1000)];
const WINDOW_SHOW: &str = "ANY 2 (\"n2\",\"n3\",\"n9\")";

#[test]
fn promotion_window_match_writes_the_post_promotion_state_first() {
    let mut io = FakeIo::new(WINDOW_SHOW, &WINDOW_ROWS);
    let o = cycle(&mut ready(), &mut io, &window_input());
    assert_eq!(o, ExecOutcome::Progressed);
    assert_eq!(io.writes(), strs(&["dcs:n0:n2,n3,n9:1:Quorum"]));
}

#[test]
fn promotion_window_mismatch_resets_to_an_empty_voter_set() {
    // The PG value is not the pre-promote one: unknown history.
    let mut io = FakeIo::new("", &WINDOW_ROWS);
    let o = cycle(&mut ready(), &mut io, &window_input());
    // Reset (V = {}, q = 0 as S is empty), then the planner starts.
    assert_eq!(io.writes()[0], "dcs:n0::0:Quorum");
    assert!(matches!(o, ExecOutcome::Progressed));
}

#[test]
fn reset_keeps_si1_when_pg_still_names_standbys() {
    // Wrong n: ANY 1 (n2,n3,n9) is not the pre-promote value (n would be 2).
    let mut io = FakeIo::new("ANY 1 (\"n2\",\"n3\",\"n9\")", &WINDOW_ROWS);
    cycle(&mut ready(), &mut io, &window_input());
    // |S| - n = 2: the reset must not create a state that breaks SI-1.
    assert_eq!(io.writes()[0], "dcs:n0::2:Quorum");
}

#[test]
fn priority_sync_of_another_leader_is_reset_not_adopted() {
    let inp = input(
        Q,
        1,
        &["n1", "n2"],
        Some(sync("n9", "n1,n2", 1, SyncKind::Priority)),
    );
    let mut io = FakeIo::new("FIRST 1 (\"n1\")", &ALL);
    cycle(&mut ready(), &mut io, &inp);
    assert_eq!(io.writes()[0], "dcs:n0::0:Quorum");
}

#[test]
fn failed_finalization_stops_the_cycle_before_any_planning() {
    for script in [Err("dcs down".to_string()), Ok(false)] {
        let mut io = FakeIo::new(WINDOW_SHOW, &WINDOW_ROWS);
        io.dcs_script.push_back(script);
        let o = cycle(&mut ready(), &mut io, &window_input());
        assert!(matches!(o, ExecOutcome::Failed(_)), "{o:?}");
        // Exactly the one failed write: no planner step, no set_pg.
        assert_eq!(io.writes(), strs(&["dcs:n0:n2,n3,n9:1:Quorum"]));
    }
    let mut io = FakeIo::new("", &WINDOW_ROWS);
    io.dcs_script.push_back(Ok(false));
    let o = cycle(&mut ready(), &mut io, &window_input());
    assert!(matches!(o, ExecOutcome::Failed(_)));
    assert_eq!(io.writes(), strs(&["dcs:n0::0:Quorum"]));
}

#[test]
fn lease_gone_blocks_the_dcs_write() {
    let mut io = FakeIo::new(WINDOW_SHOW, &WINDOW_ROWS);
    io.lease_false_from = Some(0);
    let o = cycle(&mut ready(), &mut io, &window_input());
    assert!(matches!(o, ExecOutcome::Failed(_)));
    assert!(io.writes().is_empty());
}

// ───────────────────────────── planner steps ─────────────────────────────

#[test]
fn t6_off_to_quorum_writes_dcs_then_pg_and_stops() {
    let inp = input(Q, 1, &["n1", "n2"], None);
    let mut io = FakeIo::new("", &ALL);
    let o = cycle(&mut ready(), &mut io, &inp);
    assert_eq!(o, ExecOutcome::Progressed);
    assert_eq!(
        io.writes(),
        strs(&["dcs:n0:n1,n2:1:Quorum", "set:ANY 1 (\"n1\",\"n2\")"])
    );
}

#[test]
fn t1_add_voter_dcs_first_then_pg() {
    let show = "ANY 2 (\"n1\",\"n2\")";
    let inp = input(
        Q,
        2,
        &["n1", "n2", "n3"],
        Some(sync("n0", "n1,n2", 0, SyncKind::Quorum)),
    );
    let mut io = FakeIo::new(show, &ALL);
    cycle(&mut ready_with_mark(show, 500), &mut io, &inp);
    assert_eq!(
        io.writes(),
        strs(&[
            "dcs:n0:n1,n2,n3:1:Quorum",
            "set:ANY 2 (\"n1\",\"n2\",\"n3\")"
        ])
    );
}

#[test]
fn t2_remove_voter_pg_first_and_the_cycle_stops_after_it() {
    // n3 became nosync: S shrinks first, then V (a later cycle).
    let show = "ANY 2 (\"n1\",\"n2\",\"n3\")";
    let mut inp = input(
        Q,
        2,
        &["n1", "n2"],
        Some(sync("n0", "n1,n2,n3", 1, SyncKind::Quorum)),
    );
    inp.members.push(member("n3", true));
    let mut io = FakeIo::new(show, &ALL);
    cycle(&mut ready_with_mark(show, 500), &mut io, &inp);
    // The plan is [SetPg, SetDcs, ...]; only the SetPg runs.
    assert_eq!(io.writes(), strs(&["set:ANY 2 (\"n1\",\"n2\")"]));
}

#[test]
fn t4_t5_n_changes_follow_the_planner_order() {
    // N down 2 -> 1: raise q first, then lower n.
    let show = "ANY 2 (\"n1\",\"n2\",\"n3\")";
    let inp = input(
        Q,
        1,
        &["n1", "n2", "n3"],
        Some(sync("n0", "n1,n2,n3", 1, SyncKind::Quorum)),
    );
    let mut io = FakeIo::new(show, &ALL);
    cycle(&mut ready_with_mark(show, 500), &mut io, &inp);
    assert_eq!(
        io.writes(),
        strs(&[
            "dcs:n0:n1,n2,n3:2:Quorum",
            "set:ANY 1 (\"n1\",\"n2\",\"n3\")"
        ])
    );
    // N up 1 -> 2: tighten n first.
    let show = "ANY 1 (\"n1\",\"n2\",\"n3\")";
    let inp = input(
        Q,
        2,
        &["n1", "n2", "n3"],
        Some(sync("n0", "n1,n2,n3", 2, SyncKind::Quorum)),
    );
    let mut io = FakeIo::new(show, &ALL);
    cycle(&mut ready_with_mark(show, 500), &mut io, &inp);
    assert_eq!(io.writes(), strs(&["set:ANY 2 (\"n1\",\"n2\",\"n3\")"]));
}

// ───────────────────────────── the SI-4 gate ─────────────────────────────

const SHOW2: &str = "ANY 2 (\"n1\",\"n2\")";

fn relax_input() -> ExecInput {
    // V = S = {n1,n2}, n = 2, q = 1; the target q is 0.
    input(
        Q,
        2,
        &["n1", "n2"],
        Some(sync("n0", "n1,n2", 1, SyncKind::Quorum)),
    )
}

#[test]
fn q_is_not_lowered_one_byte_short_of_the_mark_and_is_at_the_mark() {
    let mut io = FakeIo::new(SHOW2, &[("n1", 1000), ("n2", 499)]);
    let o = cycle(&mut ready_with_mark(SHOW2, 500), &mut io, &relax_input());
    assert_eq!(o, ExecOutcome::NoChange);
    assert!(io.writes().is_empty(), "no slack below the mark");
    let mut io = FakeIo::new(SHOW2, &[("n1", 1000), ("n2", 500)]);
    cycle(&mut ready_with_mark(SHOW2, 500), &mut io, &relax_input());
    assert_eq!(io.writes(), strs(&["dcs:n0:n1,n2:0:Quorum"]));
}

#[test]
fn t3_a_voter_that_stopped_streaming_leaves_s_before_v() {
    // n2 has no replication row any more: it is no longer eligible. The
    // planner lowers n first (step D), keeping n2 in S; V and q are untouched
    // in that cycle.
    let mut io = FakeIo::new(SHOW2, &[("n1", 1000)]);
    let o = cycle(&mut ready_with_mark(SHOW2, 500), &mut io, &relax_input());
    assert_eq!(o, ExecOutcome::Progressed);
    assert_eq!(io.writes(), strs(&["set:ANY 1 (\"n1\",\"n2\")"]));
}

#[test]
fn mark_is_taken_from_show_kept_while_the_shape_holds_and_retaken_on_change() {
    let mut exec = ready();
    let inp = relax_input();
    let mut io = FakeIo::new(SHOW2, &ALL);
    io.repl.primary_flush_lsn = 700;
    // First observation: mark = 700, and no relaxation in this cycle.
    assert_eq!(cycle(&mut exec, &mut io, &inp), ExecOutcome::NoChange);
    assert_eq!(exec.mark().unwrap().lsn, 700);
    // The primary moved on, the shape did not: the mark stays.
    io.repl.primary_flush_lsn = 900;
    for row in &mut io.repl.replicas {
        row.flush_lsn = 900;
    }
    // (q is relaxed here; use a state that does not change the shape.)
    cycle(&mut exec, &mut io, &inp);
    assert_eq!(exec.mark().unwrap().lsn, 700);
    // The shape changed (observed): a new, later mark.
    io.show = "ANY 1 (\"n1\",\"n2\")".into();
    io.repl.primary_flush_lsn = 950;
    cycle(&mut exec, &mut io, &inp);
    assert_eq!(exec.mark().unwrap().lsn, 950);
}

#[test]
fn set_pg_does_not_take_the_mark() {
    let show = "ANY 2 (\"n1\",\"n2\")";
    let inp = input(
        Q,
        2,
        &["n1", "n2", "n3"],
        Some(sync("n0", "n1,n2", 0, SyncKind::Quorum)),
    );
    let mut exec = ready_with_mark(show, 500);
    let mut io = FakeIo::new(show, &ALL);
    io.repl.primary_flush_lsn = 777;
    cycle(&mut exec, &mut io, &inp);
    assert_eq!(io.pg_sets.len(), 1);
    assert_eq!(
        exec.mark().unwrap().lsn,
        500,
        "the mark is not retaken at set_pg time"
    );
    assert_eq!(
        exec.mark().unwrap().shape_key,
        parse_sync_value(show).unwrap().key()
    );
}

#[test]
fn a_fresh_mark_blocks_relaxation_even_when_everyone_is_caught_up() {
    let mut exec = ready();
    let mut io = FakeIo::new(SHOW2, &ALL);
    assert_eq!(
        cycle(&mut exec, &mut io, &relax_input()),
        ExecOutcome::NoChange
    );
    assert_eq!(
        cycle(&mut exec, &mut io, &relax_input()),
        ExecOutcome::Progressed
    );
}

// ───────────────────────────── unconfirmed SetPg ─────────────────────────────

fn three() -> [&'static str; 3] {
    ["n1", "n2", "n3"]
}

#[test]
fn show_mismatch_retries_the_same_set_pg_and_counts_cycles() {
    let show = "ANY 2 (\"n1\",\"n2\")";
    let inp = input(
        Q,
        2,
        &three(),
        Some(sync("n0", "n1,n2", 0, SyncKind::Quorum)),
    );
    let mut exec = ready_with_mark(show, 500);
    let mut io = FakeIo::new(show, &ALL); // the reload never lands
    cycle(&mut exec, &mut io, &inp);
    let text = "ANY 2 (\"n1\",\"n2\",\"n3\")".to_string();
    assert_eq!(io.pg_sets, vec![text.clone()]);
    let dcs_before = io.dcs_writes.len();
    for expected in 1..=3u32 {
        // /sync as the first cycle left it; SHOW still the old value.
        let next = input(Q, 2, &three(), io.dcs_state.clone());
        cycle(&mut exec, &mut io, &next);
        assert_eq!(exec.pending.as_ref().unwrap().cycles, expected);
        assert_eq!(io.pg_sets.last().unwrap(), &text);
    }
    assert_eq!(
        io.pg_sets.len(),
        4,
        "one attempt per cycle, always the same text"
    );
    assert_eq!(
        io.dcs_writes.len(),
        dcs_before,
        "nothing else is planned meanwhile"
    );
    // The value lands: the pending record is dropped and planning resumes.
    io.show = text;
    let next = input(Q, 2, &three(), io.dcs_state.clone());
    cycle(&mut exec, &mut io, &next);
    assert!(!exec.has_pending());
}

#[test]
fn a_value_changed_behind_our_back_is_overwritten_with_what_is_in_force() {
    let show = "ANY 2 (\"n1\",\"n2\")";
    let inp = input(
        Q,
        2,
        &three(),
        Some(sync("n0", "n1,n2", 0, SyncKind::Quorum)),
    );
    let mut exec = ready_with_mark(show, 500);
    let mut io = FakeIo::new(show, &ALL);
    cycle(&mut exec, &mut io, &inp);
    assert!(exec.has_pending());
    io.show = "ANY 1 (\"n1\")".into();
    let next = input(Q, 2, &three(), io.dcs_state.clone());
    cycle(&mut exec, &mut io, &next);
    assert_eq!(io.pg_sets.last().unwrap(), "ANY 1 (\"n1\")");
    assert!(!exec.has_pending());
}

#[test]
fn a_failed_set_pg_is_recorded_as_pending_because_the_file_may_have_changed() {
    let show = "ANY 2 (\"n1\",\"n2\")";
    let inp = input(
        Q,
        2,
        &three(),
        Some(sync("n0", "n1,n2", 0, SyncKind::Quorum)),
    );
    let mut exec = ready_with_mark(show, 500);
    let mut io = FakeIo::new(show, &ALL);
    io.set_pg_err = true;
    assert!(matches!(
        cycle(&mut exec, &mut io, &inp),
        ExecOutcome::Failed(_)
    ));
    assert!(exec.has_pending());
}

#[test]
fn a_pending_set_is_not_replayed_after_the_mode_changed() {
    // An async cycle fails its `set_pg("")`; the operator goes back to quorum.
    // The retry must not empty PostgreSQL while /sync is a quorum (SB-38 CI).
    let show = "ANY 2 (\"n1\",\"n2\")";
    let s = || Some(sync("n0", "n1,n2", 0, SyncKind::Quorum));
    let mut exec = ready_with_mark(show, 500);
    let mut io = FakeIo::new(show, &ALL);
    io.set_pg_err = true;
    let leave = input(SynchronousMode::Async, 0, &[], s());
    assert!(matches!(
        cycle(&mut exec, &mut io, &leave),
        ExecOutcome::Failed(_)
    ));
    assert!(io.calls.iter().any(|c| c == "set:"));
    assert!(exec.has_pending());
    io.set_pg_err = false;
    let back = input(Q, 2, &three(), s());
    cycle(&mut exec, &mut io, &back);
    assert!(io.pg_sets.iter().all(|v| !v.is_empty()), "{:?}", io.pg_sets);
    assert!(!exec.has_pending());
}
// ───────────────────────────── unreadable values ─────────────────────────────

fn assert_never_lowers_q(io: &FakeIo, from: u32) {
    for w in &io.dcs_writes {
        assert!(
            w.quorum >= from,
            "q lowered to {} (from {from}): {:?}",
            w.quorum,
            io.calls
        );
    }
}

#[test]
fn wildcard_is_replaced_and_q_is_raised_before_it_not_lowered() {
    // V = {n1,n2}, q = 1 only because S was ANY 1 (n1,n2).
    let inp = input(
        Q,
        1,
        &["n1", "n2"],
        Some(sync("n0", "n1,n2", 1, SyncKind::Quorum)),
    );
    let mut io = FakeIo::new("*", &ALL);
    cycle(
        &mut ready_with_mark("ANY 1 (\"n1\",\"n2\")", 500),
        &mut io,
        &inp,
    );
    assert_eq!(
        io.writes(),
        strs(&["dcs:n0:n1,n2:2:Quorum", "set:ANY 1 (\"n1\",\"n2\")"])
    );
    assert_never_lowers_q(&io, 1);
}

#[test]
fn wildcard_with_nobody_eligible_is_replaced_by_async_and_q_stays() {
    let inp = input(Q, 1, &[], Some(sync("n0", "n1,n2", 1, SyncKind::Quorum)));
    let mut io = FakeIo::new("\"*\"", &ALL);
    cycle(&mut ready(), &mut io, &inp);
    assert_eq!(io.writes(), strs(&["dcs:n0:n1,n2:2:Quorum", "set:"]));
}

#[test]
fn blocking_and_unparseable_values_are_replaced_too() {
    for bad in [
        "ANY 3 (\"n1\",\"n2\")",
        "ANY 0 (n1)",
        "ANY 1 (",
        "node-1",
        "ANY 1 ()",
    ] {
        let inp = input(
            Q,
            1,
            &["n1", "n2"],
            Some(sync("n0", "n1,n2", 1, SyncKind::Quorum)),
        );
        let mut io = FakeIo::new(bad, &ALL);
        cycle(&mut ready(), &mut io, &inp);
        assert_eq!(
            io.pg_sets,
            vec!["ANY 1 (\"n1\",\"n2\")".to_string()],
            "value {bad:?}: {:?}",
            io.calls
        );
        assert_never_lowers_q(&io, 1);
    }
}

#[test]
fn q_is_not_lowered_while_a_wildcard_is_in_force_even_if_the_mark_is_old() {
    // The mark was taken for this very shape and everyone is caught up.
    let inp = input(
        Q,
        2,
        &["n1", "n2"],
        Some(sync("n0", "n1,n2", 2, SyncKind::Quorum)),
    );
    let mut io = FakeIo::new("*", &ALL);
    cycle(&mut ready_with_mark("*", 500), &mut io, &inp);
    assert_never_lowers_q(&io, 2);
    assert_eq!(io.pg_sets.len(), 1);
}

#[test]
fn relaxation_is_blocked_by_a_fresh_mark_or_an_unreadable_value() {
    assert!(!relax_blocked(true, true));
    assert!(relax_blocked(false, true), "fresh mark");
    assert!(relax_blocked(true, false), "unreadable value");
    assert!(relax_blocked(false, false));
}

// ───────────────────────────── strict mode ─────────────────────────────

fn strict_input(count: usize, names: &[&str], s: Option<SyncState>) -> ExecInput {
    ExecInput {
        strict: true,
        ..input(Q, count, names, s)
    }
}

#[test]
fn strict_with_nobody_eligible_renders_the_placeholder_once() {
    let inp = strict_input(1, &[], Some(sync("n0", "", 0, SyncKind::Quorum)));
    let mut io = FakeIo::new("", &[]);
    io.set_pg_applies = true;
    let mut exec = ready();
    cycle(&mut exec, &mut io, &inp);
    assert_eq!(
        io.pg_sets,
        vec![crate::sync::STRICT_NO_ELIGIBLE_PLACEHOLDER.to_string()]
    );
    // Not rewritten while it is in force.
    for _ in 0..3 {
        assert_eq!(cycle(&mut exec, &mut io, &inp), ExecOutcome::NoChange);
    }
    assert_eq!(io.pg_sets.len(), 1);
}

#[test]
fn strict_with_fewer_eligible_than_n_blocks_writes_instead_of_rendering_any_n() {
    // N = 2, only n1 is eligible; V = S = {n1}, n = 1 so far.
    let show = "ANY 1 (\"n1\")";
    let inp = strict_input(2, &["n1"], Some(sync("n0", "n1", 0, SyncKind::Quorum)));
    let mut io = FakeIo::new(show, &[("n1", 1000)]);
    cycle(&mut ready_with_mark(show, 500), &mut io, &inp);
    assert_eq!(
        io.writes(),
        vec![
            "dcs:n0:n1:1:Quorum".to_string(),
            format!("set:{}", crate::sync::STRICT_NO_ELIGIBLE_PLACEHOLDER)
        ]
    );
}

#[test]
fn strict_off_replaces_the_placeholder_by_the_empty_value() {
    let inp = input(Q, 1, &[], Some(sync("n0", "", 0, SyncKind::Quorum)));
    let mut io = FakeIo::new(crate::sync::STRICT_NO_ELIGIBLE_PLACEHOLDER, &[]);
    cycle(&mut ready(), &mut io, &inp);
    assert_eq!(io.pg_sets, vec![String::new()]);
}

// ───────────────────────────── lease and DCS failures mid-cycle ─────────────────────────────

#[test]
fn lease_lost_between_the_dcs_write_and_the_pg_change_stops_before_set_pg() {
    let inp = input(Q, 1, &["n1", "n2"], None);
    let mut io = FakeIo::new("", &ALL);
    io.lease_false_from = Some(1);
    let o = cycle(&mut ready(), &mut io, &inp);
    assert!(matches!(o, ExecOutcome::Failed(_)));
    assert_eq!(io.writes(), strs(&["dcs:n0:n1,n2:1:Quorum"]));
}

#[test]
fn dcs_ok_false_and_err_stop_the_cycle_before_the_pg_change() {
    for script in [Ok(false), Err("boom".to_string())] {
        let inp = input(Q, 1, &["n1", "n2"], None);
        let mut io = FakeIo::new("", &ALL);
        io.dcs_script.push_back(script);
        let o = cycle(&mut ready(), &mut io, &inp);
        assert!(matches!(o, ExecOutcome::Failed(_)));
        assert!(io.pg_sets.is_empty());
    }
}

// ───────────────────────────── T7, T8, T9 ─────────────────────────────

#[test]
fn t7_off_ends_with_the_exact_priority_off_value() {
    let show = "ANY 1 (\"n1\",\"n2\")";
    let mut inp = input(
        SynchronousMode::Async,
        1,
        &["n1", "n2"],
        Some(sync("n0", "n1,n2", 1, SyncKind::Quorum)),
    );
    let mut exec = ready();
    let mut io = FakeIo::new(show, &ALL);
    io.set_pg_applies = true;
    let outcomes = drive(&mut exec, &mut io, &mut inp, 8);
    assert_eq!(
        outcomes.last(),
        Some(&ExecOutcome::HandedOff),
        "{outcomes:?} {:?}",
        io.calls
    );
    assert_eq!(io.show, "");
    let last = io.dcs_writes.last().unwrap();
    // Byte for byte what `Ha::write_sync_state` writes when sync is off.
    assert_eq!(
        serde_json::to_string(last).unwrap(),
        r#"{"leader":"n0","sync_standby":null,"quorum":0}"#
    );
    // SB-38: PostgreSQL is emptied first; no `/sync` write publishes q >= |V|.
    let first_set = io.calls.iter().position(|c| c.starts_with("set:")).unwrap();
    assert!(
        io.calls[..first_set].iter().all(|c| !c.starts_with("dcs:")),
        "{:?}",
        io.calls
    );
    assert!(
        io.calls.iter().all(|c| c != "dcs:n0:n1,n2:2:Quorum"),
        "{:?}",
        io.calls
    );
}

// ───────────────────────────── SB-38 ─────────────────────────────

/// Async leave, PostgreSQL already empty: `/sync` is `(V={n1,n2}, q=1)`.
fn leave_async_input() -> ExecInput {
    input(
        SynchronousMode::Async,
        1,
        &["n1", "n2"],
        Some(sync("n0", "n1,n2", 1, SyncKind::Quorum)),
    )
}

#[test]
fn sb38_handoff_waits_while_a_voter_lags_and_proceeds_when_caught_up() {
    let inp = leave_async_input();
    // Mark 500 taken for the empty value; n2 is 1 byte short.
    let mut exec = ready_with_mark("", 500);
    let mut io = FakeIo::new("", &[("n1", 1000), ("n2", 499)]);
    let o = cycle(&mut exec, &mut io, &inp);
    assert_eq!(o, ExecOutcome::NoChange, "{:?}", io.calls);
    assert!(io.dcs_writes.is_empty(), "{:?}", io.calls);
    assert!(io.pg_sets.is_empty(), "{:?}", io.calls);

    // n2 reaches the mark: the hand-off is written.
    io.repl = snapshot(1000, &[("n1", 1000), ("n2", 500)]);
    let o = cycle(&mut exec, &mut io, &inp);
    assert_eq!(o, ExecOutcome::HandedOff, "{:?}", io.calls);
    assert_eq!(
        serde_json::to_string(io.dcs_writes.last().unwrap()).unwrap(),
        r#"{"leader":"n0","sync_standby":null,"quorum":0}"#
    );
}

#[test]
fn sb38_handoff_waits_for_a_voter_with_no_replication_row() {
    let inp = leave_async_input();
    let mut exec = ready_with_mark("", 500);
    let mut io = FakeIo::new("", &[("n1", 1000)]);
    assert_eq!(cycle(&mut exec, &mut io, &inp), ExecOutcome::NoChange);
    assert!(io.dcs_writes.is_empty(), "{:?}", io.calls);
}

#[test]
fn sb38_handoff_wait_is_rate_limited() {
    let mut exec = ready();
    assert!(exec.note_handoff_wait(true), "first cycle logs");
    for _ in 0..(WARN_EVERY_CYCLES - 2) {
        assert!(!exec.note_handoff_wait(true));
    }
    assert!(exec.note_handoff_wait(true), "every 30th cycle logs again");
    assert!(!exec.note_handoff_wait(false));
    assert!(exec.note_handoff_wait(true), "a new wait logs again");
}

#[test]
fn sb38_leaving_async_does_not_raise_q_to_v_even_with_a_stale_mark() {
    // A fresh mark blocks relaxing; the exemption must still keep q at 1.
    let inp = leave_async_input();
    let mut io = FakeIo::new("", &ALL);
    let mut exec = ready();
    for _ in 0..3 {
        cycle(&mut exec, &mut io, &inp);
    }
    assert!(
        io.dcs_writes.iter().all(|w| w.quorum != 2),
        "{:?}",
        io.calls
    );
}

#[test]
fn sb38_leaving_async_with_an_unreadable_value_raises_q_before_replacing_it() {
    // A wildcard is not asynchronous: the exemption does not apply, so q goes
    // to |V| before the value is replaced.
    let inp = leave_async_input();
    let mut io = FakeIo::new("\"*\"", &ALL);
    let mut exec = ready();
    cycle(&mut exec, &mut io, &inp);
    let first = io.dcs_writes.first().expect("a /sync write");
    assert_eq!((first.quorum, first.kind), (2, SyncKind::Quorum));
    assert_eq!(io.pg_sets, vec![String::new()], "{:?}", io.calls);
}

/// Sync leave: V={n1,n2,n3} drops n3, S={n1,n2}.
fn leave_sync_input(count: usize, q: u32) -> ExecInput {
    input(
        SynchronousMode::Sync,
        count,
        &["n1", "n2"],
        Some(sync("n0", "n1,n2,n3", q, SyncKind::Quorum)),
    )
}

#[test]
fn sb38_sync_leave_removes_the_voter_and_relaxes_q_in_one_write() {
    // ANY 2 of V={n1,n2,n3}: q = 1. Target q* = 0; everyone is caught up.
    let show = "ANY 2 (\"n1\",\"n2\")";
    let inp = leave_sync_input(2, 1);
    let mut io = FakeIo::new(show, &ALL);
    let o = cycle(&mut ready_with_mark(show, 500), &mut io, &inp);
    assert_eq!(o, ExecOutcome::Progressed, "{:?}", io.calls);
    // One /sync write (never q=2 = |V*| in between); the PG form follows.
    assert_eq!(io.dcs_writes.len(), 1, "{:?}", io.calls);
    assert_eq!(io.writes()[0], "dcs:n0:n1,n2:0:Quorum", "{:?}", io.calls);
}

#[test]
fn sb38_sync_leave_waits_when_the_merged_q_would_not_be_below_v() {
    // V={n1,n2,n3}, S={n1}, n=1: q = 2. Target V*={n1}, q* = 0. While n1 is
    // short of the mark q' = min(2, max(0, 1)) = 1 = |V*|: no write at all
    // (the old order removed voters with q=2 unchanged, above |V*| = 1).
    let show = "ANY 1 (\"n1\")";
    let inp = input(
        SynchronousMode::Sync,
        1,
        &["n1"],
        Some(sync("n0", "n1,n2,n3", 2, SyncKind::Quorum)),
    );
    let mut io = FakeIo::new(show, &[("n1", 499)]);
    let mut exec = ready_with_mark(show, 500);
    let o = cycle(&mut exec, &mut io, &inp);
    assert_eq!(o, ExecOutcome::NoChange, "{:?}", io.calls);
    assert!(io.dcs_writes.is_empty(), "{:?}", io.calls);

    // Once n1 has reached the mark, q' = 0 < 1 and the write goes out.
    io.repl = snapshot(1000, &[("n1", 500)]);
    let o = cycle(&mut exec, &mut io, &inp);
    assert_eq!(o, ExecOutcome::Progressed, "{:?}", io.calls);
    assert_eq!(io.writes()[0], "dcs:n0:n1:0:Quorum", "{:?}", io.calls);
}

#[test]
fn sb38_sync_leave_with_a_fresh_mark_does_not_write() {
    let show = "ANY 2 (\"n1\",\"n2\")";
    let inp = leave_sync_input(2, 1);
    let mut io = FakeIo::new(show, &ALL);
    let o = cycle(&mut ready(), &mut io, &inp);
    assert_eq!(o, ExecOutcome::NoChange, "{:?}", io.calls);
    assert!(io.dcs_writes.is_empty(), "{:?}", io.calls);
}

#[test]
fn t8_q_formula() {
    // (/sync names, SHOW, expected q)
    let table: [(&str, &str, u32); 7] = [
        ("n1,n2", "FIRST 2 (\"n1\",\"n2\")", 0),
        ("n1,n2", "FIRST 1 (\"n1\",\"n2\")", 1),
        ("n1", "FIRST 1 (\"n1\",\"n2\")", 1),
        ("n1,n2", "FIRST 1 (\"n1\")", 1),
        ("", "", 0),
        ("n1", "", 1),
        ("n1,n2", "FIRST 5 (\"n1\",\"n2\")", 2),
    ];
    for (names, show, q) in table {
        let inp = input(
            Q,
            1,
            &["n1", "n2"],
            Some(sync("n0", names, 7, SyncKind::Priority)),
        );
        let mut io = FakeIo::new(show, &ALL);
        let o = cycle(&mut ready(), &mut io, &inp);
        assert_eq!(o, ExecOutcome::Progressed, "{show}");
        assert_eq!(
            io.dcs_writes.len(),
            1,
            "one write, nothing else, for {names:?} {show}"
        );
        assert!(io.pg_sets.is_empty());
        let w = &io.dcs_writes[0];
        assert_eq!(w.kind, SyncKind::Quorum);
        assert_eq!(w.quorum, q, "names {names:?} show {show}");
    }
}

#[test]
fn t8_with_a_wildcard_or_the_placeholder_uses_the_strictest_q() {
    for show in ["*", crate::sync::STRICT_NO_ELIGIBLE_PLACEHOLDER] {
        let inp = input(
            Q,
            1,
            &["n1", "n2"],
            Some(sync("n0", "n1,n2", 1, SyncKind::Priority)),
        );
        let mut io = FakeIo::new(show, &ALL);
        cycle(&mut ready(), &mut io, &inp);
        assert_eq!(io.dcs_writes[0].quorum, 2, "{show}");
    }
}

#[test]
fn t8_then_first_is_rewritten_as_any_once_the_planner_is_done() {
    let mut inp = input(
        Q,
        2,
        &["n1", "n2"],
        Some(sync("n0", "n1,n2", 2, SyncKind::Priority)),
    );
    let mut io = FakeIo::new("FIRST 2 (\"n1\",\"n2\")", &ALL);
    io.set_pg_applies = true;
    let mut exec = ready();
    drive(&mut exec, &mut io, &mut inp, 4);
    assert_eq!(io.dcs_writes[0].kind, SyncKind::Quorum);
    assert_eq!(io.dcs_writes[0].quorum, 0);
    assert_eq!(io.pg_sets, vec!["ANY 2 (\"n1\",\"n2\")".to_string()]);
    // Then it is quiet.
    let before = io.writes().len();
    cycle(&mut exec, &mut io, &inp);
    assert_eq!(io.writes().len(), before);
}

#[test]
fn t9_quorum_back_to_priority_ends_with_the_priority_value_and_a_hand_off() {
    // V = S = {n1,n2,n3}, n = 2, q = 1; Priority wants N = 2.
    let show = "ANY 2 (\"n1\",\"n2\",\"n3\")";
    let mut inp = input(
        SynchronousMode::Sync,
        2,
        &three(),
        Some(sync("n0", "n1,n2,n3", 1, SyncKind::Quorum)),
    );
    let mut io = FakeIo::new(show, &ALL);
    io.set_pg_applies = true;
    let mut exec = ready();
    let outcomes = drive(&mut exec, &mut io, &mut inp, 20);
    assert_eq!(
        outcomes.last(),
        Some(&ExecOutcome::HandedOff),
        "{outcomes:?} {:?}",
        io.calls
    );
    let last = io.dcs_writes.last().unwrap();
    // The Priority wire form: kind omitted, `quorum` = N, names = S.
    let json = serde_json::to_string(last).unwrap();
    assert!(!json.contains("kind"), "{json}");
    assert_eq!(last.quorum, 2);
    assert_eq!(last.sync_standby_names().len(), 2);
    // PostgreSQL carries FIRST n (S) with the same names.
    let shape = parse_sync_value(&io.show).unwrap();
    assert_eq!(shape.keyword, SyncKeyword::First);
    assert_eq!(shape.n, 2);
    assert_eq!(shape.names_folded().len(), 2);
    // Every earlier write is still a Quorum one.
    let n = io.dcs_writes.len();
    assert!(
        io.dcs_writes[..n - 1]
            .iter()
            .all(|w| w.kind == SyncKind::Quorum)
    );
}

#[test]
fn t9_with_nobody_eligible_hands_off_with_the_empty_value() {
    let mut inp = input(
        SynchronousMode::Sync,
        1,
        &[],
        Some(sync("n0", "", 0, SyncKind::Quorum)),
    );
    let mut io = FakeIo::new("", &[]);
    let outcomes = drive(&mut ready(), &mut io, &mut inp, 3);
    assert_eq!(outcomes, vec![ExecOutcome::HandedOff]);
    assert_eq!(
        serde_json::to_string(&io.dcs_writes[0]).unwrap(),
        r#"{"leader":"n0","sync_standby":null,"quorum":1}"#
    );
}

// ───────────────────────────── wire form ─────────────────────────────

#[test]
fn build_sync_state_matches_the_priority_wire_form() {
    let names: Names = ["b", "a"].iter().map(|s| s.to_string()).collect();
    let p = build_sync_state("n0", &names, 3, SyncKind::Priority);
    assert_eq!(
        serde_json::to_string(&p).unwrap(),
        r#"{"leader":"n0","sync_standby":"a,b","quorum":3}"#
    );
    let q = build_sync_state("n0", &names, 1, SyncKind::Quorum);
    assert_eq!(
        serde_json::to_string(&q).unwrap(),
        r#"{"leader":"n0","sync_standby":"a,b","quorum":1,"kind":"quorum"}"#
    );
    let none = build_sync_state("n0", &Names::new(), 0, SyncKind::Priority);
    assert_eq!(
        serde_json::to_string(&none).unwrap(),
        r#"{"leader":"n0","sync_standby":null,"quorum":0}"#
    );
}

// ───────────────────────────── SB-13 PR3c: pre_promote_sync ─────────────────────────────

/// Panics on any call: the Priority/Off paths must not even look at the lease.
struct PanicIo;

#[async_trait]
impl SyncIo for PanicIo {
    async fn show_sync_names(&mut self) -> Result<String> {
        panic!("show_sync_names called");
    }
    async fn replication(&mut self) -> Result<ReplicationSnapshot> {
        panic!("replication called");
    }
    async fn set_pg(&mut self, _: &str) -> Result<()> {
        panic!("set_pg called");
    }
    async fn reload(&mut self) -> Result<()> {
        panic!("reload called");
    }
    async fn write_dcs(&mut self, _: &SyncState) -> Result<bool> {
        panic!("write_dcs called");
    }
    fn lease_ok(&self) -> bool {
        panic!("lease_ok called");
    }
}

fn paused_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap()
}

fn pre<IO: SyncIo>(
    io: &mut IO,
    mode: SynchronousMode,
    me: &str,
    s: Option<&SyncState>,
) -> std::result::Result<(), String> {
    paused_rt().block_on(pre_promote_sync(io, mode, me, s))
}

/// `/sync` of the dead leader n0: V = {n1,n2,n3}, q = 1. For n1 the value to
/// install is `ANY 2 ("n0","n2","n3")` (V' = V - n1 + n0, n' = |V| - q = 2).
fn old_sync() -> SyncState {
    sync("n0", "n1,n2,n3", 1, SyncKind::Quorum)
}
const PRE_VALUE: &str = "ANY 2 (\"n0\",\"n2\",\"n3\")";

#[test]
fn pre_promote_writes_the_post_promotion_value_then_confirms_with_show() {
    let mut io = FakeIo::new("", &ALL);
    io.set_pg_applies = true;
    pre(&mut io, Q, "n1", Some(&old_sync())).unwrap();
    // idempotency probe, the write, the confirmation: in this order.
    assert_eq!(io.calls, vec!["show", &format!("set:{PRE_VALUE}"), "show"]);
    assert!(io.dcs_writes.is_empty(), "/sync is the executor's job");
}

#[test]
fn pre_promote_is_also_used_while_the_mode_is_priority_but_the_sync_is_quorum() {
    // T7 / T9 window: the executor still applies, so a promotion installs it.
    for mode in [
        SynchronousMode::Async,
        SynchronousMode::Sync,
        SynchronousMode::Quorum,
    ] {
        let mut io = FakeIo::new("", &ALL);
        io.set_pg_applies = true;
        pre(&mut io, mode, "n1", Some(&old_sync())).unwrap();
        assert_eq!(io.pg_sets, vec![PRE_VALUE.to_string()], "{mode:?}");
    }
}

#[test]
fn pre_promote_is_idempotent_and_compares_the_shape_not_the_text() {
    // Same shape, different spelling: nothing is written (m19, m21).
    for shown in [
        PRE_VALUE,
        "any 2 ( \"n3\" , \"n0\",\"N2\" )",
        "ANY 2 (n0, n2, n3)",
        "  ANY 2 (\"n0\",\"n2\",\"n3\")  ",
    ] {
        let mut io = FakeIo::new(shown, &ALL);
        pre(&mut io, Q, "n1", Some(&old_sync())).unwrap();
        assert_eq!(io.calls, vec!["show"], "{shown:?}");
    }
}

#[test]
fn pre_promote_rewrites_every_other_shape() {
    for shown in [
        "",
        "ANY 1 (\"n0\",\"n2\",\"n3\")",
        "ANY 2 (\"n1\",\"n2\",\"n3\")",
        "FIRST 2 (\"n0\",\"n2\",\"n3\")",
        "ANY 2 (\"n0\",\"n2\")",
        "ANY 2 (*)",
        "not a value (",
    ] {
        let mut io = FakeIo::new(shown, &ALL);
        io.set_pg_applies = true;
        pre(&mut io, Q, "n1", Some(&old_sync())).unwrap();
        assert_eq!(io.pg_sets, vec![PRE_VALUE.to_string()], "{shown:?}");
    }
}

#[test]
fn pre_promote_writes_even_when_the_idempotency_probe_cannot_read() {
    let mut io = FakeIo::new("", &ALL);
    io.show_err = true;
    io.set_pg_applies = true;
    // The first SHOW fails, the write goes ahead, the confirmation fails too.
    assert!(pre(&mut io, Q, "n1", Some(&old_sync())).is_err());
    assert_eq!(io.pg_sets, vec![PRE_VALUE.to_string()]);
}

#[test]
fn pre_promote_waits_for_a_late_reload_but_not_forever() {
    // Lands after 5 stale SHOWs (0.5 s): confirmed.
    let mut io = FakeIo::new("", &ALL);
    io.set_pg_applies = true;
    io.stale_shows = 5;
    pre(&mut io, Q, "n1", Some(&old_sync())).unwrap();
    // Never lands: an error after the 2 s window, one write only (m15).
    let mut io = FakeIo::new("", &ALL);
    io.set_pg_applies = true;
    io.stale_shows = 10_000;
    let r = (paused_rt().block_on(pre_promote_sync(&mut io, Q, "n1", Some(&old_sync()))),);
    assert!(r.0.unwrap_err().contains("not confirmed"));
    assert_eq!(io.pg_sets.len(), 1);
    let shows = io.calls.iter().filter(|c| *c == "show").count();
    assert!((20..=23).contains(&shows), "polled {shows} times in ~2 s");
}

#[test]
fn pre_promote_does_not_accept_any_other_value_as_confirmation() {
    for stuck in ["", "ANY 1 (\"n9\")", "ANY 2 (\"n0\",\"n2\")", "garbage ("] {
        let mut io = FakeIo::new(stuck, &ALL);
        // set_pg_applies false: SHOW stays at `stuck`.
        assert!(
            pre(&mut io, Q, "n1", Some(&old_sync())).is_err(),
            "{stuck:?}"
        );
    }
}

#[test]
fn pre_promote_set_failure_is_an_error_without_confirmation() {
    let mut io = FakeIo::new("", &ALL);
    io.set_pg_err = true;
    let e = pre(&mut io, Q, "n1", Some(&old_sync())).unwrap_err();
    assert!(e.contains("pre-promote write failed"), "{e}");
    assert_eq!(
        io.calls.last().map(String::as_str),
        Some(&*format!("set:{PRE_VALUE}"))
    );
}

#[test]
fn pre_promote_write_is_bounded_to_five_seconds() {
    let mut io = FakeIo::new("", &ALL);
    io.hang_set_pg = true;
    let r = paused_rt().block_on(async {
        tokio::time::timeout(
            Duration::from_secs(60),
            pre_promote_sync(&mut io, Q, "n1", Some(&old_sync())),
        )
        .await
    });
    let e = r
        .expect("the write must be cut by its own bound")
        .unwrap_err();
    assert!(e.contains("timed out"), "{e}");
}

#[test]
fn pre_promote_without_a_lease_writes_nothing() {
    let mut io = FakeIo::new("", &ALL);
    io.lease_false_from = Some(0);
    assert!(pre(&mut io, Q, "n1", Some(&old_sync())).is_err());
    assert!(io.calls.is_empty());
}

#[test]
fn pre_promote_installs_nothing_for_a_non_voter_or_when_q_covers_every_voter() {
    // me is not a voter (m20); q >= |V|; both leave it to the reset path.
    for (me, s) in [
        ("n9", sync("n0", "n1,n2,n3", 1, SyncKind::Quorum)),
        ("n1", sync("n0", "n1,n2,n3", 3, SyncKind::Quorum)),
        ("n1", sync("n0", "n1,n2,n3", 5, SyncKind::Quorum)),
        ("n1", sync("n0", "", 0, SyncKind::Quorum)),
    ] {
        let r = pre(&mut PanicIo, Q, me, Some(&s));
        assert_eq!(r, Ok(()), "{me} {s:?}");
    }
}

#[test]
fn pre_promote_skips_a_sync_of_its_own_and_priority_or_absent_syncs() {
    for s in [
        Some(sync("n1", "n2,n3", 1, SyncKind::Quorum)),
        Some(sync("n0", "n1,n2", 1, SyncKind::Priority)),
        Some(sync("", "n1,n2", 1, SyncKind::Quorum)),
        None,
    ] {
        assert_eq!(pre(&mut PanicIo, Q, "n1", s.as_ref()), Ok(()), "{s:?}");
    }
}

#[test]
fn pre_promote_leader_name_is_compared_case_insensitively() {
    let s = sync("N1", "n2,n3", 1, SyncKind::Quorum);
    assert_eq!(pre(&mut PanicIo, Q, "n1", Some(&s)), Ok(()));
}

proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig::with_cases(1500))]

    /// ZERO I/O for every Priority/Off input: any mode that is not Quorum,
    /// `/sync` absent or of kind Priority (the default when `kind` is absent),
    /// any leader, names, quorum and `me`. `PanicIo` panics on ANY call,
    /// the counting fake sees none.
    #[test]
    fn pre_promote_is_zero_io_for_every_priority_and_off_input(
        mode in proptest::prelude::prop_oneof![
            proptest::prelude::Just(SynchronousMode::Async),
            proptest::prelude::Just(SynchronousMode::Sync),
        ],
        sync_in in proptest::prelude::prop::option::of((
            "[a-zA-Z0-9_-]{0,6}",
            proptest::prelude::prop::option::of("[a-z0-9,]{0,14}"),
            0u32..8,
        )),
        me in "[a-zA-Z0-9_-]{1,6}",
    ) {
        let s = sync_in.map(|(leader, standby, quorum)| SyncState {
            leader,
            sync_standby: standby,
            quorum,
            kind: SyncKind::Priority,
        });
        proptest::prop_assert_eq!(pre(&mut PanicIo, mode, &me, s.as_ref()), Ok(()));
        let mut io = FakeIo::new("", &ALL);
        proptest::prop_assert_eq!(pre(&mut io, mode, &me, s.as_ref()), Ok(()));
        proptest::prop_assert!(io.calls.is_empty(), "calls: {:?}", io.calls);
        // `kind` absent in the stored JSON means Priority.
        let json = s.as_ref().map(|s| {
            let mut v = serde_json::to_value(s).unwrap();
            v.as_object_mut().unwrap().remove("kind");
            v
        });
        if let Some(v) = json {
            let back: SyncState = serde_json::from_value(v).unwrap();
            proptest::prop_assert_eq!(back.kind, SyncKind::Priority);
            proptest::prop_assert_eq!(pre(&mut PanicIo, mode, &me, Some(&back)), Ok(()));
        }
    }

    /// Whatever the voter set, a written value is `ANY n' (V')` with
    /// `1 <= n' <= |V'|`, `V' = V - me + leader`, `n' = |V| - q`; nothing is
    /// written when `post_promotion_state` is `None`.
    #[test]
    fn pre_promote_values_are_always_valid_and_match_the_design(
        voters in proptest::prelude::prop::collection::btree_set("[a-e]", 0..6),
        q in 0u32..8,
        me in "[a-e]",
        leader in "[f-h]",
    ) {
        let names = voters.iter().cloned().collect::<Vec<_>>().join(",");
        let s = sync(&leader, &names, q, SyncKind::Quorum);
        let mut io = FakeIo::new("", &ALL);
        io.set_pg_applies = true;
        let r = pre(&mut io, Q, &me, Some(&s));
        let is_voter = voters.contains(&me);
        let applies = is_voter && (q as usize) < voters.len();
        if !applies {
            proptest::prop_assert_eq!(r, Ok(()));
            proptest::prop_assert!(io.calls.is_empty());
        } else {
            proptest::prop_assert_eq!(r, Ok(()));
            proptest::prop_assert_eq!(io.pg_sets.len(), 1);
            let shape = parse_sync_value(&io.pg_sets[0]).unwrap();
            let mut want: Names = voters.iter().cloned().collect();
            want.remove(&me);
            want.insert(leader.clone());
            proptest::prop_assert_eq!(shape.names_folded(), want.clone());
            proptest::prop_assert_eq!(shape.n, voters.len() - q as usize);
            proptest::prop_assert!(shape.n >= 1 && shape.n <= want.len());
            proptest::prop_assert_eq!(shape.keyword, SyncKeyword::Any);
            proptest::prop_assert!(!shape.names.contains(&me));
        }
    }
}

/// End to end on one fake: the pre-promote write, then the executor's own
/// finalization. After the write the executor sees an expected PromotionWindow
/// and writes `(me, V', q)`; without the write (restart, manual promote) it
/// takes the reset path.
#[test]
fn finalization_after_the_pre_promote_write_installs_v_prime_and_without_it_resets() {
    let inp = |sync: &SyncState| ExecInput {
        me: "n1".into(),
        mode: Q,
        count: 2,
        strict: false,
        lag_threshold: None,
        members: ["n0", "n2", "n3"]
            .iter()
            .map(|n| member(n, false))
            .collect(),
        sync: Some(sync.clone()),
        watchdog_on: true,
    };
    // With the write.
    let mut io = FakeIo::new("", &ALL);
    io.set_pg_applies = true;
    pre(&mut io, Q, "n1", Some(&old_sync())).unwrap();
    assert_eq!(io.show, PRE_VALUE);
    let o = cycle(&mut ready(), &mut io, &inp(&old_sync()));
    assert_eq!(o, ExecOutcome::Progressed);
    assert_eq!(
        io.dcs_writes.first().map(dcs_desc).as_deref(),
        Some("dcs:n1:n0,n2,n3:1:Quorum"),
        "the first write is V' with q kept"
    );
    // Without the write: the node runs a value of its own.
    let mut io = FakeIo::new("", &ALL);
    let o = cycle(&mut ready(), &mut io, &inp(&old_sync()));
    assert_eq!(o, ExecOutcome::Progressed);
    assert_eq!(
        io.dcs_writes.first().map(dcs_desc).as_deref(),
        Some("dcs:n1::0:Quorum"),
        "no pre-promote value in force: reset to an empty voter set"
    );
}

// ─────────────── SB-13 PR4: operator warnings and their rate limit ───────────────

fn nofailover(mut m: Member) -> Member {
    m.tags
        .insert("nofailover".to_string(), serde_json::json!(true));
    m
}

fn voters(names: &[&str]) -> Names {
    names.iter().map(|n| n.to_string()).collect()
}

#[test]
fn warning_count_one_with_three_or_more_voters() {
    let w = |mode, count, n: &[&str]| quorum_warnings(&input(mode, count, n, None), &voters(n));
    assert!(w(Q, 1, &["n1", "n2", "n3"]).contains(&QuorumWarning::CountOneWithManyVoters(3)));
    assert!(w(Q, 1, &["n1", "n2", "n3", "n4"]).contains(&QuorumWarning::CountOneWithManyVoters(4)));
    for (count, n) in [
        (1, vec!["n1", "n2"]),
        (1, vec!["n1"]),
        (2, vec!["n1", "n2", "n3"]),
    ] {
        assert!(
            !w(Q, count, &n)
                .iter()
                .any(|x| matches!(x, QuorumWarning::CountOneWithManyVoters(_))),
            "count {count} voters {n:?}"
        );
    }
    // Never outside the quorum mode.
    for mode in [SynchronousMode::Sync, SynchronousMode::Async] {
        assert!(w(mode, 1, &["n1", "n2", "n3"]).is_empty());
    }
}

#[test]
fn warning_watchdog_off_only_in_quorum_mode() {
    let mut inp = input(Q, 2, &["n1", "n2", "n3"], None);
    assert!(quorum_warnings(&inp, &voters(&["n1", "n2", "n3"])).is_empty());
    inp.watchdog_on = false;
    assert_eq!(
        quorum_warnings(&inp, &voters(&["n1", "n2", "n3"])),
        vec![QuorumWarning::WatchdogOff]
    );
    for mode in [SynchronousMode::Sync, SynchronousMode::Async] {
        inp.mode = mode;
        assert!(quorum_warnings(&inp, &voters(&["n1", "n2", "n3"])).is_empty());
    }
}

#[test]
fn warning_all_voters_nofailover() {
    let mut inp = input(Q, 2, &["n1", "n2", "n3"], None);
    inp.members = vec![
        nofailover(member("n1", false)),
        nofailover(member("n2", false)),
        nofailover(member("n3", false)),
    ];
    assert_eq!(
        quorum_warnings(&inp, &voters(&["n1", "n2", "n3"])),
        vec![QuorumWarning::AllVotersNofailover]
    );
    // One electable voter: no warning.
    inp.members[2] = member("n3", false);
    assert!(quorum_warnings(&inp, &voters(&["n1", "n2", "n3"])).is_empty());
    // Nobody eligible: nothing to say.
    assert!(quorum_warnings(&inp, &voters(&[])).is_empty());
}

#[test]
fn warnings_are_emitted_on_change_and_every_30th_cycle() {
    let mut exec = QuorumExec::default();
    let a = [QuorumWarning::WatchdogOff];
    assert!(exec.should_emit_warnings(&a));
    for _ in 1..29 {
        assert!(!exec.should_emit_warnings(&a));
    }
    assert!(exec.should_emit_warnings(&a), "30th cycle");
    assert!(!exec.should_emit_warnings(&a));
    // A changed set is emitted at once.
    let b = [
        QuorumWarning::WatchdogOff,
        QuorumWarning::AllVotersNofailover,
    ];
    assert!(exec.should_emit_warnings(&b));
    // Nothing to say, nothing emitted, ever.
    assert!(!exec.should_emit_warnings(&[]));
    for _ in 0..60 {
        assert!(!exec.should_emit_warnings(&[]));
    }
    assert!(
        exec.should_emit_warnings(&a),
        "a new set after a quiet period"
    );
}

fn unreadable_obs(raw: &str) -> Observed {
    observe_value(raw)
}

#[test]
fn unreadable_value_warning_is_rate_limited_like_the_pending_warning() {
    let mut exec = QuorumExec::default();
    let bad = unreadable_obs("ANY (");
    assert!(bad.unreadable.is_some());
    assert!(exec.note_unreadable(&bad), "first sight");
    for _ in 1..29 {
        assert!(!exec.note_unreadable(&bad), "same text, silent");
    }
    assert!(exec.note_unreadable(&bad), "30th consecutive cycle");
    assert!(!exec.note_unreadable(&bad));
    // A different unreadable text warns at once.
    assert!(exec.note_unreadable(&unreadable_obs("FIRST (")));
    // A readable value resets: the same bad text warns again.
    let good = unreadable_obs("ANY 1 (\"n1\")");
    assert!(good.unreadable.is_none());
    assert!(!exec.note_unreadable(&good));
    assert!(exec.note_unreadable(&bad));
}

/// The warnings change nothing the executor writes or plans: the same cycle
/// with and without a watchdog (the only difference that warns) writes the same.
#[test]
fn warnings_do_not_change_what_a_cycle_does() {
    let run = |watchdog_on: bool| {
        let mut inp = input(
            Q,
            1,
            &three(),
            Some(sync("n0", "n1,n2,n3", 1, SyncKind::Quorum)),
        );
        inp.watchdog_on = watchdog_on;
        let show = "ANY 1 (\"n1\",\"n2\",\"n3\")";
        let mut io = FakeIo::new(show, &ALL);
        io.set_pg_applies = true;
        let mut exec = ready_with_mark(show, 1000);
        let out = cycle(&mut exec, &mut io, &inp);
        (
            out,
            io.pg_sets.clone(),
            io.dcs_writes.iter().map(dcs_desc).collect::<Vec<_>>(),
        )
    };
    assert_eq!(run(true), run(false));
}
// ───────────────────────── q-not-relaxed window (observability) ─────────────────────────
#[test]
fn the_executor_records_n_from_show_each_cycle() {
    let two = "ANY 1 (\"n1\",\"n2\")";
    let inp = input(
        Q,
        1,
        &["n1", "n2"],
        Some(sync("n0", "n1,n2", 2, SyncKind::Quorum)),
    );
    let mut exec = ready_with_mark(two, 500);
    assert_eq!(exec.observed_n(), None);
    let mut io = FakeIo::new(two, &[("n1", 1000), ("n2", 1000)]);
    cycle(&mut exec, &mut io, &inp);
    assert_eq!(exec.observed_n(), Some(1));
    // An empty list is asynchronous: n = 0.
    let mut exec = ready();
    let mut io = FakeIo::new("", &[("n1", 1000), ("n2", 1000)]);
    cycle(&mut exec, &mut io, &inp);
    assert_eq!(exec.observed_n(), Some(0));
}
#[test]
fn the_window_warning_fires_on_open_and_every_30th_cycle_and_resets_on_close() {
    let mut exec = QuorumExec::default();
    assert!(!exec.note_window(false));
    assert!(exec.note_window(true), "first cycle of the window");
    for _ in 1..29 {
        assert!(!exec.note_window(true));
    }
    assert!(exec.note_window(true), "30th cycle");
    assert!(!exec.note_window(true));
    // Closed: silent, and the next window warns at once again.
    assert!(!exec.note_window(false));
    assert!(exec.note_window(true));
}

// ───────────────────────────── SB-36 ─────────────────────────────

/// Fix 1: an async cluster turned to quorum writes `/sync` with the settled
/// `q = |V| - n` first, then PostgreSQL, in the same cycle.
#[test]
fn sb36_enable_writes_settled_q_before_the_pg_value() {
    let inp = input(Q, 2, &three(), None);
    let mut io = FakeIo::new("", &ALL);
    assert_eq!(cycle(&mut ready(), &mut io, &inp), ExecOutcome::Progressed);
    assert_eq!(
        io.writes(),
        strs(&[
            "dcs:n0:n1,n2,n3:1:Quorum",
            "set:ANY 2 (\"n1\",\"n2\",\"n3\")"
        ])
    );
}

/// A crash between the two enabling writes: the next cycle does not raise `q`
/// and only sets PostgreSQL.
#[test]
fn sb36_enabling_window_is_not_undone_and_only_pg_follows() {
    let inp = input(
        Q,
        2,
        &three(),
        Some(sync("n0", "n1,n2,n3", 1, SyncKind::Quorum)),
    );
    let mut io = FakeIo::new("", &ALL);
    cycle(&mut ready(), &mut io, &inp);
    assert_eq!(io.writes(), strs(&["set:ANY 2 (\"n1\",\"n2\",\"n3\")"]));
}

/// A `/sync` stuck at `q = |V|` with PostgreSQL asynchronous is not lowered
/// before the SI-4 gate (fresh mark).
#[test]
fn sb36_stuck_q_is_not_lowered_before_the_gate() {
    let inp = input(
        Q,
        2,
        &three(),
        Some(sync("n0", "n1,n2,n3", 3, SyncKind::Quorum)),
    );
    let mut io = FakeIo::new("", &ALL);
    cycle(&mut ready(), &mut io, &inp);
    assert_never_lowers_q(&io, 3);
    assert!(io.pg_sets.is_empty(), "{:?}", io.calls);
}

/// Fix 2: a voter that stops streaming but keeps its member record stays in
/// `V`, so nothing is written while it is away nor when it comes back.
#[test]
fn sb36_briefly_offline_voter_is_kept_and_its_return_raises_nothing() {
    let show = "ANY 1 (\"n1\",\"n2\")";
    let inp = input(
        Q,
        1,
        &["n1", "n2"],
        Some(sync("n0", "n1,n2", 1, SyncKind::Quorum)),
    );
    let mut exec = ready_with_mark(show, 500);
    let mut io = FakeIo::new(show, &[("n1", 1000)]);
    assert_eq!(cycle(&mut exec, &mut io, &inp), ExecOutcome::NoChange);
    assert!(io.writes().is_empty(), "{:?}", io.calls);
    // Rejoin.
    let mut io = FakeIo::new(show, &ALL);
    assert_eq!(cycle(&mut exec, &mut io, &inp), ExecOutcome::NoChange);
    assert!(io.writes().is_empty(), "{:?}", io.calls);
}

/// Once the voter's member record expired (or it is `nosync`), the usual
/// removal applies: `S` shrinks first and `q` is kept.
#[test]
fn sb36_expired_or_nosync_offline_voter_is_removed_as_before() {
    let show = "ANY 1 (\"n1\",\"n2\")";
    let mut nosync = input(
        Q,
        1,
        &["n1"],
        Some(sync("n0", "n1,n2", 1, SyncKind::Quorum)),
    );
    let expired = nosync.clone();
    nosync.members.push(member("n2", true));
    for inp in [expired, nosync] {
        let mut io = FakeIo::new(show, &[("n1", 1000)]);
        cycle(&mut ready_with_mark(show, 500), &mut io, &inp);
        assert_eq!(io.writes(), strs(&["set:ANY 1 (\"n1\")"]));
        assert_never_lowers_q(&io, 1);
    }
}

#[test]
fn sb36_keep_offline_voters_rule() {
    let names = |v: &[&str]| -> Names { v.iter().map(|s| s.to_string()).collect() };
    let members = vec![
        member("n1", false),
        member("n2", false),
        member("n3", true),
        member("n0", false),
    ];
    let snap = snapshot(1000, &[("n1", 1000)]);
    let current = names(&["n0", "n1", "n2", "n3", "n4"]);
    let target = QuorumTarget::new(["n1"], 1);
    let kept = keep_offline_voters(target, &current, &members, &snap, "n0");
    // n2 is kept (record present); n3 is nosync, n4 has no record, n0 is me.
    assert_eq!(kept.voters, names(&["n1", "n2"]));
    assert_eq!(kept.n, 1);
    // An empty selection stays empty.
    let empty = keep_offline_voters(empty_target(), &current, &members, &snap, "n0");
    assert!(empty.voters.is_empty() && empty.n == 0);
    // A voter that streams but was not selected (lagging) is not kept.
    let snap = snapshot(1000, &[("n1", 1000), ("n2", 10)]);
    let target = QuorumTarget::new(["n1"], 1);
    let kept = keep_offline_voters(target, &current, &members, &snap, "n0");
    assert_eq!(kept.voters, names(&["n1"]));
}
