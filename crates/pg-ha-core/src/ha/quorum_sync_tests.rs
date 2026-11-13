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
        if self.set_pg_err {
            return Err(Error::Postgres("set failed".into()));
        }
        self.pg_sets.push(value.to_string());
        if self.set_pg_applies {
            self.show = value.to_string();
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

fn input(mode: EffectiveSyncMode, count: usize, names: &[&str], s: Option<SyncState>) -> ExecInput {
    ExecInput {
        me: "n0".into(),
        mode,
        count,
        strict: false,
        lag_threshold: None,
        members: names.iter().map(|n| member(n, false)).collect(),
        sync: s,
    }
}

const ALL: [(&str, u64); 3] = [("n1", 1000), ("n2", 1000), ("n3", 1000)];
const Q: EffectiveSyncMode = EffectiveSyncMode::Quorum;

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
        strs(&["dcs:n0:n1,n2:2:Quorum", "set:ANY 1 (\"n1\",\"n2\")"])
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
    assert_eq!(io.writes(), strs(&["dcs:n0:n1,n2:2:Quorum"]));
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
        EffectiveSyncMode::Off,
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
    // q was raised before the PG value was emptied.
    let first_set = io.calls.iter().position(|c| c.starts_with("set:")).unwrap();
    assert!(
        io.calls[..first_set]
            .iter()
            .any(|c| c == "dcs:n0:n1,n2:2:Quorum")
    );
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
        EffectiveSyncMode::Priority,
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
        EffectiveSyncMode::Priority,
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
