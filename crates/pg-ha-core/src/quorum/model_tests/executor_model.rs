//! Executor-in-model proptest (SB-13 PR3b, plan 5.2).
//!
//! The REAL `QuorumExec::cycle` runs against `ModelIo : SyncIo`, which is
//! built on the same `World` the planner model uses (real logs, real
//! `SyncManager`, real vote round, real `post_promotion_state`). What is new
//! compared with the planner-only model:
//!
//! * PostgreSQL holds TWO values, the `file` (`postgresql.auto.conf`) and the
//!   `effective` one (what the server runs and what SHOW returns). `set_pg`
//!   writes the file; the reload that makes it effective can land at once, land
//!   after the executor read SHOW `k` more times (so a decision taken from SHOW
//!   is followed by a change), or only when a later reload (an explicit
//!   `ReloadLands` action, the executor's restart-rule `reload()`, or another
//!   `set_pg`) delivers it. SI-1 and invariant I are checked after EVERY I/O
//!   call, on `(DCS, effective)` AND `(DCS, file)`.
//! * Faults at chosen call indexes: `write_dcs` as `Err` (not applied),
//!   `Err` after the write was applied, and `Ok(false)`; `set_pg` failing
//!   before or after it wrote the file; `replication()` failing; the lease
//!   expiring at a chosen check; a crash of pg-ha after ANY I/O call (the
//!   in-flight cycle is dropped, the next cycle runs on a fresh `QuorumExec`,
//!   which must follow the restart rule).
//! * Mode actions: `Off`, `Priority`, `Quorum` (T6, T7, T9), a Priority-shaped
//!   starting state (`GoPriority`, then `Quorum` is T8), strict toggles through
//!   `Reconfig`.
//! * Promotion finalization is the executor's own step, through both
//!   branches: the usual one (the pre-promote value is in force) and the
//!   "unknown history" reset (`blind` promotions, where the new leader still
//!   runs a stale value of its own).
//!
//! # Modelling assumptions (each one is a place the model is kinder than reality)
//!
//! * The executor's explicit `reload()` lands the file at once. Only the
//!   reload inside `set_pg` can be late or lost. The restart rule depends on
//!   this (probe P1: no stale SHOW was ever seen after a reload).
//! * The pre-promote `ALTER SYSTEM` (plan 3c, not built in 3b) is atomic and
//!   happens at promotion, as in the planner model. A `blind` promotion is the
//!   case where it did not happen; the new leader then runs a stale value and
//!   `Commit` is ignored until the first `/sync` finalization succeeded (the
//!   window before it is exactly what 3c closes), and SI-1 is not checked in
//!   it. Invariant I is.
//! * `Elect` only runs while `/sync` is a Quorum one: the Priority election
//!   rules are not modelled. The Priority code itself is not modelled either:
//!   `GoPriority` jumps to the state it converges to (`FIRST k (S)`, `/sync`
//!   names are the caught-up members of `S`), and the Priority invariant (every
//!   `/sync` name holds every acknowledged commit) is checked on it, which is
//!   what T8 starts from and T9 must end in.
//! * A tampered value (wildcard, blocking, unparseable) is covered by the unit
//!   tests; commits acknowledged under a wildcard cannot be described by any
//!   `(S, n)`, so the invariant cannot be checked through them.

use super::*;
use crate::cluster::{SyncKind, SyncState};
use crate::ha::quorum_sync::{EffectiveSyncMode, ExecInput, ExecOutcome, QuorumExec, SyncIo};
use crate::sync::quote_ident;
use crate::sync_value::{parse_sync_value, render_quorum_value};
use std::cell::Cell;
use tokio::sync::Notify;

// ───────────────────────────── faults ─────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DcsFault {
    /// `write_dcs` returns `Err`, nothing applied.
    Err,
    /// The write is applied, then `Err` (a timeout after the commit).
    ErrApplied,
    /// `Ok(false)`: not applied.
    NotApplied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SetPgFault {
    FailBefore,
    FailAfterFile,
}

/// When the reload of a successful `set_pg` takes effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fate {
    Now,
    /// After SHOW was read this many more times (they return the old value).
    AfterShows(usize),
    /// Only when a later reload delivers it.
    Later,
}

#[derive(Debug, Clone)]
struct Fault {
    /// (index among this cycle's `write_dcs` calls, fault)
    dcs: Option<(usize, DcsFault)>,
    /// Applies to the first `set_pg` of the cycle.
    set_pg: Option<SetPgFault>,
    fate: Fate,
    repl_err: bool,
    /// `lease_ok()` is false from this (0-based) check on.
    lease_false_from: Option<usize>,
    /// pg-ha dies right after the k-th (1-based) I/O call of the cycle.
    crash_after: Option<usize>,
}

impl Default for Fault {
    fn default() -> Self {
        Fault {
            dcs: None,
            set_pg: None,
            fate: Fate::Now,
            repl_err: false,
            lease_false_from: None,
            crash_after: None,
        }
    }
}

#[derive(Debug, Clone)]
enum XAction {
    /// Commit, Advance, Kill, Revive, KillLeader, Reconfig of the planner model.
    Base(Action),
    Elect {
        c: usize,
        reach: u8,
        late: u8,
        fail: u8,
        /// The new leader did not get the pre-promote value (unknown history).
        blind: bool,
        /// Its own stale value in that case (bit i: n_i is listed; 0: empty).
        leftover: u8,
    },
    Exec(Fault),
    /// A pending reload is delivered.
    ReloadLands,
    /// pg-ha restarts between two cycles.
    Restart,
    /// 0 Off, 1 Priority, 2 Quorum.
    SetMode(u8),
    /// Jump to the state the Priority code converges to.
    GoPriority,
}

#[derive(Debug, Clone, Default)]
struct XStats {
    exec_cycles: usize,
    crashes: usize,
    restarts: usize,
    /// `q` lowered by a write made after pg-ha restarted (crash or restart).
    relax_after_restart: usize,
    relax_writes: usize,
    /// SHOW returned a value different from the file (a reload not landed yet).
    show_mismatch: usize,
    dcs_err: usize,
    dcs_err_applied: usize,
    dcs_not_applied: usize,
    set_pg_fail_before: usize,
    set_pg_fail_after: usize,
    repl_err: usize,
    lease_fault: usize,
    t8_done: usize,
    t9_done: usize,
    t7_done: usize,
    finalize_match: usize,
    finalize_reset: usize,
    reload_delivered_late: usize,
    set_pg_calls: usize,
    commits_skipped_blind: usize,
}

impl XStats {
    fn add(&mut self, o: &XStats) {
        self.exec_cycles += o.exec_cycles;
        self.crashes += o.crashes;
        self.restarts += o.restarts;
        self.relax_after_restart += o.relax_after_restart;
        self.relax_writes += o.relax_writes;
        self.show_mismatch += o.show_mismatch;
        self.dcs_err += o.dcs_err;
        self.dcs_err_applied += o.dcs_err_applied;
        self.dcs_not_applied += o.dcs_not_applied;
        self.set_pg_fail_before += o.set_pg_fail_before;
        self.set_pg_fail_after += o.set_pg_fail_after;
        self.repl_err += o.repl_err;
        self.lease_fault += o.lease_fault;
        self.t8_done += o.t8_done;
        self.t9_done += o.t9_done;
        self.t7_done += o.t7_done;
        self.finalize_match += o.finalize_match;
        self.finalize_reset += o.finalize_reset;
        self.reload_delivered_late += o.reload_delivered_late;
        self.set_pg_calls += o.set_pg_calls;
        self.commits_skipped_blind += o.commits_skipped_blind;
    }
}

// ───────────────────────────── the world ─────────────────────────────

/// DCS `/sync` as the model stores it.
#[derive(Debug, Clone)]
struct Dcs {
    present: bool,
    leader: usize,
    voters: Names,
    quorum: usize,
    kind: SyncKind,
}

/// PostgreSQL's two values.
#[derive(Debug, Clone)]
struct PgVals {
    file: String,
    effective: String,
    /// SHOW calls left until a scheduled reload lands.
    land_after: Option<usize>,
}

/// Everything but the executor memory and the runtime that drives it.
struct Core {
    w: World,
    dcs: Dcs,
    pg: PgVals,
    mode: EffectiveSyncMode,
    /// A blind promotion has no finalized `/sync` yet: no commits, no SI-1.
    blind_open: bool,
    since_restart: bool,
    violation: Option<String>,
    x: XStats,
    // ---- per cycle ----
    fault: Fault,
    io_calls: usize,
    dcs_calls: usize,
    set_pg_calls: usize,
    repl_calls: usize,
    lease_checks: Cell<usize>,
}

struct XWorld {
    core: Core,
    exec: QuorumExec,
    /// Drives the cycle future (no timers involved).
    rt: tokio::runtime::Runtime,
}

fn quoted_list(names: &Names) -> String {
    names
        .iter()
        .map(|n| quote_ident(n))
        .collect::<Vec<_>>()
        .join(",")
}

fn any_text(names: &Names, n: usize) -> String {
    render_quorum_value(&QuorumTarget::new(names, n), false).expect("a valid ANY value")
}

/// `(S, n)` of a value as SI-1 and the acknowledgement rule read it. The strict
/// placeholder names a fake member, so it blocks every commit (nobody can
/// acknowledge) while SI-1 sees what the planner sees (`q >= |V|`).
fn sem(text: &str) -> (Names, usize) {
    let shape = parse_sync_value(text).expect("the model only holds readable values");
    if shape.is_empty() {
        return (Names::new(), 0);
    }
    if shape.strict_hold {
        return (Names::from(["~hold".to_string()]), 1);
    }
    (shape.names_folded(), shape.n)
}

impl Core {
    fn leader_name(&self) -> String {
        name(self.w.leader)
    }

    /// Mirror the DCS and the effective value into the planner model's state,
    /// which its acknowledgement rule and election code read.
    fn sync_w(&mut self) {
        let (names, n) = sem(&self.pg.effective);
        let (voters, quorum) = if self.dcs.present {
            (self.dcs.voters.clone(), self.dcs.quorum)
        } else {
            (Names::new(), 0)
        };
        self.w.st = QuorumState {
            voters,
            quorum,
            pg_names: names,
            pg_n: n,
        };
        self.w.sync_leader = self.dcs.leader;
    }

    fn settle(&mut self, ctx: &str) {
        self.sync_w();
        self.w.refresh_acks();
        if self.violation.is_none()
            && let Err(e) = self.check_x(ctx)
        {
            self.violation = Some(e);
        }
    }

    fn check_x(&self, ctx: &str) -> Result<(), String> {
        if !self.dcs.present {
            return Ok(());
        }
        let leader = self.leader_name();
        match self.dcs.kind {
            SyncKind::Quorum => {
                let readable: Vec<&String> =
                    self.dcs.voters.iter().filter(|v| **v != leader).collect();
                for &x in &self.w.acked {
                    let missing = readable
                        .iter()
                        .filter(|v| {
                            let i: usize = v[1..].parse().unwrap();
                            !self.w.nodes[i].holds(x)
                        })
                        .count();
                    if missing > self.dcs.quorum {
                        return Err(format!(
                            "invariant I violated {ctx}: acked commit {x} is missing on {missing} voters of {readable:?} > q={} (dcs {:?}, file {:?}, effective {:?})",
                            self.dcs.quorum, self.dcs, self.pg.file, self.pg.effective
                        ));
                    }
                }
                if !self.blind_open {
                    for (which, text) in
                        [("effective", &self.pg.effective), ("file", &self.pg.file)]
                    {
                        let (names, n) = sem(text);
                        let st = QuorumState {
                            voters: self.dcs.voters.clone(),
                            quorum: self.dcs.quorum,
                            pg_names: names,
                            pg_n: n,
                        };
                        if !st.si1_holds_excluding(&leader) {
                            return Err(format!(
                                "SI-1 violated {ctx} on (DCS, {which}): {st:?} (leader {leader}, dcs {:?}, file {:?}, effective {:?})",
                                self.dcs, self.pg.file, self.pg.effective
                            ));
                        }
                    }
                }
            }
            SyncKind::Priority => {
                // Every name in a Priority `/sync` holds every acknowledged commit.
                for v in &self.dcs.voters {
                    let i: usize = v[1..].parse().unwrap();
                    if let Some(&x) = self.w.acked.iter().find(|&&x| !self.w.nodes[i].holds(x)) {
                        return Err(format!(
                            "Priority invariant violated {ctx}: /sync names {v} which misses acknowledged commit {x} (dcs {:?}, pg {:?})",
                            self.dcs, self.pg.effective
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// A reload arrives: the server runs what the file says.
    fn land(&mut self) {
        if self.pg.effective != self.pg.file {
            self.pg.effective = self.pg.file.clone();
        }
        self.pg.land_after = None;
    }

    fn exec_input(&self) -> ExecInput {
        let w = &self.w;
        let members: Vec<Member> = (0..w.nodes.len())
            .filter(|&i| i != w.leader)
            .map(|i| {
                let mut tags = HashMap::new();
                if w.cfg_excluded & (1 << (i % 8)) != 0 {
                    tags.insert("nosync".to_string(), serde_json::json!(true));
                }
                Member {
                    name: name(i),
                    conn_url: String::new(),
                    api_url: String::new(),
                    state: MemberState::Running,
                    role: MemberRole::Replica,
                    wal_position: Some(w.nodes[i].lsn()),
                    timeline: Some(w.nodes[i].tl()),
                    tags,
                    version: None,
                    rewind_check: false,
                    lineage: None,
                    watchdog_failed: false,
                }
            })
            .collect();
        let sync = self.dcs.present.then(|| SyncState {
            leader: name(self.dcs.leader),
            sync_standby: if self.dcs.voters.is_empty() {
                None
            } else {
                Some(
                    self.dcs
                        .voters
                        .iter()
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(","),
                )
            },
            quorum: self.dcs.quorum as u32,
            kind: self.dcs.kind,
        });
        ExecInput {
            me: name(w.leader),
            mode: self.mode,
            count: w.cfg_n,
            strict: w.cfg_strict,
            lag_threshold: w.cfg_lag,
            members,
            sync,
        }
    }

    fn snapshot(&self) -> ReplicationSnapshot {
        let w = &self.w;
        ReplicationSnapshot {
            primary_flush_lsn: w.nodes[w.leader].lsn(),
            replicas: (0..w.nodes.len())
                .filter(|&i| i != w.leader && w.nodes[i].alive && w.nodes[i].attached)
                .map(|i| ReplicaRow {
                    application_name: name(i),
                    flush_lsn: w.nodes[i].lsn(),
                })
                .collect(),
        }
    }

    fn begin_cycle(&mut self, fault: Fault) {
        self.fault = fault;
        self.io_calls = 0;
        self.dcs_calls = 0;
        self.set_pg_calls = 0;
        self.repl_calls = 0;
        self.lease_checks.set(0);
    }
}

/// The executor's view of the world.
struct ModelIo<'a> {
    c: &'a mut Core,
    crash: std::sync::Arc<Notify>,
}

impl ModelIo<'_> {
    /// Called after the effect of every I/O call: check the invariants, and
    /// kill pg-ha here when the fault says so.
    async fn after_call(&mut self, what: &str) {
        self.c.io_calls += 1;
        let ctx = format!("after {what} (I/O call #{})", self.c.io_calls);
        self.c.settle(&ctx);
        if self.c.fault.crash_after == Some(self.c.io_calls) {
            self.c.x.crashes += 1;
            self.crash.notify_one();
            std::future::pending::<()>().await;
        }
    }
}

#[async_trait]
impl SyncIo for ModelIo<'_> {
    async fn show_sync_names(&mut self) -> crate::error::Result<String> {
        let seen = self.c.pg.effective.clone();
        if seen != self.c.pg.file {
            self.c.x.show_mismatch += 1;
        }
        if let Some(left) = self.c.pg.land_after {
            if left <= 1 {
                self.c.land();
                self.c.x.reload_delivered_late += 1;
            } else {
                self.c.pg.land_after = Some(left - 1);
            }
        }
        self.after_call("SHOW").await;
        Ok(seen)
    }

    async fn replication(&mut self) -> crate::error::Result<ReplicationSnapshot> {
        let first = self.c.repl_calls == 0;
        self.c.repl_calls += 1;
        let snap = self.c.snapshot();
        let fail = first && self.c.fault.repl_err;
        if fail {
            self.c.x.repl_err += 1;
        }
        self.after_call("replication").await;
        if fail {
            return Err(crate::error::Error::Postgres("injected".into()));
        }
        Ok(snap)
    }

    async fn set_pg(&mut self, value: &str) -> crate::error::Result<()> {
        let first = self.c.set_pg_calls == 0;
        self.c.set_pg_calls += 1;
        self.c.x.set_pg_calls += 1;
        let injected = if first { self.c.fault.set_pg } else { None };
        match injected {
            Some(SetPgFault::FailBefore) => {
                self.c.x.set_pg_fail_before += 1;
                self.after_call("set_pg (failed before the file)").await;
                return Err(crate::error::Error::Postgres("injected".into()));
            }
            Some(SetPgFault::FailAfterFile) => {
                self.c.x.set_pg_fail_after += 1;
                self.c.pg.file = value.to_string();
                self.after_call("set_pg (file written, then failed)").await;
                return Err(crate::error::Error::Postgres("injected".into()));
            }
            None => {}
        }
        self.c.pg.file = value.to_string();
        match self.c.fault.fate {
            Fate::Now => self.c.land(),
            Fate::AfterShows(k) => self.c.pg.land_after = Some(k.max(1)),
            Fate::Later => {}
        }
        self.after_call("set_pg").await;
        Ok(())
    }

    async fn reload(&mut self) -> crate::error::Result<()> {
        // Assumption (module doc): an explicit reload lands at once.
        self.c.land();
        self.after_call("reload").await;
        Ok(())
    }

    async fn write_dcs(&mut self, state: &SyncState) -> crate::error::Result<bool> {
        let idx = self.c.dcs_calls;
        self.c.dcs_calls += 1;
        let fault = match self.c.fault.dcs {
            Some((i, f)) if i == idx => Some(f),
            _ => None,
        };
        match fault {
            Some(DcsFault::Err) => {
                self.c.x.dcs_err += 1;
                self.after_call("write_dcs (Err)").await;
                return Err(crate::error::Error::Dcs("injected".into()));
            }
            Some(DcsFault::NotApplied) => {
                self.c.x.dcs_not_applied += 1;
                self.after_call("write_dcs (Ok(false))").await;
                return Ok(false);
            }
            Some(DcsFault::ErrApplied) => self.c.x.dcs_err_applied += 1,
            None => {}
        }
        self.c.apply_dcs_write(state);
        self.after_call("write_dcs").await;
        if fault == Some(DcsFault::ErrApplied) {
            return Err(crate::error::Error::Dcs("injected after commit".into()));
        }
        Ok(true)
    }

    fn lease_ok(&self) -> bool {
        let n = self.c.lease_checks.get();
        self.c.lease_checks.set(n + 1);
        let gone = self.c.fault.lease_false_from.is_some_and(|from| n >= from);
        !gone
    }
}

impl Core {
    fn apply_dcs_write(&mut self, s: &SyncState) {
        let new_leader: usize = s.leader[1..].parse().unwrap();
        let voters: Names = s
            .sync_standby_names()
            .into_iter()
            .map(|n| n.to_ascii_lowercase())
            .collect();
        let new_q = s.quorum as usize;
        let was_quorum = self.dcs.present && self.dcs.kind == SyncKind::Quorum;
        if was_quorum && s.kind == SyncKind::Quorum && new_q < self.dcs.quorum {
            self.x.relax_writes += 1;
            if self.since_restart {
                self.x.relax_after_restart += 1;
            }
        }
        if self.dcs.present && self.dcs.kind == SyncKind::Priority && s.kind == SyncKind::Quorum {
            self.x.t8_done += 1;
        }
        if new_leader == self.w.leader && self.dcs.leader != self.w.leader {
            // Finalization after a promotion: which branch was it?
            let old = QuorumState::new(self.dcs.voters.clone(), self.dcs.quorum, Names::new(), 0);
            let expected = post_promotion_state(&old, &name(self.w.leader), &name(self.dcs.leader))
                .map(|p| any_text(&p.pg_names, p.pg_n));
            if expected.is_some() && expected.as_deref() == Some(self.pg.effective.as_str()) {
                self.x.finalize_match += 1;
            } else {
                self.x.finalize_reset += 1;
            }
            self.blind_open = false;
        }
        self.dcs = Dcs {
            present: true,
            leader: new_leader,
            voters,
            quorum: new_q,
            kind: s.kind,
        };
    }
}

impl XWorld {
    fn new(standbys: usize, n0: usize, steady: bool) -> XWorld {
        let w = World::new(standbys, n0, steady);
        let names: Names = (1..=standbys).map(name).collect();
        let (dcs, text, mode) = if steady {
            let n = n0.min(standbys).max(1);
            (
                Dcs {
                    present: true,
                    leader: 0,
                    voters: names.clone(),
                    quorum: standbys - n,
                    kind: SyncKind::Quorum,
                },
                any_text(&names, n),
                EffectiveSyncMode::Quorum,
            )
        } else {
            (
                Dcs {
                    present: false,
                    leader: 0,
                    voters: Names::new(),
                    quorum: 0,
                    kind: SyncKind::Priority,
                },
                String::new(),
                EffectiveSyncMode::Off,
            )
        };
        let mut core = Core {
            w,
            dcs,
            pg: PgVals {
                file: text.clone(),
                effective: text,
                land_after: None,
            },
            mode,
            blind_open: false,
            since_restart: false,
            violation: None,
            x: XStats::default(),
            fault: Fault::default(),
            io_calls: 0,
            dcs_calls: 0,
            set_pg_calls: 0,
            repl_calls: 0,
            lease_checks: Cell::new(0),
        };
        core.w.stale_old_leader = true;
        core.sync_w();
        XWorld {
            core,
            exec: QuorumExec::default(),
            rt: tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("runtime"),
        }
    }

    fn finish(&mut self, ctx: &str) -> Result<(), String> {
        self.core.settle(ctx);
        match self.core.violation.take() {
            Some(v) => Err(v),
            None => Ok(()),
        }
    }

    fn restart(&mut self) {
        self.exec = QuorumExec::default();
        self.core.since_restart = true;
        self.core.x.restarts += 1;
    }

    fn run_exec(&mut self, fault: Fault) -> Result<(), String> {
        let c = &mut self.core;
        if !c.w.leader_alive() {
            return Ok(());
        }
        let applies = c.mode == EffectiveSyncMode::Quorum
            || (c.dcs.present && c.dcs.kind == SyncKind::Quorum);
        if !applies {
            return Ok(());
        }
        if fault.lease_false_from.is_some() {
            c.x.lease_fault += 1;
        }
        c.begin_cycle(fault);
        c.x.exec_cycles += 1;
        let input = c.exec_input();
        let crash = std::sync::Arc::new(Notify::new());
        let exec = &mut self.exec;
        let outcome = self.rt.block_on(async {
            let mut io = ModelIo {
                c: &mut *c,
                crash: crash.clone(),
            };
            tokio::select! {
                o = exec.cycle(&mut io, &input) => Some(o),
                _ = crash.notified() => None,
            }
        });
        match outcome {
            None => self.restart(),
            Some(ExecOutcome::HandedOff) => match self.core.mode {
                EffectiveSyncMode::Off => self.core.x.t7_done += 1,
                EffectiveSyncMode::Priority => self.core.x.t9_done += 1,
                EffectiveSyncMode::Quorum => {}
            },
            Some(_) => {}
        }
        self.finish("after the cycle")
    }

    fn go_priority(&mut self) {
        let c = &mut self.core;
        if !c.w.leader_alive() || (c.dcs.present && c.dcs.kind == SyncKind::Quorum) {
            return;
        }
        let w = &c.w;
        let eligible: Vec<usize> = (0..w.nodes.len())
            .filter(|&i| {
                i != w.leader
                    && w.nodes[i].alive
                    && w.nodes[i].attached
                    && w.cfg_excluded & (1 << (i % 8)) == 0
            })
            .take(w.cfg_n)
            .collect();
        let s: Names = eligible.iter().map(|&i| name(i)).collect();
        let v: Names = eligible
            .iter()
            .filter(|&&i| w.acked.iter().all(|&x| w.nodes[i].holds(x)))
            .map(|&i| name(i))
            .collect();
        let text = if s.is_empty() {
            String::new()
        } else {
            format!("FIRST {} ({})", s.len(), quoted_list(&s))
        };
        c.dcs = Dcs {
            present: true,
            leader: w.leader,
            voters: v,
            quorum: w.cfg_n,
            kind: SyncKind::Priority,
        };
        c.pg = PgVals {
            file: text.clone(),
            effective: text,
            land_after: None,
        };
        c.mode = EffectiveSyncMode::Priority;
    }

    fn elect(
        &mut self,
        (cand, reach, late, fail): (usize, u8, u8, u8),
        blind: bool,
        leftover: u8,
    ) -> Result<(), String> {
        let c = &mut self.core;
        if !(c.dcs.present && c.dcs.kind == SyncKind::Quorum) {
            return Ok(());
        }
        c.sync_w();
        let gens = c.w.stats.generations;
        c.w.elect(cand, reach, late, fail)?;
        if c.w.stats.generations == gens {
            return Ok(());
        }
        // Promoted. The planner model installed the pre-promote value in its
        // state; the DCS is still the old leader's.
        c.w.pending_dcs = None;
        let text = if blind {
            let names: Names = (0..c.w.nodes.len())
                .filter(|&i| i != c.w.leader && leftover & (1 << (i % 8)) != 0)
                .map(name)
                .collect();
            if names.is_empty() {
                String::new()
            } else {
                any_text(&names, 1)
            }
        } else {
            any_text(&c.w.st.pg_names, c.w.st.pg_n)
        };
        c.pg = PgVals {
            file: text.clone(),
            effective: text,
            land_after: None,
        };
        c.blind_open = blind;
        c.since_restart = false;
        self.exec = QuorumExec::default();
        Ok(())
    }

    fn step(&mut self, a: &XAction) -> Result<(), String> {
        match a {
            XAction::Base(b) => {
                if matches!(b, Action::Commit(_)) && self.core.blind_open {
                    self.core.x.commits_skipped_blind += 1;
                } else {
                    self.core.sync_w();
                    // The checks are the model's own (`check_x`): ignore the
                    // planner model's.
                    let _ = self.core.w.apply(b);
                }
            }
            XAction::Elect {
                c,
                reach,
                late,
                fail,
                blind,
                leftover,
            } => self.elect((*c, *reach, *late, *fail), *blind, *leftover)?,
            XAction::Exec(f) => return self.run_exec(f.clone()),
            XAction::ReloadLands => self.core.land(),
            XAction::Restart => self.restart(),
            XAction::SetMode(m) => {
                self.core.mode = match m % 3 {
                    0 => EffectiveSyncMode::Off,
                    1 => EffectiveSyncMode::Priority,
                    _ => EffectiveSyncMode::Quorum,
                }
            }
            XAction::GoPriority => self.go_priority(),
        }
        self.finish(&format!("after {a:?}"))
    }
}

// ───────────────────────────── strategies ─────────────────────────────

fn fault_strategy() -> impl Strategy<Value = Fault> {
    prop_oneof![
        12 => Just(Fault::default()),
        8 => (
            prop::option::weighted(0.7, (0usize..3, 0u8..3)),
            prop::option::weighted(0.5, 0u8..2),
            prop_oneof![3 => Just(0usize), 2 => 1usize..4, 2 => Just(99usize)],
            prop_oneof![9 => Just(false), 1 => Just(true)],
            prop::option::of(0usize..6),
            prop::option::of(1usize..9),
        ).prop_map(|(dcs, set_pg, fate, repl_err, lease, crash)| Fault {
            dcs: dcs.map(|(i, f)| (i, match f {
                0 => DcsFault::Err,
                1 => DcsFault::ErrApplied,
                _ => DcsFault::NotApplied,
            })),
            set_pg: set_pg.map(|f| if f == 0 { SetPgFault::FailBefore } else { SetPgFault::FailAfterFile }),
            fate: match fate {
                0 => Fate::Now,
                99 => Fate::Later,
                k => Fate::AfterShows(k),
            },
            repl_err,
            lease_false_from: lease,
            crash_after: crash,
        }),
    ]
}

fn base_strategy() -> impl Strategy<Value = Action> {
    prop_oneof![
        6 => (1usize..4).prop_map(Action::Commit),
        10 => (any::<usize>(), 1usize..5).prop_map(|(i, k)| Action::Advance(i, k)),
        4 => any::<usize>().prop_map(Action::Kill),
        5 => any::<usize>().prop_map(Action::Revive),
        2 => Just(Action::KillLeader),
        6 => (1usize..5, any::<bool>(), prop_oneof![Just(None), (0u64..4).prop_map(Some)], prop_oneof![3 => Just(0u8), 1 => any::<u8>()])
            .prop_map(|(n, strict, lag, excluded)| Action::Reconfig { n, strict, lag, excluded }),
    ]
}

fn xaction_strategy() -> impl Strategy<Value = XAction> {
    prop_oneof![
        30 => base_strategy().prop_map(XAction::Base),
        5 => (any::<usize>(), any::<u8>(), prop_oneof![3 => Just(0u8), 1 => any::<u8>()], prop_oneof![3 => Just(0u8), 1 => any::<u8>()], prop_oneof![5 => Just(false), 1 => Just(true)], any::<u8>())
            .prop_map(|(c, reach, late, fail, blind, leftover)| XAction::Elect { c, reach, late, fail, blind, leftover }),
        26 => fault_strategy().prop_map(XAction::Exec),
        3 => Just(XAction::ReloadLands),
        1 => Just(XAction::Restart),
        2 => (0u8..3).prop_map(XAction::SetMode),
        1 => Just(XAction::GoPriority),
    ]
}

fn run_xworld(
    standbys: usize,
    n0: usize,
    steady: bool,
    actions: &[XAction],
) -> Result<XStats, String> {
    let mut x = XWorld::new(standbys, n0, steady);
    for (k, a) in actions.iter().enumerate() {
        x.step(a).map_err(|e| format!("step {k}: {e}"))?;
    }
    Ok(x.core.x.clone())
}

const XCASES: u32 = 2500;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(XCASES))]

    /// The real executor, under crashes, delayed reloads and failing I/O, keeps
    /// SI-1 and invariant I true on (DCS, effective) and (DCS, file) after
    /// every I/O call, and no granted election loses an acknowledged commit.
    #[test]
    fn executor_keeps_the_invariants_under_every_fault(
        standbys in 2usize..6,
        n0 in 1usize..4,
        steady in any::<bool>(),
        actions in prop::collection::vec(xaction_strategy(), 20..220),
    ) {
        run_xworld(standbys, n0, steady, &actions).map_err(TestCaseError::fail)?;
    }
}

/// The generator must really reach the paths the property is about. If this
/// fails the property above proves less than it claims.
#[test]
fn model_executor_actually_exercises_the_paths() {
    use proptest::test_runner::{Config, TestRunner};
    let total = Mutex::new(XStats::default());
    let mut runner = TestRunner::new(Config {
        cases: XCASES,
        ..Config::default()
    });
    let strat = (
        2usize..6,
        1usize..4,
        any::<bool>(),
        prop::collection::vec(xaction_strategy(), 20..220),
    );
    runner
        .run(&strat, |(s, n0, steady, actions)| {
            let st = run_xworld(s, n0, steady, &actions).map_err(TestCaseError::fail)?;
            total.lock().unwrap().add(&st);
            Ok(())
        })
        .unwrap();
    let t = total.lock().unwrap().clone();
    eprintln!("executor model coverage over {XCASES} histories: {t:#?}");
    assert!(t.exec_cycles > 10_000, "executor barely ran: {t:?}");
    assert!(t.crashes > 500, "too few crashes injected: {t:?}");
    assert!(t.restarts > 2_000, "too few restarts: {t:?}");
    assert!(
        t.relax_after_restart > 200,
        "too few relaxations after a restart: {t:?}"
    );
    assert!(t.relax_writes > 400, "q rarely relaxed: {t:?}");
    assert!(t.show_mismatch > 250, "too few SHOW mismatches: {t:?}");
    assert!(
        t.reload_delivered_late > 20,
        "late reloads rarely landed: {t:?}"
    );
    assert!(t.dcs_not_applied > 45, "no Ok(false) injected: {t:?}");
    assert!(t.dcs_err > 50, "no DCS Err injected: {t:?}");
    assert!(
        t.dcs_err_applied > 50,
        "no applied-then-Err injected: {t:?}"
    );
    assert!(t.set_pg_fail_before > 150, "set_pg fail-before rare: {t:?}");
    assert!(t.set_pg_fail_after > 150, "set_pg fail-after rare: {t:?}");
    assert!(t.repl_err > 250, "replication errors rare: {t:?}");
    assert!(t.lease_fault > 1_500, "lease faults rare: {t:?}");
    assert!(t.t8_done > 60, "T8 rarely completed: {t:?}");
    assert!(t.t9_done > 70, "T9 rarely completed: {t:?}");
    assert!(t.t7_done > 120, "T7 rarely completed: {t:?}");
    assert!(t.finalize_match > 90, "match finalization rare: {t:?}");
    assert!(t.finalize_reset > 20, "reset finalization rare: {t:?}");
    assert!(t.commits_skipped_blind > 10, "no blind window: {t:?}");
}
