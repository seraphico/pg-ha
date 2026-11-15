//! The quorum-sync executor, leader side (SB-13, PR3b).
//!
//! `QuorumExec::cycle` is the quorum counterpart of
//! `Ha::enforce_synchronous_replication`. It decides which acknowledged commits
//! can be lost, because it writes `synchronous_standby_names` and DCS `/sync`.
//! It is only reached through `Ha::quorum_executor_applies`, which is false for
//! every production input in PR3b (no user setting selects Quorum yet).
//!
//! # Shape
//!
//! * All I/O goes through [`SyncIo`], so the same code runs against the real
//!   PostgreSQL and DCS ([`RealIo`]), a scripted fake (unit tests) and the
//!   model of `quorum::model_tests` (executor-in-model proptest).
//! * The executor remembers almost nothing. Each cycle it OBSERVES
//!   (`SHOW synchronous_standby_names`, the replication snapshot, `/sync`),
//!   derives `(V, q, S, n)` with `quorum::derive`, and plans with the PR1
//!   planner. What it keeps in memory (the mark, `reloaded_since_start`, and the
//!   one unconfirmed `SetPg`) can only make it slower, never less safe: losing
//!   it means no relaxation until a new mark exists.
//!
//! # One cycle, in order
//!
//! 1. Restart rule (plan decision 5): the first cycle of a fresh executor only
//!    issues an idempotent `pg_ctl reload` (so a value that a crashed
//!    predecessor left in `postgresql.auto.conf` is the one in force) and
//!    returns. SHOW and the mark come one cycle later.
//! 2. Observe. Any error: no change this cycle. An unparseable value, a value
//!    with the wildcard `*` and a value that blocks every commit cannot be
//!    described by `(S, n)`: they are *unreasonable*. They are read as `S = {}`,
//!    `n = 0` (q is first raised so SI-1 holds under that reading), are
//!    REPLACED by the executor's own value, and `q` is never lowered while one
//!    is in force (maintainer answer 1).
//! 3. Finalization, before anything else (plan 3.2 step 2): when `/sync.leader`
//!    is not this node, either write `(me, V', q)` from `post_promotion_state`
//!    (the pre-promote value is in force) or reset to an empty voter set. No
//!    planner step runs before that write succeeded.
//! 4. A `SetPg` that was issued but is not yet confirmed by SHOW is retried
//!    alone (plan 3.4); nothing is planned meanwhile, so `q` cannot drop while
//!    the file value and the effective value may differ.
//! 5. T8 (Priority `/sync` to Quorum): one write of
//!    `(V = names, q = max(0, |V ∪ S| - n))`.
//! 6. Mark (taken from SHOW, never at `set_pg` time) and strict `caught_up`.
//!    A mark taken in this very cycle blocks every relaxation of `q`: one more
//!    observation cycle after a PG change (maintainer answer 5; the walsender
//!    state may lag SHOW).
//! 7. Plan, then run the steps with `lease_ok()` before each. At most one
//!    `SetPg`, and the cycle stops after it (the reload is asynchronous).
//!    Then the mode transitions that need more than the planner: the
//!    `FIRST`-to-`ANY` rewrite after T8, the strict placeholder for an empty
//!    target, and the hand-off to the Priority code (T7, T9).
//!
//! # Documented readings
//!
//! * Strict mode with fewer eligible standbys than `N`: the target is empty and
//!   the strict placeholder is written, so writes block (answer 2).
//! * The "unknown history" reset writes `q = max(0, |S| - n)` instead of the
//!   bare `0` of plan 3.2: it is 0 whenever SI-1 already holds with an empty
//!   voter set, and otherwise keeps SI-1 true in the state it creates.
//!   Nobody is electable with an empty voter set, so the larger `q` costs
//!   nothing and is relaxed behind the SI-4 gate like any other.

use std::time::Duration;

use async_trait::async_trait;
use tracing::{debug, info, warn};

use super::Ha;
use super::lease::LeaderLease;
use crate::cluster::{Member, SyncKind, SyncState};
use crate::dcs::DcsAdapter;
use crate::error::{Error, Result};
use crate::postgresql::{Postgresql, ReplicationSnapshot};
use crate::quorum::derive::{Derived, Phase, derive_state};
use crate::quorum::mark::{Mark, caught_up, observe_mark};
use crate::quorum::{Names, QuorumState, QuorumTarget, Step, plan, post_promotion_state};
use crate::sync::{SyncManager, SyncMode};
use crate::sync_value::{
    ShapeKey, SyncKeyword, SyncValueShape, parse_sync_value, render_quorum_value,
};

/// Upper bound for each call the executor makes on PostgreSQL or the DCS.
/// `Postgresql::set_synchronous_standby_names` connects without a connect
/// timeout, so the executor bounds it from outside.
pub(crate) const IO_TIMEOUT: Duration = Duration::from_secs(5);

/// The mode the HA loop wants, as the executor sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EffectiveSyncMode {
    /// No synchronous replication.
    Off,
    /// Priority (`FIRST n`): the existing code.
    Priority,
    /// Quorum (`ANY n`). Not constructible from configuration in PR3b.
    #[allow(dead_code)] // built only by the test override until PR4 wires the setting
    Quorum,
}

/// Everything the executor does to PostgreSQL and the DCS.
// async_trait boxes the future, which clippy flags as double_must_use.
#[allow(clippy::double_must_use)]
#[async_trait]
pub(crate) trait SyncIo: Send {
    /// `SHOW synchronous_standby_names`, verbatim.
    async fn show_sync_names(&mut self) -> Result<String>;
    /// Primary-side `pg_stat_replication` snapshot.
    async fn replication(&mut self) -> Result<ReplicationSnapshot>;
    /// `ALTER SYSTEM SET synchronous_standby_names` + reload.
    async fn set_pg(&mut self, value: &str) -> Result<()>;
    /// `pg_ctl reload` alone (restart rule).
    async fn reload(&mut self) -> Result<()>;
    /// Write DCS `/sync`. `Ok(false)` means "not applied".
    async fn write_dcs(&mut self, state: &SyncState) -> Result<bool>;
    /// The leader lease still holds (the negation of `Ha::lease_lost`).
    fn lease_ok(&self) -> bool;
}

/// The real PostgreSQL and DCS, every call bounded by [`IO_TIMEOUT`].
pub(crate) struct RealIo<'a> {
    pub(crate) pg: &'a Postgresql,
    pub(crate) dcs: &'a dyn DcsAdapter,
    pub(crate) lease: Option<LeaderLease>,
    pub(crate) paused: bool,
}

async fn bounded<T>(what: &str, fut: impl std::future::Future<Output = Result<T>>) -> Result<T> {
    match tokio::time::timeout(IO_TIMEOUT, fut).await {
        Ok(r) => r,
        Err(_) => Err(Error::Timeout(format!(
            "{what} timed out after {}s",
            IO_TIMEOUT.as_secs()
        ))),
    }
}

#[async_trait]
impl SyncIo for RealIo<'_> {
    async fn show_sync_names(&mut self) -> Result<String> {
        // Already bounded (connect and query) by Postgresql itself.
        self.pg.current_synchronous_standby_names().await
    }

    async fn replication(&mut self) -> Result<ReplicationSnapshot> {
        bounded("replication_status", self.pg.replication_status()).await
    }

    async fn set_pg(&mut self, value: &str) -> Result<()> {
        bounded(
            "set synchronous_standby_names",
            self.pg.set_synchronous_standby_names(value),
        )
        .await
    }

    async fn reload(&mut self) -> Result<()> {
        bounded("pg_ctl reload", self.pg.reload()).await
    }

    async fn write_dcs(&mut self, state: &SyncState) -> Result<bool> {
        let json = serde_json::to_string(state)
            .map_err(|e| Error::Dcs(format!("serialize SyncState: {e}")))?;
        bounded("write /sync", self.dcs.set_sync_value(&json)).await
    }

    fn lease_ok(&self) -> bool {
        // The exact `Ha::lease_lost` rule, negated.
        self.paused || self.lease.is_some_and(|l| !l.is_expired())
    }
}

/// Plain data for one cycle.
#[derive(Debug, Clone)]
pub(crate) struct ExecInput {
    pub(crate) me: String,
    pub(crate) mode: EffectiveSyncMode,
    /// `synchronous_node_count` as configured (the Priority hand-off writes it
    /// into `/sync.quorum` like the Priority code does).
    pub(crate) count: usize,
    pub(crate) strict: bool,
    pub(crate) lag_threshold: Option<u64>,
    pub(crate) members: Vec<Member>,
    pub(crate) sync: Option<SyncState>,
}

/// What a cycle did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ExecOutcome {
    /// Nothing to do, or nothing safe to do (a warning says why).
    NoChange,
    /// At least one write was made.
    Progressed,
    /// A write failed (or the lease is gone); the cycle stopped there.
    Failed(String),
    /// The cluster is back on Priority/Off: the caller must clear the sync
    /// caches so the Priority code starts clean.
    HandedOff,
}

/// A `SetPg` that was issued and not yet seen in SHOW.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingSet {
    text: String,
    /// Shape key of `text`.
    key: ShapeKey,
    /// Shape key that was in force when `text` was issued.
    pre_key: ShapeKey,
    /// Cycles in which SHOW showed neither.
    cycles: u32,
}

/// The only in-memory state of the executor.
#[derive(Debug, Default)]
pub(crate) struct QuorumExec {
    mark: Option<Mark>,
    reloaded_since_start: bool,
    pending: Option<PendingSet>,
}

/// What SHOW said, reduced to what the executor decides on.
struct Observed {
    raw: String,
    shape: SyncValueShape,
    key: ShapeKey,
    /// Describable by `(S, n)` and not wildcard/blocking/unparseable.
    reasonable: bool,
}

fn observe_value(raw: &str) -> Observed {
    match parse_sync_value(raw) {
        Ok(shape) => {
            let reasonable = !shape.has_wildcard() && (!shape.is_blocking() || shape.strict_hold);
            Observed {
                raw: raw.to_string(),
                key: shape.key(),
                shape,
                reasonable,
            }
        }
        Err(e) => {
            warn!("synchronous_standby_names {raw:?} is not readable ({e}); replacing it");
            // A key no readable value can equal, so a mark is never kept across it.
            let key = ShapeKey {
                quorum: false,
                n: usize::MAX,
                names: Names::from([format!("\u{0}unparseable:{raw}")]),
            };
            Observed {
                raw: raw.to_string(),
                shape: SyncValueShape {
                    keyword: SyncKeyword::Bare,
                    n: 0,
                    names: Vec::new(),
                    strict_hold: false,
                },
                key,
                reasonable: false,
            }
        }
    }
}

/// The `/sync` value the Priority code and this executor write.
pub(crate) fn build_sync_state(
    leader: &str,
    names: &Names,
    quorum: u32,
    kind: SyncKind,
) -> SyncState {
    SyncState {
        leader: leader.to_string(),
        sync_standby: if names.is_empty() {
            None
        } else {
            Some(names.iter().cloned().collect::<Vec<_>>().join(","))
        },
        quorum,
        kind,
    }
}

/// Write `/sync` after checking the lease. True only for `Ok(true)`: for the
/// quorum path `Ok(false)` ("not applied") is a failure.
pub(crate) async fn write_quorum_sync_state<IO: SyncIo>(
    io: &mut IO,
    me: &str,
    names: &Names,
    quorum: u32,
    kind: SyncKind,
) -> bool {
    if !io.lease_ok() {
        warn!("not writing /sync: the leader lease is gone");
        return false;
    }
    let state = build_sync_state(me, names, quorum, kind);
    match io.write_dcs(&state).await {
        Ok(true) => true,
        Ok(false) => {
            warn!("/sync write was not applied");
            false
        }
        Err(e) => {
            warn!("Failed to write /sync: {e}");
            false
        }
    }
}

/// `FIRST n ("a","b")` for a target that renders as `ANY n (...)`.
fn render_first(
    target: &QuorumTarget,
) -> std::result::Result<String, crate::sync_value::RenderError> {
    let any = render_quorum_value(target, false)?;
    Ok(match any.strip_prefix("ANY") {
        Some(rest) => format!("FIRST{rest}"),
        None => any,
    })
}

fn empty_target() -> QuorumTarget {
    QuorumTarget::new(std::iter::empty::<&str>(), 0)
}

/// The standbys the mode wants as voters. An empty target stands for "nobody"
/// (also strict mode with fewer eligible standbys than `N`, answer 2).
fn build_target(input: &ExecInput, snapshot: &ReplicationSnapshot, voters: &Names) -> QuorumTarget {
    let mode = match input.mode {
        EffectiveSyncMode::Off => return empty_target(),
        EffectiveSyncMode::Priority => SyncMode::Priority,
        EffectiveSyncMode::Quorum => SyncMode::Quorum,
    };
    let current: Vec<String> = voters.iter().cloned().collect();
    let selection = SyncManager::new(mode, input.count, input.strict).select_sync_standbys(
        &input.members,
        &input.me,
        snapshot,
        input.lag_threshold,
        &current,
    );
    let Some(selection) = selection else {
        return empty_target();
    };
    let target = QuorumTarget::from_selection(&selection);
    if target.voters.is_empty() || target.n < 1 || target.n > target.voters.len() {
        return empty_target();
    }
    target
}

/// How a planned `/sync` write is shaped.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Wire {
    /// `kind: Quorum` with the real `q`.
    Quorum,
    /// The Priority wire form (kind omitted) with this `quorum` field, and the
    /// executor's job is over: the Priority code takes the cluster back.
    PriorityFinal { quorum_field: u32 },
}

/// One thing to do in a cycle, in order. A `Pg` is always last.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Act {
    Dcs {
        voters: Names,
        quorum: usize,
        wire: Wire,
    },
    Pg(String),
}

/// Is lowering `q` forbidden this cycle? Yes when the mark was taken in this
/// very cycle (one more observation first: the walsender state may lag SHOW)
/// or when the value in force cannot be read as `(S, n)`.
fn relax_blocked(mark_was_current: bool, reasonable: bool) -> bool {
    !mark_was_current || !reasonable
}

fn needed_quorum(state: &QuorumState) -> usize {
    state
        .voters
        .union(&state.pg_names)
        .count()
        .saturating_sub(state.effective_n())
}

impl QuorumExec {
    #[cfg(test)]
    pub(crate) fn mark(&self) -> Option<&Mark> {
        self.mark.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn has_pending(&self) -> bool {
        self.pending.is_some()
    }

    /// A executor that has done its restart reload and holds a mark.
    #[cfg(test)]
    pub(crate) fn warmed_for_test() -> Self {
        Self {
            mark: Some(Mark {
                shape_key: ShapeKey {
                    quorum: true,
                    n: 1,
                    names: Names::new(),
                },
                lsn: 7,
            }),
            reloaded_since_start: true,
            pending: None,
        }
    }

    /// Has the memory of a freshly started executor.
    #[cfg(test)]
    pub(crate) fn is_fresh_for_test(&self) -> bool {
        self.mark.is_none() && !self.reloaded_since_start && self.pending.is_none()
    }

    /// Run one cycle. See the module doc for the order of the phases.
    pub(crate) async fn cycle<IO: SyncIo>(
        &mut self,
        io: &mut IO,
        input: &ExecInput,
    ) -> ExecOutcome {
        // 1. Restart rule.
        if !self.reloaded_since_start {
            return match io.reload().await {
                Ok(()) => {
                    self.reloaded_since_start = true;
                    ExecOutcome::Progressed
                }
                Err(e) => {
                    warn!("pg_ctl reload failed; quorum sync waits: {e}");
                    ExecOutcome::Failed(format!("reload: {e}"))
                }
            };
        }

        // 2. Observe.
        let raw = match io.show_sync_names().await {
            Ok(raw) => raw,
            Err(e) => {
                warn!("Failed to read synchronous_standby_names; no sync change this cycle: {e}");
                return ExecOutcome::NoChange;
            }
        };
        let snapshot = match io.replication().await {
            Ok(s) => s,
            Err(e) => {
                warn!("Failed to read replication_status; no sync change this cycle: {e}");
                return ExecOutcome::NoChange;
            }
        };
        let obs = observe_value(&raw);
        let me = input.me.as_str();
        let live = input.sync.as_ref().filter(|s| !s.is_empty());

        let Derived { mut state, phase } = derive_state(live, me, &obs.shape);
        if !obs.reasonable {
            // Not describable by (S, n): read it as asynchronous, which makes
            // SI-1 the strictest (q >= |V|), and replace it below.
            state.pg_names.clear();
            state.pg_n = 0;
        }
        let mut wrote = false;

        // 3. Finalization: `/sync` belongs to another leader.
        match phase {
            Phase::Steady => {}
            Phase::PromotionWindow {
                expected_pg_matches: true,
            } => {
                let old_leader = live.map(|s| s.leader.as_str()).unwrap_or_default();
                match post_promotion_state(&state, me, old_leader) {
                    Some(post) => {
                        if !write_quorum_sync_state(
                            io,
                            me,
                            &post.voters,
                            post.quorum as u32,
                            SyncKind::Quorum,
                        )
                        .await
                        {
                            return ExecOutcome::Failed("finalization write failed".into());
                        }
                        info!("finalized /sync after promotion (voters {:?})", post.voters);
                        state.voters = post.voters;
                        state.quorum = post.quorum;
                        wrote = true;
                    }
                    None => {
                        if !self.reset_sync(io, me, &mut state).await {
                            return ExecOutcome::Failed("reset write failed".into());
                        }
                        wrote = true;
                    }
                }
            }
            Phase::PromotionWindow {
                expected_pg_matches: false,
            }
            | Phase::NotMine => {
                if !self.reset_sync(io, me, &mut state).await {
                    return ExecOutcome::Failed("reset write failed".into());
                }
                wrote = true;
            }
        }

        // 4. An unconfirmed SetPg is retried alone.
        if let Some(p) = self.pending.as_mut() {
            if obs.key == p.key {
                self.pending = None;
            } else {
                p.cycles += 1;
                if p.cycles == 3 || p.cycles % 30 == 0 {
                    warn!(
                        "synchronous_standby_names {:?} was written {} cycles ago but SHOW still reports {:?}; \
                         is it overridden in postgresql.conf or on the command line?",
                        p.text, p.cycles, obs.raw
                    );
                }
                let retry = obs.key == p.pre_key;
                // Retry the same text, or (when the value changed to something
                // else) write back what is in force, so the file cannot hold
                // a different value than the one we observe.
                let text = if retry {
                    p.text.clone()
                } else {
                    obs.raw.clone()
                };
                if !io.lease_ok() {
                    return ExecOutcome::Failed("leader lease gone".into());
                }
                return match io.set_pg(&text).await {
                    Ok(()) => {
                        if !retry {
                            self.pending = None;
                        }
                        ExecOutcome::Progressed
                    }
                    Err(e) => {
                        warn!("Failed to set synchronous_standby_names: {e}");
                        ExecOutcome::Failed(format!("set_pg: {e}"))
                    }
                };
            }
        }

        // 5. T8: a Priority `/sync` of this leader becomes a Quorum one.
        if input.mode == EffectiveSyncMode::Quorum
            && !wrote
            && let Some(s) = live
            && s.kind == SyncKind::Priority
        {
            let voters: Names = s
                .sync_standby_names()
                .into_iter()
                .map(|n| n.to_ascii_lowercase())
                .collect();
            let mut handoff = QuorumState::new(&voters, 0, &state.pg_names, state.pg_n);
            handoff.quorum = needed_quorum(&handoff);
            if !write_quorum_sync_state(
                io,
                me,
                &handoff.voters,
                handoff.quorum as u32,
                SyncKind::Quorum,
            )
            .await
            {
                return ExecOutcome::Failed("Priority to Quorum write failed".into());
            }
            info!(
                "/sync switched from Priority to Quorum (voters {:?}, q {})",
                handoff.voters, handoff.quorum
            );
            return ExecOutcome::Progressed;
        }

        // 6. Mark and caught_up.
        let mark_was_current = self.mark.as_ref().is_some_and(|m| m.shape_key == obs.key);
        let mark = observe_mark(self.mark.take(), &obs.key, snapshot.primary_flush_lsn);
        self.mark = Some(mark.clone());
        let caught = caught_up(&state.voters, &snapshot, &mark);
        // A mark taken just now, or an unreadable value in force: q stays.
        let relax_blocked = relax_blocked(mark_was_current, obs.reasonable);

        // 7. Plan.
        let strict = input.mode != EffectiveSyncMode::Off && input.strict;
        let target = build_target(input, &snapshot, &state.voters);
        let acts = match self.assemble(input, &obs, &state, &target, &caught, relax_blocked, strict)
        {
            Ok(acts) => acts,
            Err(e) => {
                warn!("quorum sync plan not executed: {e}");
                return if wrote {
                    ExecOutcome::Progressed
                } else {
                    ExecOutcome::NoChange
                };
            }
        };
        debug!("quorum sync plan: {acts:?}");

        for act in acts {
            match act {
                Act::Dcs {
                    voters,
                    quorum,
                    wire,
                } => {
                    let (kind, field) = match wire {
                        Wire::Quorum => (SyncKind::Quorum, quorum as u32),
                        Wire::PriorityFinal { quorum_field } => (SyncKind::Priority, quorum_field),
                    };
                    if !write_quorum_sync_state(io, me, &voters, field, kind).await {
                        return ExecOutcome::Failed("/sync write failed".into());
                    }
                    if kind == SyncKind::Priority {
                        info!("quorum sync finished: /sync is back to the Priority form");
                        return ExecOutcome::HandedOff;
                    }
                    wrote = true;
                }
                Act::Pg(text) => {
                    if !io.lease_ok() {
                        return ExecOutcome::Failed("leader lease gone".into());
                    }
                    let key = parse_sync_value(&text)
                        .map(|s| s.key())
                        .unwrap_or_else(|_| obs.key.clone());
                    // Recorded before the call: a failure after the file was
                    // written leaves the effective value behind the file.
                    self.pending = Some(PendingSet {
                        text: text.clone(),
                        key,
                        pre_key: obs.key.clone(),
                        cycles: 0,
                    });
                    return match io.set_pg(&text).await {
                        Ok(()) => ExecOutcome::Progressed,
                        Err(e) => {
                            warn!("Failed to set synchronous_standby_names: {e}");
                            ExecOutcome::Failed(format!("set_pg: {e}"))
                        }
                    };
                }
            }
        }
        if wrote {
            ExecOutcome::Progressed
        } else {
            ExecOutcome::NoChange
        }
    }

    /// The "unknown history" reset: `(me, V = {}, q)` with `q` the smallest
    /// value that keeps SI-1 true (0 when S is empty), kind Quorum.
    async fn reset_sync<IO: SyncIo>(&self, io: &mut IO, me: &str, state: &mut QuorumState) -> bool {
        let quorum = state.pg_names.len().saturating_sub(state.effective_n());
        let empty = Names::new();
        if !write_quorum_sync_state(io, me, &empty, quorum as u32, SyncKind::Quorum).await {
            return false;
        }
        info!("/sync reset to an empty voter set (q {quorum}): the promotion history is unknown");
        state.voters.clear();
        state.quorum = quorum;
        true
    }

    /// Turn the observed state into the list of things to do this cycle.
    #[allow(clippy::too_many_arguments)]
    fn assemble(
        &self,
        input: &ExecInput,
        obs: &Observed,
        state: &QuorumState,
        target: &QuorumTarget,
        caught: &Names,
        relax_blocked: bool,
        strict: bool,
    ) -> std::result::Result<Vec<Act>, String> {
        let mut acts: Vec<Act> = Vec::new();
        let mut cur = state.clone();

        // Keep SI-1 true in the state the planner starts from (raise only).
        if !cur.si1_holds() {
            let quorum = needed_quorum(&cur);
            acts.push(Act::Dcs {
                voters: cur.voters.clone(),
                quorum,
                wire: Wire::Quorum,
            });
            cur.quorum = quorum;
        }

        // Nobody eligible and nothing in S: PostgreSQL must carry the strict
        // placeholder when strict and the empty value otherwise (the planner
        // reads both as S = {}), and an unreadable value is replaced by the
        // same text. This comes before the voter clean-up so the final hand-off
        // never leaves a wrong value behind.
        if target.voters.is_empty()
            && cur.pg_names.is_empty()
            && (!obs.reasonable || obs.shape.strict_hold != strict)
        {
            let text = render_quorum_value(&empty_target(), strict)
                .map_err(|e| format!("cannot render the empty value: {e}"))?;
            acts.push(Act::Pg(text));
            return Ok(acts);
        }

        let mut has_pg = false;
        let mut gated = false;
        for step in plan(&cur.clone(), target, caught, 4) {
            match &step {
                Step::SetDcs { voters, quorum } => {
                    if *quorum < cur.quorum && relax_blocked {
                        gated = true;
                        break;
                    }
                    let wire = if input.mode == EffectiveSyncMode::Off
                        && voters.is_empty()
                        && *quorum == 0
                    {
                        Wire::PriorityFinal { quorum_field: 0 }
                    } else {
                        Wire::Quorum
                    };
                    acts.push(Act::Dcs {
                        voters: voters.clone(),
                        quorum: *quorum,
                        wire,
                    });
                }
                Step::SetPg { names, n } => {
                    let text = render_quorum_value(&QuorumTarget::new(names, *n), strict)
                        .map_err(|e| format!("cannot render {names:?} / {n}: {e}"))?;
                    acts.push(Act::Pg(text));
                    has_pg = true;
                }
            }
            cur.apply(&step);
            if has_pg {
                break;
            }
        }
        if has_pg {
            return Ok(acts);
        }
        if !obs.reasonable {
            return Err(
                "an unreadable synchronous_standby_names has no replacement planned".into(),
            );
        }
        if gated {
            return Ok(acts);
        }

        // From here the planner has converged: S and V equal the target.
        match input.mode {
            EffectiveSyncMode::Quorum => {
                // T8 left `FIRST n (S)` or a bare list: same (S, n), as `ANY`.
                if !cur.pg_names.is_empty() && obs.shape.keyword != SyncKeyword::Any {
                    let text =
                        render_quorum_value(&QuorumTarget::new(&cur.pg_names, cur.pg_n), false)
                            .map_err(|e| format!("cannot rewrite as ANY: {e}"))?;
                    acts.push(Act::Pg(text));
                }
            }
            EffectiveSyncMode::Priority => {
                // T9: V = S, |S| = n, q = 0: the Priority code can take over.
                let done = cur.quorum == 0
                    && cur.voters == cur.pg_names
                    && cur.pg_names.len() == cur.effective_n();
                if done {
                    if !cur.pg_names.is_empty() && obs.shape.keyword != SyncKeyword::First {
                        let text = render_first(&QuorumTarget::new(&cur.pg_names, cur.pg_n))
                            .map_err(|e| format!("cannot render FIRST: {e}"))?;
                        acts.push(Act::Pg(text));
                    } else {
                        acts.push(Act::Dcs {
                            voters: cur.voters.clone(),
                            quorum: input.count,
                            wire: Wire::PriorityFinal {
                                quorum_field: input.count as u32,
                            },
                        });
                    }
                }
            }
            EffectiveSyncMode::Off => {
                // T7: nothing left; hand the cluster back.
                if cur.voters.is_empty()
                    && cur.pg_names.is_empty()
                    && cur.quorum == 0
                    && !acts.iter().any(|a| {
                        matches!(
                            a,
                            Act::Dcs {
                                wire: Wire::PriorityFinal { .. },
                                ..
                            }
                        )
                    })
                {
                    acts.push(Act::Dcs {
                        voters: Names::new(),
                        quorum: 0,
                        wire: Wire::PriorityFinal { quorum_field: 0 },
                    });
                }
            }
        }
        Ok(acts)
    }
}

// Test-only stand-in for the PR4 setting. A thread-local so that `Ha` itself
// carries no `cfg(test)` field (the source-order tests in `ha/mod.rs` cut the
// production text at the first `#[cfg(test)]`).
#[cfg(test)]
thread_local! {
    static MODE_OVERRIDE: std::cell::Cell<Option<EffectiveSyncMode>> =
        const { std::cell::Cell::new(None) };
}

/// Sets the test-only mode for this thread and clears it on drop.
#[cfg(test)]
pub(crate) struct ModeOverrideGuard;

#[cfg(test)]
impl ModeOverrideGuard {
    pub(crate) fn set(mode: Option<EffectiveSyncMode>) -> Self {
        MODE_OVERRIDE.with(|m| m.set(mode));
        ModeOverrideGuard
    }
}

#[cfg(test)]
impl Drop for ModeOverrideGuard {
    fn drop(&mut self) {
        MODE_OVERRIDE.with(|m| m.set(None));
    }
}

impl Ha {
    /// The mode the HA loop wants. PR3b can say `Priority` or `Off` only; the
    /// `Quorum` answer exists for tests until PR4 adds the setting.
    pub(super) fn effective_sync_mode(&self) -> EffectiveSyncMode {
        #[cfg(test)]
        if let Some(mode) = MODE_OVERRIDE.with(|m| m.get()) {
            return mode;
        }
        if self
            .dynamic_config_state
            .last_config
            .synchronous_mode
            .unwrap_or(false)
        {
            EffectiveSyncMode::Priority
        } else {
            EffectiveSyncMode::Off
        }
    }

    /// Does the quorum executor drive `/sync` and `synchronous_standby_names`
    /// this cycle? Only for the Quorum mode, or while a `kind: Quorum` `/sync`
    /// still has to be taken back (T7, T9). False for every Priority or Off
    /// cluster whose `/sync` is not a Quorum one.
    pub(super) fn quorum_executor_applies(&self) -> bool {
        self.effective_sync_mode() == EffectiveSyncMode::Quorum
            || self
                .cluster
                .sync_state
                .as_ref()
                .is_some_and(|s| s.kind == SyncKind::Quorum)
    }

    /// One cycle of the quorum executor on the real PostgreSQL and DCS.
    pub(super) async fn enforce_quorum_sync(&mut self) {
        let cfg = &self.dynamic_config_state.last_config;
        let input = ExecInput {
            me: self.config.name.clone(),
            mode: self.effective_sync_mode(),
            count: cfg.synchronous_node_count.unwrap_or(1) as usize,
            strict: cfg.synchronous_mode_strict.unwrap_or(false),
            lag_threshold: cfg.maximum_lag_on_syncnode,
            members: self.cluster.members.clone(),
            sync: self.cluster.sync_state.clone(),
        };
        let mut io = RealIo {
            pg: &self.postgresql,
            dcs: &*self.dcs,
            lease: self.leader_lease,
            paused: self.is_paused,
        };
        let outcome = self.quorum_exec.cycle(&mut io, &input).await;
        match outcome {
            ExecOutcome::Failed(why) => warn!("quorum sync cycle stopped: {why}"),
            ExecOutcome::HandedOff => self.reset_sync_cache(),
            ExecOutcome::NoChange | ExecOutcome::Progressed => {}
        }
    }
}

#[cfg(test)]
#[path = "quorum_sync_tests.rs"]
mod tests;
