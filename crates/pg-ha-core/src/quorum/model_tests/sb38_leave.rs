//! SB-38 model proof: leaving quorum (`synchronous_mode` to `"async"` or
//! `"sync"`) without ever publishing `q >= |V|`.
//!
//! # The window today
//!
//! * **async**: the planner's nobody-eligible branch first writes DCS
//!   `q = |V ∪ S|` (= `|V|`), then PG `S = {}`, then (a cycle later, behind the
//!   mark) the hand-off `(V = {}, q = 0)`. Between the first and the last
//!   write nobody can win the vote, so a primary lost there leaves the cluster
//!   leaderless.
//! * **sync**: the target is `V*` of `n` names, `q* = 0`. The planner removes
//!   voters with `q` unchanged (step F) and lowers `q` only afterwards, behind
//!   the SI-4 gate (step G). F leaves `(V*, q)` with `q = |V| - n >= |V*| = n`
//!   whenever `|V| >= 2n`: again nobody can win.
//!
//! # The chosen order (see `plan_leave` in `model_tests.rs`)
//!
//! * **async**: PG `S = {}` first, `/sync` untouched (`q = |V| - n < |V|`, the
//!   vote passes and invariant I covers every commit acknowledged under
//!   quorum). Then one hand-off write `(V = {}, q = 0)` (Priority wire form).
//!   The planner's "raise q to |U|" step is not taken.
//! * **sync**: the planner's F and G are one write `(V*, q')` with
//!   `q' = min(q, max(q*, |V* ∖ caught_up|))`, taken only when `q' < |V*|`;
//!   otherwise wait with `/sync` as it is. SI-4 is the same gate as G.
//!
//! The model keeps `acked` (commits acknowledged while quorum was in force)
//! checked by invariant I and by the vote (`LOST ACKNOWLEDGED COMMIT`) until the
//! hand-off. Commits made after PG went asynchronous are not covered
//! (`refresh_acks`); the guarantee ends at the hand-off (`handed_off`).

use super::*;
use proptest::test_runner::{Config, TestRunner};

const OLD: Fixes = Fixes {
    enable_dcs_first: true,
    enable_pg_first: false,
    enable_q_too_low: false,
    keep_offline: true,
    leave_new: false,
    leave_no_gate: false,
    leave_dcs_first: false,
};
const NEW: Fixes = Fixes {
    leave_new: true,
    ..OLD
};

fn leave_world(fix: Fixes, standbys: usize, n0: usize, mode: Tm) -> World {
    let mut w = World::new(standbys, n0, true);
    w.fix = fix;
    w.mode = mode;
    w
}

#[derive(Debug, Clone)]
enum LAction {
    Base(Action),
    Expire(usize),
}

fn laction_strategy() -> impl Strategy<Value = LAction> {
    prop_oneof![
        14 => action_strategy().prop_map(LAction::Base),
        2 => any::<usize>().prop_map(LAction::Expire),
    ]
}

fn lstep(w: &mut World, a: &LAction) -> Result<(), String> {
    match a {
        LAction::Base(b) => w.apply(b),
        LAction::Expire(i) => {
            let i = w.idx(*i);
            if i != w.leader && !w.nodes[i].alive {
                w.expired |= 1 << (i % 8);
            }
            w.refresh_acks();
            w.check(&format!("after Expire({i})"))
        }
    }
}

fn finds_counterexample<S: Strategy>(
    cases: u32,
    strat: S,
    prop: impl Fn(S::Value) -> Result<(), String>,
) -> Option<String> {
    let mut runner = TestRunner::new(Config {
        cases,
        failure_persistence: None,
        ..Config::default()
    });
    runner
        .run(&strat, |v| prop(v).map_err(TestCaseError::fail))
        .err()
        .map(|e| e.to_string())
}

// ---- deterministic: one planner step at a time, then the primary dies --------

#[derive(Debug, PartialEq, Eq)]
enum Death {
    /// The hand-off already happened: the Priority/async code elects.
    HandedOff,
    /// A candidate won the vote.
    Elected,
    /// Nobody can win: the cluster is leaderless.
    Leaderless,
}

/// All standbys caught up and one commit, then `plans` planner steps (one per
/// cycle, standbys re-attached and caught up between them), then the primary
/// dies and every standby stands for election with every voter reachable.
fn death_after(fix: Fixes, standbys: usize, n0: usize, mode: Tm, plans: usize) -> Death {
    let mut w = leave_world(fix, standbys, n0, mode);
    w.apply(&Action::Commit(2)).unwrap();
    let catch_up = |w: &mut World| {
        for i in 1..=standbys {
            w.apply(&Action::Advance(i, 64)).unwrap();
        }
    };
    catch_up(&mut w);
    for _ in 0..plans {
        w.apply(&Action::Plan(1)).unwrap();
        catch_up(&mut w);
    }
    if w.handed_off {
        return Death::HandedOff;
    }
    w.apply(&Action::KillLeader).unwrap();
    for c in 1..=standbys {
        w.apply(&Action::Elect(c, 0xFF, 0, 0)).unwrap();
    }
    if w.stats.generations == 1 {
        Death::Elected
    } else {
        Death::Leaderless
    }
}

/// Every crash point of the switch for the given shape.
fn crash_points(fix: Fixes, standbys: usize, n0: usize, mode: Tm) -> Vec<Death> {
    (0..=6)
        .map(|plans| death_after(fix, standbys, n0, mode, plans))
        .collect()
}

#[test]
fn old_order_async_is_leaderless_at_a_crash_point() {
    for standbys in [2usize, 4] {
        for n0 in 1..=standbys.min(3) {
            let pts = crash_points(OLD, standbys, n0, Tm::Async);
            assert!(
                pts.contains(&Death::Leaderless),
                "{standbys} standbys, n {n0}: {pts:?}"
            );
            assert!(pts.contains(&Death::HandedOff), "{pts:?}");
        }
    }
}

#[test]
fn new_order_async_never_leaderless() {
    for standbys in [2usize, 3, 4, 5] {
        for n0 in 1..=standbys.min(4) {
            let pts = crash_points(NEW, standbys, n0, Tm::Async);
            assert!(
                !pts.contains(&Death::Leaderless),
                "{standbys} standbys, n {n0}: {pts:?}"
            );
            assert!(pts.contains(&Death::Elected), "{pts:?}");
            assert!(pts.contains(&Death::HandedOff), "{pts:?}");
        }
    }
}

/// Old order, sync: the window exists exactly when `|V| >= 2n` (q = |V| - n
/// is then `>= n = |V*|`).
#[test]
fn old_order_sync_window_exists_when_v_at_least_2n() {
    for standbys in [2usize, 4] {
        for n0 in 1..=standbys {
            let pts = crash_points(OLD, standbys, n0, Tm::Sync);
            let leaderless = pts.contains(&Death::Leaderless);
            assert_eq!(
                leaderless,
                standbys >= 2 * n0,
                "{standbys} standbys, n {n0}: {pts:?}"
            );
        }
    }
}

#[test]
fn new_order_sync_never_leaderless() {
    for standbys in [2usize, 3, 4, 5] {
        for n0 in 1..=standbys.min(4) {
            let pts = crash_points(NEW, standbys, n0, Tm::Sync);
            assert!(
                !pts.contains(&Death::Leaderless),
                "{standbys} standbys, n {n0}: {pts:?}"
            );
        }
    }
}

/// Spelled out for 3 nodes (2 standbys, n = 1): old order publishes `q = |V|`.
#[test]
fn three_node_async_published_states() {
    let mut old = leave_world(OLD, 2, 1, Tm::Async);
    old.apply(&Action::Plan(1)).unwrap();
    assert_eq!((old.st.voters.len(), old.st.quorum), (2, 2), "q = |V|");
    let mut new = leave_world(NEW, 2, 1, Tm::Async);
    new.apply(&Action::Plan(1)).unwrap();
    assert!(new.st.pg_names.is_empty(), "PG goes asynchronous first");
    assert_eq!((new.st.voters.len(), new.st.quorum), (2, 1), "q < |V|");
    new.apply(&Action::Plan(1)).unwrap();
    assert!(new.handed_off);
}

// ---- random histories ---------------------------------------------------------

fn history() -> impl Strategy<Value = Vec<LAction>> {
    prop::collection::vec(laction_strategy(), 10..140)
}

fn run_leave(
    fix: Fixes,
    standbys: usize,
    n0: usize,
    mode: Tm,
    actions: &[LAction],
) -> Result<Stats, String> {
    let mut w = leave_world(fix, standbys, n0, mode);
    for (k, a) in actions.iter().enumerate() {
        lstep(&mut w, a).map_err(|e| format!("step {k} {a:?}: {e}"))?;
        if w.stats.leave_vote_impossible > 0 {
            return Err(format!(
                "step {k} {a:?}: q >= |V| published while leaving quorum: {:?}",
                w.st
            ));
        }
    }
    Ok(w.stats)
}

fn mode_strategy() -> impl Strategy<Value = Tm> {
    prop_oneof![Just(Tm::Async), Just(Tm::Sync)]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(CASES))]

    /// New order: in every history (kills, expiries, promotions, crashes
    /// between planner steps, reconfigs) no state with q >= |V| is published
    /// while leaving quorum, SI-1 outside the leaving window, invariant I, and
    /// the vote never grants a candidate missing an acknowledged commit.
    #[test]
    fn new_order_keeps_vote_possible_and_acked_commits(
        standbys in 2usize..6,
        n0 in 1usize..4,
        mode in mode_strategy(),
        actions in history(),
    ) {
        run_leave(NEW, standbys, n0, mode, &actions).map_err(TestCaseError::fail)?;
    }
}

/// Crash point property with random pre-history: after any number of planner
/// steps the primary dies; unless the hand-off happened a candidate wins.
fn death_property(
    fix: Fixes,
    standbys: usize,
    n0: usize,
    mode: Tm,
    pre: &[Action],
    plans: usize,
) -> Result<(), String> {
    let mut w = leave_world(fix, standbys, n0, mode);
    for a in pre {
        w.apply(a)?;
    }
    for _ in 0..plans {
        w.apply(&Action::Plan(1))?;
    }
    if w.handed_off {
        return Ok(());
    }
    w.apply(&Action::KillLeader)?;
    for c in 1..=standbys {
        w.apply(&Action::Elect(c, 0xFF, 0, 0))?;
    }
    if w.stats.generations != 1 {
        return Err(format!(
            "leaderless after {plans} planner step(s): state {:?}, stats {:?}",
            w.st, w.stats
        ));
    }
    Ok(())
}

fn pre_history() -> impl Strategy<Value = Vec<Action>> {
    prop::collection::vec(
        prop_oneof![
            (1usize..4).prop_map(Action::Commit),
            (1usize..6, 1usize..5).prop_map(|(i, k)| Action::Advance(i, k)),
            Just(Action::Plan(1)),
        ],
        0..24,
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1500))]

    #[test]
    fn new_order_primary_death_at_any_step_elects(
        standbys in 2usize..6,
        n0 in 1usize..4,
        mode in mode_strategy(),
        pre in pre_history(),
        plans in 0usize..6,
    ) {
        death_property(NEW, standbys, n0, mode, &pre, plans).map_err(TestCaseError::fail)?;
    }
}

/// The histories above must reach the leaving paths.
#[test]
fn sb38_model_actually_exercises_the_leave() {
    use proptest::test_runner::TestRng;
    let total = Mutex::new(Stats::default());
    let config = Config {
        cases: CASES,
        ..Config::default()
    };
    let mut runner = TestRunner::new_with_rng(
        config.clone(),
        TestRng::deterministic_rng(config.rng_algorithm),
    );
    let strat = (2usize..6, 1usize..4, mode_strategy(), history());
    runner
        .run(&strat, |(s, n0, mode, actions)| {
            let st = run_leave(NEW, s, n0, mode, &actions).map_err(TestCaseError::fail)?;
            total.lock().unwrap().add(&st);
            Ok(())
        })
        .unwrap();
    let t = total.lock().unwrap().clone();
    eprintln!("sb38 coverage over {CASES} histories: {t:#?}");
    assert!(t.handed_off > 200, "hand-off rarely reached: {t:?}");
    assert!(t.acked > 1_000, "too few acknowledged commits: {t:?}");
    assert!(t.generations > 300, "too few promotions: {t:?}");
    assert!(
        t.refused_with_acked_missing > 50,
        "the vote never refused a data-losing candidate: {t:?}"
    );
}

// ---- mutations ----------------------------------------------------------------

/// The old order is caught by the model, from histories and from the
/// crash-point property, for async and for sync.
#[test]
fn mutation_old_order_is_caught() {
    for mode in [Tm::Async, Tm::Sync] {
        let found = finds_counterexample(
            1500,
            (2usize..6, 1usize..4, Just(mode), history()),
            |(s, n0, m, actions)| run_leave(OLD, s, n0, m, &actions).map(|_| ()),
        );
        let msg =
            found.unwrap_or_else(|| panic!("old order survived ({mode:?}) the history model"));
        assert!(msg.contains("q >= |V| published"), "{msg}");

        let found = finds_counterexample(
            1500,
            (2usize..6, 1usize..4, Just(mode), pre_history(), 0usize..6),
            |(s, n0, m, pre, k)| death_property(OLD, s, n0, m, &pre, k),
        );
        let msg =
            found.unwrap_or_else(|| panic!("old order survived ({mode:?}) the death property"));
        assert!(msg.contains("leaderless"), "{msg}");
    }
}

/// Mutation (async): `/sync` emptied before PG goes asynchronous. PG still
/// carries quorum while `/sync` has no voters.
#[test]
fn mutation_async_dcs_first_is_caught() {
    let bad = Fixes {
        leave_dcs_first: true,
        ..NEW
    };
    let mut w = leave_world(bad, 2, 1, Tm::Async);
    let err = w.apply(&Action::Plan(1)).unwrap_err();
    assert!(
        err.contains("quorum in force in PG while DCS /sync has no voters"),
        "{err}"
    );
}

/// Mutation (sync): voters removed without the SI-4 gate. A voter of `V*` that
/// does not hold an acknowledged commit makes invariant I fail.
#[test]
fn mutation_sync_no_gate_is_caught() {
    let bad = Fixes {
        leave_no_gate: true,
        ..NEW
    };
    let found = finds_counterexample(
        3000,
        (2usize..6, 1usize..4, history()),
        |(s, n0, actions)| run_leave(bad, s, n0, Tm::Sync, &actions).map(|_| ()),
    );
    let msg = found.expect("no-gate mutation survived the model");
    assert!(
        msg.contains("invariant I violated")
            || msg.contains("LOST ACKNOWLEDGED")
            || msg.contains("SI-1 violated"),
        "{msg}"
    );
}

/// The gate in the new order: while `V*` has not reached the mark the write
/// waits and `/sync` stays at `q < |V|` (4 standbys, n = 1, nobody caught up
/// after the PG change: `q' = 1 >= |V*| = 1` would be a window).
#[test]
fn sync_new_order_waits_for_the_gate_with_the_vote_possible() {
    let mut w = leave_world(NEW, 4, 1, Tm::Sync);
    w.apply(&Action::Commit(1)).unwrap();
    for _ in 0..4 {
        w.apply(&Action::Plan(8)).unwrap();
    }
    assert!(!w.handed_off);
    assert!(w.st.quorum < w.st.voters.len(), "{:?}", w.st);
    w.apply(&Action::KillLeader).unwrap();
    for c in 1..=4 {
        w.apply(&Action::Elect(c, 0xFF, 0, 0)).unwrap();
    }
    assert_eq!(w.stats.generations, 1, "{:?}", w.stats);
}
