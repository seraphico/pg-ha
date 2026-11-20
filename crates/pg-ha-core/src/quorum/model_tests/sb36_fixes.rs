//! SB-36 model proof: the two fixes, proven before any production change.
//!
//! * **Fix 1 (enable quorum).** From an empty `S` (asynchronous) the planner
//!   writes DCS `(V*, q*)` with `q* = |V*| - n*` (`QuorumTarget::quorum`, the
//!   existing settled formula) FIRST, then PG `ANY n* (V*)`. Commits made while
//!   `S` is empty are not covered (maintainer decision), so the model records
//!   acknowledgements only while `S` is non-empty and restarts `acked` at the
//!   enabling DCS write. From the moment PG has the list, SI-1 and invariant I
//!   are checked as before.
//! * **Fix 2 (rejoin window).** A voter that is offline but whose member
//!   record has not expired stays in `V*`, so a rejoin does not go through
//!   step A (`q + 1`).
//!
//! The fix logic lives in `World::plan_steps` and `World::target`, driven by
//! the `Fixes` switches. All switches off is the code before the fixes, which
//! the rest of the model keeps testing unchanged. Each mutation test flips a
//! switch the wrong way and requires the model to produce a counterexample.

use super::*;
use proptest::test_runner::{Config, TestRunner};

#[derive(Debug, Clone)]
enum FAction {
    Base(Action),
    /// The member record of an offline node expires (grace over).
    Expire(usize),
}

fn faction_strategy() -> impl Strategy<Value = FAction> {
    prop_oneof![
        14 => action_strategy().prop_map(FAction::Base),
        2 => any::<usize>().prop_map(FAction::Expire),
    ]
}

const FIX: Fixes = Fixes {
    enable_dcs_first: true,
    enable_pg_first: false,
    enable_q_too_low: false,
    keep_offline: true,
    leave_new: false,
    leave_no_gate: false,
    leave_dcs_first: false,
    leave_gate: false,
};

fn world(fix: Fixes, standbys: usize, n0: usize, steady: bool) -> World {
    let mut w = World::new(standbys, n0, steady);
    w.fix = fix;
    w
}

fn step(w: &mut World, a: &FAction) -> Result<(), String> {
    match a {
        FAction::Base(b) => w.apply(b),
        FAction::Expire(i) => {
            let i = w.idx(*i);
            if i != w.leader && !w.nodes[i].alive {
                w.expired |= 1 << (i % 8);
            }
            w.refresh_acks();
            w.check(&format!("after Expire({i})"))
        }
    }
}

fn run_fixed(
    fix: Fixes,
    standbys: usize,
    n0: usize,
    steady: bool,
    actions: &[FAction],
) -> Result<Stats, String> {
    let mut w = world(fix, standbys, n0, steady);
    for (k, a) in actions.iter().enumerate() {
        step(&mut w, a).map_err(|e| format!("step {k} {a:?}: {e}"))?;
    }
    Ok(w.stats)
}

/// Run `prop` under a fresh runner and return whether a counterexample exists.
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

// ---- fix 1: enable, then the primary dies ----------------------------------

/// Enabling write(s) (`plan_limit` steps of the planner), then the primary
/// dies, then every standby stands for election with every voter reachable.
fn enable_then_death(standbys: usize) -> Vec<Action> {
    let mut a = vec![Action::KillLeader];
    for c in 1..=standbys {
        a.push(Action::Elect(c, 0xFF, 0, 0));
    }
    a
}

#[test]
fn enable_then_immediate_primary_death_elects_with_fix_1() {
    for standbys in [2usize, 3, 4, 5] {
        for n0 in 1..=standbys.min(3) {
            let mut w = world(FIX, standbys, n0, false);
            w.apply(&Action::Plan(1)).unwrap();
            // Only the DCS write happened: V is published, PG is still async.
            assert!(w.st.pg_names.is_empty() && !w.st.voters.is_empty());
            let n = n0.min(standbys);
            assert_eq!(w.st.quorum, standbys - n, "settled q, not |V|");
            for a in enable_then_death(standbys) {
                w.apply(&a).unwrap();
            }
            assert_eq!(
                w.stats.generations, 1,
                "a candidate must win ({standbys} standbys, n {n0}): {:?}",
                w.stats
            );
            assert_eq!(w.stats.granted_in_enable_window, 1);
        }
    }
}

/// Control: without fix 1 the same history leaves the cluster leaderless.
#[test]
fn control_without_fix_1_the_same_history_is_stuck() {
    for standbys in [2usize, 4] {
        let mut w = world(Fixes::default(), standbys, 1, false);
        w.apply(&Action::Plan(1)).unwrap();
        assert_eq!(w.st.quorum, standbys, "q = |V| (the SB-36 window)");
        for a in enable_then_death(standbys) {
            w.apply(&a).unwrap();
        }
        assert_eq!(w.stats.generations, 0);
        assert!(w.stats.refused_insufficient >= standbys, "{:?}", w.stats);
    }
}

fn pre_history() -> impl Strategy<Value = Vec<Action>> {
    prop::collection::vec(
        prop_oneof![
            (1usize..4).prop_map(Action::Commit),
            (1usize..6, 1usize..5).prop_map(|(i, k)| Action::Advance(i, k)),
        ],
        0..20,
    )
}

fn enable_death_property(
    fix: Fixes,
    standbys: usize,
    n0: usize,
    pre: &[Action],
    plan_steps: usize,
) -> Result<(), String> {
    let mut w = world(fix, standbys, n0, false);
    for a in pre {
        w.apply(a)?;
    }
    // Every crash point of the enabling sequence: 1 = DCS only, 2 = DCS + PG, ...
    w.apply(&Action::Plan(plan_steps))?;
    for a in enable_then_death(standbys) {
        w.apply(&a)?;
    }
    if w.stats.generations != 1 {
        return Err(format!(
            "nobody won after {plan_steps} enabling step(s): state {:?}, stats {:?}",
            w.st, w.stats
        ));
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1500))]

    /// After fix 1, an enable followed at any crash point by the death of the
    /// primary still lets a candidate win the vote, and the end-to-end check
    /// (a granted candidate holds every acknowledged commit) holds.
    #[test]
    fn fix_1_enable_then_primary_death_always_elects(
        standbys in 2usize..6,
        n0 in 1usize..4,
        pre in pre_history(),
        plan_steps in 1usize..5,
    ) {
        enable_death_property(FIX, standbys, n0, &pre, plan_steps).map_err(TestCaseError::fail)?;
    }
}

// ---- fix 2: a brief offline voter rejoins ----------------------------------

/// Kill some voters, let the planner run, revive them, let the planner run.
fn rejoin_property(
    fix: Fixes,
    standbys: usize,
    n0: usize,
    kills: &[usize],
    plans_down: usize,
    plans_up: usize,
) -> Result<(), String> {
    let n = n0.min(standbys);
    let q0 = standbys - n;
    let guard = |w: &World, ctx: &str| -> Result<(), String> {
        if w.st.quorum > q0 || w.st.quorum >= w.st.voters.len() {
            return Err(format!(
                "q raised or unelectable {ctx}: q={} (settled q0={q0}), |V|={} state {:?}",
                w.st.quorum,
                w.st.voters.len(),
                w.st
            ));
        }
        Ok(())
    };
    let mut downed: Vec<usize> = Vec::new();
    for &k in kills {
        let i = 1 + k % standbys;
        if !downed.contains(&i) {
            downed.push(i);
        }
    }
    downed.truncate(standbys - n);
    let mut hist = Vec::new();
    for &i in &downed {
        hist.push(Action::Kill(i));
    }
    for _ in 0..plans_down {
        hist.push(Action::Plan(8));
    }
    for &i in &downed {
        hist.push(Action::Revive(i));
        hist.push(Action::Advance(i, 64));
    }
    for _ in 0..plans_up {
        hist.push(Action::Plan(8));
    }
    let mut w = world(fix, standbys, n0, true);
    for (k, a) in hist.iter().enumerate() {
        guard_after(&mut w, a, &guard).map_err(|e| format!("action {k} {a:?}: {e}"))?;
    }
    Ok(())
}

fn guard_after(
    w: &mut World,
    a: &Action,
    guard: &impl Fn(&World, &str) -> Result<(), String>,
) -> Result<(), String> {
    w.apply(a)?;
    guard(w, &format!("after {a:?}"))
}

fn rejoin_strategy() -> impl Strategy<Value = (usize, usize, Vec<usize>, usize, usize)> {
    (2usize..6, 1usize..4)
        .prop_filter("some voters must be able to go offline", |(s, n0)| s > n0)
        .prop_flat_map(|(s, n0)| {
            (
                Just(s),
                Just(n0),
                prop::collection::vec(any::<usize>(), 1..4),
                1usize..4,
                1usize..4,
            )
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1500))]

    /// After fix 2 a voter that goes offline and comes back inside its grace
    /// never raises q above the settled value, and q stays below |V|.
    #[test]
    fn fix_2_brief_offline_voter_rejoin_does_not_raise_q(
        (standbys, n0, kills, down, up) in rejoin_strategy(),
    ) {
        rejoin_property(FIX, standbys, n0, &kills, down, up).map_err(TestCaseError::fail)?;
    }
}

/// The 3-node case spelled out: V={n1,n2}, q=1. n2 goes offline, the planner
/// runs once, n2 comes back.
#[test]
fn fix_2_three_node_case_keeps_q_at_one() {
    let mut w = world(FIX, 2, 1, true);
    assert_eq!((w.st.voters.len(), w.st.quorum), (2, 1));
    for a in [
        Action::Kill(2),
        Action::Plan(8),
        Action::Plan(8),
        Action::Revive(2),
        Action::Advance(2, 64),
        Action::Plan(8),
        Action::Plan(8),
    ] {
        w.apply(&a).unwrap();
        assert_eq!((w.st.voters.len(), w.st.quorum), (2, 1), "after {a:?}");
    }
    assert!(w.stats.kept_offline >= 1);
}

/// Control: without fix 2 the voter is removed and re-added, and q reaches |V|.
#[test]
fn control_without_fix_2_three_node_rejoin_makes_q_equal_v() {
    let mut w = world(Fixes::default(), 2, 1, true);
    for a in [
        Action::Kill(2),
        Action::Plan(8),
        Action::Revive(2),
        Action::Advance(2, 64),
        Action::Plan(8),
    ] {
        w.apply(&a).unwrap();
    }
    assert_eq!(w.st.voters.len(), 2);
    assert_eq!(w.st.quorum, 2, "q = |V|: nobody can be elected");
}

/// Expiry ends the grace: the offline voter is removed normally and the usual
/// re-add path (q + 1) applies when it returns. This is the documented limit
/// of fix 2, not a safety problem (the general property still holds).
#[test]
fn expired_voter_is_removed_and_re_added_the_old_way() {
    let mut w = world(FIX, 3, 1, true);
    assert_eq!((w.st.voters.len(), w.st.quorum), (3, 2));
    w.apply(&Action::Kill(3)).unwrap();
    step(&mut w, &FAction::Expire(3)).unwrap();
    w.apply(&Action::Plan(8)).unwrap();
    assert_eq!(w.st.voters.len(), 2, "expired voter dropped from V");
}

// ---- no acknowledged loss, with both fixes on -------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(CASES))]

    /// Invariant I and the end-to-end check, from the moment quorum is in force
    /// in PG, over random histories with both fixes, including enables from
    /// async, kills, expiries, promotions and crashes between planner steps.
    #[test]
    fn fixes_keep_acknowledged_commits_in_every_history(
        standbys in 2usize..6,
        n0 in 1usize..4,
        steady in any::<bool>(),
        actions in prop::collection::vec(faction_strategy(), 10..140),
    ) {
        run_fixed(FIX, standbys, n0, steady, &actions).map_err(TestCaseError::fail)?;
    }
}

/// The histories above must reach the new paths, or the property proves nothing.
#[test]
fn sb36_model_actually_exercises_the_fixes() {
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
    let strat = (
        2usize..6,
        1usize..4,
        any::<bool>(),
        prop::collection::vec(faction_strategy(), 10..140),
    );
    runner
        .run(&strat, |(s, n0, steady, actions)| {
            let st = run_fixed(FIX, s, n0, steady, &actions).map_err(TestCaseError::fail)?;
            total.lock().unwrap().add(&st);
            Ok(())
        })
        .unwrap();
    let t = total.lock().unwrap().clone();
    eprintln!("sb36 coverage over {CASES} histories: {t:#?}");
    assert!(t.enable_writes > 200, "enabling write rarely run: {t:?}");
    assert!(t.kept_offline > 500, "offline voters rarely kept: {t:?}");
    assert!(
        t.granted_in_enable_window > 5,
        "no election inside the enabling window: {t:?}"
    );
    assert!(t.acked > 2_000, "too few acknowledged commits: {t:?}");
    assert!(t.generations > 500, "too few promotions: {t:?}");
    assert!(
        t.refused_with_acked_missing > 100,
        "the vote never refused a data-losing candidate: {t:?}"
    );
}

// ---- mutations: the model must catch each wrong variant ---------------------

/// (a) The reverse order. PG `ANY n (V)` first while DCS `/sync` has no voters
/// would acknowledge quorum commits that the DCS does not know about.
#[test]
fn mutation_a_pg_first_then_dcs_is_caught() {
    let bad = Fixes {
        enable_dcs_first: false,
        enable_pg_first: true,
        ..FIX
    };
    // Deterministic: one planner step is enough.
    let mut w = world(bad, 3, 1, false);
    let err = w.apply(&Action::Plan(1)).unwrap_err();
    assert!(
        err.contains("quorum in force in PG while DCS /sync has no voters"),
        "{err}"
    );
    // Random: the model finds it from histories, not only from the script.
    let found = finds_counterexample(
        1000,
        (
            2usize..6,
            1usize..4,
            prop::collection::vec(faction_strategy(), 5..60),
        ),
        |(s, n0, actions)| run_fixed(bad, s, n0, false, &actions).map(|_| ()),
    );
    assert!(found.is_some(), "mutation (a) survived the random model");
    // And the targeted fix-1 property fails for it too.
    let found = finds_counterexample(
        200,
        (2usize..6, 1usize..4, pre_history(), 1usize..5),
        |(s, n0, pre, k)| enable_death_property(bad, s, n0, &pre, k),
    );
    assert!(found.is_some(), "mutation (a) survived the fix-1 property");
}

/// Mutation of the fix-1 formula: `q` one below the settled value. The model
/// finds an acknowledged commit missing on `q + 1` voters.
#[test]
fn mutation_enable_q_one_too_low_is_caught() {
    let bad = Fixes {
        enable_q_too_low: true,
        ..FIX
    };
    let found = finds_counterexample(
        3000,
        (
            2usize..6,
            1usize..4,
            prop::collection::vec(faction_strategy(), 10..100),
        ),
        |(s, n0, actions)| run_fixed(bad, s, n0, false, &actions).map(|_| ()),
    );
    let msg = found.expect("q too low survived the model");
    assert!(
        msg.contains("SI-1 violated")
            || msg.contains("invariant I violated")
            || msg.contains("LOST ACKNOWLEDGED"),
        "{msg}"
    );
}

/// (b) Remove the offline voter and add it again. The rejoin property fails:
/// q goes above the settled value, up to |V|.
#[test]
fn mutation_b_remove_then_re_add_is_caught() {
    let bad = Fixes {
        keep_offline: false,
        ..FIX
    };
    let found = finds_counterexample(1500, rejoin_strategy(), |(s, n0, kills, down, up)| {
        rejoin_property(bad, s, n0, &kills, down, up)
    });
    let msg = found.expect("mutation (b) survived the rejoin property");
    assert!(msg.contains("q raised or unelectable"), "{msg}");
    // Deterministic 3-node instance of the same mutation.
    assert!(rejoin_property(bad, 2, 1, &[1], 1, 1).is_err());
    assert!(rejoin_property(FIX, 2, 1, &[1], 1, 1).is_ok());
}

// ---- post-failover situations (analysis in model-proof.md) ------------------

/// After a failover with the old primary down, the survivors restart to follow
/// the new leader. If they have re-attached before the leader's next planner
/// run, the old primary is dropped from `V` with `q` unchanged and `q < |V|`
/// holds all along, with or without the fixes (no window).
#[test]
fn post_failover_survivors_reattached_has_no_window() {
    for fix in [Fixes::default(), FIX] {
        let mut w = world(fix, 4, 2, true);
        for a in [
            Action::KillLeader,
            Action::Elect(1, 0xFF, 0, 0),
            Action::Advance(2, 64),
            Action::Advance(3, 64),
            Action::Advance(4, 64),
            Action::Finalize,
            Action::Plan(1),
            Action::Plan(1),
            Action::Plan(1),
            Action::Plan(8),
            Action::Plan(8),
        ] {
            w.apply(&a).unwrap();
            assert!(
                w.st.quorum < w.st.voters.len(),
                "{fix:?} after {a:?}: {:?}",
                w.st
            );
        }
    }
}

/// KNOWN GAP, not covered by fixes 1 and 2 (this is the 5b window of the PR4
/// evidence). Right after the promotion no standby is streaming yet (they are
/// restarting to follow the new timeline), so the selection has no eligible
/// standby. The planner then takes the "nobody eligible" path, whose first
/// step writes `q = |U|` while `S` is still non-empty. Fix 1 only acts once
/// `S` is empty, fix 2 only keeps voters while the target is non-empty, so
/// `q = |V|` is reached with both fixes on. This test pins that behaviour so a
/// later change (a separate decision, see model-proof.md) has to touch it.
#[test]
fn known_gap_post_failover_without_streaming_standbys_reaches_q_equal_v() {
    for fix in [Fixes::default(), FIX] {
        let mut w = world(fix, 4, 2, true);
        for a in [
            Action::KillLeader,
            Action::Elect(1, 0xFF, 0, 0),
            Action::Finalize,
            Action::Plan(1),
        ] {
            w.apply(&a).unwrap();
        }
        assert_eq!(w.st.voters.len(), 4);
        assert_eq!(w.st.quorum, 4, "{fix:?}: q = |V| with S still non-empty");
        assert!(!w.st.pg_names.is_empty());
    }
}
