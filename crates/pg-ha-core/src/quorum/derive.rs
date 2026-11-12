//! Deriving the planner's [`QuorumState`] from what is actually observed
//! (SB-13, PR3a). Pure.
//!
//! The executor remembers nothing between cycles (plan 3.2): each cycle it
//! reads DCS `/sync` and `SHOW synchronous_standby_names` and calls
//! [`derive_state`]. Every crash point is then recovered by the next cycle
//! planning from whatever it sees.
//!
//! # Mapping to `QuorumState` `(V, q, S, n)`
//!
//! | field | source |
//! |---|---|
//! | `voters` (V) | `/sync.sync_standby` names, case-folded, ONLY when `/sync` exists, has a leader and `kind == Quorum`; otherwise empty |
//! | `quorum` (q) | `/sync.quorum` under the same condition; otherwise 0 |
//! | `pg_names` (S) | the observed value's names, case-folded |
//! | `pg_n` (n) | the observed value's number (`QuorumState::effective_n` makes it 0 when `S` is empty) |
//!
//! Special observed values:
//!
//! * empty value: `S = {}`, `n = 0` (asynchronous);
//! * the strict placeholder (`strict_hold`): also `S = {}`, `n = 0`, exactly
//!   as the PR1 planner treats the strict "nobody eligible" case (writes
//!   really block, which is stricter than asynchronous, so the mapping is
//!   safe);
//! * a value containing the wildcard `*`: also `S = {}`, `n = 0`. **This is
//!   not sound by itself**: with a wildcard any connected standby, voter or
//!   not, can acknowledge, so no `(S, n)` describes who holds an acknowledged
//!   commit. The caller must check [`SyncValueShape::has_wildcard`] and
//!   replace the value before relying on the derived state (open question for
//!   3b, see the PR3a report);
//! * `FIRST n (S)` and the bare forms map to `(S, n)` like `ANY`. The plan
//!   (T8) argues this is sound for SI-1 because `FIRST n` acknowledgement sets
//!   are a subset of `ANY n` sets; the semantics difference is carried by
//!   `SyncValueShape::keyword`, not by `QuorumState`.
//! * a value with `n` larger than the number of names (blocking, probe P7)
//!   keeps its raw `n`: nothing can be acknowledged under it, so the larger
//!   `n` can only make SI-1 easier to satisfy and never hides a held commit.
//!
//! `/sync` that is absent, has an empty leader, or has `kind == Priority`
//! (`quorum` there is the legacy N and must be ignored) yields `V = {}`,
//! `q = 0`: "unknown history" (plan 3.2 step 2). Whether that state satisfies
//! SI-1 is the planner's business (`QuorumState::si1_holds`); for a Priority
//! `FIRST n (S)` with `|S| > n` it does not, and the planner then rebuilds the
//! voters, raising `q` with each add. The Priority-to-Quorum transition T8
//! ("keep V") needs the `SyncState` itself and is the executor's job (3b); this
//! function deliberately does not do it.
//!
//! # Phase
//!
//! * [`Phase::Steady`]: `/sync` is absent/empty or its leader is `me`
//!   (compared ASCII case-insensitively, as `ha/mod.rs` does).
//! * [`Phase::PromotionWindow`]: `/sync.leader` is another node (the previous
//!   leader), `kind == Quorum`, and [`post_promotion_state`] (the real
//!   function) returns a state for `(me, previous leader)`. The pre-promote
//!   write is expected to have installed exactly that PG value, so
//!   `expected_pg_matches` says whether the observed value IS it
//!   (`ANY n' (V')` with `V' = (V ∖ me) ∪ {previous leader}`,
//!   `n' = |V| - q`, same names, same `n`, keyword `ANY`). While in this
//!   window the state must be judged with
//!   [`QuorumState::si1_holds_excluding`]`(me)` and no planner step may run.
//! * [`Phase::NotMine`]: `/sync.leader` is another node and the window rule
//!   does not apply: the `/sync` is `Priority`, or `Quorum` with no
//!   post-promotion state (`me` not a voter, or `q >= |V|`). There is nothing
//!   safe to install, so the executor takes the "unknown history" path of
//!   plan 3.2 step 2 (write `(leader = me, V = {}, q = 0)` and rebuild).
//!
//! Decision taken here that the plan text leaves loose: the plan lists
//! "NotMine when `/sync.leader != me`" and "PromotionWindow when a Quorum
//! state of a previous leader ..." as if exclusive. They overlap for a
//! Quorum `/sync` of a previous leader; this module resolves it by letting
//! `PromotionWindow` win there (it is the more specific case, and the
//! `expected_pg_matches: bool` is only meaningful for it). `Steady` is never
//! returned while the leader is another node.

use crate::cluster::{SyncKind, SyncState};
use crate::quorum::{QuorumState, post_promotion_state};
use crate::sync_value::{SyncKeyword, SyncValueShape};

/// Where this node stands relative to the `/sync` record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Steady,
    PromotionWindow { expected_pg_matches: bool },
    NotMine,
}

/// The derived planner state and the phase. See the module doc.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Derived {
    pub state: QuorumState,
    pub phase: Phase,
}

/// Derive `(V, q, S, n)` and the phase from `/sync` and the observed value.
pub fn derive_state(sync: Option<&SyncState>, me: &str, observed: &SyncValueShape) -> Derived {
    // PostgreSQL side.
    let (pg_names, pg_n) = if observed.is_empty() || observed.strict_hold || observed.has_wildcard()
    {
        (Vec::new(), 0)
    } else {
        (observed.names.clone(), observed.n)
    };

    // DCS side.
    let live = sync.filter(|s| !s.is_empty());
    let (voters, quorum) = match live {
        Some(s) if s.kind == SyncKind::Quorum => (s.sync_standby_names(), s.quorum as usize),
        _ => (Vec::new(), 0),
    };
    let state = QuorumState::new(voters, quorum, pg_names, pg_n);

    let phase = match live {
        None => Phase::Steady,
        Some(s) if s.leader.eq_ignore_ascii_case(me) => Phase::Steady,
        Some(s) if s.kind == SyncKind::Quorum => {
            match post_promotion_state(&state, me, &s.leader) {
                Some(expected) => Phase::PromotionWindow {
                    expected_pg_matches: observed.keyword == SyncKeyword::Any
                        && !observed.has_wildcard()
                        && !observed.strict_hold
                        && observed.n == expected.pg_n
                        && observed.names_folded() == expected.pg_names,
                },
                None => Phase::NotMine,
            }
        }
        Some(_) => Phase::NotMine,
    };
    Derived { state, phase }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quorum::Names;
    use crate::sync::STRICT_NO_ELIGIBLE_PLACEHOLDER;
    use crate::sync_value::{parse_sync_value, render_quorum_value};
    use proptest::prelude::*;

    fn sync(leader: &str, standbys: Option<&str>, quorum: u32, kind: SyncKind) -> SyncState {
        SyncState {
            leader: leader.to_string(),
            sync_standby: standbys.map(str::to_string),
            quorum,
            kind,
        }
    }

    fn obs(v: &str) -> SyncValueShape {
        parse_sync_value(v).unwrap()
    }

    fn set(v: &[&str]) -> Names {
        v.iter().map(|s| s.to_string()).collect()
    }

    // ── mapping ──

    #[test]
    fn quorum_state_of_my_own_leadership_maps_v_q_s_n() {
        let s = sync("n1", Some("N2,n3,n4"), 1, SyncKind::Quorum);
        let d = derive_state(Some(&s), "n1", &obs(r#"ANY 2 ("n2","n3","n4")"#));
        assert_eq!(d.phase, Phase::Steady);
        assert_eq!(
            d.state,
            QuorumState::new(["n2", "n3", "n4"], 1, ["n2", "n3", "n4"], 2)
        );
        assert!(d.state.si1_holds());
    }

    #[test]
    fn leader_comparison_ignores_case() {
        let s = sync("Node1", Some("n2"), 0, SyncKind::Quorum);
        assert_eq!(
            derive_state(Some(&s), "node1", &obs("ANY 1 (n2)")).phase,
            Phase::Steady
        );
    }

    #[test]
    fn absent_or_empty_sync_is_steady_with_unknown_history() {
        let d = derive_state(None, "n1", &obs(r#"ANY 1 ("n2","n3")"#));
        assert_eq!(d.phase, Phase::Steady);
        assert!(d.state.voters.is_empty());
        assert_eq!(d.state.quorum, 0);
        assert_eq!(d.state.pg_names, set(&["n2", "n3"]));
        assert_eq!(d.state.pg_n, 1);
        assert!(!d.state.si1_holds(), "unknown history must not look safe");

        let empty = sync("", None, 0, SyncKind::Quorum);
        let d = derive_state(Some(&empty), "n1", &obs(""));
        assert_eq!(d.phase, Phase::Steady);
        assert!(d.state.voters.is_empty());
    }

    #[test]
    fn priority_sync_ignores_the_legacy_quorum_number() {
        let s = sync("n1", Some("n2,n3"), 2, SyncKind::Priority);
        let d = derive_state(Some(&s), "n1", &obs(r#"FIRST 2 ("n2","n3")"#));
        assert_eq!(d.phase, Phase::Steady);
        assert!(d.state.voters.is_empty(), "Priority V is not a Quorum V");
        assert_eq!(d.state.quorum, 0);
        assert_eq!(d.state.pg_names, set(&["n2", "n3"]));
        assert_eq!(d.state.pg_n, 2);
    }

    #[test]
    fn empty_observed_value_is_asynchronous() {
        let s = sync("n1", Some("n2"), 1, SyncKind::Quorum);
        let d = derive_state(Some(&s), "n1", &obs(""));
        assert!(d.state.pg_names.is_empty());
        assert_eq!(d.state.effective_n(), 0);
        assert_eq!(d.state.voters, set(&["n2"]));
    }

    #[test]
    fn strict_placeholder_maps_to_asynchronous_not_to_a_voter() {
        let s = sync("n1", Some("n2"), 1, SyncKind::Quorum);
        let d = derive_state(Some(&s), "n1", &obs(STRICT_NO_ELIGIBLE_PLACEHOLDER));
        assert!(d.state.pg_names.is_empty());
        assert_eq!(d.state.pg_n, 0);
        assert!(
            !d.state
                .pg_names
                .contains("_pg_ha_no_eligible_sync_standby_")
        );
    }

    #[test]
    fn wildcard_maps_to_asynchronous_and_is_flagged_by_the_shape() {
        let s = sync("n1", Some("n2"), 1, SyncKind::Quorum);
        for v in ["*", "ANY 1 (*)", "FIRST 2 (n2,*)"] {
            let o = obs(v);
            assert!(o.has_wildcard());
            let d = derive_state(Some(&s), "n1", &o);
            assert!(d.state.pg_names.is_empty(), "{v}");
            assert_eq!(d.state.pg_n, 0, "{v}");
        }
    }

    #[test]
    fn blocking_n_is_kept_raw() {
        let s = sync("n1", Some("n2,n3"), 0, SyncKind::Quorum);
        let d = derive_state(Some(&s), "n1", &obs(r#"ANY 3 ("n2","n3")"#));
        assert_eq!(d.state.pg_n, 3);
    }

    #[test]
    fn names_are_folded() {
        let s = sync("n1", Some("N2"), 0, SyncKind::Quorum);
        let d = derive_state(Some(&s), "n1", &obs(r#"ANY 1 ("N2")"#));
        assert_eq!(d.state.voters, set(&["n2"]));
        assert_eq!(d.state.pg_names, set(&["n2"]));
    }

    // ── phase ──

    /// A Quorum `/sync` written by the previous leader `old`, V = {n2, n3, n4}
    /// (n2 is `me` below), q = 1.
    fn old_leader_sync() -> SyncState {
        sync("old", Some("n2,n3,n4"), 1, SyncKind::Quorum)
    }

    #[test]
    fn promotion_window_matches_post_promotion_state() {
        let s = old_leader_sync();
        // V' = {n3, n4, old}, n' = |V| - q = 2
        let installed = obs(r#"ANY 2 ("n3","n4","old")"#);
        let d = derive_state(Some(&s), "n2", &installed);
        assert_eq!(
            d.phase,
            Phase::PromotionWindow {
                expected_pg_matches: true
            }
        );
        // the equality really is the real function's output
        let post = post_promotion_state(&d.state_with_empty_pg(), "n2", "old").unwrap();
        assert_eq!(installed.names_folded(), post.pg_names);
        assert_eq!(installed.n, post.pg_n);
        // the derived state keeps the OLD V and q until finalization
        assert_eq!(d.state.voters, set(&["n2", "n3", "n4"]));
        assert_eq!(d.state.quorum, 1);
    }

    #[test]
    fn promotion_window_spelling_does_not_matter() {
        let s = old_leader_sync();
        for v in [
            r#"  any 2 (N3, "n4", OLD)"#,
            r#"ANY 2 ("old","n4","n3")"#,
            r#"ANY 2 (n3,n4,old,n4)"#,
        ] {
            assert_eq!(
                derive_state(Some(&s), "n2", &obs(v)).phase,
                Phase::PromotionWindow {
                    expected_pg_matches: true
                },
                "{v}"
            );
        }
    }

    #[test]
    fn promotion_window_pg_mismatches() {
        let s = old_leader_sync();
        for v in [
            // never written: still the old standby value
            r#"ANY 1 ("n3","n4")"#,
            // the leader swap forgotten: me kept, old missing
            r#"ANY 2 ("n2","n3","n4")"#,
            // old present, me still present
            r#"ANY 2 ("n2","n3","n4","old")"#,
            // wrong n (q lowered instead of kept)
            r#"ANY 3 ("n3","n4","old")"#,
            r#"ANY 1 ("n3","n4","old")"#,
            // FIRST is not the same acknowledgement rule
            r#"FIRST 2 ("n3","n4","old")"#,
            "2 (n3,n4,old)",
            "",
            STRICT_NO_ELIGIBLE_PLACEHOLDER,
            "*",
        ] {
            assert_eq!(
                derive_state(Some(&s), "n2", &obs(v)).phase,
                Phase::PromotionWindow {
                    expected_pg_matches: false
                },
                "{v}"
            );
        }
    }

    #[test]
    fn other_leader_is_never_steady() {
        let kinds = [SyncKind::Priority, SyncKind::Quorum];
        for kind in kinds {
            for standbys in [None, Some("n2"), Some("n2,n3"), Some("n3")] {
                for q in 0..4 {
                    for v in ["", r#"ANY 1 ("n3")"#, "FIRST 1 (n2)", "*"] {
                        let s = sync("old", standbys, q, kind);
                        let d = derive_state(Some(&s), "n2", &obs(v));
                        assert_ne!(d.phase, Phase::Steady, "{kind:?} {standbys:?} {q} {v}");
                    }
                }
            }
        }
    }

    #[test]
    fn priority_sync_of_another_leader_is_not_mine() {
        let s = sync("old", Some("n2,n3"), 2, SyncKind::Priority);
        let d = derive_state(Some(&s), "n2", &obs(r#"ANY 1 ("n3","old")"#));
        assert_eq!(d.phase, Phase::NotMine);
        assert!(d.state.voters.is_empty());
    }

    #[test]
    fn quorum_sync_without_a_post_promotion_state_is_not_mine() {
        // me is not a voter
        let s = sync("old", Some("n3,n4"), 1, SyncKind::Quorum);
        assert_eq!(derive_state(Some(&s), "n2", &obs("")).phase, Phase::NotMine);
        // q >= |V|: no election could have been granted
        let s = sync("old", Some("n2,n3"), 2, SyncKind::Quorum);
        assert_eq!(derive_state(Some(&s), "n2", &obs("")).phase, Phase::NotMine);
    }

    #[test]
    fn promotion_window_state_is_judged_excluding_me() {
        // After the pre-promote write, before finalization:
        // V = {n2,n3,n4} (old), q = 1, S = {n3,n4,old}, n = 2.
        let s = old_leader_sync();
        let d = derive_state(Some(&s), "n2", &obs(r#"ANY 2 ("n3","n4","old")"#));
        assert!(d.state.si1_holds_excluding("n2"));
    }

    // ── properties ──

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        /// For every promotable Quorum state, the value the pre-promote write
        /// renders from the real `post_promotion_state` is recognised as the
        /// window's expected value, and nothing else with a different n or
        /// different names is.
        #[test]
        fn window_equality_is_post_promotion_state(
            voters in proptest::collection::btree_set("[a-e]", 1..5),
            q in 0usize..5,
            pick in 0usize..5,
            extra_n in 0usize..3,
        ) {
            let vlist: Vec<String> = voters.iter().cloned().collect();
            let me = vlist[pick % vlist.len()].clone();
            let csv = vlist.join(",");
            let s = sync("old", Some(&csv), q as u32, SyncKind::Quorum);
            let base = QuorumState::new(&vlist, q, Vec::<String>::new(), 0);
            let want = post_promotion_state(&base, &me, "old");
            match want {
                Some(w) => {
                    let installed = render_quorum_value(
                        &crate::quorum::QuorumTarget::new(w.pg_names.iter(), w.pg_n),
                        false,
                    ).unwrap();
                    let d = derive_state(Some(&s), &me, &obs(&installed));
                    prop_assert_eq!(d.phase, Phase::PromotionWindow { expected_pg_matches: true });
                    // a different n is a mismatch
                    if extra_n > 0 && w.pg_n + extra_n <= w.pg_names.len() {
                        let other = render_quorum_value(
                            &crate::quorum::QuorumTarget::new(w.pg_names.iter(), w.pg_n + extra_n),
                            false,
                        ).unwrap();
                        let d = derive_state(Some(&s), &me, &obs(&other));
                        prop_assert_eq!(d.phase, Phase::PromotionWindow { expected_pg_matches: false });
                    }
                }
                None => {
                    let d = derive_state(Some(&s), &me, &obs(""));
                    prop_assert_eq!(d.phase, Phase::NotMine);
                }
            }
        }

        /// Whatever is observed, a `/sync` of another leader is never Steady and
        /// a `/sync` of mine (or none) is never anything else.
        #[test]
        fn phase_follows_the_leader(
            mine in any::<bool>(),
            kind_q in any::<bool>(),
            q in 0u32..4,
            standbys in proptest::collection::btree_set("[a-d]", 0..4),
            v in prop_oneof![
                Just(""), Just("ANY 1 (a,b)"), Just("FIRST 2 (a,b)"), Just("*"),
                Just("ANY 3 (a)"), Just(STRICT_NO_ELIGIBLE_PLACEHOLDER)
            ],
        ) {
            let kind = if kind_q { SyncKind::Quorum } else { SyncKind::Priority };
            let list = standbys.iter().cloned().collect::<Vec<_>>().join(",");
            let leader = if mine { "ME" } else { "other" };
            let s = sync(leader, if list.is_empty() { None } else { Some(&list) }, q, kind);
            let d = derive_state(Some(&s), "me", &obs(v));
            if mine {
                prop_assert_eq!(d.phase, Phase::Steady);
            } else {
                prop_assert_ne!(d.phase, Phase::Steady);
            }
            if kind == SyncKind::Priority {
                prop_assert!(d.state.voters.is_empty());
                prop_assert_eq!(d.state.quorum, 0);
            }
        }
    }

    impl Derived {
        /// Test helper: the derived DCS side with an empty PG side.
        fn state_with_empty_pg(&self) -> QuorumState {
            QuorumState {
                pg_names: Names::new(),
                pg_n: 0,
                ..self.state.clone()
            }
        }
    }
}
