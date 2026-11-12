//! The SI-4 "mark" and the strict `caught_up` test (SB-13, PR3a). Pure.
//!
//! The mark is the primary's flush LSN taken when `synchronous_standby_names`
//! was last OBSERVED (by `SHOW`, not at the time of `ALTER SYSTEM`) to have a
//! given shape. Every commit acknowledged under an older shape is at or below
//! it, so a voter whose flush LSN reached the mark holds all of them. See the
//! module doc of `crate::quorum` for how the planner uses `caught_up`.
//!
//! * A later mark is always safe (standbys then have to reach further), an
//!   earlier one is not. So a mark is replaced when the shape changed and kept
//!   while it did not.
//! * `caught_up` is STRICT (`flush_lsn >= mark.lsn`): unlike Priority's
//!   `sync_standby_caught_up` there is no `maximum_lag_on_syncnode` slack,
//!   because the slack would let `q` drop while a voter is still behind the
//!   mark (maintainer decision 4).
//!
//! What this module does NOT know (3b must supply it): whether the walsenders
//! have noticed the new list. The probe saw `pg_stat_replication.sync_state`
//! lag `SHOW` by an unknown short time after a change; the helpers take no
//! `sync_state` input and `ReplicaRow` has none.

use crate::postgresql::ReplicationSnapshot;
use crate::quorum::Names;
use crate::sync_value::ShapeKey;

/// The primary's flush LSN when `shape_key` was first observed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mark {
    pub shape_key: ShapeKey,
    pub lsn: u64,
}

/// Keep `prev` while the observed shape is unchanged, otherwise take a new
/// mark at `primary_flush_lsn` (also when there is no mark yet).
///
/// The caller must have observed the shape by `SHOW` (not merely written it)
/// and must pass the primary's flush LSN read after that observation.
pub fn observe_mark(
    prev: Option<Mark>,
    observed_shape_key: &ShapeKey,
    primary_flush_lsn: u64,
) -> Mark {
    match prev {
        Some(mark) if mark.shape_key == *observed_shape_key => mark,
        _ => Mark {
            shape_key: observed_shape_key.clone(),
            lsn: primary_flush_lsn,
        },
    }
}

/// The voters (lowercased) whose flushed LSN has reached the mark.
///
/// * strict: `flush_lsn >= mark.lsn` (equal counts, one byte short does not);
/// * `application_name` is matched case-insensitively (ASCII);
/// * a voter with no row in the snapshot is not caught up;
/// * if several rows carry the same name, ALL of them must have reached the
///   mark (a lagging duplicate connection delays a relaxation, it never
///   allows one that is unsafe).
pub fn caught_up(voters: &Names, snapshot: &ReplicationSnapshot, mark: &Mark) -> Names {
    voters
        .iter()
        .filter(|voter| {
            let mut rows = snapshot
                .replicas
                .iter()
                .filter(|r| r.application_name.eq_ignore_ascii_case(voter))
                .peekable();
            rows.peek().is_some() && rows.all(|r| r.flush_lsn >= mark.lsn)
        })
        .map(|v| v.to_ascii_lowercase())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgresql::ReplicaRow;
    use crate::sync_value::parse_sync_value;
    use proptest::prelude::*;

    fn key(v: &str) -> ShapeKey {
        parse_sync_value(v).unwrap().key()
    }

    fn snap(rows: &[(&str, u64)]) -> ReplicationSnapshot {
        ReplicationSnapshot {
            primary_flush_lsn: 1_000_000,
            replicas: rows
                .iter()
                .map(|(n, l)| ReplicaRow {
                    application_name: n.to_string(),
                    flush_lsn: *l,
                })
                .collect(),
        }
    }

    fn names(v: &[&str]) -> Names {
        v.iter().map(|s| s.to_ascii_lowercase()).collect()
    }

    fn mark(lsn: u64) -> Mark {
        Mark {
            shape_key: key("ANY 1 (a,b)"),
            lsn,
        }
    }

    // ── observe_mark ──

    #[test]
    fn first_observation_takes_a_mark() {
        let k = key("ANY 1 (a,b)");
        let m = observe_mark(None, &k, 500);
        assert_eq!(
            m,
            Mark {
                shape_key: k,
                lsn: 500
            }
        );
    }

    #[test]
    fn unchanged_shape_keeps_the_old_mark_even_when_the_primary_moved() {
        let k = key("ANY 1 (a,b)");
        let m0 = observe_mark(None, &k, 500);
        let m1 = observe_mark(Some(m0.clone()), &k, 9_000);
        assert_eq!(m1, m0);
        assert_eq!(m1.lsn, 500);
        // same shape spelled differently is the same shape
        let m2 = observe_mark(Some(m0.clone()), &key("any 1 (\"B\" , a)"), 9_000);
        assert_eq!(m2, m0);
    }

    #[test]
    fn changed_shape_replaces_the_mark_with_a_later_one() {
        let m0 = observe_mark(None, &key("ANY 1 (a,b)"), 500);
        for other in [
            "ANY 2 (a,b)",
            "ANY 1 (a,b,c)",
            "ANY 1 (a)",
            "FIRST 1 (a,b)",
            "",
            r#"FIRST 1 ("_pg_ha_no_eligible_sync_standby_")"#,
        ] {
            let k = key(other);
            let m = observe_mark(Some(m0.clone()), &k, 9_000);
            assert_eq!(
                m,
                Mark {
                    shape_key: k,
                    lsn: 9_000
                },
                "{other:?}"
            );
        }
    }

    #[test]
    fn shape_change_and_back_takes_a_fresh_mark() {
        let a = key("ANY 1 (a,b)");
        let b = key("ANY 2 (a,b)");
        let m = observe_mark(None, &a, 100);
        let m = observe_mark(Some(m), &b, 200);
        let m = observe_mark(Some(m), &a, 300);
        assert_eq!(m.lsn, 300);
    }

    // ── caught_up ──

    #[test]
    fn caught_up_is_strict_greater_or_equal() {
        let s = snap(&[("a", 99), ("b", 100), ("c", 101)]);
        let got = caught_up(&names(&["a", "b", "c"]), &s, &mark(100));
        assert_eq!(got, names(&["b", "c"]));
    }

    #[test]
    fn caught_up_has_no_slack() {
        // 1 byte behind is behind; a Priority-style slack would accept it.
        let s = snap(&[("a", 1_000_000 - 1)]);
        assert!(caught_up(&names(&["a"]), &s, &mark(1_000_000)).is_empty());
        let s = snap(&[("a", 1_000_000)]);
        assert_eq!(
            caught_up(&names(&["a"]), &s, &mark(1_000_000)),
            names(&["a"])
        );
    }

    #[test]
    fn voter_without_a_row_is_not_caught_up() {
        let s = snap(&[("a", 500)]);
        assert_eq!(
            caught_up(&names(&["a", "b"]), &s, &mark(100)),
            names(&["a"])
        );
        assert!(caught_up(&names(&["b"]), &snap(&[]), &mark(0)).is_empty());
    }

    #[test]
    fn rows_of_non_voters_are_ignored() {
        let s = snap(&[("x", 10_000), ("a", 5)]);
        assert!(caught_up(&names(&["a"]), &s, &mark(100)).is_empty());
    }

    #[test]
    fn application_name_matches_case_insensitively() {
        let s = snap(&[("NoDe1", 500), ("NODE2", 5)]);
        let got = caught_up(&names(&["node1", "node2"]), &s, &mark(100));
        assert_eq!(got, names(&["node1"]));
        // the voter side may arrive in any case; the result is lowercase
        let mut v = Names::new();
        v.insert("NODE1".to_string());
        assert_eq!(caught_up(&v, &s, &mark(100)), names(&["node1"]));
    }

    #[test]
    fn duplicate_rows_must_all_have_reached_the_mark() {
        let s = snap(&[("a", 500), ("A", 50)]);
        assert!(caught_up(&names(&["a"]), &s, &mark(100)).is_empty());
        let s = snap(&[("a", 500), ("A", 150)]);
        assert_eq!(caught_up(&names(&["a"]), &s, &mark(100)), names(&["a"]));
    }

    #[test]
    fn mark_zero_counts_every_voter_with_a_row() {
        let s = snap(&[("a", 0)]);
        assert_eq!(caught_up(&names(&["a", "b"]), &s, &mark(0)), names(&["a"]));
    }

    // ── properties ──

    fn voter_names() -> impl Strategy<Value = Vec<String>> {
        proptest::collection::vec("[a-d]{1,2}", 0..6)
    }

    fn rows() -> impl Strategy<Value = Vec<(String, u64)>> {
        proptest::collection::vec(("[a-dA-D]{1,2}", 0u64..2000), 0..10)
    }

    fn build(rows: &[(String, u64)]) -> ReplicationSnapshot {
        ReplicationSnapshot {
            primary_flush_lsn: 0,
            replicas: rows
                .iter()
                .map(|(n, l)| ReplicaRow {
                    application_name: n.clone(),
                    flush_lsn: *l,
                })
                .collect(),
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        /// Oracle written independently of the implementation.
        #[test]
        fn caught_up_matches_the_definition(
            voters in voter_names(),
            rows in rows(),
            lsn in 0u64..2000,
        ) {
            let voters: Names = voters.into_iter().collect();
            let snapshot = build(&rows);
            let got = caught_up(&voters, &snapshot, &mark(lsn));
            for v in &voters {
                let theirs: Vec<u64> = rows
                    .iter()
                    .filter(|(n, _)| n.to_ascii_lowercase() == *v)
                    .map(|(_, l)| *l)
                    .collect();
                let expected = !theirs.is_empty() && theirs.iter().all(|l| *l >= lsn);
                prop_assert_eq!(got.contains(v), expected, "voter {} rows {:?} mark {}", v, theirs, lsn);
            }
            prop_assert!(got.is_subset(&voters));
        }

        /// A higher mark never adds a voter.
        #[test]
        fn caught_up_is_monotone_in_the_mark(
            voters in voter_names(),
            rows in rows(),
            lo in 0u64..2000,
            bump in 0u64..2000,
        ) {
            let voters: Names = voters.into_iter().collect();
            let snapshot = build(&rows);
            let low = caught_up(&voters, &snapshot, &mark(lo));
            let high = caught_up(&voters, &snapshot, &mark(lo + bump));
            prop_assert!(high.is_subset(&low));
        }

        /// Voters without any row are never caught up, whatever the mark.
        #[test]
        fn voters_without_rows_are_ignored(
            voters in voter_names(),
            rows in rows(),
            lsn in 0u64..2000,
        ) {
            let voters: Names = voters.into_iter().collect();
            let snapshot = build(&rows);
            let got = caught_up(&voters, &snapshot, &mark(lsn));
            for v in &got {
                prop_assert!(rows.iter().any(|(n, _)| n.eq_ignore_ascii_case(v)));
            }
        }

        /// The mark is kept while the shape is unchanged and replaced when it
        /// changes; it never moves backwards for the same shape.
        #[test]
        fn observe_mark_keeps_until_the_shape_changes(
            seq in proptest::collection::vec((0usize..3, 0u64..10_000), 1..30),
        ) {
            let keys = [key("ANY 1 (a,b)"), key("ANY 2 (a,b)"), key("")];
            let mut cur: Option<Mark> = None;
            for (ki, lsn) in seq {
                let k = &keys[ki];
                let prev = cur.clone();
                let next = observe_mark(prev.clone(), k, lsn);
                prop_assert_eq!(&next.shape_key, k);
                match prev {
                    Some(p) if p.shape_key == *k => prop_assert_eq!(next.lsn, p.lsn),
                    _ => prop_assert_eq!(next.lsn, lsn),
                }
                cur = Some(next);
            }
        }
    }
}
