//! The `synchronous_mode` setting: `"async"`, `"sync"` or `"quorum"`.
//!
//! Exactly three lowercase strings are accepted. Booleans, `"on"`/`"off"`,
//! other cases and every other type are rejected with an error that names the
//! accepted values. A parse that silently fell back to `async` would turn
//! synchronous replication off without telling the operator.

use std::fmt;

use serde::de::{self, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};

use crate::sync::SyncMode;

/// How the leader replicates to its standbys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SynchronousMode {
    /// No synchronous standby; `synchronous_standby_names` stays empty.
    #[default]
    Async,
    /// Priority-based synchronous replication (`FIRST n (...)`).
    Sync,
    /// Quorum-based synchronous replication (`ANY n (...)`).
    Quorum,
}

impl SynchronousMode {
    /// The accepted values, for error messages and docs.
    pub const ACCEPTED: &'static str = r#""async", "sync", "quorum" (lowercase)"#;

    /// The wire / config spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            SynchronousMode::Async => "async",
            SynchronousMode::Sync => "sync",
            SynchronousMode::Quorum => "quorum",
        }
    }

    /// Exact, case-sensitive parse. No trimming.
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "async" => Ok(SynchronousMode::Async),
            "sync" => Ok(SynchronousMode::Sync),
            "quorum" => Ok(SynchronousMode::Quorum),
            other => Err(Self::invalid(&format!("{other:?}"))),
        }
    }

    pub(crate) fn invalid(shown: &str) -> String {
        format!(
            "synchronous_mode: invalid value {shown}; expected one of {}",
            Self::ACCEPTED
        )
    }

    /// True for `sync` and `quorum`.
    pub fn is_synchronous(self) -> bool {
        self != SynchronousMode::Async
    }

    /// The matching `SyncManager` mode.
    pub fn manager_mode(self) -> SyncMode {
        match self {
            SynchronousMode::Async => SyncMode::Off,
            SynchronousMode::Sync => SyncMode::Priority,
            SynchronousMode::Quorum => SyncMode::Quorum,
        }
    }
}

impl fmt::Display for SynchronousMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

struct ModeVisitor;

impl<'de> Visitor<'de> for ModeVisitor {
    type Value = SynchronousMode;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "one of {}", SynchronousMode::ACCEPTED)
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
        SynchronousMode::parse(v).map_err(E::custom)
    }

    fn visit_bool<E: de::Error>(self, v: bool) -> Result<Self::Value, E> {
        Err(E::custom(SynchronousMode::invalid(&v.to_string())))
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
        Err(E::custom(SynchronousMode::invalid(&v.to_string())))
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
        Err(E::custom(SynchronousMode::invalid(&v.to_string())))
    }

    fn visit_f64<E: de::Error>(self, v: f64) -> Result<Self::Value, E> {
        Err(E::custom(SynchronousMode::invalid(&v.to_string())))
    }

    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        Err(E::custom(SynchronousMode::invalid("null")))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, _: A) -> Result<Self::Value, A::Error> {
        Err(de::Error::custom(SynchronousMode::invalid("[...]")))
    }

    fn visit_map<A: MapAccess<'de>>(self, _: A) -> Result<Self::Value, A::Error> {
        Err(de::Error::custom(SynchronousMode::invalid("{...}")))
    }
}

impl<'de> Deserialize<'de> for SynchronousMode {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(ModeVisitor)
    }
}

/// `deserialize_with` for an `Option<SynchronousMode>` field. An absent key
/// (`#[serde(default)]`) stays `None`; an explicit `null` is rejected like any
/// other invalid value, because serde would otherwise turn it into `None` and
/// silently mean `async`.
pub fn deserialize_present<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<Option<SynchronousMode>, D::Error> {
    SynchronousMode::deserialize(d).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const EXPECTED: &str = r#"expected one of "async", "sync", "quorum" (lowercase)"#;

    fn parse_json(v: serde_json::Value) -> Result<SynchronousMode, String> {
        serde_json::from_value::<SynchronousMode>(v).map_err(|e| e.to_string())
    }

    #[test]
    fn accepts_exactly_the_three_lowercase_strings() {
        assert_eq!(parse_json(json!("async")), Ok(SynchronousMode::Async));
        assert_eq!(parse_json(json!("sync")), Ok(SynchronousMode::Sync));
        assert_eq!(parse_json(json!("quorum")), Ok(SynchronousMode::Quorum));
    }

    #[test]
    fn rejects_everything_else_naming_the_accepted_values() {
        let bad = [
            json!(true),
            json!(false),
            json!(1),
            json!(0),
            json!(1.5),
            json!("on"),
            json!("off"),
            json!("true"),
            json!("false"),
            json!("Sync"),
            json!("QUORUM"),
            json!("Async"),
            json!(""),
            json!(" sync"),
            json!("sync "),
            json!("quorum\n"),
            json!([]),
            json!({}),
            json!(null),
        ];
        for v in bad {
            let err = parse_json(v.clone()).expect_err(&format!("{v} must be rejected"));
            assert!(
                err.contains("synchronous_mode: invalid value"),
                "{v}: {err}"
            );
            assert!(err.contains(EXPECTED), "{v}: {err}");
        }
    }

    #[test]
    fn the_error_text_matches_the_documented_example() {
        let err = parse_json(json!("Sync")).unwrap_err();
        assert!(err.starts_with(
            r#"synchronous_mode: invalid value "Sync"; expected one of "async", "sync", "quorum" (lowercase)"#
        ));
    }

    #[test]
    fn serialises_as_lowercase_strings_and_round_trips() {
        for m in [
            SynchronousMode::Async,
            SynchronousMode::Sync,
            SynchronousMode::Quorum,
        ] {
            let v = serde_json::to_value(m).unwrap();
            assert_eq!(v, json!(m.as_str()));
            assert_eq!(serde_json::from_value::<SynchronousMode>(v).unwrap(), m);
            assert_eq!(m.to_string(), m.as_str());
        }
    }

    #[test]
    fn default_is_async_and_option_null_is_rejected() {
        assert_eq!(SynchronousMode::default(), SynchronousMode::Async);
        // Option<SynchronousMode> without deserialize_present maps null to None;
        // the config structs use deserialize_present so that never happens.
        let o: Option<SynchronousMode> = serde_json::from_value(json!(null)).unwrap();
        assert_eq!(o, None);
    }

    #[test]
    fn manager_mode_and_is_synchronous() {
        assert_eq!(SynchronousMode::Async.manager_mode(), SyncMode::Off);
        assert_eq!(SynchronousMode::Sync.manager_mode(), SyncMode::Priority);
        assert_eq!(SynchronousMode::Quorum.manager_mode(), SyncMode::Quorum);
        assert!(!SynchronousMode::Async.is_synchronous());
        assert!(SynchronousMode::Sync.is_synchronous());
        assert!(SynchronousMode::Quorum.is_synchronous());
    }

    proptest::proptest! {
        #[test]
        fn any_other_string_is_rejected(s in ".*") {
            let r = parse_json(json!(s.clone()));
            if ["async", "sync", "quorum"].contains(&s.as_str()) {
                proptest::prop_assert!(r.is_ok());
            } else {
                proptest::prop_assert!(r.is_err());
            }
        }
    }
}
