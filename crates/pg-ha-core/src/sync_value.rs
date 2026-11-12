//! Parsing and rendering of `synchronous_standby_names` values for the
//! quorum executor (SB-13, PR3a). Pure: no I/O, nothing here is wired yet.
//!
//! `SHOW synchronous_standby_names` returns the text exactly as it was
//! written (probe report `.agents/tasks/sb-13-pr3/probe/report.md`, section
//! 4, identical on PostgreSQL 16, 17, 18 and 19rc1): any keyword case, a
//! missing keyword, optional spaces anywhere, quoted or unquoted names,
//! doubled double quotes, leading and trailing whitespace are all kept. So
//! there is no canonical form on the PostgreSQL side. This module provides
//!
//! * [`parse_sync_value`]: read any value PostgreSQL accepts into a
//!   [`SyncValueShape`];
//! * [`classify_sync_value`]: what the executor should believe about it;
//! * [`render_quorum_value`]: the one canonical text pg-ha itself writes.
//!
//! A value is *ours* only when `trim(value) == render_quorum_value(..)`.
//!
//! # Grammar read (PostgreSQL `syncrep_gram.y`)
//!
//! ```text
//! value  := ""                                  asynchronous
//!         | list                                bare list  => FIRST 1 (list)
//!         | NUM "(" list ")"                    bare       => FIRST NUM (list)
//!         | (ANY | FIRST) NUM "(" list ")"
//! list   := name ("," name)*
//! name   := unquoted identifier | NUM | "*" | "quoted ""identifier"""
//! ```
//!
//! Keywords are matched case-insensitively and are never names unless quoted.
//! An unquoted name is `[A-Za-z_\x80-]` followed by letters, digits, `_`,
//! `$` or non-ASCII characters (`node-1` is a syntax error, as in PostgreSQL).
//! Whitespace is space, tab, newline, carriage return and form feed, as in the
//! PostgreSQL scanner (not Unicode whitespace). Anything else is a
//! [`ParseError`]: the executor must then leave the value alone (fail closed).
//!
//! # Name matching
//!
//! PostgreSQL matches `application_name` against the listed names
//! case-insensitively (probe P6). Names are kept as written in
//! [`SyncValueShape::names`] and compared through
//! [`SyncValueShape::names_folded`] (ASCII lowercase, like
//! `crate::quorum::Names`).
//!
//! # Facts the executor must not forget (probe P7)
//!
//! PostgreSQL ACCEPTS `ANY 3 ("s1","s2")` silently and then blocks every
//! commit, so [`SyncValueClass::Blocking`] exists. A wildcard `*` (also the
//! quoted `"*"`, which PostgreSQL treats the same) lets ANY connected standby
//! count, so the listed names are not the set of possible acknowledgers: such
//! a value never counts as blocking on its length and the quorum safety
//! argument (SI-1, `crate::quorum`) cannot be applied to it.

use std::fmt;

use crate::quorum::{Names, QuorumTarget};
use crate::sync::{STRICT_NO_ELIGIBLE_PLACEHOLDER, quote_ident};

/// The keyword of a value as written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SyncKeyword {
    /// `FIRST n (..)` (any case).
    First,
    /// `ANY n (..)` (any case).
    Any,
    /// No keyword: `n (..)`, `a,b` or `*`. Means the same as `FIRST` (priority).
    Bare,
}

/// A parsed `synchronous_standby_names` value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncValueShape {
    pub keyword: SyncKeyword,
    /// The number: as written, `1` for a bare list without a number, `0` for
    /// the empty (asynchronous) value. May be `0` or larger than the number of
    /// names when the value was written behind PostgreSQL's back or accepted
    /// by it silently; see [`SyncValueClass::Blocking`].
    pub n: usize,
    /// Names as written (unquoted, doubled quotes undone), in order, with
    /// duplicates.
    pub names: Vec<String>,
    /// The value is (equivalent to) the strict "nobody eligible" placeholder
    /// [`STRICT_NO_ELIGIBLE_PLACEHOLDER`]: it blocks every commit on purpose and
    /// must not be rewritten as the empty (asynchronous) value.
    pub strict_hold: bool,
}

/// What a shape contributes to deciding "has anything changed since the mark".
/// `FIRST` and the bare form are the same semantics and share a key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShapeKey {
    /// `true` for `ANY`, `false` for `FIRST`, bare and empty.
    pub quorum: bool,
    pub n: usize,
    /// Case-folded, deduplicated names.
    pub names: Names,
}

/// What the executor should believe about an observed value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncValueClass {
    /// Exactly what [`render_quorum_value`] emits for some non-empty target
    /// (after trimming): `ANY n ("a","b")`, lowercase names, sorted, distinct.
    Ours,
    /// Parses and does not block by itself, but is not our canonical text
    /// (other case, spacing, `FIRST`, bare, wildcard, unsorted, duplicates).
    ForeignValid,
    /// Parses, but no commit can ever be acknowledged: `n` larger than the
    /// number of distinct names (no wildcard), `n = 0` with names, or the
    /// strict placeholder.
    Blocking,
    /// The empty string: asynchronous, no standby.
    Empty,
    /// Not a value this module can read.
    Unparseable,
}

/// Why a value could not be parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    UnexpectedChar(char),
    UnterminatedQuote,
    EmptyName,
    NumberTooLarge,
    Syntax(&'static str),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::UnexpectedChar(c) => write!(f, "unexpected character {c:?}"),
            ParseError::UnterminatedQuote => write!(f, "unterminated quoted name"),
            ParseError::EmptyName => write!(f, "empty quoted name"),
            ParseError::NumberTooLarge => write!(f, "number too large"),
            ParseError::Syntax(what) => write!(f, "syntax error: {what}"),
        }
    }
}

impl std::error::Error for ParseError {}

/// Why a target cannot be rendered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RenderError {
    /// Voters exist but `n < 1`.
    NTooSmall { n: usize },
    /// `n` is larger than the number of distinct names: PostgreSQL would accept
    /// it and then block every commit (probe P7).
    NTooLarge { n: usize, names: usize },
    /// No voters but `n != 0`.
    NWithoutVoters { n: usize },
    /// A voter name is empty.
    EmptyName,
    /// A voter is named `*`: PostgreSQL reads it as "any standby", also when
    /// quoted.
    WildcardName,
}

impl fmt::Display for RenderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RenderError::NTooSmall { n } => write!(f, "refusing to render n = {n} (< 1)"),
            RenderError::NTooLarge { n, names } => write!(
                f,
                "refusing to render n = {n} for {names} name(s): every commit would block"
            ),
            RenderError::NWithoutVoters { n } => {
                write!(f, "refusing to render n = {n} with no voters")
            }
            RenderError::EmptyName => write!(f, "refusing to render an empty name"),
            RenderError::WildcardName => {
                write!(
                    f,
                    "refusing to render a name `*` (PostgreSQL reads it as a wildcard)"
                )
            }
        }
    }
}

impl std::error::Error for RenderError {}

impl SyncValueShape {
    /// Names compared the way PostgreSQL compares them: ASCII case-insensitive.
    pub fn names_folded(&self) -> Names {
        self.names.iter().map(|n| n.to_ascii_lowercase()).collect()
    }

    /// Does the value contain the wildcard `*` (quoted or not)?
    pub fn has_wildcard(&self) -> bool {
        self.names.iter().any(|n| n == "*")
    }

    /// Key for [`crate::quorum::mark::observe_mark`].
    pub fn key(&self) -> ShapeKey {
        ShapeKey {
            quorum: self.keyword == SyncKeyword::Any,
            n: self.n,
            names: self.names_folded(),
        }
    }

    /// The shape as a quorum target (names folded, `n` as written).
    pub fn to_target(&self) -> QuorumTarget {
        QuorumTarget::new(&self.names, self.n)
    }

    /// No names: asynchronous.
    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    /// Parsed but no commit can be acknowledged under it (see
    /// [`SyncValueClass::Blocking`]). `false` for the empty value.
    pub fn is_blocking(&self) -> bool {
        if self.strict_hold {
            return true;
        }
        if self.names.is_empty() {
            return false;
        }
        if self.n == 0 {
            return true;
        }
        !self.has_wildcard() && self.n > self.names_folded().len()
    }
}

/// Parse a value as `SHOW synchronous_standby_names` returns it.
pub fn parse_sync_value(raw: &str) -> Result<SyncValueShape, ParseError> {
    let toks = tokenize(raw)?;
    if toks.is_empty() {
        return Ok(SyncValueShape {
            keyword: SyncKeyword::Bare,
            n: 0,
            names: Vec::new(),
            strict_hold: false,
        });
    }

    let (keyword, n, parenthesised, mut i) = match &toks[0] {
        Tok::Word(w) if w.eq_ignore_ascii_case("any") || w.eq_ignore_ascii_case("first") => {
            let keyword = if w.eq_ignore_ascii_case("any") {
                SyncKeyword::Any
            } else {
                SyncKeyword::First
            };
            let Some(Tok::Num(digits)) = toks.get(1) else {
                return Err(ParseError::Syntax("expected a number after ANY/FIRST"));
            };
            (keyword, parse_number(digits)?, true, 2)
        }
        Tok::Num(digits) if toks.get(1) == Some(&Tok::LParen) => {
            (SyncKeyword::Bare, parse_number(digits)?, true, 1)
        }
        _ => (SyncKeyword::Bare, 1, false, 0),
    };

    if parenthesised {
        if toks.get(i) != Some(&Tok::LParen) {
            return Err(ParseError::Syntax("expected ("));
        }
        i += 1;
    }
    let mut names = Vec::new();
    loop {
        let Some(tok) = toks.get(i) else {
            return Err(ParseError::Syntax("expected a name"));
        };
        names.push(match tok {
            Tok::Word(w) if is_keyword(w) => {
                return Err(ParseError::Syntax("keyword used as a name"));
            }
            Tok::Word(w) => w.clone(),
            Tok::Num(d) => d.clone(),
            Tok::Quoted(q) => q.clone(),
            Tok::Star => "*".to_string(),
            _ => return Err(ParseError::Syntax("expected a name")),
        });
        i += 1;
        if toks.get(i) == Some(&Tok::Comma) {
            i += 1;
        } else {
            break;
        }
    }
    if parenthesised {
        if toks.get(i) != Some(&Tok::RParen) {
            return Err(ParseError::Syntax("expected )"));
        }
        i += 1;
    }
    if i != toks.len() {
        return Err(ParseError::Syntax("unexpected trailing input"));
    }

    let strict_hold = keyword != SyncKeyword::Any && n == 1 && names.len() == 1 && {
        let folded = names[0].to_ascii_lowercase();
        format!("FIRST 1 ({})", quote_ident(&folded)) == STRICT_NO_ELIGIBLE_PLACEHOLDER
    };
    Ok(SyncValueShape {
        keyword,
        n,
        names,
        strict_hold,
    })
}

/// Classify a raw observed value. See [`SyncValueClass`].
pub fn classify_sync_value(raw: &str) -> SyncValueClass {
    let Ok(shape) = parse_sync_value(raw) else {
        return SyncValueClass::Unparseable;
    };
    if shape.is_empty() {
        return SyncValueClass::Empty;
    }
    if shape.is_blocking() {
        return SyncValueClass::Blocking;
    }
    if is_ours(raw, &shape.to_target()) {
        return SyncValueClass::Ours;
    }
    SyncValueClass::ForeignValid
}

/// Is `raw`, after trimming, exactly what [`render_quorum_value`] emits for
/// `target` (non-strict)? This is the only definition of "ours".
pub fn is_ours(raw: &str, target: &QuorumTarget) -> bool {
    match render_quorum_value(target, false) {
        Ok(rendered) => !rendered.is_empty() && trim_pg(raw) == rendered,
        Err(_) => false,
    }
}

/// Trim PostgreSQL whitespace (not Unicode whitespace) from both ends.
pub fn trim_pg(s: &str) -> &str {
    s.trim_matches(is_pg_space)
}

/// The canonical text pg-ha writes for a quorum target.
///
/// * voters non-empty: `ANY n ("a","b")`: names lowercased, distinct, sorted
///   by name (the order `SyncManager::select_sync_standbys` uses in Quorum
///   mode), each double-quoted with embedded double quotes doubled, comma
///   separated without spaces. `n` must satisfy `1 <= n <= |names|`, else
///   `Err` (PostgreSQL would accept `n > |names|` and block every commit).
/// * no voters, `strict`: [`STRICT_NO_ELIGIBLE_PLACEHOLDER`] (writes block).
/// * no voters, not `strict`: the empty string (asynchronous).
///
/// A target with no voters and `n != 0` is inconsistent and refused, as are
/// an empty name and the name `*` (a wildcard to PostgreSQL).
pub fn render_quorum_value(target: &QuorumTarget, strict: bool) -> Result<String, RenderError> {
    // `QuorumTarget` fields are public: fold again so the text is canonical
    // whatever the caller put there.
    let names: Names = target
        .voters
        .iter()
        .map(|n| n.to_ascii_lowercase())
        .collect();
    if names.is_empty() {
        if target.n != 0 {
            return Err(RenderError::NWithoutVoters { n: target.n });
        }
        return Ok(if strict {
            STRICT_NO_ELIGIBLE_PLACEHOLDER.to_string()
        } else {
            String::new()
        });
    }
    if names.iter().any(|n| n.is_empty()) {
        return Err(RenderError::EmptyName);
    }
    if names.contains("*") {
        return Err(RenderError::WildcardName);
    }
    if target.n < 1 {
        return Err(RenderError::NTooSmall { n: target.n });
    }
    if target.n > names.len() {
        return Err(RenderError::NTooLarge {
            n: target.n,
            names: names.len(),
        });
    }
    let list = names
        .iter()
        .map(|n| quote_ident(n))
        .collect::<Vec<_>>()
        .join(",");
    Ok(format!("ANY {} ({list})", target.n))
}

// ───────────────────────────── tokenizer ─────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
enum Tok {
    Word(String),
    Num(String),
    Quoted(String),
    Star,
    LParen,
    RParen,
    Comma,
}

fn is_pg_space(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\r' | '\x0c')
}

fn is_ident_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_' || !c.is_ascii()
}

fn is_ident_cont(c: char) -> bool {
    is_ident_start(c) || c.is_ascii_digit() || c == '$'
}

fn is_keyword(w: &str) -> bool {
    w.eq_ignore_ascii_case("any") || w.eq_ignore_ascii_case("first")
}

fn parse_number(digits: &str) -> Result<usize, ParseError> {
    digits.parse().map_err(|_| ParseError::NumberTooLarge)
}

fn tokenize(s: &str) -> Result<Vec<Tok>, ParseError> {
    let mut out = Vec::new();
    let mut it = s.chars().peekable();
    while let Some(&c) = it.peek() {
        if is_pg_space(c) {
            it.next();
        } else if c == '(' {
            it.next();
            out.push(Tok::LParen);
        } else if c == ')' {
            it.next();
            out.push(Tok::RParen);
        } else if c == ',' {
            it.next();
            out.push(Tok::Comma);
        } else if c == '*' {
            it.next();
            out.push(Tok::Star);
        } else if c == '"' {
            it.next();
            let mut name = String::new();
            loop {
                match it.next() {
                    None => return Err(ParseError::UnterminatedQuote),
                    Some('"') => {
                        if it.peek() == Some(&'"') {
                            it.next();
                            name.push('"');
                        } else {
                            break;
                        }
                    }
                    Some(ch) => name.push(ch),
                }
            }
            if name.is_empty() {
                return Err(ParseError::EmptyName);
            }
            out.push(Tok::Quoted(name));
        } else if c.is_ascii_digit() {
            let mut d = String::new();
            while let Some(&ch) = it.peek() {
                if ch.is_ascii_digit() {
                    d.push(ch);
                    it.next();
                } else {
                    break;
                }
            }
            out.push(Tok::Num(d));
        } else if is_ident_start(c) {
            let mut w = String::new();
            while let Some(&ch) = it.peek() {
                if is_ident_cont(ch) {
                    w.push(ch);
                    it.next();
                } else {
                    break;
                }
            }
            out.push(Tok::Word(w));
        } else {
            return Err(ParseError::UnexpectedChar(c));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::{SyncManager, SyncMode, SyncSelection};
    use proptest::prelude::*;

    fn target(names: &[&str], n: usize) -> QuorumTarget {
        QuorumTarget::new(names.iter(), n)
    }

    fn shape(keyword: SyncKeyword, n: usize, names: &[&str]) -> SyncValueShape {
        SyncValueShape {
            keyword,
            n,
            names: names.iter().map(|s| s.to_string()).collect(),
            strict_hold: false,
        }
    }

    /// Every row of probe report section 4 (the exact `SHOW` text) with what
    /// the parser and the classifier must say. Rows are `(shown, keyword, n,
    /// names, class)`.
    #[allow(clippy::type_complexity)]
    fn probe_rows() -> Vec<(
        &'static str,
        SyncKeyword,
        usize,
        Vec<&'static str>,
        SyncValueClass,
    )> {
        use SyncKeyword::*;
        use SyncValueClass::*;
        vec![
            (r#"ANY 1 ("s1","s2")"#, Any, 1, vec!["s1", "s2"], Ours),
            ("any 1 (s1, s2)", Any, 1, vec!["s1", "s2"], ForeignValid),
            (
                r#"FIRST 1 ("s1","s2")"#,
                First,
                1,
                vec!["s1", "s2"],
                ForeignValid,
            ),
            ("1 (s1,s2)", Bare, 1, vec!["s1", "s2"], ForeignValid),
            ("s1,s2", Bare, 1, vec!["s1", "s2"], ForeignValid),
            ("*", Bare, 1, vec!["*"], ForeignValid),
            ("", Bare, 0, vec![], Empty),
            (r#"ANY 1 ("a""b")"#, Any, 1, vec![r#"a"b"#], Ours),
            (r#"ANY 1 ("node-1")"#, Any, 1, vec!["node-1"], Ours),
            (r#"ANY 1 ("Node1")"#, Any, 1, vec!["Node1"], ForeignValid),
            (
                r#"ANY 1 ("node-1","Node1")"#,
                Any,
                1,
                vec!["node-1", "Node1"],
                ForeignValid,
            ),
            ("FIRST 2 (s1,s2)", First, 2, vec!["s1", "s2"], ForeignValid),
            (
                r#"  ANY   1  ( "s1" ,  "s2" )  "#,
                Any,
                1,
                vec!["s1", "s2"],
                ForeignValid,
            ),
            (r#""s1","s2""#, Bare, 1, vec!["s1", "s2"], ForeignValid),
            ("ANY 2 (s1, s2)", Any, 2, vec!["s1", "s2"], ForeignValid),
            ("First 1 (S1)", First, 1, vec!["S1"], ForeignValid),
            (r#"ANY 3 ("s1","s2")"#, Any, 3, vec!["s1", "s2"], Blocking),
            (
                r#"FIRST 1 ("_pg_ha_no_eligible_sync_standby_")"#,
                First,
                1,
                vec!["_pg_ha_no_eligible_sync_standby_"],
                Blocking,
            ),
        ]
    }

    #[test]
    fn probe_table_parses_and_classifies() {
        for (shown, keyword, n, names, class) in probe_rows() {
            let got = parse_sync_value(shown).unwrap_or_else(|e| panic!("{shown:?}: {e}"));
            assert_eq!(got.keyword, keyword, "{shown:?}");
            assert_eq!(got.n, n, "{shown:?}");
            assert_eq!(got.names, names, "{shown:?}");
            assert_eq!(classify_sync_value(shown), class, "{shown:?}");
        }
    }

    #[test]
    fn probe_rejected_and_invalid_values_are_unparseable_or_blocking() {
        // Rejected by PostgreSQL itself (probe P5/P8): we must not read them.
        for bad in [
            "ANY 1 (node-1)",
            "ANY 1 (",
            "ANY 1 ()",
            "ANY -1 (s1)",
            "ANY (s1)",
            "ANY 1 s1",
            "ANY 1 (s1",
            "ANY 1 (s1))",
            "ANY 1 (s1) x",
            "ANY 1 (s1,)",
            "ANY 1 (,s1)",
            "ANY 1 (any)",
            "any",
            "first",
            "1abc",
            r#"ANY 1 ("s1)"#,
            r#"ANY 1 ("")"#,
            "ANY 99999999999999999999999 (s1)",
            "ANY 1 (a b)",
            "ANY 1 (s1;)",
            "ANY 1 ('s1')",
        ] {
            assert!(parse_sync_value(bad).is_err(), "{bad:?}");
            assert_eq!(
                classify_sync_value(bad),
                SyncValueClass::Unparseable,
                "{bad:?}"
            );
        }
        // Injected behind ALTER SYSTEM's back (P8): readable, and n = 0 blocks.
        assert_eq!(classify_sync_value("ANY 0 (s1)"), SyncValueClass::Blocking);
        assert_eq!(
            classify_sync_value("FIRST 0 (s1)"),
            SyncValueClass::Blocking
        );
    }

    #[test]
    fn whitespace_variants() {
        for v in [
            "ANY 1 (a,b)",
            "  ANY 1 (a,b)  ",
            "ANY\t1\n(a ,\r\x0c b)",
            "ANY 1(a,b)",
            "ANY 1 ( a , b )",
            "any 1 (A,B)",
        ] {
            let s = parse_sync_value(v).unwrap_or_else(|e| panic!("{v:?}: {e}"));
            assert_eq!(s.keyword, SyncKeyword::Any, "{v:?}");
            assert_eq!(s.n, 1, "{v:?}");
            assert_eq!(
                s.names_folded(),
                Names::from(["a".into(), "b".into()]),
                "{v:?}"
            );
        }
        // Only PostgreSQL whitespace is whitespace: a non-breaking space is an
        // identifier character, so this is ONE name, not two.
        let nb = parse_sync_value("ANY 1 (a\u{a0}b)").unwrap();
        assert_eq!(nb.names, vec!["a\u{a0}b".to_string()]);
        assert_eq!(trim_pg("\u{a0}x"), "\u{a0}x");
    }

    #[test]
    fn numbers_as_names_and_bare_forms() {
        // A lone number is a standby name, not a count.
        let s = parse_sync_value("1").unwrap();
        assert_eq!(
            (s.keyword, s.n, s.names),
            (SyncKeyword::Bare, 1, vec!["1".into()])
        );
        let s = parse_sync_value("2 (1,2)").unwrap();
        assert_eq!((s.keyword, s.n), (SyncKeyword::Bare, 2));
        assert_eq!(s.names, vec!["1".to_string(), "2".to_string()]);
        let s = parse_sync_value("ANY 1 (3)").unwrap();
        assert_eq!(s.names, vec!["3".to_string()]);
        // Names with `$`, digits and non-ASCII characters are plain names.
        let s = parse_sync_value("a$1,ünï").unwrap();
        assert_eq!(s.names, vec!["a$1".to_string(), "ünï".to_string()]);
    }

    #[test]
    fn keyword_semantics_n_is_read_from_the_number() {
        // The number after the keyword is `n` for both keywords; the keyword
        // is not swapped and `n` is neither shifted nor defaulted.
        let a = parse_sync_value("ANY 2 (a,b,c)").unwrap();
        let f = parse_sync_value("FIRST 3 (a,b,c)").unwrap();
        assert_eq!((a.keyword, a.n), (SyncKeyword::Any, 2));
        assert_eq!((f.keyword, f.n), (SyncKeyword::First, 3));
        assert_eq!(parse_sync_value("a,b,c").unwrap().n, 1);
        assert_eq!(parse_sync_value("3 (a,b,c)").unwrap().n, 3);
    }

    #[test]
    fn quoted_names_keep_content_and_doubled_quotes() {
        let s = parse_sync_value(r#"ANY 1 ("a""b", "c,d", "e (f)", "ANY")"#).unwrap();
        assert_eq!(
            s.names,
            vec![
                "a\"b".to_string(),
                "c,d".to_string(),
                "e (f)".to_string(),
                "ANY".to_string()
            ]
        );
        // `""""` is a name consisting of one double quote.
        let s = parse_sync_value(r#"ANY 1 ("""")"#).unwrap();
        assert_eq!(s.names, vec!["\"".to_string()]);
    }

    #[test]
    fn case_insensitive_names_and_keywords() {
        let a = parse_sync_value(r#"ANY 2 ("S1","S2")"#).unwrap();
        let b = parse_sync_value("any 2 (s1,s2)").unwrap();
        let c = parse_sync_value("aNy 2 (S1, s2)").unwrap();
        assert_eq!(a.key(), b.key());
        assert_eq!(b.key(), c.key());
        assert_ne!(a.names, b.names, "names are kept as written");
        assert_eq!(
            parse_sync_value("First 1 (a)").unwrap().keyword,
            SyncKeyword::First
        );
        assert_eq!(
            parse_sync_value("FIRST 1 (a)").unwrap().keyword,
            SyncKeyword::First
        );
    }

    #[test]
    fn shape_key_distinguishes_semantics_but_not_spelling() {
        let any = parse_sync_value("ANY 1 (a,b)").unwrap().key();
        let first = parse_sync_value("FIRST 1 (a,b)").unwrap().key();
        let bare = parse_sync_value("1 (a,b)").unwrap().key();
        let bare_list = parse_sync_value("a,b").unwrap().key();
        assert_ne!(any, first);
        assert_eq!(first, bare);
        assert_eq!(first, bare_list);
        assert_ne!(any, parse_sync_value("ANY 2 (a,b)").unwrap().key());
        assert_ne!(any, parse_sync_value("ANY 1 (a,c)").unwrap().key());
        assert_eq!(
            any,
            parse_sync_value("ANY 1 (B , \"a\" , b)").unwrap().key()
        );
        assert_eq!(parse_sync_value("").unwrap().key().names.len(), 0);
    }

    // ── placeholder ──

    #[test]
    fn strict_placeholder_is_strict_hold_and_never_empty() {
        let s = parse_sync_value(STRICT_NO_ELIGIBLE_PLACEHOLDER).unwrap();
        assert!(s.strict_hold);
        assert!(!s.is_empty(), "the placeholder is not the empty value");
        assert!(s.is_blocking());
        assert_eq!(
            classify_sync_value(STRICT_NO_ELIGIBLE_PLACEHOLDER),
            SyncValueClass::Blocking
        );
        assert_ne!(
            classify_sync_value(STRICT_NO_ELIGIBLE_PLACEHOLDER),
            SyncValueClass::Empty
        );
        assert_ne!(
            classify_sync_value(STRICT_NO_ELIGIBLE_PLACEHOLDER),
            SyncValueClass::Ours
        );
        // With surrounding whitespace, other case and the bare spelling.
        for v in [
            format!("  {STRICT_NO_ELIGIBLE_PLACEHOLDER}\n"),
            STRICT_NO_ELIGIBLE_PLACEHOLDER.to_lowercase(),
            r#"1 ("_PG_HA_NO_ELIGIBLE_SYNC_STANDBY_")"#.to_string(),
            r#""_pg_ha_no_eligible_sync_standby_""#.to_string(),
        ] {
            assert!(parse_sync_value(&v).unwrap().strict_hold, "{v:?}");
        }
        // Not the placeholder: ANY, a different n, another name, extra names.
        for v in [
            r#"ANY 1 ("_pg_ha_no_eligible_sync_standby_")"#,
            r#"FIRST 2 ("_pg_ha_no_eligible_sync_standby_")"#,
            r#"FIRST 1 ("other")"#,
            r#"FIRST 1 ("_pg_ha_no_eligible_sync_standby_","other")"#,
        ] {
            assert!(!parse_sync_value(v).unwrap().strict_hold, "{v:?}");
        }
    }

    #[test]
    fn placeholder_is_rendered_only_for_strict_with_no_voters() {
        let none = target(&[], 0);
        assert_eq!(
            render_quorum_value(&none, true).unwrap(),
            STRICT_NO_ELIGIBLE_PLACEHOLDER
        );
        assert_eq!(render_quorum_value(&none, false).unwrap(), "");
        // Strict does not change a non-empty list.
        let t = target(&["a"], 1);
        assert_eq!(
            render_quorum_value(&t, true).unwrap(),
            render_quorum_value(&t, false).unwrap()
        );
        // The empty string and the placeholder are different observations.
        assert_ne!(
            classify_sync_value(&render_quorum_value(&none, true).unwrap()),
            classify_sync_value(&render_quorum_value(&none, false).unwrap())
        );
        // The placeholder re-reads as strict_hold, the empty string does not.
        assert!(
            parse_sync_value(&render_quorum_value(&none, true).unwrap())
                .unwrap()
                .strict_hold
        );
        assert!(!parse_sync_value("").unwrap().strict_hold);
    }

    // ── classification ──

    #[test]
    fn blocking_counts_distinct_names_case_insensitively() {
        assert_eq!(classify_sync_value("ANY 2 (a,A)"), SyncValueClass::Blocking);
        assert_eq!(
            classify_sync_value("FIRST 2 (a,a)"),
            SyncValueClass::Blocking
        );
        assert_eq!(
            classify_sync_value("ANY 2 (a,b)"),
            SyncValueClass::ForeignValid
        );
        assert_eq!(
            classify_sync_value("FIRST 3 (a,b)"),
            SyncValueClass::Blocking
        );
        assert_eq!(classify_sync_value("5 (a)"), SyncValueClass::Blocking);
    }

    #[test]
    fn wildcard_is_never_ours_and_never_blocking_by_length() {
        for v in [
            "*",
            r#""*""#,
            "ANY 3 (*)",
            "FIRST 2 (*)",
            r#"ANY 2 ("a",*)"#,
        ] {
            let s = parse_sync_value(v).unwrap();
            assert!(s.has_wildcard(), "{v:?}");
            assert_eq!(
                classify_sync_value(v),
                SyncValueClass::ForeignValid,
                "{v:?}"
            );
        }
        // Even in canonical-looking text the wildcard is not a voter we wrote,
        // and the renderer refuses to write one.
        assert_eq!(
            classify_sync_value(r#"ANY 1 ("*")"#),
            SyncValueClass::ForeignValid
        );
        assert_eq!(
            render_quorum_value(&target(&["*"], 1), false),
            Err(RenderError::WildcardName)
        );
    }

    #[test]
    fn ours_requires_the_exact_canonical_text_after_trimming() {
        let t = target(&["a", "b"], 1);
        let canon = render_quorum_value(&t, false).unwrap();
        assert_eq!(canon, r#"ANY 1 ("a","b")"#);
        assert!(is_ours(&canon, &t));
        assert!(is_ours(&format!(" \t{canon}\n"), &t));
        for not in [
            "ANY 1 (\"a\", \"b\")",
            "any 1 (\"a\",\"b\")",
            "ANY 1 (a,b)",
            "ANY  1 (\"a\",\"b\")",
            "ANY 1 (\"b\",\"a\")",
            "ANY 1 (\"A\",\"B\")",
            "FIRST 1 (\"a\",\"b\")",
            "ANY 2 (\"a\",\"b\")",
            "ANY 1 (\"a\")",
            "",
        ] {
            assert!(!is_ours(not, &t), "{not:?}");
        }
        assert!(!is_ours("", &target(&[], 0)), "empty is Empty, never ours");
        assert!(!is_ours(STRICT_NO_ELIGIBLE_PLACEHOLDER, &target(&[], 0)));
        // A value that is ours for one target is not ours for another.
        assert!(!is_ours(&canon, &target(&["a", "b"], 2)));
        assert!(!is_ours(&canon, &target(&["a", "c"], 1)));
    }

    // ── renderer ──

    #[test]
    fn render_canonical_forms() {
        assert_eq!(
            render_quorum_value(&target(&["r1", "r2", "r3"], 2), false).unwrap(),
            r#"ANY 2 ("r1","r2","r3")"#
        );
        // folded and sorted, whatever the input order and case
        assert_eq!(
            render_quorum_value(&target(&["B", "a"], 1), false).unwrap(),
            r#"ANY 1 ("a","b")"#
        );
        assert_eq!(
            render_quorum_value(&target(&["a", "a", "A"], 1), false).unwrap(),
            r#"ANY 1 ("a")"#
        );
        // direct struct literal with unfolded names is folded too
        let t = QuorumTarget {
            voters: Names::from(["N2".to_string(), "n1".to_string()]),
            n: 2,
        };
        assert_eq!(
            render_quorum_value(&t, false).unwrap(),
            r#"ANY 2 ("n1","n2")"#
        );
    }

    #[test]
    fn render_quotes_every_name_and_doubles_quotes() {
        let t = target(&[r#"a"b"#, "node-1", "x y", "ü", "1", "any", "a'b"], 1);
        let v = render_quorum_value(&t, false).unwrap();
        assert_eq!(v, r#"ANY 1 ("1","a""b","a'b","any","node-1","x y","ü")"#);
        // every name is inside quotes: removing the quoted spans leaves only punctuation
        let s = parse_sync_value(&v).unwrap();
        assert_eq!(s.names_folded(), t.voters);
        let doubled = render_quorum_value(&target(&[r#""""#], 1), false).unwrap();
        assert_eq!(doubled, r#"ANY 1 ("""""")"#);
        assert_eq!(
            parse_sync_value(&doubled).unwrap().names,
            vec![r#""""#.to_string()]
        );
    }

    #[test]
    fn render_refuses_bad_n() {
        assert_eq!(
            render_quorum_value(&target(&["a"], 0), false),
            Err(RenderError::NTooSmall { n: 0 })
        );
        assert_eq!(
            render_quorum_value(&target(&["a", "b"], 3), false),
            Err(RenderError::NTooLarge { n: 3, names: 2 })
        );
        assert_eq!(
            render_quorum_value(&target(&["a", "A"], 2), true),
            Err(RenderError::NTooLarge { n: 2, names: 1 }),
            "distinct names are counted after folding, and strict does not relax it"
        );
        assert_eq!(
            render_quorum_value(&target(&[], 1), true),
            Err(RenderError::NWithoutVoters { n: 1 })
        );
        assert_eq!(
            render_quorum_value(&target(&[""], 1), false),
            Err(RenderError::EmptyName)
        );
        // boundary values are fine
        assert!(render_quorum_value(&target(&["a", "b"], 1), false).is_ok());
        assert!(render_quorum_value(&target(&["a", "b"], 2), false).is_ok());
        for e in [
            RenderError::NTooSmall { n: 0 },
            RenderError::NTooLarge { n: 3, names: 2 },
            RenderError::NWithoutVoters { n: 1 },
            RenderError::EmptyName,
            RenderError::WildcardName,
        ] {
            assert!(!e.to_string().is_empty());
        }
    }

    /// The renderer and the existing Quorum formatter in `sync.rs` agree for
    /// every selection that formatter can produce with lowercase names.
    #[test]
    fn render_matches_the_existing_quorum_formatter() {
        let mgr = SyncManager::new(SyncMode::Quorum, 1, false);
        for (names, count) in [
            (vec!["r1", "r2", "r3"], 2),
            (vec!["a"], 1),
            (vec!["a", "b"], 2),
            (vec!["a\"b", "node-1", "node_2"], 1),
        ] {
            let sel = SyncSelection::Voters {
                names: names.iter().map(|s| s.to_string()).collect(),
                count,
            };
            let old = mgr.format_sync_standby_names(&sel).unwrap();
            let new = render_quorum_value(&QuorumTarget::from_selection(&sel), false).unwrap();
            assert_eq!(old, new);
        }
        // and for the strict / non-strict "nobody" selections
        for strict in [true, false] {
            let sel = SyncSelection::NoEligible { strict };
            let old = SyncManager::new(SyncMode::Quorum, 1, strict)
                .format_sync_standby_names(&sel)
                .unwrap();
            let new = render_quorum_value(&QuorumTarget::from_selection(&sel), strict).unwrap();
            assert_eq!(old, new);
        }
    }

    #[test]
    fn display_of_parse_errors() {
        for e in [
            ParseError::UnexpectedChar('-'),
            ParseError::UnterminatedQuote,
            ParseError::EmptyName,
            ParseError::NumberTooLarge,
            ParseError::Syntax("x"),
        ] {
            assert!(!e.to_string().is_empty());
        }
        assert_eq!(
            parse_sync_value("a-b"),
            Err(ParseError::UnexpectedChar('-'))
        );
    }

    #[test]
    fn to_target_and_helpers() {
        let s = shape(SyncKeyword::Any, 2, &["B", "a"]);
        assert_eq!(s.to_target(), target(&["a", "b"], 2));
        assert!(!s.is_blocking());
        assert!(shape(SyncKeyword::Any, 3, &["a", "b"]).is_blocking());
        assert!(shape(SyncKeyword::Any, 0, &["a"]).is_blocking());
        assert!(!shape(SyncKeyword::Bare, 0, &[]).is_blocking());
    }

    // ── properties ──

    fn name_strategy() -> impl Strategy<Value = String> {
        // lowercase, includes characters that need quoting and a double quote
        "[a-z0-9_\\-\" .$']{1,8}"
    }

    fn target_strategy() -> impl Strategy<Value = QuorumTarget> {
        proptest::collection::btree_set(name_strategy(), 1..7).prop_flat_map(|names| {
            let len = names.len();
            (Just(names), 1..=len).prop_map(|(names, n)| QuorumTarget::new(names.iter(), n))
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        #[test]
        fn parse_never_panics_on_arbitrary_strings(s in any::<String>()) {
            let _ = parse_sync_value(&s);
            let _ = classify_sync_value(&s);
        }

        #[test]
        fn parse_never_panics_on_token_soup(s in "[ A-Za-z0-9_$*\",()\\t\\n.\\-'\u{a0}ü]{0,40}") {
            let _ = parse_sync_value(&s);
            let _ = classify_sync_value(&s);
        }

        #[test]
        fn round_trip_parse_of_render(t in target_strategy()) {
            let text = render_quorum_value(&t, false).unwrap();
            let shape = parse_sync_value(&text).unwrap();
            prop_assert_eq!(shape.keyword, SyncKeyword::Any);
            prop_assert_eq!(&shape.to_target(), &t);
            prop_assert_eq!(shape.n, t.n);
            prop_assert!(!shape.strict_hold);
            prop_assert_eq!(classify_sync_value(&text), SyncValueClass::Ours);
            // render is a fixed point of parse then render
            let again = render_quorum_value(&shape.to_target(), false).unwrap();
            prop_assert_eq!(again, text);
        }

        #[test]
        fn render_never_emits_a_bad_n(
            names in proptest::collection::btree_set(name_strategy(), 0..6),
            n in 0usize..9,
            strict in any::<bool>(),
        ) {
            let t = QuorumTarget::new(names.iter(), n);
            match render_quorum_value(&t, strict) {
                Ok(text) => {
                    if names.is_empty() {
                        prop_assert_eq!(n, 0);
                    } else {
                        prop_assert!(n >= 1 && n <= names.len());
                        let s = parse_sync_value(&text).unwrap();
                        prop_assert!(s.n >= 1 && s.n <= s.names_folded().len());
                        prop_assert!(!s.is_blocking());
                    }
                }
                Err(_) => {
                    prop_assert!(
                        (!names.is_empty() && (n < 1 || n > names.len()))
                            || (names.is_empty() && n != 0)
                    );
                }
            }
        }

        #[test]
        fn case_variants_parse_to_the_same_key(
            t in target_strategy(),
            mask in proptest::collection::vec(any::<bool>(), 0..200),
        ) {
            let text = render_quorum_value(&t, false).unwrap();
            let varied: String = text
                .chars()
                .enumerate()
                .map(|(i, c)| {
                    if mask.get(i).copied().unwrap_or(false) {
                        c.to_ascii_uppercase()
                    } else {
                        c
                    }
                })
                .collect();
            let a = parse_sync_value(&text).unwrap();
            let b = parse_sync_value(&varied).unwrap();
            prop_assert_eq!(a.key(), b.key());
            prop_assert_eq!(a.n, b.n);
            prop_assert_eq!(a.keyword, b.keyword);
            prop_assert_eq!(a.names_folded(), b.names_folded());
        }

        #[test]
        fn whitespace_padding_does_not_change_the_shape(
            t in target_strategy(),
            pad in "[ \\t\\n\\r]{0,4}",
        ) {
            let text = render_quorum_value(&t, false).unwrap();
            let padded = format!("{pad}{text}{pad}");
            prop_assert_eq!(parse_sync_value(&padded).unwrap(), parse_sync_value(&text).unwrap());
            prop_assert!(is_ours(&padded, &t));
        }

        #[test]
        fn blocking_iff_n_exceeds_distinct_names(
            names in proptest::collection::vec("[a-c]", 1..5),
            n in 0usize..7,
            kw in prop_oneof![Just("ANY"), Just("FIRST")],
        ) {
            let list = names.iter().map(|x| format!("\"{x}\"")).collect::<Vec<_>>().join(",");
            let text = format!("{kw} {n} ({list})");
            let distinct = names.iter().collect::<std::collections::BTreeSet<_>>().len();
            let blocking = classify_sync_value(&text) == SyncValueClass::Blocking;
            prop_assert_eq!(blocking, n == 0 || n > distinct);
        }
    }
}
