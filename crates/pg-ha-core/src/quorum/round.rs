//! One bounded round of live WAL reads from the voters (SB-13, PR2).
//!
//! The pure vote rule lives in the parent module. This file only gathers its
//! input: one live reading per reachable voter, with hard time limits so a
//! hung peer (or a hung DNS lookup) can never stall the election cycle.
//!
//! Partial answers are safe: every answer is a valid live reading, and fewer
//! answers can only make [`super::quorum_vote_decision`] refuse more.
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::task::JoinSet;

use super::WalPosition;

/// Longest a single peer request may take.
pub const PEER_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

/// Longest the peer round (all peers together) may take. The whole vote also
/// includes the candidate's own live read, which runs concurrently with this
/// round, so the vote is bounded by `max(LIVE_WAL_READ_TIMEOUT, VOTE_ROUND_TIMEOUT)`.
pub const VOTE_ROUND_TIMEOUT: Duration = Duration::from_secs(3);

/// A voter to ask: its member name and REST base URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerTarget {
    pub name: String,
    pub api_url: String,
}

/// Why a peer gave no vote. Every variant simply means "no vote".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerProbeError {
    /// No answer within the time limit.
    Timeout,
    /// Connection, TLS or protocol failure.
    Transport(String),
    /// The peer answered with this non-200 HTTP status.
    Status(u16),
    /// The body was not a usable live reading.
    BadBody(String),
    /// The answering node is not the one asked (misrouted address).
    Mismatch,
}

impl std::fmt::Display for PeerProbeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timeout => write!(f, "timed out"),
            Self::Transport(e) => write!(f, "transport error: {e}"),
            Self::Status(code) => write!(f, "HTTP status {code}"),
            Self::BadBody(e) => write!(f, "unusable response body: {e}"),
            Self::Mismatch => write!(f, "answered by a different node than asked"),
        }
    }
}

impl std::error::Error for PeerProbeError {}

/// Reads one peer's live WAL position over the network.
// async_trait boxes the future, which clippy flags as double_must_use.
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait PeerWalProbe: Send + Sync {
    async fn read(&self, target: &PeerTarget) -> Result<WalPosition, PeerProbeError>;
}

/// What a round collected.
#[derive(Debug, Default)]
pub struct RoundResult {
    /// Live readings received before the round deadline.
    pub answers: Vec<WalPosition>,
    /// Peers that gave no vote, with the reason.
    pub failures: Vec<(String, PeerProbeError)>,
}

/// Ask every target concurrently. Each request is limited to `per_request`;
/// the whole collection is limited to `round` by ONE timeout around the join
/// loop (the only protection against a future that never yields). Answers that
/// arrived before the deadline are kept; requests still running at the
/// deadline are aborted (the `JoinSet` is dropped) and reported as `Timeout`.
pub async fn collect_wal_positions(
    probe: Arc<dyn PeerWalProbe>,
    targets: Vec<PeerTarget>,
    per_request: Duration,
    round: Duration,
) -> RoundResult {
    let mut result = RoundResult::default();
    if targets.is_empty() {
        return result;
    }

    let mut set: JoinSet<Result<WalPosition, PeerProbeError>> = JoinSet::new();
    let mut pending: HashMap<tokio::task::Id, String> = HashMap::new();
    for target in targets {
        let probe = Arc::clone(&probe);
        let name = target.name.clone();
        let handle = set.spawn(async move {
            match tokio::time::timeout(per_request, probe.read(&target)).await {
                Ok(answer) => answer,
                Err(_) => Err(PeerProbeError::Timeout),
            }
        });
        pending.insert(handle.id(), name);
    }

    let collect = async {
        while let Some(joined) = set.join_next_with_id().await {
            match joined {
                Ok((id, answer)) => {
                    let name = pending.remove(&id).unwrap_or_default();
                    match answer {
                        Ok(position) => result.answers.push(position),
                        Err(err) => result.failures.push((name, err)),
                    }
                }
                Err(join_err) => {
                    let name = pending.remove(&join_err.id()).unwrap_or_default();
                    result.failures.push((
                        name,
                        PeerProbeError::Transport(format!("probe task failed: {join_err}")),
                    ));
                }
            }
        }
    };
    let _ = tokio::time::timeout(round, collect).await;

    // Anything still pending missed the round deadline. Dropping `set` aborts
    // those tasks.
    let mut late: Vec<String> = pending.into_values().collect();
    late.sort();
    for name in late {
        result.failures.push((name, PeerProbeError::Timeout));
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Instant;

    #[derive(Clone)]
    enum Script {
        Immediate,
        DelayThenAnswer(Duration),
        Fail(PeerProbeError),
    }

    struct FakeProbe {
        scripts: HashMap<String, Script>,
    }

    impl FakeProbe {
        fn arc(scripts: &[(&str, Script)]) -> Arc<dyn PeerWalProbe> {
            Arc::new(Self {
                scripts: scripts
                    .iter()
                    .map(|(n, s)| ((*n).to_string(), s.clone()))
                    .collect(),
            })
        }
    }

    fn answer(name: &str) -> WalPosition {
        WalPosition {
            name: name.into(),
            in_recovery: true,
            timeline: 1,
            lsn: 100,
        }
    }

    #[async_trait]
    impl PeerWalProbe for FakeProbe {
        async fn read(&self, target: &PeerTarget) -> Result<WalPosition, PeerProbeError> {
            match self.scripts.get(&target.name).cloned().expect("scripted") {
                Script::Immediate => Ok(answer(&target.name)),
                Script::DelayThenAnswer(d) => {
                    tokio::time::sleep(d).await;
                    Ok(answer(&target.name))
                }
                Script::Fail(e) => Err(e),
            }
        }
    }

    fn targets(names: &[&str]) -> Vec<PeerTarget> {
        names
            .iter()
            .map(|n| PeerTarget {
                name: (*n).into(),
                api_url: format!("http://{n}:8008"),
            })
            .collect()
    }

    fn names(positions: &[WalPosition]) -> Vec<String> {
        let mut v: Vec<String> = positions.iter().map(|p| p.name.clone()).collect();
        v.sort();
        v
    }

    #[tokio::test(start_paused = true)]
    async fn answer_at_1_9s_counts_and_2_5s_is_timeout() {
        let probe = FakeProbe::arc(&[
            ("a", Script::DelayThenAnswer(Duration::from_millis(1900))),
            ("b", Script::DelayThenAnswer(Duration::from_millis(2500))),
            ("c", Script::Immediate),
        ]);
        let r = collect_wal_positions(
            probe,
            targets(&["a", "b", "c"]),
            PEER_REQUEST_TIMEOUT,
            VOTE_ROUND_TIMEOUT,
        )
        .await;
        assert_eq!(names(&r.answers), vec!["a", "c"]);
        assert_eq!(r.failures, vec![("b".to_string(), PeerProbeError::Timeout)]);
    }

    #[tokio::test(start_paused = true)]
    async fn round_never_exceeds_deadline_even_with_larger_request_timeout() {
        let probe = FakeProbe::arc(&[
            ("a", Script::DelayThenAnswer(Duration::from_secs(60))),
            ("b", Script::Immediate),
        ]);
        let start = Instant::now();
        let r = collect_wal_positions(
            probe,
            targets(&["a", "b"]),
            Duration::from_secs(30),
            VOTE_ROUND_TIMEOUT,
        )
        .await;
        assert_eq!(start.elapsed(), VOTE_ROUND_TIMEOUT);
        assert_eq!(names(&r.answers), vec!["b"]);
        assert_eq!(r.failures, vec![("a".to_string(), PeerProbeError::Timeout)]);
    }

    #[tokio::test(start_paused = true)]
    async fn early_answers_kept_when_round_deadline_hits() {
        let probe = FakeProbe::arc(&[
            ("a", Script::DelayThenAnswer(Duration::from_secs(1))),
            ("b", Script::DelayThenAnswer(Duration::from_secs(2))),
            ("c", Script::DelayThenAnswer(Duration::from_secs(5))),
        ]);
        let r = collect_wal_positions(
            probe,
            targets(&["a", "b", "c"]),
            Duration::from_secs(10),
            Duration::from_secs(3),
        )
        .await;
        assert_eq!(names(&r.answers), vec!["a", "b"]);
        assert_eq!(r.failures, vec![("c".to_string(), PeerProbeError::Timeout)]);
    }

    #[tokio::test(start_paused = true)]
    async fn answer_after_round_deadline_is_not_counted() {
        let probe = FakeProbe::arc(&[("a", Script::DelayThenAnswer(Duration::from_millis(3500)))]);
        let r = collect_wal_positions(
            probe,
            targets(&["a"]),
            Duration::from_secs(10),
            VOTE_ROUND_TIMEOUT,
        )
        .await;
        assert!(r.answers.is_empty());
        assert_eq!(r.failures.len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn failures_carry_the_reason() {
        let probe = FakeProbe::arc(&[
            ("a", Script::Fail(PeerProbeError::Status(503))),
            ("b", Script::Fail(PeerProbeError::Mismatch)),
        ]);
        let mut r = collect_wal_positions(
            probe,
            targets(&["a", "b"]),
            PEER_REQUEST_TIMEOUT,
            VOTE_ROUND_TIMEOUT,
        )
        .await;
        r.failures.sort_by(|x, y| x.0.cmp(&y.0));
        assert!(r.answers.is_empty());
        assert_eq!(
            r.failures,
            vec![
                ("a".to_string(), PeerProbeError::Status(503)),
                ("b".to_string(), PeerProbeError::Mismatch),
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn empty_targets_return_immediately() {
        let probe = FakeProbe::arc(&[]);
        let start = Instant::now();
        let r =
            collect_wal_positions(probe, vec![], PEER_REQUEST_TIMEOUT, VOTE_ROUND_TIMEOUT).await;
        assert_eq!(start.elapsed(), Duration::ZERO);
        assert!(r.answers.is_empty() && r.failures.is_empty());
    }

    #[test]
    fn error_display_is_readable() {
        assert_eq!(PeerProbeError::Timeout.to_string(), "timed out");
        assert_eq!(PeerProbeError::Status(503).to_string(), "HTTP status 503");
    }
}
