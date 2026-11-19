//! Seam for reading this node's live WAL position on demand.
//!
//! Used by the REST `GET /wal` endpoint (answering a candidate's vote
//! request) and by the election for the candidate's own reading.
use std::time::Duration;

use async_trait::async_trait;

use crate::error::Result;
use crate::postgresql::WalStatus;

/// Upper bound for one live read (connect and queries together). Shorter than
/// the 1800 ms endpoint timeout and the 2 s per-peer request timeout.
pub const LIVE_WAL_READ_TIMEOUT: Duration = Duration::from_millis(1500);

/// Reads the local PostgreSQL's current WAL position and timeline.
// async_trait boxes the future, which clippy flags as double_must_use.
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait LiveWalProbe: Send + Sync {
    async fn probe(&self) -> Result<WalStatus>;
}

/// SB-40: what a voter does when a candidate asks for its vote in quorum mode.
///
/// It must stop streaming from its upstream, confirm that the stop took
/// effect, and only then read its WAL position. An error means "no vote".
// async_trait boxes the future, which clippy flags as double_must_use.
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait VoteFencer: Send + Sync {
    async fn fence_and_read(&self) -> Result<WalStatus>;
}
