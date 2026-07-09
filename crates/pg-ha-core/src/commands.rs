//! Management commands sent from REST API to HA loop
//!
//! These are requests from operators (via CLI or API) that the HA engine
//! processes in its next cycle.

use serde::{Deserialize, Serialize};

/// Commands that can be sent to the HA engine via the API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ManagementCommand {
    /// Graceful switchover: demote current leader, promote candidate
    Switchover {
        /// Current leader name (for validation)
        leader: Option<String>,
        /// Target candidate to promote
        candidate: Option<String>,
        /// Schedule for later (ISO 8601)
        scheduled_at: Option<String>,
    },

    /// Emergency failover: promote candidate regardless of current leader
    Failover {
        /// Target candidate to promote
        candidate: Option<String>,
    },

    /// Cancel a scheduled switchover
    CancelSwitchover,

    /// Restart PostgreSQL on this node
    Restart,

    /// Reinitialize this node (wipe data, re-clone)
    Reinitialize,
}

/// Response to a management command
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandResponse {
    pub status: CommandStatus,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CommandStatus {
    Accepted,
    Rejected,
    Error,
}

impl CommandResponse {
    pub fn accepted(msg: impl Into<String>) -> Self {
        Self {
            status: CommandStatus::Accepted,
            message: msg.into(),
        }
    }

    pub fn rejected(msg: impl Into<String>) -> Self {
        Self {
            status: CommandStatus::Rejected,
            message: msg.into(),
        }
    }

    pub fn error(msg: impl Into<String>) -> Self {
        Self {
            status: CommandStatus::Error,
            message: msg.into(),
        }
    }
}
