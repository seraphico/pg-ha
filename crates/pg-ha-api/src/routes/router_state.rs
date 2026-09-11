//! Shared types for the router: state, auth config, command sender, query params.

use serde::Deserialize;
use tokio::sync::mpsc;

use crate::state::AppState;
use pg_ha_core::commands::{CommandResponse, ManagementCommand};

/// Sender for management commands to the HA loop
pub type CommandSender = mpsc::Sender<(ManagementCommand, mpsc::Sender<CommandResponse>)>;

/// Auth configuration for the router
#[derive(Clone, Debug)]
pub struct AuthConfig {
    pub username: Option<String>,
    pub password: Option<String>,
}

impl AuthConfig {
    /// Returns true if authentication is enabled (both username and password are set)
    pub fn is_enabled(&self) -> bool {
        self.username.is_some() && self.password.is_some()
    }
}

/// Combined state for the router
#[derive(Clone)]
pub(crate) struct RouterState {
    pub app: AppState,
    pub cmd_tx: Option<CommandSender>,
    pub auth: AuthConfig,
}

/// Query parameters for /replica endpoint
#[derive(Debug, Deserialize, Default)]
pub(crate) struct ReplicaQuery {
    pub lag: Option<u64>,
}
