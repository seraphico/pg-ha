//! Callbacks and event hooks
//!
//! Executes user-configured scripts on HA events (on_start, on_stop,
//! on_role_change, etc.) asynchronously so they don't block the HA loop.

use std::time::Duration;
use tokio::process::Command;
use tracing::{debug, error, info};

/// HA events that can trigger callbacks
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallbackEvent {
    OnStart,
    OnStop,
    OnRestart,
    OnRoleChange,
    OnReload,
}

impl CallbackEvent {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::OnStart => "on_start",
            Self::OnStop => "on_stop",
            Self::OnRestart => "on_restart",
            Self::OnRoleChange => "on_role_change",
            Self::OnReload => "on_reload",
        }
    }
}

/// Configuration for callbacks
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct CallbacksConfig {
    #[serde(default)]
    pub on_start: Option<String>,
    #[serde(default)]
    pub on_stop: Option<String>,
    #[serde(default)]
    pub on_restart: Option<String>,
    #[serde(default)]
    pub on_role_change: Option<String>,
    #[serde(default)]
    pub on_reload: Option<String>,
}

/// Executes callbacks asynchronously
pub struct CallbackExecutor {
    config: CallbacksConfig,
    scope: String,
    timeout: Duration,
}

impl CallbackExecutor {
    pub fn new(config: CallbacksConfig, scope: String) -> Self {
        Self {
            config,
            scope,
            timeout: Duration::from_secs(30),
        }
    }

    /// Fire a callback for the given event. Non-blocking — spawns a task.
    pub fn fire(&self, event: CallbackEvent, action: &str, role: &str) {
        let cmd = match event {
            CallbackEvent::OnStart => self.config.on_start.clone(),
            CallbackEvent::OnStop => self.config.on_stop.clone(),
            CallbackEvent::OnRestart => self.config.on_restart.clone(),
            CallbackEvent::OnRoleChange => self.config.on_role_change.clone(),
            CallbackEvent::OnReload => self.config.on_reload.clone(),
        };

        let Some(command) = cmd else {
            return; // No callback configured for this event
        };

        let action = action.to_string();
        let role = role.to_string();
        let scope = self.scope.clone();
        let timeout = self.timeout;
        let event_name = event.as_str().to_string();

        tokio::spawn(async move {
            info!(event = %event_name, command = %command, "Executing callback");
            let result = tokio::time::timeout(
                timeout,
                Command::new("sh")
                    .args(["-c", &command])
                    .env("PG_HA_ACTION", &action)
                    .env("PG_HA_ROLE", &role)
                    .env("PG_HA_SCOPE", &scope)
                    .env("PG_HA_EVENT", &event_name)
                    .output(),
            )
            .await;

            match result {
                Ok(Ok(output)) => {
                    if output.status.success() {
                        debug!(event = %event_name, "Callback completed successfully");
                    } else {
                        let stderr = String::from_utf8_lossy(&output.stderr);
                        error!(
                            event = %event_name,
                            exit_code = ?output.status.code(),
                            %stderr,
                            "Callback failed"
                        );
                    }
                }
                Ok(Err(e)) => {
                    error!(event = %event_name, error = %e, "Callback execution error");
                }
                Err(_) => {
                    error!(event = %event_name, "Callback timed out after {:?}", timeout);
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_as_str() {
        assert_eq!(CallbackEvent::OnStart.as_str(), "on_start");
        assert_eq!(CallbackEvent::OnRoleChange.as_str(), "on_role_change");
    }

    #[test]
    fn test_no_callback_configured() {
        let executor = CallbackExecutor::new(CallbacksConfig::default(), "test".into());
        // Should not panic when no callback is configured
        executor.fire(CallbackEvent::OnStart, "start", "primary");
    }

    #[tokio::test]
    async fn test_callback_executes_successfully() {
        let config = CallbacksConfig {
            on_start: Some("true".into()), // `true` command always succeeds
            ..Default::default()
        };
        let executor = CallbackExecutor::new(config, "test-scope".into());
        executor.fire(CallbackEvent::OnStart, "start", "primary");
        // Give the spawned task time to complete
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn test_callback_failure_does_not_panic() {
        let config = CallbacksConfig {
            on_stop: Some("false".into()), // `false` command always fails
            ..Default::default()
        };
        let executor = CallbackExecutor::new(config, "test-scope".into());
        executor.fire(CallbackEvent::OnStop, "stop", "replica");
        tokio::time::sleep(Duration::from_millis(100)).await;
        // Should not panic — errors are logged but don't affect HA
    }
}
