//! Configuration handlers for the router.

use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use serde_json::json;

use super::router_state::RouterState;
use pg_ha_core::dynamic_config::{GlobalConfig, patch_config};

// ─────────────────── Config Endpoints ───────────────────

/// GET /config — Read dynamic configuration from DCS
pub(crate) async fn get_config(State(state): State<RouterState>) -> impl IntoResponse {
    let dcs = match state.app.dcs() {
        Some(dcs) => dcs.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "DCS not available"})),
            )
                .into_response();
        }
    };

    match dcs.get_config_value().await {
        Ok(Some(value)) => {
            match serde_json::from_str::<GlobalConfig>(&value) {
                Ok(config) => {
                    let json_val = serde_json::to_value(&config).unwrap_or(json!({}));
                    (StatusCode::OK, Json(json_val)).into_response()
                }
                Err(_) => {
                    // Return raw value if it doesn't parse as GlobalConfig
                    match serde_json::from_str::<serde_json::Value>(&value) {
                        Ok(v) => (StatusCode::OK, Json(v)).into_response(),
                        Err(e) => (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(json!({"error": format!("Invalid config in DCS: {e}")})),
                        )
                            .into_response(),
                    }
                }
            }
        }
        Ok(None) => (StatusCode::OK, Json(json!({}))).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("Failed to read config from DCS: {e}")})),
        )
            .into_response(),
    }
}

/// PUT /config — Full replacement of dynamic configuration
pub(crate) async fn put_config(
    State(state): State<RouterState>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let dcs = match state.app.dcs() {
        Some(dcs) => dcs.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "DCS not available"})),
            )
                .into_response();
        }
    };

    // Validate that the body can be parsed as a GlobalConfig
    let config: GlobalConfig = match serde_json::from_value(body.clone()) {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("Invalid config: {e}")})),
            )
                .into_response();
        }
    };

    // Serialize and write to DCS
    let value = serde_json::to_string(&config).unwrap_or_default();
    match dcs.set_config_value(&value).await {
        Ok(true) => (
            StatusCode::OK,
            Json(serde_json::to_value(&config).unwrap_or(json!({}))),
        )
            .into_response(),
        Ok(false) => (
            StatusCode::CONFLICT,
            Json(json!({"error": "Failed to write config to DCS"})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("DCS write failed: {e}")})),
        )
            .into_response(),
    }
}

/// PATCH /config — Partial update of dynamic configuration.
/// Keys set to null are removed from the configuration.
pub(crate) async fn patch_config_endpoint(
    State(state): State<RouterState>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let dcs = match state.app.dcs() {
        Some(dcs) => dcs.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "DCS not available"})),
            )
                .into_response();
        }
    };

    // Read current config from DCS
    let current_config: GlobalConfig = match dcs.get_config_value().await {
        Ok(Some(value)) => serde_json::from_str(&value).unwrap_or_default(),
        Ok(None) => GlobalConfig::default(),
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("Failed to read current config: {e}")})),
            )
                .into_response();
        }
    };

    // Apply patch
    let patched = match patch_config(&current_config, &body) {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("Invalid patch: {e}")})),
            )
                .into_response();
        }
    };

    // Write back to DCS
    let value = serde_json::to_string(&patched).unwrap_or_default();
    match dcs.set_config_value(&value).await {
        Ok(true) => (
            StatusCode::OK,
            Json(serde_json::to_value(&patched).unwrap_or(json!({}))),
        )
            .into_response(),
        Ok(false) => (
            StatusCode::CONFLICT,
            Json(json!({"error": "Failed to write config to DCS"})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("DCS write failed: {e}")})),
        )
            .into_response(),
    }
}
