//! Basic Auth middleware for protected management endpoints.

use axum::{
    Json,
    extract::{Request, State},
    http::{StatusCode, header},
    middleware::Next,
    response::IntoResponse,
};
use serde_json::json;

use super::router_state::RouterState;

/// Basic Auth middleware — checks credentials on protected management endpoints.
/// If auth is not configured, all requests pass through.
pub(crate) async fn basic_auth_middleware(
    State(state): State<RouterState>,
    request: Request,
    next: Next,
) -> impl IntoResponse {
    if !state.auth.is_enabled() {
        // Auth not configured — pass through
        return next.run(request).await.into_response();
    }

    let expected_user = state.auth.username.as_deref().unwrap_or("");
    let expected_pass = state.auth.password.as_deref().unwrap_or("");

    // Extract Authorization header
    let auth_header = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());

    let authorized = if let Some(auth_value) = auth_header {
        if let Some(encoded) = auth_value.strip_prefix("Basic ") {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD
                .decode(encoded.trim())
                .ok()
                .and_then(|bytes| String::from_utf8(bytes).ok())
                .and_then(|decoded| {
                    decoded
                        .split_once(':')
                        .map(|(u, p)| u == expected_user && p == expected_pass)
                })
                .unwrap_or(false)
        } else if let Some(token) = auth_value.strip_prefix("Bearer ") {
            use base64::Engine;
            let expected_token = base64::engine::general_purpose::STANDARD
                .encode(format!("{expected_user}:{expected_pass}"));
            token.trim() == expected_token
        } else {
            false
        }
    } else {
        false
    };

    if authorized {
        next.run(request).await.into_response()
    } else {
        (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Basic realm=\"pg-ha\"")],
            Json(json!({"error": "Unauthorized"})),
        )
            .into_response()
    }
}
