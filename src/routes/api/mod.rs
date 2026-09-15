use super::{GetState, State};
use crate::response::ApiResponse;
use axum::{
    body::Body,
    extract::Request,
    http::{Response, StatusCode},
    middleware::Next,
    response::IntoResponse,
};
use utoipa_axum::router::OpenApiRouter;

mod health;
mod instances;
mod status;
mod system;

enum AuthResult {
    Ok,
    InvalidHeader,
    InvalidToken,
}

fn check_auth(state: &State, headers: &axum::http::HeaderMap) -> AuthResult {
    let key = headers
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let (r#type, token) = match key.split_once(' ') {
        Some((t, tok)) => (t, tok),
        None => return AuthResult::InvalidHeader,
    };

    if r#type != "Bearer" {
        return AuthResult::InvalidHeader;
    }

    let config = state.config.load();
    if config.api.token.is_empty()
        || !constant_time_eq::constant_time_eq(token.as_bytes(), config.api.token.as_bytes())
    {
        return AuthResult::InvalidToken;
    }

    AuthResult::Ok
}

/// Whether the request carries a valid api token, for routes that are reachable without one but
/// reveal more when it is present.
pub fn is_authenticated(state: &State, headers: &axum::http::HeaderMap) -> bool {
    matches!(check_auth(state, headers), AuthResult::Ok)
}

pub async fn auth(state: GetState, req: Request, next: Next) -> Result<Response<Body>, StatusCode> {
    let error = match check_auth(&state, req.headers()) {
        AuthResult::Ok => return Ok(next.run(req).await),
        AuthResult::InvalidHeader => "invalid authorization header",
        AuthResult::InvalidToken => "invalid authorization token",
    };

    Ok(ApiResponse::error(error)
        .with_status(StatusCode::UNAUTHORIZED)
        .with_header("WWW-Authenticate", "Bearer")
        .into_response())
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .nest("/health", health::router(state))
        .nest(
            "/instances",
            instances::router(state)
                .route_layer(axum::middleware::from_fn_with_state(state.clone(), auth)),
        )
        .nest(
            "/status",
            status::router(state)
                .route_layer(axum::middleware::from_fn_with_state(state.clone(), auth)),
        )
        .nest(
            "/system",
            system::router(state)
                .route_layer(axum::middleware::from_fn_with_state(state.clone(), auth)),
        )
        .with_state(state.clone())
}
