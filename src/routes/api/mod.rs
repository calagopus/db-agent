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

mod instances;
mod status;
mod system;

pub async fn auth(state: GetState, req: Request, next: Next) -> Result<Response<Body>, StatusCode> {
    let key = req
        .headers()
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let (r#type, token) = match key.split_once(' ') {
        Some((t, tok)) => (t, tok),
        None => {
            return Ok(ApiResponse::error("invalid authorization header")
                .with_status(StatusCode::UNAUTHORIZED)
                .with_header("WWW-Authenticate", "Bearer")
                .into_response());
        }
    };

    if r#type != "Bearer" {
        return Ok(ApiResponse::error("invalid authorization header")
            .with_status(StatusCode::UNAUTHORIZED)
            .with_header("WWW-Authenticate", "Bearer")
            .into_response());
    }

    let expected = state.config.load().api.token.clone();
    if expected.is_empty()
        || !constant_time_eq::constant_time_eq(token.as_bytes(), expected.as_bytes())
    {
        return Ok(ApiResponse::error("invalid authorization token")
            .with_status(StatusCode::UNAUTHORIZED)
            .with_header("WWW-Authenticate", "Bearer")
            .into_response());
    }

    Ok(next.run(req).await)
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
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
