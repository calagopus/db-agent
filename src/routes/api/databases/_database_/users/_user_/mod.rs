use super::State;
use crate::{response::ApiResponse, routes::api::databases::_database_::GetDatabase};
use axum::{
    body::Body,
    extract::{Path, Request},
    http::{Response, StatusCode},
    middleware::Next,
    response::IntoResponse,
};
use utoipa_axum::{router::OpenApiRouter, routes};

mod rotate_password;

pub type GetUser = axum::extract::Extension<crate::database::data::StoredDatabaseUser>;

pub async fn auth(
    database: GetDatabase,
    Path(parts): Path<Vec<String>>,
    mut req: Request,
    next: Next,
) -> Result<Response<Body>, StatusCode> {
    let uuid = match parts.get(1).map(|s| s.parse::<uuid::Uuid>()) {
        Some(Ok(uuid)) => uuid,
        Some(Err(_)) => {
            return Ok(ApiResponse::error("invalid user uuid")
                .with_status(StatusCode::BAD_REQUEST)
                .into_response());
        }
        None => {
            return Ok(ApiResponse::error("missing user uuid")
                .with_status(StatusCode::BAD_REQUEST)
                .into_response());
        }
    };

    let user = match database.get_user(uuid).await {
        Ok(Some(user)) => user,
        Ok(None) => {
            return Ok(ApiResponse::error("user not found")
                .with_status(StatusCode::NOT_FOUND)
                .into_response());
        }
        Err(err) => {
            tracing::error!("failed to fetch database user {uuid}: {err}");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    req.extensions_mut().insert(user);

    Ok(next.run(req).await)
}

mod delete {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::api::databases::_database_::{GetDatabase, users::_user_::GetUser},
    };
    use serde::Serialize;
    use utoipa::ToSchema;

    #[derive(ToSchema, Serialize)]
    struct Response {}

    #[utoipa::path(delete, path = "/", responses(
        (status = OK, body = inline(Response)),
    ), params(
        ("database" = uuid::Uuid, description = "The database uuid"),
        ("user" = uuid::Uuid, description = "The database user uuid"),
    ))]
    pub async fn route(database: GetDatabase, user: GetUser) -> ApiResponseResult {
        database.delete_user(&user).await?;

        ApiResponse::new_serialized(Response {}).ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .nest("/rotate-password", rotate_password::router(state))
        .routes(routes!(delete::route))
        .route_layer(axum::middleware::from_fn(auth))
        .with_state(state.clone())
}
