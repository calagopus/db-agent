use super::State;
use crate::{response::ApiResponse, routes::api::instances::_instance_::GetInstance};
use axum::{
    body::Body,
    extract::{Path, Request},
    http::{Response, StatusCode},
    middleware::Next,
    response::IntoResponse,
};
use utoipa_axum::{router::OpenApiRouter, routes};

mod rotate_password;

pub type GetUser = axum::extract::Extension<crate::database::data::StoredUser>;

pub async fn auth(
    instance: GetInstance,
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

    let user = match instance.get_user(uuid).await {
        Ok(Some(user)) => user,
        Ok(None) => {
            return Ok(ApiResponse::error("user not found")
                .with_status(StatusCode::NOT_FOUND)
                .into_response());
        }
        Err(err) => {
            tracing::error!("failed to fetch user {uuid}: {err}");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    req.extensions_mut().insert(user);

    Ok(next.run(req).await)
}

mod get {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::{ApiError, api::instances::_instance_::users::_user_::GetUser},
    };
    use serde::Serialize;
    use utoipa::ToSchema;

    #[derive(ToSchema, Serialize)]
    struct Response {
        user: crate::database::data::StoredUser,
    }

    #[utoipa::path(get, path = "/", responses(
        (status = OK, body = inline(Response)),
        (status = NOT_FOUND, body = ApiError),
    ), params(
        ("instance" = uuid::Uuid, description = "The instance uuid"),
        ("user" = uuid::Uuid, description = "The user uuid"),
    ))]
    pub async fn route(user: GetUser) -> ApiResponseResult {
        ApiResponse::new_serialized(Response { user: user.0 }).ok()
    }
}

mod delete {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::{
            ApiError,
            api::instances::_instance_::{GetInstance, users::_user_::GetUser},
        },
    };
    use serde::Serialize;
    use utoipa::ToSchema;

    #[derive(ToSchema, Serialize)]
    struct Response {}

    #[utoipa::path(delete, path = "/", responses(
        (status = OK, body = inline(Response)),
        (status = NOT_FOUND, body = ApiError),
    ), params(
        ("instance" = uuid::Uuid, description = "The instance uuid"),
        ("user" = uuid::Uuid, description = "The user uuid"),
    ))]
    pub async fn route(instance: GetInstance, user: GetUser) -> ApiResponseResult {
        instance.delete_user(&user).await?;

        ApiResponse::new_serialized(Response {}).ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .nest("/rotate-password", rotate_password::router(state))
        .routes(routes!(get::route))
        .routes(routes!(delete::route))
        .route_layer(axum::middleware::from_fn(auth))
        .with_state(state.clone())
}
