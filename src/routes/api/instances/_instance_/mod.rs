use super::State;
use crate::{response::ApiResponse, routes::GetState};
use axum::{
    body::Body,
    extract::{Path, Request},
    http::{Response, StatusCode},
    middleware::Next,
    response::IntoResponse,
};
use utoipa_axum::{router::OpenApiRouter, routes};

pub mod databases;
mod export;
mod import;
mod logs;
mod power;
mod query;
pub mod users;
mod utilization;

pub type GetInstance = axum::extract::Extension<crate::instance::Instance>;

pub async fn auth(
    state: GetState,
    Path(parts): Path<Vec<String>>,
    mut req: Request,
    next: Next,
) -> Result<Response<Body>, StatusCode> {
    let uuid = match parts.first().map(|s| s.parse::<uuid::Uuid>()) {
        Some(Ok(uuid)) => uuid,
        Some(Err(_)) => {
            return Ok(ApiResponse::error("invalid database uuid")
                .with_status(StatusCode::BAD_REQUEST)
                .into_response());
        }
        None => {
            return Ok(ApiResponse::error("missing database uuid")
                .with_status(StatusCode::BAD_REQUEST)
                .into_response());
        }
    };

    let instance = match state.instance_manager.get_instance(uuid).await {
        Some(instance) => instance,
        None => {
            return Ok(ApiResponse::error("instance not found")
                .with_status(StatusCode::NOT_FOUND)
                .into_response());
        }
    };

    req.extensions_mut().insert(instance);

    Ok(next.run(req).await)
}

mod get {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::{ApiError, api::instances::_instance_::GetInstance},
    };
    use serde::Serialize;
    use utoipa::ToSchema;

    #[derive(ToSchema, Serialize)]
    struct Response {
        instance: crate::instance::ApiInstance,
    }

    #[utoipa::path(get, path = "/", responses(
        (status = OK, body = inline(Response)),
        (status = NOT_FOUND, body = ApiError),
    ), params(
        ("instance" = uuid::Uuid, description = "The instance uuid"),
    ))]
    pub async fn route(instance: GetInstance) -> ApiResponseResult {
        ApiResponse::new_serialized(Response {
            instance: instance.to_api_response().await,
        })
        .ok()
    }
}

mod patch {
    use crate::{
        database::data::StoredInstanceUpdate,
        response::{ApiResponse, ApiResponseResult},
        routes::{ApiError, api::instances::_instance_::GetInstance},
    };
    use serde::Serialize;
    use utoipa::ToSchema;

    #[derive(ToSchema, Serialize)]
    struct Response {}

    #[utoipa::path(patch, path = "/", responses(
        (status = OK, body = inline(Response)),
        (status = NOT_FOUND, body = ApiError),
    ), params(
        ("instance" = uuid::Uuid, description = "The instance uuid"),
    ), request_body = inline(StoredInstanceUpdate))]
    pub async fn route(
        instance: GetInstance,
        crate::Payload(update): crate::Payload<StoredInstanceUpdate>,
    ) -> ApiResponseResult {
        let newly_suspended = {
            let mut data = instance.data.write().await;
            let was_suspended = data.suspended;
            update
                .apply(&instance.app_state.database, &mut data)
                .await?;
            !was_suspended && data.suspended
        };

        instance.sync_container_resources().await?;

        if newly_suspended && let Err(err) = instance.stop().await {
            tracing::error!(
                "failed to stop instance {} after being suspended: {err}",
                instance.uuid
            );
        }

        ApiResponse::new_serialized(Response {}).ok()
    }
}

mod delete {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::{ApiError, GetState, api::instances::_instance_::GetInstance},
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
    ))]
    pub async fn route(state: GetState, instance: GetInstance) -> ApiResponseResult {
        state.instance_manager.delete_instance(&instance).await?;

        ApiResponse::new_serialized(Response {}).ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .nest("/export", export::router(state))
        .nest("/import", import::router(state))
        .nest("/logs", logs::router(state))
        .nest("/power", power::router(state))
        .nest("/query", query::router(state))
        .nest("/utilization", utilization::router(state))
        .nest("/databases", databases::router(state))
        .nest("/users", users::router(state))
        .routes(routes!(get::route))
        .routes(routes!(patch::route))
        .routes(routes!(delete::route))
        .route_layer(axum::middleware::from_fn_with_state(state.clone(), auth))
        .with_state(state.clone())
}
