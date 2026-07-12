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

mod size;

pub type GetDatabase = axum::extract::Extension<crate::database::data::StoredDatabase>;

pub async fn auth(
    instance: GetInstance,
    Path(parts): Path<Vec<String>>,
    mut req: Request,
    next: Next,
) -> Result<Response<Body>, StatusCode> {
    let uuid = match parts.get(1).map(|s| s.parse::<uuid::Uuid>()) {
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

    let database = match instance.get_database(uuid).await {
        Ok(Some(database)) => database,
        Ok(None) => {
            return Ok(ApiResponse::error("database not found")
                .with_status(StatusCode::NOT_FOUND)
                .into_response());
        }
        Err(err) => {
            tracing::error!("failed to fetch database {uuid}: {err}");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    req.extensions_mut().insert(database);

    Ok(next.run(req).await)
}

mod get {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::{ApiError, api::instances::_instance_::databases::_database_::GetDatabase},
    };
    use serde::Serialize;
    use utoipa::ToSchema;

    #[derive(ToSchema, Serialize)]
    struct Response {
        database: crate::database::data::StoredDatabase,
    }

    #[utoipa::path(get, path = "/", responses(
        (status = OK, body = inline(Response)),
        (status = NOT_FOUND, body = ApiError),
    ), params(
        ("instance" = uuid::Uuid, description = "The instance uuid"),
        ("database" = uuid::Uuid, description = "The database uuid"),
    ))]
    pub async fn route(database: GetDatabase) -> ApiResponseResult {
        ApiResponse::new_serialized(Response {
            database: database.0,
        })
        .ok()
    }
}

mod delete {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::{
            ApiError,
            api::instances::_instance_::{GetInstance, databases::_database_::GetDatabase},
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
        ("database" = uuid::Uuid, description = "The database uuid"),
    ))]
    pub async fn route(instance: GetInstance, database: GetDatabase) -> ApiResponseResult {
        instance.delete_database(&database).await?;

        ApiResponse::new_serialized(Response {}).ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .nest("/size", size::router(state))
        .routes(routes!(get::route))
        .routes(routes!(delete::route))
        .route_layer(axum::middleware::from_fn(auth))
        .with_state(state.clone())
}
