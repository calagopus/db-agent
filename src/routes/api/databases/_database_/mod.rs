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

mod export;
mod import;
mod logs;
mod power;
mod query;
pub mod users;
mod utilization;

pub type GetDatabase = axum::extract::Extension<crate::subsystems::database::Database>;

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

    let database = match state.database_manager.get_database(uuid).await {
        Some(database) => database,
        None => {
            return Ok(ApiResponse::error("database not found")
                .with_status(StatusCode::NOT_FOUND)
                .into_response());
        }
    };

    req.extensions_mut().insert(database);

    Ok(next.run(req).await)
}

mod get {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::api::databases::_database_::GetDatabase,
    };
    use serde::Serialize;
    use utoipa::ToSchema;

    #[derive(ToSchema, Serialize)]
    struct Response {
        database: crate::subsystems::database::ApiDatabase,
    }

    #[utoipa::path(get, path = "/", responses(
        (status = OK, body = inline(Response)),
    ), params(
        ("database" = uuid::Uuid, description = "The database uuid"),
    ))]
    pub async fn route(database: GetDatabase) -> ApiResponseResult {
        ApiResponse::new_serialized(Response {
            database: database.to_api_response().await,
        })
        .ok()
    }
}

mod patch {
    use crate::{
        database::data::StoredDatabaseUpdate,
        response::{ApiResponse, ApiResponseResult},
        routes::api::databases::_database_::GetDatabase,
    };
    use serde::Serialize;
    use utoipa::ToSchema;

    #[derive(ToSchema, Serialize)]
    struct Response {}

    #[utoipa::path(patch, path = "/", responses(
        (status = OK, body = inline(Response)),
    ), params(
        ("database" = uuid::Uuid, description = "The database uuid"),
    ), request_body = inline(StoredDatabaseUpdate))]
    pub async fn route(
        database: GetDatabase,
        crate::Payload(update): crate::Payload<StoredDatabaseUpdate>,
    ) -> ApiResponseResult {
        let newly_suspended = {
            let mut data = database.data.write().await;
            let was_suspended = data.suspended;
            update
                .apply(&database.app_state.database, &mut data)
                .await?;
            !was_suspended && data.suspended
        };

        database.sync_container_resources().await?;

        if newly_suspended && let Err(err) = database.stop().await {
            tracing::error!(
                "failed to stop database {} after being suspended: {err}",
                database.uuid
            );
        }

        ApiResponse::new_serialized(Response {}).ok()
    }
}

mod delete {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::{GetState, api::databases::_database_::GetDatabase},
    };
    use serde::Serialize;
    use utoipa::ToSchema;

    #[derive(ToSchema, Serialize)]
    struct Response {}

    #[utoipa::path(delete, path = "/", responses(
        (status = OK, body = inline(Response)),
    ), params(
        ("database" = uuid::Uuid, description = "The database uuid"),
    ))]
    pub async fn route(state: GetState, database: GetDatabase) -> ApiResponseResult {
        state.database_manager.delete_database(&database).await?;

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
        .nest("/users", users::router(state))
        .routes(routes!(get::route))
        .routes(routes!(patch::route))
        .routes(routes!(delete::route))
        .route_layer(axum::middleware::from_fn_with_state(state.clone(), auth))
        .with_state(state.clone())
}
