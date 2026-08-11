use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod delete;
mod insert;
mod update;

mod post {
    use crate::{
        Path,
        instance::explorer::{BrowseOptions, QueryResultSet},
        response::{ApiResponse, ApiResponseResult},
        routes::{ApiError, api::instances::_instance_::GetInstance},
    };
    use axum::http::StatusCode;
    use serde::Serialize;
    use utoipa::ToSchema;

    #[derive(ToSchema, Serialize)]
    struct Response {
        result: QueryResultSet,
    }

    #[utoipa::path(post, path = "/", responses(
        (status = OK, body = inline(Response)),
        (status = BAD_REQUEST, body = ApiError),
        (status = NOT_FOUND, body = ApiError),
        (status = CONFLICT, body = ApiError),
        (status = EXPECTATION_FAILED, body = ApiError),
    ), params(
        (
            "instance" = uuid::Uuid,
            description = "The instance uuid",
            example = "123e4567-e89b-12d3-a456-426614174000",
        ),
        (
            "database" = uuid::Uuid,
            description = "The database uuid",
            example = "123e4567-e89b-12d3-a456-426614174000",
        ),
    ), request_body = inline(BrowseOptions))]
    pub async fn route(
        instance: GetInstance,
        Path((_instance, database_id)): Path<(uuid::Uuid, uuid::Uuid)>,
        crate::Payload(data): crate::Payload<BrowseOptions>,
    ) -> ApiResponseResult {
        if let Err(errors) = crate::utils::validate_data(&data) {
            return ApiResponse::error(&errors.join(", "))
                .with_status(StatusCode::BAD_REQUEST)
                .ok();
        }

        let database = match instance.get_database(database_id).await? {
            Some(database) => database,
            None => {
                return ApiResponse::error("database not found")
                    .with_status(StatusCode::NOT_FOUND)
                    .ok();
            }
        };

        let result = instance.browse_explorer_rows(&database.name, &data).await?;

        ApiResponse::new_serialized(Response { result }).ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .routes(routes!(post::route))
        .nest("/insert", insert::router(state))
        .nest("/update", update::router(state))
        .nest("/delete", delete::router(state))
        .with_state(state.clone())
}
