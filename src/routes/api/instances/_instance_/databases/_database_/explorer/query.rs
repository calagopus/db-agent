use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod post {
    use crate::{
        Path,
        response::{ApiResponse, ApiResponseResult},
        routes::{ApiError, api::instances::_instance_::GetInstance},
    };
    use axum::http::StatusCode;
    use garde::Validate;
    use serde::{Deserialize, Serialize};
    use utoipa::ToSchema;

    fn default_rows() -> u32 {
        crate::instance::explorer::QUERY_DEFAULT_ROWS
    }

    fn default_read_only() -> bool {
        true
    }

    #[derive(ToSchema, Validate, Deserialize)]
    pub struct Payload {
        #[garde(length(chars, min = 1, max = crate::instance::explorer::QUERY_MAX_LENGTH))]
        #[schema(min_length = 1, max_length = 65535)]
        query: String,

        #[garde(range(min = 1, max = crate::instance::explorer::QUERY_MAX_ROWS))]
        #[schema(minimum = 1, maximum = 1000)]
        #[serde(default = "default_rows")]
        rows: u32,

        #[garde(skip)]
        #[serde(default = "default_read_only")]
        read_only: bool,
    }

    #[derive(ToSchema, Serialize)]
    struct Response {
        results: Vec<crate::instance::explorer::QueryResultSet>,
    }

    #[utoipa::path(post, path = "/", responses(
        (status = OK, body = inline(Response)),
        (status = BAD_REQUEST, body = ApiError),
        (status = NOT_FOUND, body = ApiError),
        (status = CONFLICT, body = ApiError),
        (status = REQUEST_TIMEOUT, body = ApiError),
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
    ), request_body = inline(Payload))]
    pub async fn route(
        instance: GetInstance,
        Path((_instance, database_id)): Path<(uuid::Uuid, uuid::Uuid)>,
        crate::Payload(data): crate::Payload<Payload>,
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

        let results = instance
            .run_explorer_query(&database.name, &data.query, data.rows, data.read_only)
            .await?;

        ApiResponse::new_serialized(Response { results }).ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .routes(routes!(post::route))
        .with_state(state.clone())
}
