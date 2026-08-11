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

    #[derive(ToSchema, Validate, Deserialize)]
    pub struct Payload {
        #[garde(inner(length(chars, min = 1, max = 255)))]
        #[schema(min_length = 1, max_length = 255)]
        schema: Option<String>,

        #[garde(length(chars, min = 1, max = 255))]
        #[schema(min_length = 1, max_length = 255)]
        table: String,

        #[garde(length(min = 1, max = crate::instance::explorer::MUTATE_MAX_ROWS), dive)]
        #[schema(min_items = 1, max_items = 100)]
        rows: Vec<crate::instance::explorer::RowUpdate>,
    }

    #[derive(ToSchema, Serialize)]
    struct Response {
        affected: u64,
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

        let affected = instance
            .mutate_explorer_rows(
                &database.name,
                data.schema.as_deref(),
                &data.table,
                crate::instance::explorer::RowOperation::Update(&data.rows),
            )
            .await?;

        ApiResponse::new_serialized(Response { affected }).ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .routes(routes!(post::route))
        .with_state(state.clone())
}
