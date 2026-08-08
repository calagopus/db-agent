use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod _operation_;

mod get {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::{ApiError, api::instances::_instance_::GetInstance},
    };
    use serde::Serialize;
    use utoipa::ToSchema;

    #[derive(ToSchema, Serialize)]
    struct ApiOperation<'a> {
        uuid: uuid::Uuid,
        operation: &'a crate::instance::operations::DatabaseOperation,
    }

    #[derive(ToSchema, Serialize)]
    struct Response<'a> {
        operations: Vec<ApiOperation<'a>>,
    }

    #[utoipa::path(get, path = "/", responses(
        (status = OK, body = inline(Response)),
        (status = NOT_FOUND, body = ApiError),
    ), params(
        (
            "instance" = uuid::Uuid,
            description = "The instance uuid",
            example = "123e4567-e89b-12d3-a456-426614174000",
        ),
    ))]
    pub async fn route(instance: GetInstance) -> ApiResponseResult {
        let values = instance.operations.operations().await;
        let mut operations = Vec::new();
        operations.reserve_exact(values.len());

        for (uuid, operation) in values.iter() {
            operations.push(ApiOperation {
                uuid: *uuid,
                operation: &operation.database_operation,
            });
        }

        ApiResponse::new_serialized(Response { operations }).ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .nest("/{operation}", _operation_::router(state))
        .routes(routes!(get::route))
        .with_state(state.clone())
}
