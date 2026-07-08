use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod get {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::{ApiError, api::databases::_database_::GetDatabase},
    };
    use serde::Serialize;
    use utoipa::ToSchema;

    #[derive(ToSchema, Serialize)]
    struct Response {
        utilization: crate::subsystems::database::resources::ResourceUsage,
    }

    #[utoipa::path(get, path = "/", responses(
        (status = OK, body = inline(Response)),
        (status = NOT_FOUND, body = ApiError),
    ), params(
        ("database" = uuid::Uuid, description = "The database uuid"),
    ))]
    pub async fn route(database: GetDatabase) -> ApiResponseResult {
        ApiResponse::new_serialized(Response {
            utilization: database.resource_usage().await,
        })
        .ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .routes(routes!(get::route))
        .with_state(state.clone())
}
