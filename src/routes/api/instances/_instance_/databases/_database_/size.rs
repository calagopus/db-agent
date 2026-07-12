use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod get {
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
    struct Response {
        size: i64,
    }

    #[utoipa::path(get, path = "/", responses(
        (status = OK, body = inline(Response)),
        (status = NOT_FOUND, body = ApiError),
    ), params(
        ("instance" = uuid::Uuid, description = "The instance uuid"),
        ("database" = uuid::Uuid, description = "The database uuid"),
    ))]
    pub async fn route(instance: GetInstance, database: GetDatabase) -> ApiResponseResult {
        let size = instance
            .connection()
            .await?
            .get_size(&database.name)
            .await?;

        ApiResponse::new_serialized(Response { size }).ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .routes(routes!(get::route))
        .with_state(state.clone())
}
