use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod get {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::GetState,
        subsystems::status::SubsystemStatus,
    };
    use serde::Serialize;
    use utoipa::ToSchema;

    #[derive(ToSchema, Serialize)]
    struct Response {
        postgres: SubsystemStatus,
        mariadb: SubsystemStatus,
        mongodb: SubsystemStatus,
        redis: SubsystemStatus,
    }

    #[utoipa::path(get, path = "/", responses(
        (status = OK, body = inline(Response)),
    ))]
    pub async fn route(state: GetState) -> ApiResponseResult {
        ApiResponse::new_serialized(Response {
            postgres: state.subsystem_registry.postgres.to_api_response(),
            mariadb: state.subsystem_registry.mariadb.to_api_response(),
            mongodb: state.subsystem_registry.mongodb.to_api_response(),
            redis: state.subsystem_registry.redis.to_api_response(),
        })
        .ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .routes(routes!(get::route))
        .with_state(state.clone())
}
