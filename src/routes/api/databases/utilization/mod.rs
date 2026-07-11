use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod ws;

mod get {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::GetState,
        subsystems::database::resources::ResourceUsage,
    };
    use std::collections::HashMap;

    #[utoipa::path(get, path = "/", responses(
        (status = OK, body = HashMap<uuid::Uuid, ResourceUsage>),
    ))]
    pub async fn route(state: GetState) -> ApiResponseResult {
        let mut utilization = HashMap::new();

        for database in state.database_manager.get_databases().await.iter() {
            utilization.insert(database.uuid, database.resource_usage().await);
        }

        ApiResponse::new_serialized(utilization).ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .routes(routes!(get::route))
        .nest("/ws", ws::router(state))
        .with_state(state.clone())
}
