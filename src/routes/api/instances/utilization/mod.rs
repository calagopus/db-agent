use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod ws;

mod get {
    use crate::{
        instance::resources::ResourceUsage,
        response::{ApiResponse, ApiResponseResult},
        routes::GetState,
    };
    use std::collections::HashMap;

    #[utoipa::path(get, path = "/", responses(
        (status = OK, body = HashMap<uuid::Uuid, ResourceUsage>),
    ))]
    pub async fn route(state: GetState) -> ApiResponseResult {
        let mut utilization = HashMap::new();

        for instance in state.instance_manager.get_instances().await.iter() {
            utilization.insert(instance.uuid, instance.resource_usage().await);
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
