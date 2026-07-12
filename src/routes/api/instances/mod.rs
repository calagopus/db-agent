use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod _instance_;
mod utilization;

mod get {
    use crate::{
        instance::ApiInstance,
        response::{ApiResponse, ApiResponseResult},
        routes::GetState,
    };
    use serde::Serialize;
    use utoipa::ToSchema;

    #[derive(ToSchema, Serialize)]
    struct Response {
        instances: Vec<ApiInstance>,
    }

    #[utoipa::path(get, path = "/", responses(
        (status = OK, body = inline(Response)),
    ))]
    pub async fn route(state: GetState) -> ApiResponseResult {
        let mut instances = Vec::new();
        for instance in state.instance_manager.get_instances().await.iter() {
            instances.push(instance.to_api_response().await);
        }

        ApiResponse::new_serialized(Response { instances }).ok()
    }
}

mod post {
    use crate::{
        database::data::StoredInstanceCreate,
        response::{ApiResponse, ApiResponseResult},
        routes::GetState,
    };
    use serde::Serialize;
    use utoipa::ToSchema;

    #[derive(ToSchema, Serialize)]
    struct Response {
        instance: crate::instance::ApiInstance,
    }

    #[utoipa::path(post, path = "/", responses(
        (status = OK, body = inline(Response)),
    ), request_body = inline(StoredInstanceCreate))]
    pub async fn route(
        state: GetState,
        crate::Payload(create): crate::Payload<StoredInstanceCreate>,
    ) -> ApiResponseResult {
        let instance = state
            .instance_manager
            .create_instance(state.0.clone(), create)
            .await?;

        ApiResponse::new_serialized(Response {
            instance: instance.to_api_response().await,
        })
        .ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .nest("/{instance}", _instance_::router(state))
        .nest("/utilization", utilization::router(state))
        .routes(routes!(get::route))
        .routes(routes!(post::route))
        .with_state(state.clone())
}
