use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod post {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::{ApiError, api::instances::_instance_::GetInstance},
    };
    use axum::http::StatusCode;
    use serde::{Deserialize, Serialize};
    use utoipa::ToSchema;

    #[derive(ToSchema, Deserialize, Clone, Copy)]
    #[serde(rename_all = "lowercase")]
    pub enum PowerAction {
        Start,
        Stop,
        Restart,
        Kill,
    }

    #[derive(ToSchema, Deserialize)]
    pub struct Payload {
        action: PowerAction,
    }

    #[derive(ToSchema, Serialize)]
    struct Response {}

    #[utoipa::path(post, path = "/", responses(
        (status = OK, body = inline(Response)),
        (status = EXPECTATION_FAILED, body = ApiError),
        (status = NOT_FOUND, body = ApiError),
    ), params(
        ("instance" = uuid::Uuid, description = "The instance uuid"),
    ), request_body = inline(Payload))]
    pub async fn route(
        instance: GetInstance,
        crate::Payload(data): crate::Payload<Payload>,
    ) -> ApiResponseResult {
        let result = match data.action {
            PowerAction::Start => instance.start().await,
            PowerAction::Stop => instance.stop().await,
            PowerAction::Kill => instance.kill().await,
            PowerAction::Restart => match instance.stop().await {
                Ok(()) => instance.start().await,
                Err(err) => Err(err),
            },
        };

        if let Err(err) = result {
            tracing::error!(
                "failed to run power action on instance {}: {err}",
                instance.uuid
            );

            return ApiResponse::error(&format!(
                "failed to run power action on instance {}: {err}",
                instance.uuid
            ))
            .with_status(StatusCode::EXPECTATION_FAILED)
            .ok();
        }

        ApiResponse::new_serialized(Response {}).ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .routes(routes!(post::route))
        .with_state(state.clone())
}
