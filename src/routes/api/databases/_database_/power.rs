use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod post {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::{ApiError, api::databases::_database_::GetDatabase},
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
        ("database" = uuid::Uuid, description = "The database uuid"),
    ), request_body = inline(Payload))]
    pub async fn route(
        database: GetDatabase,
        crate::Payload(data): crate::Payload<Payload>,
    ) -> ApiResponseResult {
        let result = match data.action {
            PowerAction::Start => database.start().await,
            PowerAction::Stop => database.stop().await,
            PowerAction::Kill => database.kill().await,
            PowerAction::Restart => match database.stop().await {
                Ok(()) => database.start().await,
                Err(err) => Err(err),
            },
        };

        if let Err(err) = result {
            tracing::error!(
                "failed to run power action on database {}: {err}",
                database.uuid
            );

            return ApiResponse::error(&format!(
                "failed to run power action on database {}: {err}",
                database.uuid
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
