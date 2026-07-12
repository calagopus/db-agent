use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod post {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::{
            ApiError,
            api::instances::_instance_::{GetInstance, users::_user_::GetUser},
        },
    };
    use serde::Serialize;
    use utoipa::ToSchema;

    #[derive(ToSchema, Serialize)]
    struct Response {
        password: String,
    }

    #[utoipa::path(post, path = "/", responses(
        (status = OK, body = inline(Response)),
        (status = NOT_FOUND, body = ApiError),
    ), params(
        ("instance" = uuid::Uuid, description = "The instance uuid"),
        ("user" = uuid::Uuid, description = "The user uuid"),
    ))]
    pub async fn route(instance: GetInstance, user: GetUser) -> ApiResponseResult {
        let password = instance.rotate_password(&user).await?;

        ApiResponse::new_serialized(Response { password }).ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .routes(routes!(post::route))
        .with_state(state.clone())
}
