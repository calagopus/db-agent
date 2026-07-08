use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod post {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::{
            ApiError,
            api::databases::_database_::{GetDatabase, users::_user_::GetUser},
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
        ("database" = uuid::Uuid, description = "The database uuid"),
        ("user" = uuid::Uuid, description = "The database user uuid"),
    ))]
    pub async fn route(database: GetDatabase, user: GetUser) -> ApiResponseResult {
        let password = database.rotate_user_password(&user).await?;

        ApiResponse::new_serialized(Response { password }).ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .routes(routes!(post::route))
        .with_state(state.clone())
}
