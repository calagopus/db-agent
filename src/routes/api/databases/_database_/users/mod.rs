use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

pub mod _user_;

mod post {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::api::databases::_database_::GetDatabase,
    };
    use serde::{Deserialize, Serialize};
    use utoipa::ToSchema;

    #[derive(ToSchema, Deserialize)]
    pub struct Payload {
        username: String,
    }

    #[derive(ToSchema, Serialize)]
    struct Response {
        user: crate::database::data::StoredDatabaseUser,
        username: String,
        db: Option<String>,
    }

    #[utoipa::path(post, path = "/", responses(
        (status = OK, body = inline(Response)),
    ), params(
        ("database" = uuid::Uuid, description = "The database uuid"),
    ), request_body = inline(Payload))]
    pub async fn route(
        database: GetDatabase,
        crate::Payload(data): crate::Payload<Payload>,
    ) -> ApiResponseResult {
        let (user, db) = database.create_db(&data.username).await?;
        let username = crate::subsystems::database::identifier::UserIdentifier::from_parts(
            user.uuid.as_fields().0,
            &user.username,
        )?;

        ApiResponse::new_serialized(Response {
            username: username.to_string(),
            db: db.map(|db| db.to_string()),
            user,
        })
        .ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .nest("/{user}", _user_::router(state))
        .routes(routes!(post::route))
        .with_state(state.clone())
}
