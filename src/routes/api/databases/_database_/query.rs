use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod post {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::{ApiError, api::databases::_database_::GetDatabase},
        subsystems::database::{connection::QueryResult, identifier::DbIdentifier},
    };
    use axum::http::StatusCode;
    use serde::{Deserialize, Serialize};
    use utoipa::ToSchema;

    #[derive(ToSchema, Deserialize)]
    pub struct Payload {
        db: Option<String>,
        query: String,
    }

    #[derive(ToSchema, Serialize)]
    struct Response {
        result: QueryResult,
    }

    #[utoipa::path(post, path = "/", responses(
        (status = OK, body = inline(Response)),
        (status = NOT_FOUND, body = ApiError),
    ), params(
        ("database" = uuid::Uuid, description = "The database uuid"),
    ), request_body = inline(Payload))]
    pub async fn route(
        database: GetDatabase,
        crate::Payload(data): crate::Payload<Payload>,
    ) -> ApiResponseResult {
        let db = match data.db.as_deref().map(str::parse::<DbIdentifier>) {
            Some(Ok(db)) => Some(db),
            Some(Err(err)) => {
                return ApiResponse::error(&format!("invalid db identifier: {err}"))
                    .with_status(StatusCode::BAD_REQUEST)
                    .ok();
            }
            None => None,
        };

        let result = database
            .connection()
            .await?
            .query(db.as_ref(), &data.query)
            .await?;

        ApiResponse::new_serialized(Response { result }).ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .routes(routes!(post::route))
        .with_state(state.clone())
}
