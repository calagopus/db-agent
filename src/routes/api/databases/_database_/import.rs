use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod post {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::{ApiError, api::databases::_database_::GetDatabase},
        subsystems::database::identifier::DbIdentifier,
    };
    use axum::{extract::Query, http::StatusCode};
    use futures_util::TryStreamExt;
    use serde::{Deserialize, Serialize};
    use utoipa::ToSchema;

    #[derive(ToSchema, Deserialize)]
    pub struct Params {
        db: Option<String>,
        #[serde(default)]
        wipe: bool,
    }

    #[derive(ToSchema, Serialize)]
    struct Response {}

    #[utoipa::path(post, path = "/", responses(
        (status = OK, body = inline(Response)),
        (status = NOT_FOUND, body = ApiError),
    ), params(
        ("database" = uuid::Uuid, description = "The database uuid"),
        (
            "db" = Option<String>, Query,
            description = "The db to import into, the dump decides if omitted",
        ),
        (
            "wipe" = Option<bool>, Query,
            description = "Clear existing data in the target before importing",
        ),
    ), request_body = String)]
    pub async fn route(
        database: GetDatabase,
        Query(params): Query<Params>,
        body: axum::body::Body,
    ) -> ApiResponseResult {
        let db = match params.db.as_deref().map(str::parse::<DbIdentifier>) {
            Some(Ok(db)) => Some(db),
            Some(Err(err)) => {
                return ApiResponse::error(&format!("invalid db identifier: {err}"))
                    .with_status(StatusCode::BAD_REQUEST)
                    .ok();
            }
            None => None,
        };

        let mut reader = tokio_util::io::StreamReader::new(
            body.into_data_stream().map_err(std::io::Error::other),
        );

        database
            .import(db.as_ref(), params.wipe, &mut reader)
            .await?;

        ApiResponse::new_serialized(Response {}).ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .routes(routes!(post::route))
        .with_state(state.clone())
}
