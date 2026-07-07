use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod get {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::api::databases::_database_::GetDatabase,
        subsystems::database::identifier::DbIdentifier,
    };
    use axum::{extract::Query, http::StatusCode};
    use serde::Deserialize;
    use utoipa::ToSchema;

    #[derive(ToSchema, Deserialize)]
    pub struct Params {
        db: Option<String>,
    }

    #[utoipa::path(get, path = "/", responses(
        (status = OK, body = String, content_type = "application/octet-stream"),
    ), params(
        ("database" = uuid::Uuid, description = "The database uuid"),
        (
            "db" = Option<String>, Query,
            description = "The db to export, everything if omitted",
        ),
    ))]
    pub async fn route(database: GetDatabase, Query(params): Query<Params>) -> ApiResponseResult {
        let db = match params.db.as_deref().map(str::parse::<DbIdentifier>) {
            Some(Ok(db)) => Some(db),
            Some(Err(err)) => {
                return ApiResponse::error(&format!("invalid db identifier: {err}"))
                    .with_status(StatusCode::BAD_REQUEST)
                    .ok();
            }
            None => None,
        };

        let reader = database.export(db.as_ref()).await?;

        ApiResponse::new(axum::body::Body::from_stream(
            tokio_util::io::ReaderStream::new(reader),
        ))
        .with_header("Content-Type", "application/octet-stream")
        .ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .routes(routes!(get::route))
        .with_state(state.clone())
}
