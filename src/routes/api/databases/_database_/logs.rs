use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod get {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::api::databases::_database_::GetDatabase,
    };
    use axum::extract::Query;
    use serde::Deserialize;
    use utoipa::ToSchema;

    #[derive(ToSchema, Deserialize)]
    pub struct Params {
        lines: Option<usize>,
    }

    #[utoipa::path(get, path = "/", responses(
        (status = OK, body = String),
    ), params(
        ("database" = uuid::Uuid, description = "The database uuid"),
        (
            "lines" = Option<usize>, Query,
            description = "The number of lines to tail from the log",
            example = "100",
        ),
    ))]
    pub async fn route(database: GetDatabase, Query(params): Query<Params>) -> ApiResponseResult {
        let log_stream = database.logs(params.lines).await;

        ApiResponse::new(axum::body::Body::from_stream(log_stream))
            .with_header("Content-Type", "text/plain")
            .ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .routes(routes!(get::route))
        .with_state(state.clone())
}
