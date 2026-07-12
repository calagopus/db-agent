use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod post {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::{ApiError, api::instances::_instance_::GetInstance},
    };
    use axum::{extract::Query, http::StatusCode};
    use futures_util::TryStreamExt;
    use serde::{Deserialize, Serialize};
    use utoipa::ToSchema;

    #[derive(ToSchema, Deserialize)]
    pub struct Params {
        db: String,
        #[serde(default)]
        wipe: bool,
    }

    #[derive(ToSchema, Serialize)]
    struct Response {}

    #[utoipa::path(post, path = "/", responses(
        (status = OK, body = inline(Response)),
        (status = BAD_REQUEST, body = ApiError),
        (status = NOT_FOUND, body = ApiError),
    ), params(
        ("instance" = uuid::Uuid, description = "The instance uuid"),
        (
            "db" = String, Query,
            description = "The db to import into",
        ),
        (
            "wipe" = Option<bool>, Query,
            description = "Clear existing data in the target before importing",
        ),
    ), request_body = String)]
    pub async fn route(
        instance: GetInstance,
        Query(params): Query<Params>,
        body: axum::body::Body,
    ) -> ApiResponseResult {
        if let Err(err) = crate::instance::validate_database_name(&params.db, &()) {
            return ApiResponse::error(&format!("invalid db name: {err}"))
                .with_status(StatusCode::BAD_REQUEST)
                .ok();
        }

        let mut reader = tokio_util::io::StreamReader::new(
            body.into_data_stream().map_err(std::io::Error::other),
        );

        instance
            .import(&params.db, params.wipe, &mut reader)
            .await?;

        ApiResponse::new_serialized(Response {}).ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .routes(routes!(post::route))
        .with_state(state.clone())
}
