use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod get {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::{ApiError, api::instances::_instance_::GetInstance},
    };
    use axum::{extract::Query, http::StatusCode};
    use garde::Validate;
    use serde::Deserialize;
    use utoipa::ToSchema;

    #[derive(ToSchema, Validate, Deserialize)]
    pub struct Params {
        #[garde(inner(custom(crate::instance::validate_database_name)))]
        db: Option<String>,
    }

    #[utoipa::path(get, path = "/", responses(
        (status = OK, body = String),
        (status = BAD_REQUEST, body = ApiError),
        (status = NOT_FOUND, body = ApiError),
    ), params(
        ("instance" = uuid::Uuid, description = "The instance uuid"),
        (
            "db" = Option<String>, Query,
            description = "The db to export, everything if omitted",
        ),
    ))]
    pub async fn route(instance: GetInstance, Query(params): Query<Params>) -> ApiResponseResult {
        if let Err(errors) = crate::utils::validate_data(&params) {
            return ApiResponse::error(&errors.join(", "))
                .with_status(StatusCode::BAD_REQUEST)
                .ok();
        }

        let reader = instance.export(params.db.as_deref()).await?;

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
