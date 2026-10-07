use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod remote;

mod post {
    use crate::{
        Query,
        io::compression::flate::AsyncFlateReader,
        response::{ApiResponse, ApiResponseResult},
        routes::{ApiError, api::instances::_instance_::GetInstance},
    };
    use axum::{body::Body, http::StatusCode};
    use futures_util::TryStreamExt;
    use garde::Validate;
    use serde::{Deserialize, Serialize};
    use tokio::io::{AsyncBufReadExt, AsyncRead};
    use utoipa::ToSchema;

    #[derive(ToSchema, Validate, Deserialize)]
    pub struct Params {
        #[garde(inner(custom(crate::instance::validate_source_database_name)))]
        source_db: Option<String>,
        #[garde(inner(custom(crate::instance::validate_database_name)))]
        db: Option<String>,
        #[garde(skip)]
        #[serde(default)]
        wipe: bool,
        #[garde(skip)]
        #[serde(default)]
        lock: bool,
    }

    #[derive(ToSchema, Serialize)]
    struct Response {}

    #[utoipa::path(post, path = "/", responses(
        (status = OK, body = inline(Response)),
        (status = BAD_REQUEST, body = ApiError),
        (status = NOT_FOUND, body = ApiError),
        (status = CONFLICT, body = ApiError),
    ), params(
        (
            "instance" = uuid::Uuid,
            description = "The instance uuid",
            example = "123e4567-e89b-12d3-a456-426614174000",
        ),
        (
            "source_db" = Option<String>, Query,
            description = "The db the dump was taken from, mongodb only, requires db",
        ),
        (
            "db" = Option<String>, Query,
            description = "The db to import into, whole instance if omitted; must be omitted for redis, requires source_db for mongodb",
        ),
        (
            "wipe" = Option<bool>, Query,
            description = "Clear existing data in the target before importing, requires db except for redis",
        ),
        (
            "lock" = Option<bool>, Query,
            description = "Write lock the instance for the duration of the import, refusing writes through the api and dropping client connections",
        ),
    ), request_body(content = String, description = "The dump to import, gzip compressed dumps are detected and decompressed"))]
    pub async fn route(
        instance: GetInstance,
        Query(params): Query<Params>,
        body: Body,
    ) -> ApiResponseResult {
        if let Err(errors) = crate::utils::validate_data(&params) {
            return ApiResponse::error(&errors.join(", "))
                .with_status(StatusCode::BAD_REQUEST)
                .ok();
        }

        let mut reader = tokio_util::io::StreamReader::new(
            body.into_data_stream().map_err(std::io::Error::other),
        );
        let gzipped = reader.fill_buf().await?.starts_with(&[0x1f, 0x8b]);
        let mut reader: Box<dyn AsyncRead + Send + Unpin> = if gzipped {
            Box::new(AsyncFlateReader::gzip_decode(reader))
        } else {
            Box::new(reader)
        };

        instance
            .import(
                params.db.as_deref(),
                params.source_db.as_deref(),
                params.wipe,
                params.lock,
                &mut reader,
            )
            .await?;

        ApiResponse::new_serialized(Response {}).ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .nest("/remote", remote::router(state))
        .routes(routes!(post::route))
        .with_state(state.clone())
}
