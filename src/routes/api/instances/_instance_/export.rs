use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod get {
    use crate::{
        Query,
        instance::DatabaseType,
        io::compression::flate::AsyncFlateReader,
        response::{ApiResponse, ApiResponseResult},
        routes::{ApiError, api::instances::_instance_::GetInstance},
    };
    use axum::http::StatusCode;
    use garde::Validate;
    use serde::Deserialize;
    use utoipa::ToSchema;

    #[derive(ToSchema, Validate, Deserialize)]
    pub struct Params {
        #[garde(inner(custom(crate::instance::validate_database_name)))]
        db: Option<String>,
        #[garde(skip)]
        #[serde(default)]
        lock: bool,
    }

    #[utoipa::path(get, path = "/", responses(
        (status = OK, body = String, description = "A gzip compressed .sql.gz dump for postgres and mariadb, the raw dump otherwise"),
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
            "db" = Option<String>, Query,
            description = "The db to export, everything if omitted",
        ),
        (
            "lock" = Option<bool>, Query,
            description = "Write lock the instance for the duration of the export, refusing writes through the api and dropping client connections",
        ),
    ))]
    pub async fn route(instance: GetInstance, Query(params): Query<Params>) -> ApiResponseResult {
        if let Err(errors) = crate::utils::validate_data(&params) {
            return ApiResponse::error(&errors.join(", "))
                .with_status(StatusCode::BAD_REQUEST)
                .ok();
        }

        let database_type = instance.data.read().await.database_type;
        let reader = instance.export(params.db.as_deref(), params.lock).await?;

        match database_type {
            DatabaseType::Postgres | DatabaseType::Mariadb => {
                let file_name = params.db.unwrap_or_else(|| instance.uuid.to_string());

                ApiResponse::new_stream(AsyncFlateReader::gzip_encode(reader))
                    .with_header("Content-Type", "application/gzip")
                    .with_header(
                        "Content-Disposition",
                        &format!("attachment; filename=\"{file_name}.sql.gz\""),
                    )
                    .ok()
            }
            DatabaseType::Mongodb | DatabaseType::Redis => ApiResponse::new_stream(reader)
                .with_header("Content-Type", "application/octet-stream")
                .ok(),
        }
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .routes(routes!(get::route))
        .with_state(state.clone())
}
