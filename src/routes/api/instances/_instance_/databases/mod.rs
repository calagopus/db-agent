use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

pub mod _database_;

mod get {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::{ApiError, api::instances::_instance_::GetInstance},
    };
    use serde::Serialize;
    use utoipa::ToSchema;

    #[derive(ToSchema, Serialize)]
    struct Response {
        databases: Vec<crate::database::data::StoredDatabase>,
    }

    #[utoipa::path(get, path = "/", responses(
        (status = OK, body = inline(Response)),
        (status = NOT_FOUND, body = ApiError),
    ), params(
        ("instance" = uuid::Uuid, description = "The instance uuid"),
    ))]
    pub async fn route(instance: GetInstance) -> ApiResponseResult {
        ApiResponse::new_serialized(Response {
            databases: instance.get_databases().await?,
        })
        .ok()
    }
}

mod post {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::{ApiError, api::instances::_instance_::GetInstance},
    };
    use axum::http::StatusCode;
    use garde::Validate;
    use serde::{Deserialize, Serialize};
    use utoipa::ToSchema;

    #[derive(ToSchema, Validate, Deserialize)]
    pub struct Payload {
        #[garde(custom(crate::instance::validate_database_name))]
        name: String,
    }

    #[derive(ToSchema, Serialize)]
    struct Response {
        database: crate::database::data::StoredDatabase,
    }

    #[utoipa::path(post, path = "/", responses(
        (status = OK, body = inline(Response)),
        (status = BAD_REQUEST, body = ApiError),
        (status = NOT_FOUND, body = ApiError),
    ), params(
        ("instance" = uuid::Uuid, description = "The instance uuid"),
    ), request_body = inline(Payload))]
    pub async fn route(
        instance: GetInstance,
        crate::Payload(data): crate::Payload<Payload>,
    ) -> ApiResponseResult {
        if let Err(errors) = crate::utils::validate_data(&data) {
            return ApiResponse::error(&errors.join(", "))
                .with_status(StatusCode::BAD_REQUEST)
                .ok();
        }

        let database = instance.create_database(&data.name).await?;

        ApiResponse::new_serialized(Response { database }).ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .nest("/{database}", _database_::router(state))
        .routes(routes!(get::route))
        .routes(routes!(post::route))
        .with_state(state.clone())
}
