use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod post {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::{ApiError, GetState, api::instances::_instance_::GetInstance},
    };
    use axum::http::StatusCode;
    use garde::Validate;
    use serde::{Deserialize, Serialize};
    use std::sync::{Arc, atomic::AtomicU64};
    use utoipa::ToSchema;

    #[derive(ToSchema, Validate, Deserialize)]
    pub struct Payload {
        #[garde(url, length(max = 2048))]
        url: String,
        #[garde(inner(custom(crate::instance::validate_source_database_name)))]
        source_db: Option<String>,
        #[garde(inner(custom(crate::instance::validate_database_name)))]
        db: Option<String>,
        #[garde(skip)]
        #[serde(default)]
        wipe: bool,
    }

    #[derive(ToSchema, Serialize)]
    struct Response {
        operation: uuid::Uuid,
    }

    #[utoipa::path(post, path = "/", responses(
        (status = OK, body = inline(Response)),
        (status = BAD_REQUEST, body = ApiError),
        (status = NOT_FOUND, body = ApiError),
        (status = CONFLICT, body = ApiError),
        (status = EXPECTATION_FAILED, body = ApiError),
    ), params(
        (
            "instance" = uuid::Uuid,
            description = "The instance uuid",
            example = "123e4567-e89b-12d3-a456-426614174000",
        ),
    ), request_body = inline(Payload))]
    pub async fn route(
        state: GetState,
        instance: GetInstance,
        crate::Payload(data): crate::Payload<Payload>,
    ) -> ApiResponseResult {
        if state.config.load().api.disable_remote_import {
            return ApiResponse::error("remote imports are disabled")
                .with_status(StatusCode::EXPECTATION_FAILED)
                .ok();
        }

        if let Err(errors) = crate::utils::validate_data(&data) {
            return ApiResponse::error(&errors.join(", "))
                .with_status(StatusCode::BAD_REQUEST)
                .ok();
        }

        let import = instance
            .prepare_remote_import(&data.url, data.source_db.as_deref())
            .await?;
        instance
            .check_import(data.db.as_deref(), import.source_db.as_deref(), data.wipe)
            .await?;

        let bytes_processed = Arc::new(AtomicU64::new(0));
        let operation = crate::instance::operations::DatabaseOperation::RemoteImport {
            source_host: import.source_host.clone(),
            source_db: import.source_db.clone(),
            db: data.db.clone(),
            wipe: data.wipe,
            start_time: chrono::Utc::now(),
            bytes_processed: Arc::clone(&bytes_processed),
        };

        let (operation, _) = instance
            .operations
            .add_operation(operation, {
                let instance = instance.0.clone();

                async move {
                    instance
                        .run_remote_import(import, data.db.as_deref(), data.wipe, bytes_processed)
                        .await
                }
            })
            .await;

        ApiResponse::new_serialized(Response { operation }).ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .routes(routes!(post::route))
        .with_state(state.clone())
}
