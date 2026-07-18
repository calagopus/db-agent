use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod post {
    use crate::{
        instance::connection::QueryResult,
        response::{ApiResponse, ApiResponseResult},
        routes::{ApiError, api::instances::_instance_::GetInstance},
    };
    use axum::http::StatusCode;
    use garde::Validate;
    use serde::{Deserialize, Serialize};
    use utoipa::ToSchema;

    #[derive(ToSchema, Validate, Deserialize)]
    pub struct Payload {
        #[garde(inner(custom(crate::instance::validate_database_name)))]
        db: Option<String>,
        #[garde(skip)]
        query: String,
    }

    #[derive(ToSchema, Serialize)]
    struct Response {
        result: QueryResult,
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

        let result = instance
            .connection()
            .await?
            .query(data.db.as_deref(), &data.query)
            .await?;

        ApiResponse::new_serialized(Response { result }).ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .routes(routes!(post::route))
        .with_state(state.clone())
}
