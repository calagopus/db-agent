use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod post {
    use crate::{
        instance::connection::QueryResult,
        response::{ApiResponse, ApiResponseResult},
        routes::{ApiError, api::instances::_instance_::GetInstance},
    };
    use axum::http::StatusCode;
    use serde::{Deserialize, Serialize};
    use utoipa::ToSchema;

    #[derive(ToSchema, Deserialize)]
    pub struct Payload {
        db: Option<String>,
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
        if let Some(db) = &data.db
            && let Err(err) = crate::instance::validate_database_name(db, &())
        {
            return ApiResponse::error(&format!("invalid db name: {err}"))
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
