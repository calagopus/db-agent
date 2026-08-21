use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod put {
    use crate::{
        Path,
        response::{ApiResponse, ApiResponseResult},
        routes::{ApiError, api::instances::_instance_::GetInstance},
    };
    use axum::http::StatusCode;
    use serde::{Deserialize, Serialize};
    use utoipa::ToSchema;

    #[derive(ToSchema, Deserialize)]
    pub struct Payload {
        permission: crate::instance::DatabasePermission,
    }

    #[derive(ToSchema, Serialize)]
    struct Response {
        database: Option<crate::database::data::StoredUserDatabase>,
    }

    #[utoipa::path(put, path = "/", responses(
        (status = OK, body = inline(Response)),
        (status = CONFLICT, body = ApiError),
        (status = NOT_FOUND, body = ApiError),
    ), params(
        (
            "instance" = uuid::Uuid,
            description = "The instance uuid",
            example = "123e4567-e89b-12d3-a456-426614174000",
        ),
        (
            "user" = uuid::Uuid,
            description = "The user uuid",
            example = "123e4567-e89b-12d3-a456-426614174000",
        ),
        (
            "database" = uuid::Uuid,
            description = "The database uuid",
            example = "123e4567-e89b-12d3-a456-426614174000",
        ),
    ), request_body = inline(Payload))]
    pub async fn route(
        instance: GetInstance,
        Path((_instance, user_id, database_id)): Path<(uuid::Uuid, uuid::Uuid, uuid::Uuid)>,
        crate::Payload(data): crate::Payload<Payload>,
    ) -> ApiResponseResult {
        let user = match instance.get_user(user_id).await? {
            Some(user) => user,
            None => {
                return ApiResponse::error("user not found")
                    .with_status(StatusCode::NOT_FOUND)
                    .ok();
            }
        };
        let database = match instance.get_database(database_id).await? {
            Some(database) => database,
            None => {
                return ApiResponse::error("database not found")
                    .with_status(StatusCode::NOT_FOUND)
                    .ok();
            }
        };

        let database = instance
            .set_user_permission(&user, &database, data.permission)
            .await?;

        ApiResponse::new_serialized(Response { database }).ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .routes(routes!(put::route))
        .with_state(state.clone())
}
