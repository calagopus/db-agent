use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

pub mod _database_;

mod put {
    use crate::{
        Path,
        response::{ApiResponse, ApiResponseResult},
        routes::{ApiError, api::instances::_instance_::GetInstance},
    };
    use axum::http::StatusCode;
    use garde::Validate;
    use serde::{Deserialize, Serialize};
    use utoipa::ToSchema;

    #[derive(ToSchema, Validate, Deserialize)]
    pub struct Payload {
        #[serde(default)]
        #[garde(dive)]
        databases: Vec<crate::routes::api::instances::_instance_::users::UserDatabase>,
    }

    #[derive(ToSchema, Serialize)]
    struct Response {
        databases: Vec<crate::database::data::StoredUserDatabase>,
    }

    #[utoipa::path(put, path = "/", responses(
        (status = OK, body = inline(Response)),
        (status = BAD_REQUEST, body = ApiError),
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
    ), request_body = inline(Payload))]
    pub async fn route(
        instance: GetInstance,
        Path((_instance, user_id)): Path<(uuid::Uuid, uuid::Uuid)>,
        crate::Payload(data): crate::Payload<Payload>,
    ) -> ApiResponseResult {
        if let Err(errors) = crate::utils::validate_data(&data) {
            return ApiResponse::error(&errors.join(", "))
                .with_status(StatusCode::BAD_REQUEST)
                .ok();
        }

        let user = match instance.get_user(user_id).await? {
            Some(user) => user,
            None => {
                return ApiResponse::error("user not found")
                    .with_status(StatusCode::NOT_FOUND)
                    .ok();
            }
        };

        let databases: Vec<_> = data
            .databases
            .into_iter()
            .map(crate::routes::api::instances::_instance_::users::UserDatabase::into_pair)
            .collect();

        ApiResponse::new_serialized(Response {
            databases: instance.set_user_permissions(&user, &databases).await?,
        })
        .ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .nest("/{database}", _database_::router(state))
        .routes(routes!(put::route))
        .with_state(state.clone())
}
