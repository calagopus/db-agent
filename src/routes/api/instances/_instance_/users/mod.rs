use super::State;
use garde::Validate;
use serde::Deserialize;
use utoipa::ToSchema;
use utoipa_axum::{router::OpenApiRouter, routes};

pub mod _user_;

#[derive(ToSchema, Validate, Deserialize)]
pub struct UserDatabase {
    #[garde(skip)]
    pub database_uuid: uuid::Uuid,
    #[garde(skip)]
    pub permission: crate::instance::DatabasePermission,
}

impl UserDatabase {
    #[inline]
    pub fn into_pair(self) -> (uuid::Uuid, crate::instance::DatabasePermission) {
        (self.database_uuid, self.permission)
    }
}

mod get {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::{ApiError, api::instances::_instance_::GetInstance},
    };
    use serde::Serialize;
    use utoipa::ToSchema;

    #[derive(ToSchema, Serialize)]
    struct Response {
        users: Vec<crate::database::data::StoredUser>,
    }

    #[utoipa::path(get, path = "/", responses(
        (status = OK, body = inline(Response)),
        (status = NOT_FOUND, body = ApiError),
    ), params(
        (
            "instance" = uuid::Uuid,
            description = "The instance uuid",
            example = "123e4567-e89b-12d3-a456-426614174000",
        ),
    ))]
    pub async fn route(instance: GetInstance) -> ApiResponseResult {
        ApiResponse::new_serialized(Response {
            users: instance.get_users().await?,
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
        #[garde(length(chars, min = 2, max = 23), ascii, alphanumeric)]
        username: String,
        #[serde(default)]
        #[garde(dive)]
        databases: Vec<super::UserDatabase>,
    }

    #[derive(ToSchema, Serialize)]
    struct Response {
        user: crate::database::data::StoredUser,
        username: String,
    }

    #[utoipa::path(post, path = "/", responses(
        (status = OK, body = inline(Response)),
        (status = BAD_REQUEST, body = ApiError),
        (status = NOT_FOUND, body = ApiError),
    ), params(
        (
            "instance" = uuid::Uuid,
            description = "The instance uuid",
            example = "123e4567-e89b-12d3-a456-426614174000",
        ),
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

        let databases: Vec<_> = data
            .databases
            .into_iter()
            .map(super::UserDatabase::into_pair)
            .collect();

        let user = instance.create_user(&data.username, &databases).await?;
        let username = crate::instance::identifier::UserIdentifier::from_parts(
            user.uuid.as_fields().0,
            &user.username,
        )?;

        ApiResponse::new_serialized(Response {
            username: username.to_string(),
            user,
        })
        .ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .nest("/{user}", _user_::router(state))
        .routes(routes!(get::route))
        .routes(routes!(post::route))
        .with_state(state.clone())
}
