use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod _database_;
mod utilization;

mod get {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::GetState,
        subsystems::database::ApiDatabase,
    };
    use serde::Serialize;
    use utoipa::ToSchema;

    #[derive(ToSchema, Serialize)]
    struct Response {
        databases: Vec<ApiDatabase>,
    }

    #[utoipa::path(get, path = "/", responses(
        (status = OK, body = inline(Response)),
    ))]
    pub async fn route(state: GetState) -> ApiResponseResult {
        let mut databases = Vec::new();
        for database in state.database_manager.get_databases().await.iter() {
            databases.push(database.to_api_response().await);
        }

        ApiResponse::new_serialized(Response { databases }).ok()
    }
}

mod post {
    use crate::{
        database::data::StoredDatabaseCreate,
        response::{ApiResponse, ApiResponseResult},
        routes::GetState,
    };
    use serde::Serialize;
    use utoipa::ToSchema;

    #[derive(ToSchema, Serialize)]
    struct Response {
        database: crate::subsystems::database::ApiDatabase,
    }

    #[utoipa::path(post, path = "/", responses(
        (status = OK, body = inline(Response)),
    ), request_body = inline(StoredDatabaseCreate))]
    pub async fn route(
        state: GetState,
        crate::Payload(create): crate::Payload<StoredDatabaseCreate>,
    ) -> ApiResponseResult {
        let database = state
            .database_manager
            .create_database(state.0.clone(), create)
            .await?;

        ApiResponse::new_serialized(Response {
            database: database.to_api_response().await,
        })
        .ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .nest("/{database}", _database_::router(state))
        .nest("/utilization", utilization::router(state))
        .routes(routes!(get::route))
        .routes(routes!(post::route))
        .with_state(state.clone())
}
