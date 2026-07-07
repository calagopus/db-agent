use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod _database_;
mod utilization;

mod get {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::{GetState, Paginated, PaginationParams},
        subsystems::database::ApiDatabase,
    };
    use axum::extract::Query;
    use serde::Serialize;
    use utoipa::ToSchema;

    #[derive(ToSchema, Serialize)]
    struct Response {
        #[schema(inline)]
        databases: Paginated<ApiDatabase>,
    }

    #[utoipa::path(get, path = "/", params(PaginationParams), responses(
        (status = OK, body = inline(Response)),
    ))]
    pub async fn route(
        state: GetState,
        Query(pagination): Query<PaginationParams>,
    ) -> ApiResponseResult {
        let (page, per_page, offset) = pagination.resolve();

        let databases = state.database_manager.get_databases().await;
        let total = databases.len() as u64;

        let mut data = Vec::new();
        for database in databases.iter().skip(offset).take(per_page as usize) {
            data.push(database.to_api_response().await);
        }

        ApiResponse::new_serialized(Response {
            databases: Paginated {
                data,
                total,
                page,
                per_page,
            },
        })
        .ok()
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
