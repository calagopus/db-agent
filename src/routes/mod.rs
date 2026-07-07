use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Instant};
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;

pub mod api;

#[derive(Debug, ToSchema, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AppContainerType {
    Official,
    Unknown,
    None,
}

pub struct AppState {
    pub start_time: Instant,
    pub version: String,
    pub container_type: AppContainerType,

    pub config: Arc<crate::config::Config>,
    pub database: Arc<crate::database::Database>,
    pub stats_manager: Arc<crate::stats::StatsManager>,
    pub subsystem_registry: Arc<crate::subsystems::SubsystemRegistry>,
    pub database_manager: Arc<crate::subsystems::database::manager::DatabaseManager>,
    pub database_route_manager: Arc<crate::subsystems::database::manager::DatabaseRouteManager>,
    pub container_executor: Arc<dyn crate::subsystems::database::executor::ContainerExecutor>,
}

fn default_page() -> u64 {
    1
}
fn default_per_page() -> u64 {
    50
}

#[derive(Deserialize, utoipa::IntoParams)]
pub struct PaginationParams {
    #[serde(default = "default_page")]
    pub page: u64,
    #[serde(default = "default_per_page")]
    pub per_page: u64,
}

impl PaginationParams {
    pub fn resolve(&self) -> (u64, u64, usize) {
        let per_page = self.per_page.clamp(1, 100);
        let page = self.page.max(1);
        let offset = ((page - 1) * per_page) as usize;

        (page, per_page, offset)
    }
}

#[derive(ToSchema, Serialize)]
pub struct Paginated<T> {
    pub data: Vec<T>,
    pub total: u64,
    pub page: u64,
    pub per_page: u64,
}

#[derive(ToSchema, Serialize, Deserialize)]
pub struct ApiError<'a> {
    pub error: &'a str,
}

impl<'a> ApiError<'a> {
    #[inline]
    pub fn new(error: &'a str) -> Self {
        Self { error }
    }
}

pub type State = Arc<AppState>;
pub type GetState = axum::extract::State<State>;

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .nest("/api", api::router(state))
        .with_state(state.clone())
}
