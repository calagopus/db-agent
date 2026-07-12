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
    pub instance_manager: Arc<crate::instance::manager::InstanceManager>,
    pub database_route_manager: Arc<crate::instance::manager::DatabaseRouteManager>,
    pub container_executor: Arc<dyn crate::instance::executor::ContainerExecutor>,
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
