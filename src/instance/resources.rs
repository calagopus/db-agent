use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Clone, Copy, Debug, Default, ToSchema, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ContainerState {
    #[default]
    Offline,
    Starting,
    Stopping,
    Running,
}

#[derive(Default, ToSchema, Deserialize, Serialize, Debug, Clone, Copy, PartialEq)]
pub struct ResourceUsage {
    pub memory_bytes: u64,
    pub memory_limit_bytes: u64,
    pub disk_bytes: u64,

    pub state: ContainerState,

    pub cpu_absolute: f64,
    pub uptime: u64,
}
