use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(ToSchema, Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
#[schema(rename_all = "lowercase")]
#[repr(u8)]
pub enum ContainerState {
    #[default]
    Offline,
    Starting,
    Stopping,
    Running,
}

impl ContainerState {
    #[inline]
    pub fn to_str(self) -> &'static str {
        match self {
            ContainerState::Offline => "offline",
            ContainerState::Starting => "starting",
            ContainerState::Stopping => "stopping",
            ContainerState::Running => "running",
        }
    }

    #[inline]
    pub fn from_db_str(s: &str) -> Option<Self> {
        match s {
            "offline" => Some(ContainerState::Offline),
            "starting" => Some(ContainerState::Starting),
            "stopping" => Some(ContainerState::Stopping),
            "running" => Some(ContainerState::Running),
            _ => None,
        }
    }
}

#[derive(ToSchema, Default, Deserialize, Serialize, Debug, Clone, Copy, PartialEq)]
pub struct ResourceUsage {
    pub memory_bytes: u64,
    pub memory_limit_bytes: u64,
    pub disk_bytes: u64,

    pub state: ContainerState,

    pub cpu_absolute: f64,
    pub uptime: u64,
}

impl ResourceUsage {
    /// Resets all metrics tied to a live container, keeping only disk usage.
    pub fn wipe(&mut self, state: ContainerState) {
        *self = Self {
            disk_bytes: self.disk_bytes,
            state,
            ..Default::default()
        };
    }
}

pub trait ResourceUsageWatchExt {
    fn publish_disk_usage(&self, disk_bytes: u64);
    /// Wipes all container-bound metrics, keeping only disk usage.
    fn wipe(&self, state: ContainerState);
}

impl ResourceUsageWatchExt for tokio::sync::watch::Sender<ResourceUsage> {
    fn publish_disk_usage(&self, disk_bytes: u64) {
        self.send_if_modified(|usage| {
            if usage.disk_bytes == disk_bytes {
                return false;
            }

            usage.disk_bytes = disk_bytes;
            true
        });
    }

    fn wipe(&self, state: ContainerState) {
        self.send_modify(|usage| usage.wipe(state));
    }
}
