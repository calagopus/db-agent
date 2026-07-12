use status::SubsystemConnections;
use std::sync::Arc;

pub mod mariadb;
pub mod mongodb;
pub mod postgres;
pub mod redis;
pub mod status;

#[derive(Default)]
pub struct SubsystemRegistry {
    pub postgres: Arc<SubsystemConnections>,
    pub mariadb: Arc<SubsystemConnections>,
    pub mongodb: Arc<SubsystemConnections>,
    pub redis: Arc<SubsystemConnections>,
}
