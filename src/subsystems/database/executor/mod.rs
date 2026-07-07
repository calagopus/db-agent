use std::{pin::Pin, sync::Arc};
use tokio::io::AsyncWrite;

pub mod docker;

pub struct ExecOptions {
    pub command: Vec<String>,
    pub tty: bool,
    pub user: Option<String>,
    pub working_dir: Option<String>,
}

impl ExecOptions {
    pub fn new(command: Vec<String>) -> Self {
        Self {
            command,
            tty: false,
            user: None,
            working_dir: None,
        }
    }
}

pub struct ExecStream {
    pub output: futures_util::stream::BoxStream<'static, Result<bytes::Bytes, anyhow::Error>>,
    pub stdin: Pin<Box<dyn AsyncWrite + Send>>,
}

#[async_trait::async_trait]
pub trait ProcessHandle: Send + Sync {
    async fn resource_usage(&self) -> Result<super::resources::ResourceUsage, anyhow::Error>;

    async fn exec(&self, options: ExecOptions) -> Result<ExecStream, anyhow::Error>;

    async fn logs(
        &self,
        lines: Option<usize>,
    ) -> Result<
        futures_util::stream::BoxStream<'static, Result<bytes::Bytes, anyhow::Error>>,
        anyhow::Error,
    >;

    async fn update_resources(
        &self,
        data: &crate::database::data::StoredDatabase,
    ) -> Result<(), anyhow::Error>;

    async fn start(&self) -> Result<(), anyhow::Error>;
    async fn stop(&self) -> Result<(), anyhow::Error>;
    async fn kill(&self) -> Result<(), anyhow::Error>;
}

#[async_trait::async_trait]
pub trait ContainerExecutor: Send + Sync {
    async fn boot(&self) -> Result<(), anyhow::Error>;

    async fn create_container(
        &self,
        database: &super::Database,
    ) -> Result<Arc<dyn ProcessHandle>, anyhow::Error>;
    async fn attach_container(
        &self,
        database: &super::Database,
    ) -> Result<Option<Arc<dyn ProcessHandle>>, anyhow::Error>;
    async fn destroy_container(&self, database: &super::Database) -> Result<(), anyhow::Error>;
}
