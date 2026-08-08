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

    pub fn with_user(mut self, user: String) -> Self {
        self.user = Some(user);
        self
    }
}

pub struct ExecStream {
    pub output: futures_util::stream::BoxStream<'static, Result<bytes::Bytes, anyhow::Error>>,
    pub stdin: Pin<Box<dyn AsyncWrite + Send>>,
}

pub struct NetworkedContainerOptions {
    pub command: Vec<String>,
    pub env: Vec<String>,
    /// `hostname:ip` entries pinned into the container's hosts file
    pub extra_hosts: Vec<String>,
}

impl NetworkedContainerOptions {
    pub fn new(command: Vec<String>) -> Self {
        Self {
            command,
            env: Vec::new(),
            extra_hosts: Vec::new(),
        }
    }

    pub fn with_env(mut self, env: Vec<String>) -> Self {
        self.env = env;
        self
    }

    pub fn with_extra_hosts(mut self, extra_hosts: Vec<String>) -> Self {
        self.extra_hosts = extra_hosts;
        self
    }
}

#[async_trait::async_trait]
pub trait ProcessHandle: Send + Sync {
    async fn exec(&self, options: ExecOptions) -> Result<ExecStream, anyhow::Error>;

    async fn logs(
        &self,
        lines: Option<usize>,
    ) -> Result<Box<dyn tokio::io::AsyncRead + Send + Unpin>, anyhow::Error>;

    async fn update_resources(
        &self,
        data: &crate::database::data::StoredInstance,
    ) -> Result<(), anyhow::Error>;

    async fn start(&self) -> Result<(), anyhow::Error>;
    async fn stop(&self) -> Result<(), anyhow::Error>;
    async fn kill(&self) -> Result<(), anyhow::Error>;
}

#[async_trait::async_trait]
pub trait ContainerExecutor: Send + Sync {
    async fn boot(&self) -> Result<(), anyhow::Error>;

    async fn setup_instance_process(
        &self,
        instance: &super::Instance,
    ) -> Result<Arc<dyn ProcessHandle>, anyhow::Error>;
    async fn attach_instance_process(
        &self,
        instance: &super::Instance,
    ) -> Result<Arc<dyn ProcessHandle>, anyhow::Error>;
    async fn cleanup_instance_process(
        &self,
        instance: &super::Instance,
    ) -> Result<(), anyhow::Error>;

    async fn run_networked_container(
        &self,
        instance: &super::Instance,
        options: NetworkedContainerOptions,
    ) -> Result<
        futures_util::stream::BoxStream<'static, Result<bytes::Bytes, anyhow::Error>>,
        anyhow::Error,
    >;
}
