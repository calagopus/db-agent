use super::{ExecOptions, ExecStream};
use crate::{
    database::data::StoredInstance,
    instance::{
        resources::{ContainerState, ResourceUsage, ResourceUsageWatchExt},
        websocket::{WebsocketEvent, WebsocketMessage},
    },
    io::SafeSliceExt,
};
use bollard::errors::Error::{DockerContainerWaitError, DockerResponseServerError};
use futures_util::StreamExt;
use itertools::Itertools;
use parking_lot::RwLock;
use rand::distr::SampleString;
use serde::Serialize;
use std::{
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::io::ReadBuf;

pub mod host_mounts;

const CONTAINER_TYPE_DATABASE: &str = "database";
const CONTAINER_TYPE_SCRIPT_RUNNER: &str = "script_runner";

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum PullProgressStatus {
    Pulling,
    Extracting,
}

#[derive(Serialize)]
struct PullProgress {
    status: PullProgressStatus,
    bytes_processed: i64,
    bytes_total: i64,
}

fn pull_progress(
    id: &str,
    status: PullProgressStatus,
    detail: Option<bollard::models::ProgressDetail>,
) -> Option<WebsocketMessage> {
    let detail = detail?;

    Some(
        WebsocketMessage::builder(WebsocketEvent::InstanceImagePullProgress)
            .arg(id)
            .structured_arg(PullProgress {
                status,
                bytes_processed: detail.current.unwrap_or_default(),
                bytes_total: detail.total.unwrap_or_default(),
            })
            .build(),
    )
}

/// force-removes a container when dropped. auto_remove only fires once the process
/// exits by itself, an aborted or dropped stream would strand it forever
struct ContainerGuard {
    docker: Arc<bollard::Docker>,
    container_id: String,
}

impl Drop for ContainerGuard {
    fn drop(&mut self) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };

        let docker = Arc::clone(&self.docker);
        let container_id = std::mem::take(&mut self.container_id);

        handle.spawn(async move {
            match docker
                .remove_container(
                    &container_id,
                    Some(bollard::query_parameters::RemoveContainerOptions {
                        force: true,
                        ..Default::default()
                    }),
                )
                .await
            {
                // 404 is the container removing itself first, 409 a removal already
                // in progress
                Ok(())
                | Err(DockerResponseServerError {
                    status_code: 404 | 409,
                    ..
                }) => {}
                Err(err) => {
                    tracing::warn!(container = %container_id, "failed to remove container: {err}");
                }
            }
        });
    }
}

struct GuardedStream<S> {
    stream: S,
    _guard: ContainerGuard,
}

impl<S: futures_util::Stream + Unpin> futures_util::Stream for GuardedStream<S> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().stream).poll_next(cx)
    }
}

#[inline]
fn string_to_option(s: &str) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

trait DockerStoredInstanceExt {
    fn convert_container_resources(
        &self,
        config: &crate::config::Config,
    ) -> bollard::models::Resources;
    fn container_update_config(
        &self,
        config: &crate::config::Config,
    ) -> bollard::models::ContainerUpdateBody;

    fn base_host_config(&self, config: &crate::config::Config) -> bollard::models::HostConfig;
    fn host_config(
        &self,
        config: &crate::config::Config,
        host_mounts: Option<&host_mounts::HostMountTable>,
    ) -> bollard::models::HostConfig;
    fn container_config(
        &self,
        config: &crate::config::Config,
        host_mounts: Option<&host_mounts::HostMountTable>,
    ) -> bollard::models::ContainerCreateBody;
}

impl DockerStoredInstanceExt for StoredInstance {
    fn convert_container_resources(
        &self,
        config: &crate::config::Config,
    ) -> bollard::models::Resources {
        let memory = if self.memory > 0 {
            self.memory * 1024 * 1024
        } else {
            0
        };

        let mut resources = bollard::models::Resources {
            memory: (memory > 0).then_some(memory),
            memory_reservation: (memory > 0).then_some(memory),
            memory_swap: match self.swap {
                0 => None,
                -1 => Some(-1),
                swap => Some(memory + (swap * 1024 * 1024)),
            },
            blkio_weight: self.io_weight.and_then(|w| u16::try_from(w).ok()),
            pids_limit: match config.load().docker.container_pid_limit {
                0 => None,
                limit => Some(limit as i64),
            },
            ..Default::default()
        };

        if self.cpu > 0 {
            resources.cpu_quota = Some(self.cpu * 1000);
            resources.cpu_period = Some(100_000);
            resources.cpu_shares = Some(1024);
        } else {
            resources.cpu_quota = Some(-1);
        }

        resources
    }

    fn container_update_config(
        &self,
        config: &crate::config::Config,
    ) -> bollard::models::ContainerUpdateBody {
        let resources = self.convert_container_resources(config);

        bollard::models::ContainerUpdateBody {
            memory: resources.memory,
            memory_reservation: resources.memory_reservation,
            memory_swap: resources.memory_swap,
            cpu_quota: resources.cpu_quota,
            cpu_period: resources.cpu_period,
            cpu_shares: resources.cpu_shares,
            blkio_weight: resources.blkio_weight,
            pids_limit: resources.pids_limit,
            ..Default::default()
        }
    }

    fn base_host_config(&self, config: &crate::config::Config) -> bollard::models::HostConfig {
        let resources = self.convert_container_resources(config);
        let cfg = config.load();

        bollard::models::HostConfig {
            memory: resources.memory,
            memory_reservation: resources.memory_reservation,
            memory_swap: resources.memory_swap,
            cpu_quota: resources.cpu_quota,
            cpu_period: resources.cpu_period,
            cpu_shares: resources.cpu_shares,
            blkio_weight: resources.blkio_weight,
            pids_limit: resources.pids_limit,

            tmpfs: Some(HashMap::from([(
                "/tmp".to_string(),
                format!("rw,exec,nosuid,size={}M", cfg.docker.tmpfs_size),
            )])),
            security_opt: Some(vec!["no-new-privileges".to_string()]),
            cap_drop: Some(vec![
                "setpcap".to_string(),
                "mknod".to_string(),
                "audit_write".to_string(),
                "net_raw".to_string(),
                "dac_override".to_string(),
                "fowner".to_string(),
                "fsetid".to_string(),
                "net_bind_service".to_string(),
                "sys_chroot".to_string(),
                "setfcap".to_string(),
                "sys_ptrace".to_string(),
            ]),
            userns_mode: if cfg.docker.rootless.enabled {
                Some(format!(
                    "keep-id:uid={},gid={}",
                    self.image_uid, self.image_gid
                ))
            } else {
                string_to_option(&cfg.docker.userns_mode)
            },
            ..Default::default()
        }
    }

    fn host_config(
        &self,
        config: &crate::config::Config,
        host_mounts: Option<&host_mounts::HostMountTable>,
    ) -> bollard::models::HostConfig {
        let mut mounts = vec![bollard::models::Mount {
            typ: Some(bollard::models::MountType::BIND),
            source: Some(host_mounts::translate_source(
                host_mounts,
                &config.socket_path(self.uuid).to_string_lossy(),
            )),
            target: Some(
                self.socket_path
                    .split('/')
                    .rev()
                    .skip(1)
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .join("/"),
            ),
            ..Default::default()
        }];

        for mapping in &self.volumes {
            mounts.push(bollard::models::Mount {
                typ: Some(bollard::models::MountType::BIND),
                source: Some(host_mounts::translate_source(
                    host_mounts,
                    &mapping.host_path(config, self.uuid).to_string_lossy(),
                )),
                target: Some(mapping.container_path().to_string_lossy().into_owned()),
                ..Default::default()
            });
        }

        let cfg = config.load();

        bollard::models::HostConfig {
            mounts: Some(mounts),
            log_config: Some(bollard::models::HostConfigLogConfig {
                typ: Some(cfg.docker.log_config.r#type.clone()),
                config: Some(cfg.docker.log_config.config.clone().into_iter().collect()),
            }),
            network_mode: Some("none".to_string()),
            ..self.base_host_config(config)
        }
    }

    fn container_config(
        &self,
        config: &crate::config::Config,
        host_mounts: Option<&host_mounts::HostMountTable>,
    ) -> bollard::models::ContainerCreateBody {
        let cfg = config.load();
        let timezone = self
            .timezone
            .clone()
            .unwrap_or_else(|| cfg.docker.timezone.clone());

        let mut env = vec![format!("TZ={timezone}")];
        env.extend(self.env.iter().map(|(k, v)| format!("{k}={v}")));

        bollard::models::ContainerCreateBody {
            hostname: Some(self.uuid.to_string()),
            image: Some(self.image.trim_end_matches('~').to_string()),
            env: Some(env),
            cmd: self.cmd.clone(),
            user: Some(format!("{}:{}", self.image_uid, self.image_gid)),
            labels: Some(HashMap::from([
                ("Service".to_string(), crate::SERVICE_NAME.to_string()),
                (
                    "ContainerType".to_string(),
                    CONTAINER_TYPE_DATABASE.to_string(),
                ),
            ])),
            host_config: Some(self.host_config(config, host_mounts)),
            attach_stdin: Some(true),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            open_stdin: Some(true),
            tty: Some(true),
            ..Default::default()
        }
    }
}

pub struct DockerExecutor {
    docker: Arc<bollard::Docker>,
    app_config: Arc<crate::config::Config>,
    host_mounts: std::sync::OnceLock<Option<host_mounts::HostMountTable>>,
}

impl DockerExecutor {
    pub fn new(docker: Arc<bollard::Docker>, app_config: Arc<crate::config::Config>) -> Self {
        Self {
            docker,
            app_config,
            host_mounts: std::sync::OnceLock::new(),
        }
    }

    #[inline]
    fn host_mounts(&self) -> Option<&host_mounts::HostMountTable> {
        self.host_mounts.get().and_then(Option::as_ref)
    }

    async fn image_exists(&self, image_name: &str) -> bool {
        self.docker
            .list_images(Some(bollard::query_parameters::ListImagesOptions {
                all: true,
                filters: Some(HashMap::from([(
                    "reference".to_string(),
                    vec![image_name.to_string()],
                )])),
                ..Default::default()
            }))
            .await
            .is_ok_and(|images| !images.is_empty())
    }

    async fn pull_image(
        &self,
        instance: &super::super::Instance,
        image: &str,
    ) -> Result<(), anyhow::Error> {
        if image.ends_with('~') {
            return Ok(());
        }

        let uuid = instance.uuid;

        let (image_name, tag) = match image.rsplit_once(':') {
            Some((name, tag)) if !tag.is_empty() => {
                let colon_is_tag_sep = image.rfind('/').is_none_or(|slash| slash < name.len());
                if colon_is_tag_sep {
                    (name, tag)
                } else {
                    (image, "latest")
                }
            }
            _ => (image, "latest"),
        };

        let pull_cache = {
            type InnerMap = HashMap<String, Arc<tokio::sync::Mutex<Option<std::time::Instant>>>>;
            static IMAGE_PULL_CACHE: std::sync::OnceLock<Arc<parking_lot::Mutex<InnerMap>>> =
                std::sync::OnceLock::new();

            IMAGE_PULL_CACHE.get_or_init(|| {
                let cache = Arc::new(parking_lot::Mutex::new(HashMap::new()));

                tokio::spawn({
                    let cache = Arc::clone(&cache);
                    let config = Arc::clone(&self.app_config);

                    async move {
                        loop {
                            tokio::time::sleep(std::time::Duration::from_secs(60)).await;

                            let mut cache = cache.lock();
                            let now = std::time::Instant::now();
                            let duration = config.load().docker.registry_image_fetch_cache.duration;
                            cache.retain(
                                |_,
                                 timestamp: &mut Arc<
                                    tokio::sync::Mutex<Option<std::time::Instant>>,
                                >| {
                                    timestamp.try_lock().is_ok_and(|t| {
                                        now.duration_since(t.unwrap_or(std::time::Instant::now()))
                                            .as_secs()
                                            < duration
                                    })
                                },
                            );
                        }
                    }
                });

                cache
            })
        };

        let cache_config = self.app_config.load().docker.registry_image_fetch_cache;

        let mut last_pull = if cache_config.enabled {
            let entry = {
                let mut cache = pull_cache.lock();
                Arc::clone(cache.entry(image.into()).or_default())
            };

            Some(entry.lock_owned().await)
        } else {
            None
        };

        if let Some(guard) = &last_pull
            && let Some(pulled_at) = **guard
            && pulled_at.elapsed().as_secs() < cache_config.duration
            && self.image_exists(image_name).await
        {
            tracing::debug!(
                instance = %uuid,
                image = %image_name,
                "image pull skipped, cached as recently pulled"
            );

            return Ok(());
        }

        instance
            .websocket
            .send(
                WebsocketMessage::builder(WebsocketEvent::InstanceDaemonMessage)
                    .arg("Pulling database image, this could take a few minutes to complete...")
                    .build(),
            )
            .ok();

        let mut registry_auth = None;
        for (registry, config) in self.app_config.load().docker.registries.iter() {
            if image.starts_with(registry.as_str()) {
                registry_auth = Some(bollard::auth::DockerCredentials {
                    username: Some(config.username.clone()),
                    password: Some(config.password.clone()),
                    serveraddress: Some(registry.clone()),
                    ..Default::default()
                });
                break;
            }
        }

        let mut stream = self.docker.create_image(
            Some(bollard::query_parameters::CreateImageOptions {
                from_image: Some(image_name.to_string()),
                tag: Some(tag.to_string()),
                ..Default::default()
            }),
            None,
            registry_auth,
        );

        while let Some(status) = stream.next().await {
            match status {
                Ok(info) => {
                    let Some(id) = info.id else { continue };

                    let message = match info.status.as_deref().map(str::to_lowercase).as_deref() {
                        Some("downloading") => {
                            pull_progress(&id, PullProgressStatus::Pulling, info.progress_detail)
                        }
                        Some("extracting") => {
                            pull_progress(&id, PullProgressStatus::Extracting, info.progress_detail)
                        }
                        Some("download complete" | "pull complete") => Some(
                            WebsocketMessage::builder(WebsocketEvent::InstanceImagePullCompleted)
                                .arg(id)
                                .build(),
                        ),
                        _ => None,
                    };

                    if let Some(message) = message {
                        instance.websocket.send(message).ok();
                    }
                }
                Err(err) => {
                    tracing::error!(
                        instance = %uuid,
                        image = %image_name,
                        "failed to pull image: {:?}",
                        err
                    );

                    if !self.image_exists(image_name).await {
                        return Err(err.into());
                    }

                    tracing::warn!(
                        instance = %uuid,
                        image = %image_name,
                        "image already exists locally, ignoring pull error"
                    );
                }
            }
        }

        if let Some(guard) = &mut last_pull {
            **guard = Some(std::time::Instant::now());
        }

        Ok(())
    }
}

fn container_filters(
    name_filter: &str,
    container_type: Option<&str>,
) -> HashMap<String, Vec<String>> {
    let mut filters = HashMap::from([("name".to_string(), vec![name_filter.to_string()])]);
    if let Some(container_type) = container_type {
        filters.insert(
            "label".to_string(),
            vec![format!("ContainerType={container_type}")],
        );
    }

    filters
}

async fn find_container(
    docker: &bollard::Docker,
    name_filter: &str,
    container_type: Option<&str>,
) -> Option<String> {
    let containers = docker
        .list_containers(Some(bollard::query_parameters::ListContainersOptions {
            all: true,
            filters: Some(container_filters(name_filter, container_type)),
            ..Default::default()
        }))
        .await
        .unwrap_or_default();

    containers.into_iter().find_map(|c| c.id)
}

struct LogsReader {
    stream: futures_util::stream::BoxStream<'static, Result<Vec<u8>, std::io::Error>>,
    buffer: Vec<u8>,
    pos: usize,
}

impl tokio::io::AsyncRead for LogsReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        loop {
            if self.pos < self.buffer.len() {
                let n = buf.remaining().min(self.buffer.len() - self.pos);
                let buffer_slice = match self.buffer.get_slice(self.pos..self.pos + n) {
                    Ok(slice) => slice,
                    Err(err) => return Poll::Ready(Err(err)),
                };
                buf.put_slice(buffer_slice);
                self.pos += n;

                return Poll::Ready(Ok(()));
            }

            self.buffer.clear();
            self.pos = 0;

            match self.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(chunk))) => self.buffer = chunk,
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Err(err)),
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

struct DockerProcessHandle {
    container_id: String,
    docker: Arc<bollard::Docker>,
    app_config: Arc<crate::config::Config>,

    resource_usage: tokio::sync::watch::Sender<ResourceUsage>,

    state_task: tokio::task::JoinHandle<()>,
    stats_task: tokio::task::JoinHandle<()>,
}

impl DockerProcessHandle {
    fn new(
        container_id: String,
        docker: Arc<bollard::Docker>,
        app_config: Arc<crate::config::Config>,
        uuid: uuid::Uuid,
        resource_usage: tokio::sync::watch::Sender<ResourceUsage>,
    ) -> Self {
        resource_usage.wipe(ContainerState::Offline);

        let stats_docker = Arc::clone(&docker);
        let stats_id = container_id.clone();
        let stats_usage = resource_usage.clone();

        let stats_task = tokio::spawn(async move {
            let mut prev_cpu_total = 0;
            let mut prev_instant = None;

            loop {
                let mut stream = stats_docker.stats(
                    &stats_id,
                    Some(bollard::query_parameters::StatsOptions {
                        stream: false,
                        one_shot: true,
                    }),
                );

                let (stats, _) =
                    tokio::join!(stream.next(), tokio::time::sleep(Duration::from_secs(1)));

                let Some(stats) = stats else { break };
                let stats = match stats {
                    Ok(stats) => stats,
                    Err(err) => {
                        tracing::warn!(instance = %uuid, "failed to get container stats: {err:?}");
                        continue;
                    }
                };

                stats_usage.send_modify(|usage| {
                    if let Some(memory_stats) = &stats.memory_stats {
                        let mut memory_bytes = memory_stats.usage.unwrap_or(0);

                        if let Some(stats) = &memory_stats.stats {
                            if let Some(&inactive_file) = stats.get("total_inactive_file")
                                && inactive_file < memory_bytes
                            {
                                memory_bytes -= inactive_file;
                            } else if let Some(&inactive_file) = stats.get("inactive_file")
                                && inactive_file < memory_bytes
                            {
                                memory_bytes -= inactive_file;
                            }
                        }

                        usage.memory_bytes = memory_bytes;
                        usage.memory_limit_bytes = memory_stats.limit.unwrap_or(0);
                    }

                    if let Some(cpu_stats) = &stats.cpu_stats
                        && let Some(cpu_usage) = &cpu_stats.cpu_usage
                    {
                        let total_usage = cpu_usage.total_usage.unwrap_or(0);
                        let now = Instant::now();

                        usage.cpu_absolute = if let Some(prev) = prev_instant {
                            let cpu_delta_ns = total_usage.saturating_sub(prev_cpu_total) as f64;
                            let wall_delta_ns = now.duration_since(prev).as_nanos() as f64;

                            if wall_delta_ns > 0.0 && cpu_delta_ns > 0.0 {
                                ((cpu_delta_ns / wall_delta_ns) * 100.0 * 1000.0).round() / 1000.0
                            } else {
                                0.0
                            }
                        } else {
                            0.0
                        };

                        prev_cpu_total = total_usage;
                        prev_instant = Some(now);
                    }
                });
            }
        });

        let state_docker = Arc::clone(&docker);
        let state_id = container_id.clone();
        let state_usage = resource_usage.clone();

        let state_task = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;

                let inspect = match state_docker.inspect_container(&state_id, None).await {
                    Ok(inspect) => inspect,
                    Err(DockerResponseServerError {
                        status_code: 404, ..
                    }) => Default::default(),
                    Err(err) => {
                        tracing::warn!(instance = %uuid, "failed to inspect container for state: {err:?}");
                        continue;
                    }
                };
                let state = inspect.state.unwrap_or_default();

                let (container_state, uptime) = match state.status {
                    Some(bollard::models::ContainerStateStatusEnum::RUNNING) => {
                        let uptime = state
                            .started_at
                            .as_deref()
                            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                            .map(|started| {
                                chrono::Utc::now()
                                    .signed_duration_since(started.with_timezone(&chrono::Utc))
                                    .num_milliseconds()
                                    .max(0) as u64
                            })
                            .unwrap_or(0);
                        (ContainerState::Running, uptime)
                    }
                    Some(bollard::models::ContainerStateStatusEnum::PAUSED) => {
                        (ContainerState::Stopping, 0)
                    }
                    _ => (ContainerState::Offline, 0),
                };

                state_usage.send_modify(|usage| {
                    usage.state = container_state;
                    usage.uptime = uptime;
                });
            }
        });

        Self {
            container_id,
            docker,
            app_config,
            resource_usage,
            state_task,
            stats_task,
        }
    }
}

impl Drop for DockerProcessHandle {
    fn drop(&mut self) {
        self.state_task.abort();
        self.stats_task.abort();

        self.resource_usage.wipe(ContainerState::Offline);
    }
}

#[async_trait::async_trait]
impl super::ProcessHandle for DockerProcessHandle {
    async fn exec(&self, options: ExecOptions) -> Result<ExecStream, anyhow::Error> {
        let exec = self
            .docker
            .create_exec(
                &self.container_id,
                bollard::exec::CreateExecOptions {
                    attach_stdin: Some(true),
                    attach_stdout: Some(true),
                    attach_stderr: Some(true),
                    tty: Some(options.tty),
                    cmd: Some(options.command),
                    user: options.user,
                    working_dir: options.working_dir,
                    ..Default::default()
                },
            )
            .await?;

        match self
            .docker
            .start_exec(
                &exec.id,
                Some(bollard::exec::StartExecOptions {
                    detach: false,
                    tty: options.tty,
                    ..Default::default()
                }),
            )
            .await?
        {
            bollard::exec::StartExecResults::Attached { output, input } => {
                let docker = Arc::clone(&self.docker);
                let exec_id = exec.id;
                let stderr = Arc::new(RwLock::new(Vec::new()));

                Ok(ExecStream {
                    // stderr is kept out of the payload, a dump would carry the
                    // tool's chatter otherwise
                    output: output
                        .filter_map({
                            let stderr = Arc::clone(&stderr);

                            move |result| {
                                std::future::ready(match result {
                                    Ok(bollard::container::LogOutput::StdErr { message }) => {
                                        let mut stderr = stderr.write();
                                        if stderr.len() < crate::instance::STDERR_CAPTURE_LIMIT {
                                            stderr.extend_from_slice(&message);
                                        }

                                        None
                                    }
                                    Ok(log) => Some(Ok(log.into_bytes())),
                                    Err(err) => Some(Err(anyhow::Error::from(err))),
                                })
                            }
                        })
                        .chain(futures_util::stream::once(async move {
                            match docker.inspect_exec(&exec_id).await?.exit_code {
                                Some(code) if code != 0 => {
                                    let message = {
                                        let stderr = stderr.read();
                                        String::from_utf8_lossy(&stderr).trim().to_string()
                                    };

                                    Err(if message.is_empty() {
                                        anyhow::anyhow!("exec exited with code {code}")
                                    } else {
                                        anyhow::anyhow!("exec exited with code {code}: {message}")
                                    })
                                }
                                _ => Ok(bytes::Bytes::new()),
                            }
                        }))
                        .boxed(),
                    stdin: input,
                })
            }
            bollard::exec::StartExecResults::Detached => {
                Err(anyhow::anyhow!("exec session detached unexpectedly"))
            }
        }
    }

    async fn logs(
        &self,
        lines: Option<usize>,
    ) -> Result<Box<dyn tokio::io::AsyncRead + Send + Unpin>, anyhow::Error> {
        let stream = self
            .docker
            .logs(
                &self.container_id,
                Some(bollard::query_parameters::LogsOptions {
                    follow: false,
                    stdout: true,
                    stderr: true,
                    timestamps: false,
                    tail: lines.map_or_else(|| "all".to_string(), |n| n.to_string()),
                    ..Default::default()
                }),
            )
            .map(|result| {
                result
                    .map(|log| log.into_bytes().to_vec())
                    .map_err(std::io::Error::other)
            });

        Ok(Box::new(LogsReader {
            stream: stream.boxed(),
            buffer: Vec::new(),
            pos: 0,
        }))
    }

    async fn update_resources(&self, data: &StoredInstance) -> Result<(), anyhow::Error> {
        self.docker
            .update_container(
                &self.container_id,
                data.container_update_config(&self.app_config),
            )
            .await
            .map_err(Into::into)
    }

    async fn start(&self) -> Result<(), anyhow::Error> {
        self.resource_usage
            .send_modify(|usage| usage.state = ContainerState::Starting);
        self.docker
            .start_container(
                &self.container_id,
                None::<bollard::query_parameters::StartContainerOptions>,
            )
            .await
            .map_err(Into::into)
    }

    async fn stop(&self) -> Result<(), anyhow::Error> {
        self.resource_usage
            .send_modify(|usage| usage.state = ContainerState::Stopping);
        self.docker
            .stop_container(
                &self.container_id,
                Some(bollard::query_parameters::StopContainerOptions {
                    t: Some(30),
                    ..Default::default()
                }),
            )
            .await
            .map_err(Into::into)
    }

    async fn kill(&self) -> Result<(), anyhow::Error> {
        self.docker
            .kill_container(
                &self.container_id,
                Some(bollard::query_parameters::KillContainerOptions {
                    signal: "SIGKILL".to_string(),
                }),
            )
            .await
            .map_err(Into::into)
    }
}

#[async_trait::async_trait]
impl super::ContainerExecutor for DockerExecutor {
    async fn boot(&self) -> Result<(), anyhow::Error> {
        self.docker.version().await?;

        if std::env::var("OCI_CONTAINER").is_ok() {
            match host_mounts::HostMountTable::discover(&self.docker).await {
                Ok(table) => {
                    table.validate_directories(&self.app_config.load())?;

                    tracing::info!(
                        "running in container {}, translating bind mount sources to host paths",
                        table.container_id().get(..12).unwrap_or_default()
                    );
                    for (destination, source) in table.mounts() {
                        if destination != source {
                            tracing::info!(
                                "translating bind mount sources under {} to {}",
                                destination.display(),
                                source.display()
                            );
                        }
                    }

                    let _ = self.host_mounts.set(Some(table));
                }
                Err(err) => {
                    tracing::warn!(
                        "running in a container, but failed to inspect own container: {err:#}"
                    );
                    tracing::warn!(
                        "bind mount sources will be passed to the container engine untranslated, host paths must match the db-agent container's paths exactly"
                    );
                    let _ = self.host_mounts.set(None);
                }
            }
        }

        Ok(())
    }

    async fn setup_instance_process(
        &self,
        instance: &super::super::Instance,
    ) -> Result<Arc<dyn super::ProcessHandle>, anyhow::Error> {
        let data = instance.data.read().await.clone();
        let data_dir = self.app_config.data_path(data.uuid);

        let rootless = self.app_config.load().docker.rootless.enabled;

        self.pull_image(instance, &data.image).await?;
        tokio::fs::create_dir_all(data_dir.join("volumes")).await?;
        for mapping in &data.volumes {
            let host_path = mapping.host_path(&self.app_config, data.uuid);
            tokio::fs::create_dir_all(&host_path).await?;
            if !rootless {
                std::os::unix::fs::chown(&host_path, Some(data.image_uid), Some(data.image_gid))?;
            }
        }

        let socket_dir = self.app_config.socket_path(data.uuid);
        tokio::fs::create_dir_all(&socket_dir).await?;
        if !rootless {
            std::os::unix::fs::chown(&socket_dir, Some(data.image_uid), Some(data.image_gid))?;
        }

        let bollard_config = data.container_config(&self.app_config, self.host_mounts());

        let container = self
            .docker
            .create_container(
                Some(bollard::query_parameters::CreateContainerOptions {
                    name: Some(data.uuid.to_string()),
                    ..Default::default()
                }),
                bollard_config,
            )
            .await?;

        Ok(Arc::new(DockerProcessHandle::new(
            container.id,
            Arc::clone(&self.docker),
            Arc::clone(&self.app_config),
            data.uuid,
            instance.resource_usage.clone(),
        )))
    }

    async fn attach_instance_process(
        &self,
        instance: &super::super::Instance,
    ) -> Result<Arc<dyn super::ProcessHandle>, anyhow::Error> {
        let container_id = find_container(
            &self.docker,
            &instance.uuid.to_string(),
            Some(CONTAINER_TYPE_DATABASE),
        )
        .await
        .ok_or_else(|| anyhow::anyhow!("no running database container found"))?;

        Ok(Arc::new(DockerProcessHandle::new(
            container_id,
            Arc::clone(&self.docker),
            Arc::clone(&self.app_config),
            instance.uuid,
            instance.resource_usage.clone(),
        )))
    }

    async fn cleanup_instance_process(
        &self,
        instance: &super::super::Instance,
    ) -> Result<(), anyhow::Error> {
        let containers = self
            .docker
            .list_containers(Some(bollard::query_parameters::ListContainersOptions {
                all: true,
                filters: Some(container_filters(
                    &instance.uuid.to_string(),
                    Some(CONTAINER_TYPE_DATABASE),
                )),
                ..Default::default()
            }))
            .await?;

        for c in containers {
            let Some(id) = c.id else { continue };
            if let Err(err) = self
                .docker
                .remove_container(
                    &id,
                    Some(bollard::query_parameters::RemoveContainerOptions {
                        force: true,
                        ..Default::default()
                    }),
                )
                .await
            {
                tracing::error!(instance = %instance.uuid, container = %id, "failed to remove container: {err}");
            }
        }

        Ok(())
    }

    async fn run_networked_container(
        &self,
        instance: &super::super::Instance,
        options: super::NetworkedContainerOptions,
    ) -> Result<
        futures_util::stream::BoxStream<'static, Result<bytes::Bytes, anyhow::Error>>,
        anyhow::Error,
    > {
        let data = instance.data.read().await.clone();
        self.pull_image(instance, &data.image).await?;

        let name = format!(
            "{}_{CONTAINER_TYPE_SCRIPT_RUNNER}_{}",
            instance.uuid,
            rand::distr::Alphanumeric.sample_string(&mut rand::rng(), 8)
        );

        let container = self
            .docker
            .create_container(
                Some(bollard::query_parameters::CreateContainerOptions {
                    name: Some(name),
                    ..Default::default()
                }),
                bollard::models::ContainerCreateBody {
                    image: Some(data.image.trim_end_matches('~').to_string()),
                    entrypoint: Some(vec![String::new()]),
                    cmd: Some(options.command),
                    env: Some(options.env),
                    user: Some(format!("{}:{}", data.image_uid, data.image_gid)),
                    labels: Some(HashMap::from([
                        ("Service".to_string(), crate::SERVICE_NAME.to_string()),
                        (
                            "ContainerType".to_string(),
                            CONTAINER_TYPE_SCRIPT_RUNNER.to_string(),
                        ),
                    ])),
                    host_config: Some(bollard::models::HostConfig {
                        extra_hosts: Some(options.extra_hosts),
                        log_config: Some(bollard::models::HostConfigLogConfig {
                            typ: Some("none".to_string()),
                            config: None,
                        }),
                        auto_remove: Some(true),
                        ..data.base_host_config(&self.app_config)
                    }),
                    attach_stdout: Some(true),
                    attach_stderr: Some(true),
                    tty: Some(false),
                    ..Default::default()
                },
            )
            .await?;

        let guard = ContainerGuard {
            docker: Arc::clone(&self.docker),
            container_id: container.id.clone(),
        };

        let attach = self
            .docker
            .attach_container(
                &container.id,
                Some(bollard::query_parameters::AttachContainerOptions {
                    stream: true,
                    stdout: true,
                    stderr: true,
                    ..Default::default()
                }),
            )
            .await?;

        let (exit_tx, exit_rx) = tokio::sync::oneshot::channel();
        tokio::spawn({
            let docker = Arc::clone(&self.docker);
            let id = container.id.clone();

            async move {
                exit_tx.send(
                    docker
                        .wait_container(
                            &id,
                            Some(bollard::query_parameters::WaitContainerOptions {
                                condition: "removed".to_string(),
                            }),
                        )
                        .next()
                        .await,
                )
            }
        });

        self.docker
            .start_container(
                &container.id,
                None::<bollard::query_parameters::StartContainerOptions>,
            )
            .await?;

        let stderr = Arc::new(RwLock::new(Vec::new()));

        let stream = attach
            .output
            .filter_map({
                let stderr = Arc::clone(&stderr);

                move |result| {
                    std::future::ready(match result {
                        Ok(bollard::container::LogOutput::StdOut { message }) => Some(Ok(message)),
                        Ok(bollard::container::LogOutput::StdErr { message }) => {
                            let mut stderr = stderr.write();
                            if stderr.len() < crate::instance::STDERR_CAPTURE_LIMIT {
                                stderr.extend_from_slice(&message);
                            }

                            None
                        }
                        Ok(_) => None,
                        Err(err) => Some(Err(anyhow::Error::from(err))),
                    })
                }
            })
            .chain(futures_util::stream::once(async move {
                let code = match exit_rx.await {
                    Ok(Some(Err(DockerContainerWaitError { code, .. }))) => code,
                    Ok(Some(Err(err))) => return Err(err.into()),
                    Ok(Some(Ok(_))) => 0,
                    Ok(None) | Err(_) => {
                        anyhow::bail!("could not determine the exit status of the container");
                    }
                };

                if code != 0 {
                    let message = {
                        let stderr = stderr.read();
                        String::from_utf8_lossy(&stderr).trim().to_string()
                    };

                    return Err(if message.is_empty() {
                        anyhow::anyhow!("container exited with code {code}")
                    } else {
                        anyhow::anyhow!("container exited with code {code}: {message}")
                    });
                }

                Ok(bytes::Bytes::new())
            }))
            .boxed();

        Ok(GuardedStream {
            stream,
            _guard: guard,
        }
        .boxed())
    }
}
