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
    path::Path,
    pin::Pin,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::io::ReadBuf;

pub mod cgroup;
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

#[cfg(target_os = "linux")]
fn nofile_ceiling(requested: u64) -> u64 {
    use rustix::process::{Resource, Rlimit, getrlimit, setrlimit};

    let current = getrlimit(Resource::Nofile);
    let current_hard = current.maximum.unwrap_or(u64::MAX);
    if requested <= current_hard {
        return requested;
    }

    let probe = Rlimit {
        current: current.current,
        maximum: Some(requested),
    };

    match setrlimit(Resource::Nofile, probe) {
        Ok(()) => {
            setrlimit(Resource::Nofile, current).ok();

            requested
        }
        Err(_) => current_hard,
    }
}

#[cfg(not(target_os = "linux"))]
fn nofile_ceiling(requested: u64) -> u64 {
    requested
}

fn convert_ulimits(
    config: &crate::config::Config,
) -> Option<Vec<bollard::models::ResourcesUlimits>> {
    static WARNED_CLAMP: OnceLock<()> = OnceLock::new();

    let config = config.load();
    if config.docker.container_ulimits.is_empty() {
        return None;
    }

    Some(
        config
            .docker
            .container_ulimits
            .iter()
            .map(|ulimit| {
                let (mut soft, mut hard) = (ulimit.soft, ulimit.hard);
                if ulimit.name == "nofile" && hard > 0 {
                    let ceiling = nofile_ceiling(hard as u64) as i64;
                    if ceiling < hard {
                        if WARNED_CLAMP.set(()).is_ok() {
                            tracing::warn!(
                                "configured nofile ulimit {} exceeds what this host can set, clamping to {}",
                                hard,
                                ceiling
                            );
                        }

                        hard = ceiling;
                        soft = soft.min(ceiling);
                    }
                }

                bollard::models::ResourcesUlimits {
                    name: Some(ulimit.name.clone()),
                    soft: Some(soft),
                    hard: Some(hard),
                }
            })
            .collect(),
    )
}

fn convert_sysctls(
    config: &crate::config::Config,
    network_mode: &str,
) -> Option<HashMap<String, String>> {
    let config = config.load();
    if config.docker.container_sysctls.is_empty() {
        return None;
    }

    let foreign_netns = network_mode == "host" || network_mode.starts_with("container:");
    let sysctls: HashMap<String, String> = config
        .docker
        .container_sysctls
        .iter()
        .filter(|(key, _)| {
            if key.starts_with("net.") && foreign_netns {
                tracing::debug!(
                    sysctl = %key,
                    network_mode = %network_mode,
                    "skipping net sysctl, container shares a foreign network namespace"
                );

                return false;
            }

            true
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();

    if sysctls.is_empty() {
        None
    } else {
        Some(sysctls)
    }
}

fn selinux_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();

    *ENABLED.get_or_init(|| Path::new("/sys/fs/selinux/enforce").exists())
}

fn is_relabelable(source: &str) -> bool {
    let source = Path::new(source);

    !source.starts_with("/dev") && !source.starts_with("/proc") && !source.starts_with("/sys")
}

fn split_selinux_binds(
    mounts: Vec<bollard::models::Mount>,
) -> (Vec<bollard::models::Mount>, Option<Vec<String>>) {
    split_binds_for_relabel(mounts, selinux_enabled())
}

fn split_binds_for_relabel(
    mounts: Vec<bollard::models::Mount>,
    relabel: bool,
) -> (Vec<bollard::models::Mount>, Option<Vec<String>>) {
    if !relabel {
        return (mounts, None);
    }

    let mut binds = Vec::new();
    let mut structured = Vec::new();

    for mount in mounts {
        match (mount.source.as_deref(), mount.target.as_deref()) {
            (Some(source), Some(target)) if is_relabelable(source) => {
                let mode = if mount.read_only.unwrap_or(false) {
                    "ro"
                } else {
                    "rw"
                };

                binds.push(format!("{source}:{target}:{mode},z"));
            }
            _ => structured.push(mount),
        }
    }

    (structured, (!binds.is_empty()).then_some(binds))
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

    fn base_host_config(
        &self,
        config: &crate::config::Config,
        network_mode: &str,
    ) -> bollard::models::HostConfig;
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
        let memory = (self.memory > 0).then(|| self.memory * 1024 * 1024);

        let mut resources = bollard::models::Resources {
            memory,
            memory_reservation: memory,
            memory_swap: match memory {
                None => {
                    if self.swap != 0 {
                        tracing::warn!(
                            instance = %self.uuid,
                            swap = self.swap,
                            "ignoring the swap limit, it cannot be set without a memory limit"
                        );
                    }

                    None
                }
                Some(memory) => match self.swap {
                    0 => Some(memory),
                    -1 => Some(-1),
                    limit => Some(memory + limit * 1024 * 1024),
                },
            },
            blkio_weight: self.io_weight.and_then(|w| u16::try_from(w).ok()),
            pids_limit: match config.load().docker.container_pid_limit {
                0 => None,
                limit => Some(limit as i64),
            },
            ..Default::default()
        };

        if resources.blkio_weight.is_some() && !cgroup::io_weight_effective() {
            static WARNED_IO_WEIGHT: OnceLock<()> = OnceLock::new();

            if WARNED_IO_WEIGHT.set(()).is_ok() {
                tracing::warn!(
                    instance = %self.uuid,
                    "io weights are configured, but no io scheduler on this host enforces them (needs bfq or an iocost model) - they will have no effect"
                );
            }
        }

        if self.cpu > 0 {
            let period = config.load().docker.cpu_period_us();

            resources.cpu_quota = Some(self.cpu * period / 100);
            resources.cpu_period = Some(period);
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
            blkio_weight: resources.blkio_weight,
            pids_limit: resources.pids_limit,
            ..Default::default()
        }
    }

    fn base_host_config(
        &self,
        config: &crate::config::Config,
        network_mode: &str,
    ) -> bollard::models::HostConfig {
        let resources = self.convert_container_resources(config);
        let cfg = config.load();

        let mut security_opt = vec!["no-new-privileges".to_string()];
        if let Some(profile) = string_to_option(&cfg.docker.container_apparmor_profile) {
            security_opt.push(format!("apparmor={profile}"));
        }

        bollard::models::HostConfig {
            memory: resources.memory,
            memory_reservation: resources.memory_reservation,
            memory_swap: resources.memory_swap,
            cpu_quota: resources.cpu_quota,
            cpu_period: resources.cpu_period,
            blkio_weight: resources.blkio_weight,
            pids_limit: resources.pids_limit,
            shm_size: match cfg.docker.shm_size.as_bytes() {
                0 => None,
                size => Some(size as i64),
            },

            network_mode: string_to_option(network_mode),
            tmpfs: Some(HashMap::from([(
                "/tmp".to_string(),
                format!("rw,exec,nosuid,size={}M", cfg.docker.tmpfs_size.as_mib()),
            )])),
            security_opt: Some(security_opt),
            ulimits: convert_ulimits(config),
            sysctls: convert_sysctls(config, network_mode),
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
        let (mounts, binds) = split_selinux_binds(mounts);

        bollard::models::HostConfig {
            mounts: Some(mounts),
            binds,
            log_config: Some(bollard::models::HostConfigLogConfig {
                typ: Some(cfg.docker.log_config.r#type.clone()),
                config: Some(cfg.docker.log_config.config.clone().into_iter().collect()),
            }),
            ..self.base_host_config(config, "none")
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

const TRANSIENT_JSON_ATTEMPTS: u32 = 4;
const TRANSIENT_JSON_BACKOFF: Duration = Duration::from_millis(50);

#[inline]
fn is_transient_json_error(err: &bollard::errors::Error) -> bool {
    matches!(
        err,
        bollard::errors::Error::JsonDataError { .. }
            | bollard::errors::Error::JsonSerdeError { .. }
    )
}

#[async_trait::async_trait]
trait DockerCompatJsonExt {
    async fn list_containers_settled(
        &self,
        options: Option<bollard::query_parameters::ListContainersOptions>,
    ) -> Result<Vec<bollard::models::ContainerSummary>, bollard::errors::Error>;

    async fn inspect_container_settled(
        &self,
        container_id: &str,
        options: Option<bollard::query_parameters::InspectContainerOptions>,
    ) -> Result<bollard::models::ContainerInspectResponse, bollard::errors::Error>;
}

#[async_trait::async_trait]
impl DockerCompatJsonExt for bollard::Docker {
    async fn list_containers_settled(
        &self,
        options: Option<bollard::query_parameters::ListContainersOptions>,
    ) -> Result<Vec<bollard::models::ContainerSummary>, bollard::errors::Error> {
        for attempt in 1..TRANSIENT_JSON_ATTEMPTS {
            match self.list_containers(options.clone()).await {
                Err(err) if is_transient_json_error(&err) => {
                    tracing::debug!(
                        "container list returned an unparseable state, retrying ({}/{}): {}",
                        attempt,
                        TRANSIENT_JSON_ATTEMPTS,
                        err
                    );

                    tokio::time::sleep(TRANSIENT_JSON_BACKOFF * attempt).await;
                }
                result => return result,
            }
        }

        self.list_containers(options).await
    }

    async fn inspect_container_settled(
        &self,
        container_id: &str,
        options: Option<bollard::query_parameters::InspectContainerOptions>,
    ) -> Result<bollard::models::ContainerInspectResponse, bollard::errors::Error> {
        for attempt in 1..TRANSIENT_JSON_ATTEMPTS {
            match self.inspect_container(container_id, options.clone()).await {
                Err(err) if is_transient_json_error(&err) => {
                    tracing::debug!(
                        container = %container_id,
                        "container inspect returned an unparseable state, retrying ({}/{}): {}",
                        attempt,
                        TRANSIENT_JSON_ATTEMPTS,
                        err
                    );

                    tokio::time::sleep(TRANSIENT_JSON_BACKOFF * attempt).await;
                }
                result => return result,
            }
        }

        self.inspect_container(container_id, options).await
    }
}

#[async_trait::async_trait]
trait DockerRemoveExt {
    async fn remove_container_forgiving(
        &self,
        container_id: &str,
    ) -> Result<(), bollard::errors::Error>;
}

#[async_trait::async_trait]
impl DockerRemoveExt for bollard::Docker {
    async fn remove_container_forgiving(
        &self,
        container_id: &str,
    ) -> Result<(), bollard::errors::Error> {
        let result = self
            .remove_container(
                container_id,
                Some(bollard::query_parameters::RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                }),
            )
            .await;

        match result {
            Ok(()) => Ok(()),
            Err(DockerResponseServerError {
                status_code: 404, ..
            }) => Ok(()),
            Err(err) => match self.inspect_container_settled(container_id, None).await {
                Err(DockerResponseServerError {
                    status_code: 404, ..
                }) => {
                    tracing::debug!(
                        container = %container_id,
                        "container removal reported an error but the container is gone, treating as removed: {}",
                        err
                    );

                    Ok(())
                }
                _ => Err(err),
            },
        }
    }
}

#[async_trait::async_trait]
trait DockerCfsBurstExt {
    async fn write_cfs_burst(&self, container_id: &str, multiple: f64);
    async fn apply_cfs_burst(&self, container_id: &str, config: &crate::config::Config);
    async fn clear_cfs_burst(&self, container_id: &str);
}

#[async_trait::async_trait]
impl DockerCfsBurstExt for bollard::Docker {
    async fn write_cfs_burst(&self, container_id: &str, multiple: f64) {
        for attempt in 0..2 {
            let inspect = match self.inspect_container_settled(container_id, None).await {
                Ok(inspect) => inspect,
                Err(err) => {
                    tracing::debug!(
                        container = %container_id,
                        "failed to inspect container for cfs burst: {}",
                        err
                    );

                    return;
                }
            };

            let Some(pid) = inspect
                .state
                .and_then(|state| state.pid)
                .filter(|pid| *pid > 0)
            else {
                return;
            };

            match cgroup::CpuCgroup::write_process_burst(pid, multiple) {
                cgroup::BurstOutcome::CgroupGone if attempt == 0 => continue,
                _ => return,
            }
        }
    }

    async fn apply_cfs_burst(&self, container_id: &str, config: &crate::config::Config) {
        let burst = config.load().docker.cfs_burst;

        if burst.enabled {
            self.write_cfs_burst(container_id, burst.multiple).await;
        }
    }

    async fn clear_cfs_burst(&self, container_id: &str) {
        self.write_cfs_burst(container_id, 0.0).await;
    }
}

pub struct DockerExecutor {
    docker: Arc<bollard::Docker>,
    app_config: Arc<crate::config::Config>,
    stats_sampler: Arc<cgroup::StatsSampler>,
    host_mounts: OnceLock<Option<host_mounts::HostMountTable>>,
    chown_refused: AtomicBool,
}

impl DockerExecutor {
    pub fn new(docker: Arc<bollard::Docker>, app_config: Arc<crate::config::Config>) -> Self {
        Self {
            docker,
            app_config,
            stats_sampler: Arc::new(cgroup::StatsSampler::default()),
            host_mounts: OnceLock::new(),
            chown_refused: AtomicBool::new(false),
        }
    }

    /// A rootless engine can refuse to hand ownership to the image's uid/gid. The
    /// files are already owned by the mapped user in that case, so the refusal is
    /// absorbed once and every later chown is skipped.
    fn chown(&self, path: &Path, uid: u32, gid: u32) -> Result<(), std::io::Error> {
        if self.chown_refused.load(Ordering::Relaxed) {
            return Ok(());
        }

        let Err(err) = std::os::unix::fs::chown(path, Some(uid), Some(gid)) else {
            return Ok(());
        };

        if !self.app_config.load().docker.rootless.enabled {
            return Err(err);
        }

        if !self.chown_refused.swap(true, Ordering::Relaxed) {
            tracing::debug!(
                "chown refused under a rootless engine, leaving ownership as written: {}",
                err
            );
        }

        Ok(())
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
        quiet: bool,
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
                            let duration = config.load().docker.registry_image_fetch_cache.duration;
                            cache.retain(
                                |_,
                                 timestamp: &mut Arc<
                                    tokio::sync::Mutex<Option<std::time::Instant>>,
                                >| {
                                    match timestamp.try_lock() {
                                        Ok(timestamp) => timestamp
                                            .is_some_and(|t| t.elapsed().as_secs() < duration),
                                        Err(_) => true,
                                    }
                                },
                            );
                        }
                    }
                });

                cache
            })
        };

        let cache_config = self.app_config.load().docker.registry_image_fetch_cache;

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

        if cache_config.background_refresh && self.image_exists(image_name).await {
            let entry = {
                let mut cache = pull_cache.lock();
                Arc::clone(cache.entry(image.into()).or_default())
            };

            if let Ok(mut last_pull) = entry.try_lock_owned() {
                let stale = !cache_config.enabled
                    || last_pull.is_none_or(|pulled_at| {
                        pulled_at.elapsed().as_secs() >= cache_config.duration
                    });

                if stale {
                    let docker = Arc::clone(&self.docker);
                    let image_name = image_name.to_string();
                    let tag = tag.to_string();

                    tokio::spawn(async move {
                        let mut stream = docker.create_image(
                            Some(bollard::query_parameters::CreateImageOptions {
                                from_image: Some(image_name.clone()),
                                tag: Some(tag),
                                ..Default::default()
                            }),
                            None,
                            registry_auth,
                        );

                        while let Some(status) = stream.next().await {
                            if let Err(err) = status {
                                tracing::debug!(
                                    image = %image_name,
                                    "background image refresh failed: {}",
                                    err
                                );

                                return;
                            }
                        }

                        *last_pull = Some(std::time::Instant::now());

                        tracing::debug!(image = %image_name, "background image refresh finished");
                    });
                }
            }

            tracing::debug!(
                instance = %uuid,
                image = %image_name,
                "image exists locally, starting from it and refreshing in the background"
            );

            return Ok(());
        }

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

        if !quiet {
            instance
                .log_daemon("Pulling database image, this could take a few minutes to complete...");
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
                    let message = match info.id {
                        Some(id) => {
                            match info.status.as_deref().map(str::to_lowercase).as_deref() {
                                Some("downloading") => pull_progress(
                                    &id,
                                    PullProgressStatus::Pulling,
                                    info.progress_detail,
                                ),
                                Some("extracting") => pull_progress(
                                    &id,
                                    PullProgressStatus::Extracting,
                                    info.progress_detail,
                                ),
                                Some("download complete" | "pull complete") => Some(
                                    WebsocketMessage::builder(
                                        WebsocketEvent::InstanceImagePullCompleted,
                                    )
                                    .arg(id)
                                    .build(),
                                ),
                                _ => None,
                            }
                        }
                        None => match info.status {
                            Some(status) if !quiet => Some(
                                WebsocketMessage::builder(WebsocketEvent::InstanceConsoleOutput)
                                    .arg(status)
                                    .build(),
                            ),
                            _ => None,
                        },
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

                    if !quiet {
                        instance.log_daemon(format!("failed to pull image: {err}"));
                    }

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

        if !quiet {
            instance.log_daemon("Finished pulling database image");
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

async fn find_running_container(
    docker: &bollard::Docker,
    name_filter: &str,
    container_type: Option<&str>,
) -> Option<String> {
    let mut filters = container_filters(name_filter, container_type);
    filters.insert("status".to_string(), vec!["running".to_string()]);

    let containers = docker
        .list_containers_settled(Some(bollard::query_parameters::ListContainersOptions {
            all: true,
            filters: Some(filters),
            ..Default::default()
        }))
        .await
        .unwrap_or_default();

    for c in containers {
        if c.state != Some(bollard::models::ContainerSummaryStateEnum::RUNNING) {
            continue;
        }

        if let Some(id) = c.id {
            return Some(id);
        }
    }

    None
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
    cfs_lock: tokio::sync::Mutex<()>,
    stdout_rx: tokio::sync::broadcast::Receiver<Arc<String>>,

    state_task: tokio::task::JoinHandle<()>,
    stats_task: tokio::task::JoinHandle<()>,
}

impl DockerProcessHandle {
    async fn new(
        container_id: String,
        docker: Arc<bollard::Docker>,
        app_config: Arc<crate::config::Config>,
        stats_sampler: Arc<cgroup::StatsSampler>,
        uuid: uuid::Uuid,
        resource_usage: tokio::sync::watch::Sender<ResourceUsage>,
    ) -> Result<Self, anyhow::Error> {
        resource_usage.wipe(ContainerState::Offline);

        let (stdout_tx, stdout_rx) =
            tokio::sync::broadcast::channel::<Arc<String>>(app_config.load().websocket_log_count);

        let mut attach = docker
            .attach_container(
                &container_id,
                Some(bollard::query_parameters::AttachContainerOptions {
                    stdout: true,
                    stderr: true,
                    stream: true,
                    ..Default::default()
                }),
            )
            .await?;

        // intentionally not aborted on drop so that it can finish writing any remaining logs to the channel
        tokio::spawn(async move {
            let mut line_buffer = crate::io::line_buffer::LineBuffer::new();

            let emit = |slice: &[u8]| {
                stdout_tx
                    .send(Arc::new(String::from_utf8_lossy(slice).into_owned()))
                    .ok();
            };

            while let Some(Ok(data)) = attach.output.next().await {
                line_buffer.extend(&data.into_bytes());

                while let Some(line) = line_buffer.next_line() {
                    emit(line);
                }

                line_buffer.compact();
            }

            if let Some(line) = line_buffer.flush() {
                emit(line);
            }

            tracing::debug!(instance = %uuid, "stdout task ended");
        });

        let stats_docker = Arc::clone(&docker);
        let stats_id = container_id.clone();
        let stats_usage = resource_usage.clone();

        let stats_task = tokio::spawn(async move {
            enum StatsSource {
                Unresolved,
                Cgroup(cgroup::SampleReceiver),
                Api,
            }

            let mut source = StatsSource::Unresolved;
            let mut prev_cpu_total = 0;
            let mut prev_at = None;

            let mut tick = tokio::time::interval_at(
                tokio::time::Instant::now() + Duration::from_secs(1),
                Duration::from_secs(1),
            );
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                let received = match &mut source {
                    StatsSource::Cgroup(samples) => {
                        match tokio::time::timeout(Duration::from_secs(3), samples.recv()).await {
                            Ok(Some(result)) => Some(result),
                            Ok(None) | Err(_) => {
                                tracing::debug!(
                                    instance = %uuid,
                                    "cgroup stats sampler stopped delivering, using the stats api"
                                );
                                source = StatsSource::Api;

                                continue;
                            }
                        }
                    }
                    _ => {
                        tick.tick().await;

                        None
                    }
                };

                let offline = stats_usage.borrow().state == ContainerState::Offline;
                if offline {
                    stats_usage.wipe(ContainerState::Offline);
                    source = StatsSource::Unresolved;
                    prev_at = None;

                    continue;
                }

                if matches!(source, StatsSource::Unresolved) {
                    source = match stats_docker
                        .inspect_container_settled(&stats_id, None)
                        .await
                    {
                        Ok(inspect) => {
                            match inspect
                                .state
                                .and_then(|state| state.pid)
                                .filter(|pid| *pid > 0)
                            {
                                Some(pid) => match cgroup::StatFiles::resolve(pid) {
                                    Some(files) => {
                                        StatsSource::Cgroup(stats_sampler.register(files))
                                    }
                                    None => {
                                        tracing::debug!(
                                            instance = %uuid,
                                            "container cgroup not resolvable from here, using the stats api"
                                        );

                                        StatsSource::Api
                                    }
                                },
                                None => continue,
                            }
                        }
                        Err(_) => continue,
                    };

                    if matches!(source, StatsSource::Cgroup(_)) {
                        continue;
                    }
                }

                let sample = match received {
                    Some(Ok(sample)) => sample,
                    Some(Err(err)) if err.kind() == std::io::ErrorKind::NotFound => {
                        source = StatsSource::Unresolved;

                        continue;
                    }
                    Some(Err(err)) => {
                        tracing::debug!(
                            instance = %uuid,
                            "failed to read container cgroup stats, using the stats api: {err}"
                        );
                        source = StatsSource::Api;

                        continue;
                    }
                    None => {
                        let mut stream = stats_docker.stats(
                            &stats_id,
                            Some(bollard::query_parameters::StatsOptions {
                                stream: false,
                                one_shot: true,
                            }),
                        );

                        let stats = match stream.next().await {
                            Some(Ok(stats)) => stats,
                            Some(Err(err)) => {
                                tracing::warn!(instance = %uuid, "failed to get container stats: {err:?}");
                                continue;
                            }
                            None => break,
                        };

                        let mut memory_bytes = stats
                            .memory_stats
                            .as_ref()
                            .and_then(|memory| memory.usage)
                            .unwrap_or(0);
                        if let Some(stats) = stats
                            .memory_stats
                            .as_ref()
                            .and_then(|memory| memory.stats.as_ref())
                            && let Some(&inactive_file) = stats
                                .get("total_inactive_file")
                                .or_else(|| stats.get("inactive_file"))
                            && inactive_file < memory_bytes
                        {
                            memory_bytes -= inactive_file;
                        }

                        cgroup::StatSample {
                            memory_bytes,
                            memory_limit_bytes: stats
                                .memory_stats
                                .as_ref()
                                .and_then(|memory| memory.limit)
                                .unwrap_or(0),
                            cpu_total_ns: stats
                                .cpu_stats
                                .as_ref()
                                .and_then(|cpu| cpu.cpu_usage.as_ref())
                                .and_then(|cpu| cpu.total_usage)
                                .unwrap_or(0),
                            at: Instant::now(),
                        }
                    }
                };

                stats_usage.send_modify(|usage| {
                    usage.memory_bytes = sample.memory_bytes;
                    usage.memory_limit_bytes = sample.memory_limit_bytes;

                    usage.cpu_absolute = if let Some(prev) = prev_at {
                        let cpu_delta_ns =
                            sample.cpu_total_ns.saturating_sub(prev_cpu_total) as f64;
                        let wall_delta_ns = sample.at.duration_since(prev).as_nanos() as f64;

                        if wall_delta_ns > 0.0 && cpu_delta_ns > 0.0 {
                            ((cpu_delta_ns / wall_delta_ns) * 100.0 * 1000.0).round() / 1000.0
                        } else {
                            0.0
                        }
                    } else {
                        0.0
                    };

                    prev_cpu_total = sample.cpu_total_ns;
                    prev_at = Some(sample.at);
                });
            }
        });

        let state_docker = Arc::clone(&docker);
        let state_id = container_id.clone();
        let state_usage = resource_usage.clone();

        let state_task = tokio::spawn(async move {
            const RECONCILE_INTERVAL: Duration = Duration::from_secs(10);

            struct CachedState {
                status: Option<bollard::models::ContainerStateStatusEnum>,
                started_at: Option<chrono::DateTime<chrono::Utc>>,
            }

            let arm_wait = || {
                state_docker.wait_container(
                    &state_id,
                    Some(bollard::query_parameters::WaitContainerOptions {
                        condition: "next-exit".to_string(),
                    }),
                )
            };

            let mut wait_stream = arm_wait();
            let mut wait_armed = true;
            let mut cached: Option<CachedState> = None;
            let mut last_inspect: Option<Instant> = None;

            let mut tick = tokio::time::interval_at(
                tokio::time::Instant::now() + Duration::from_secs(1),
                Duration::from_secs(1),
            );
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                let died = tokio::select! {
                    _ = wait_stream.next(), if wait_armed => {
                        wait_armed = false;

                        true
                    }
                    _ = tick.tick() => false,
                };

                if died
                    || cached.is_none()
                    || last_inspect.is_none_or(|at| at.elapsed() >= RECONCILE_INTERVAL)
                {
                    let inspect = match state_docker
                        .inspect_container_settled(&state_id, None)
                        .await
                    {
                        Ok(inspect) => inspect,
                        Err(DockerResponseServerError {
                            status_code: 404, ..
                        }) => Default::default(),
                        Err(err) => {
                            tracing::warn!(instance = %uuid, "failed to inspect container for state: {err:?}");
                            continue;
                        }
                    };
                    last_inspect = Some(Instant::now());

                    let state = inspect.state.unwrap_or_default();

                    cached = Some(CachedState {
                        status: state.status,
                        started_at: state.started_at.as_deref().and_then(|started_at| {
                            chrono::DateTime::parse_from_rfc3339(started_at)
                                .ok()
                                .map(|started_at| started_at.with_timezone(&chrono::Utc))
                        }),
                    });
                }

                let Some(state) = &cached else {
                    continue;
                };

                let (container_state, uptime) = match state.status {
                    Some(bollard::models::ContainerStateStatusEnum::RUNNING) => {
                        let uptime = state
                            .started_at
                            .map(|started| {
                                chrono::Utc::now()
                                    .signed_duration_since(started)
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

                if !wait_armed && container_state == ContainerState::Running {
                    wait_stream = arm_wait();
                    wait_armed = true;
                }

                state_usage.send_modify(|usage| {
                    usage.state = container_state;
                    usage.uptime = uptime;
                });
            }
        });

        Ok(Self {
            container_id,
            docker,
            app_config,
            resource_usage,
            cfs_lock: tokio::sync::Mutex::new(()),
            stdout_rx,
            state_task,
            stats_task,
        })
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

    async fn subscribe_stdout_lines(
        &self,
    ) -> Result<tokio::sync::broadcast::Receiver<Arc<String>>, anyhow::Error> {
        Ok(self.stdout_rx.resubscribe())
    }

    async fn update_resources(&self, data: &StoredInstance) -> Result<(), anyhow::Error> {
        let _cfs_guard = self.cfs_lock.lock().await;

        self.docker.clear_cfs_burst(&self.container_id).await;
        self.docker
            .update_container(
                &self.container_id,
                data.container_update_config(&self.app_config),
            )
            .await?;
        self.docker
            .apply_cfs_burst(&self.container_id, &self.app_config)
            .await;

        Ok(())
    }

    async fn start(&self) -> Result<(), anyhow::Error> {
        self.resource_usage
            .send_modify(|usage| usage.state = ContainerState::Starting);
        self.docker
            .start_container(
                &self.container_id,
                None::<bollard::query_parameters::StartContainerOptions>,
            )
            .await?;

        let _cfs_guard = self.cfs_lock.lock().await;
        self.docker
            .apply_cfs_burst(&self.container_id, &self.app_config)
            .await;

        Ok(())
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

        self.pull_image(instance, &data.image, false).await?;
        tokio::fs::create_dir_all(data_dir.join("volumes")).await?;
        for mapping in &data.volumes {
            let host_path = mapping.host_path(&self.app_config, data.uuid);
            tokio::fs::create_dir_all(&host_path).await?;
            self.chown(&host_path, data.image_uid, data.image_gid)?;
        }

        let socket_dir = self.app_config.socket_path(data.uuid);
        tokio::fs::create_dir_all(&socket_dir).await?;
        self.chown(&socket_dir, data.image_uid, data.image_gid)?;

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

        Ok(Arc::new(
            DockerProcessHandle::new(
                container.id,
                Arc::clone(&self.docker),
                Arc::clone(&self.app_config),
                Arc::clone(&self.stats_sampler),
                data.uuid,
                instance.resource_usage.clone(),
            )
            .await?,
        ))
    }

    async fn attach_instance_process(
        &self,
        instance: &super::super::Instance,
    ) -> Result<Arc<dyn super::ProcessHandle>, anyhow::Error> {
        let container_id = find_running_container(
            &self.docker,
            &instance.uuid.to_string(),
            Some(CONTAINER_TYPE_DATABASE),
        )
        .await
        .ok_or_else(|| anyhow::anyhow!("no running database container found"))?;

        self.docker
            .apply_cfs_burst(&container_id, &self.app_config)
            .await;

        Ok(Arc::new(
            DockerProcessHandle::new(
                container_id,
                Arc::clone(&self.docker),
                Arc::clone(&self.app_config),
                Arc::clone(&self.stats_sampler),
                instance.uuid,
                instance.resource_usage.clone(),
            )
            .await?,
        ))
    }

    async fn cleanup_instance_process(
        &self,
        instance: &super::super::Instance,
    ) -> Result<(), anyhow::Error> {
        let containers = self
            .docker
            .list_containers_settled(Some(bollard::query_parameters::ListContainersOptions {
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
            if let Err(err) = self.docker.remove_container_forgiving(&id).await {
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
        self.pull_image(instance, &data.image, true).await?;

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
                        ..data.base_host_config(&self.app_config, "")
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

#[cfg(test)]
mod tests {
    use super::*;

    // selinux relabelling

    fn bind(source: &str, target: &str, read_only: bool) -> bollard::models::Mount {
        bollard::models::Mount {
            typ: Some(bollard::models::MountType::BIND),
            source: Some(source.to_string()),
            target: Some(target.to_string()),
            read_only: Some(read_only),
            ..Default::default()
        }
    }

    #[test]
    fn without_selinux_mounts_stay_structured() {
        let (mounts, binds) = split_binds_for_relabel(
            vec![bind(
                "/var/lib/calagopus-db-agent/volumes/a",
                "/data",
                false,
            )],
            false,
        );

        assert_eq!(mounts.len(), 1);
        assert_eq!(binds, None);
    }

    #[test]
    fn with_selinux_binds_carry_the_shared_relabel_option() {
        let (mounts, binds) = split_binds_for_relabel(
            vec![
                bind("/var/lib/calagopus-db-agent/volumes/a", "/data", false),
                bind("/run/calagopus-db-agent/sockets/a", "/var/run/mysqld", true),
            ],
            true,
        );

        assert!(mounts.is_empty());
        assert_eq!(
            binds,
            Some(vec![
                "/var/lib/calagopus-db-agent/volumes/a:/data:rw,z".to_string(),
                "/run/calagopus-db-agent/sockets/a:/var/run/mysqld:ro,z".to_string(),
            ])
        );
    }

    #[test]
    fn kernel_filesystems_are_never_relabelled() {
        let (mounts, binds) = split_binds_for_relabel(
            vec![
                bind("/dev/hugepages", "/dev/hugepages", false),
                bind("/var/lib/calagopus-db-agent/volumes/a", "/data", false),
            ],
            true,
        );

        assert_eq!(
            mounts.first().and_then(|mount| mount.source.as_deref()),
            Some("/dev/hugepages")
        );
        assert_eq!(
            binds,
            Some(vec![
                "/var/lib/calagopus-db-agent/volumes/a:/data:rw,z".to_string()
            ])
        );
    }
}
