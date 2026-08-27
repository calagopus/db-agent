use anyhow::Context;
use arc_swap::ArcSwap;
use axum::{extract::ConnectInfo, http::HeaderMap};
use serde::{Deserialize, Serialize};
use serde_default::DefaultFromSerde;
use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    io::BufRead,
    net::SocketAddr,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{
    filter::Targets,
    fmt::writer::MakeWriterExt,
    layer::{Layered, SubscriberExt},
    util::SubscriberInitExt,
};
use utoipa::ToSchema;

fn tls_cert() -> String {
    "cert.pem".to_string()
}
fn tls_key() -> String {
    "key.pem".to_string()
}

fn socket_dir() -> String {
    "/run/calagopus-db-agent".to_string()
}
fn data_dir() -> String {
    "/var/lib/calagopus-db-agent/data".to_string()
}
fn log_dir() -> String {
    "/var/log/calagopus-db-agent".to_string()
}

fn disk_check_interval() -> u64 {
    60
}
fn disk_check_concurrency() -> usize {
    5
}
fn boot_autostart_concurrency() -> usize {
    5
}
fn websocket_log_count() -> usize {
    150
}

fn database_url() -> String {
    "sqlite:///var/lib/calagopus-db-agent/data/database.db".to_string()
}
fn database_migrate() -> bool {
    true
}

fn postgres_enabled() -> bool {
    true
}
fn postgres_bind() -> SocketAddr {
    SocketAddr::from(([0, 0, 0, 0], 5432))
}
fn mariadb_enabled() -> bool {
    true
}
fn mariadb_bind() -> SocketAddr {
    SocketAddr::from(([0, 0, 0, 0], 3306))
}
fn mongodb_enabled() -> bool {
    true
}
fn mongodb_bind() -> SocketAddr {
    SocketAddr::from(([0, 0, 0, 0], 27017))
}
fn redis_enabled() -> bool {
    true
}
fn redis_bind() -> SocketAddr {
    SocketAddr::from(([0, 0, 0, 0], 6379))
}
fn api_bind() -> String {
    "0.0.0.0:8090".to_string()
}
fn api_remote_import_blocked_cidrs() -> Vec<cidr::IpCidr> {
    // SAFETY: every literal below is a valid cidr
    unsafe {
        Vec::from([
            cidr::IpCidr::from_str("0.0.0.0/8").unwrap_unchecked(),
            cidr::IpCidr::from_str("127.0.0.0/8").unwrap_unchecked(),
            cidr::IpCidr::from_str("10.0.0.0/8").unwrap_unchecked(),
            cidr::IpCidr::from_str("100.64.0.0/10").unwrap_unchecked(),
            cidr::IpCidr::from_str("172.16.0.0/12").unwrap_unchecked(),
            cidr::IpCidr::from_str("192.168.0.0/16").unwrap_unchecked(),
            cidr::IpCidr::from_str("169.254.0.0/16").unwrap_unchecked(),
            cidr::IpCidr::from_str("192.0.0.0/24").unwrap_unchecked(),
            cidr::IpCidr::from_str("198.18.0.0/15").unwrap_unchecked(),
            cidr::IpCidr::from_str("224.0.0.0/4").unwrap_unchecked(),
            cidr::IpCidr::from_str("240.0.0.0/4").unwrap_unchecked(),
            cidr::IpCidr::from_str("::/128").unwrap_unchecked(),
            cidr::IpCidr::from_str("::1/128").unwrap_unchecked(),
            cidr::IpCidr::from_str("fe80::/10").unwrap_unchecked(),
            cidr::IpCidr::from_str("fc00::/7").unwrap_unchecked(),
            cidr::IpCidr::from_str("2002::/16").unwrap_unchecked(),
            cidr::IpCidr::from_str("ff00::/8").unwrap_unchecked(),
        ])
    }
}

fn tcp_congestion_control() -> String {
    "bbr".to_string()
}

fn docker_socket() -> String {
    "/var/run/docker.sock".to_string()
}
fn docker_tmpfs_size() -> MiB {
    100u64.into()
}
fn docker_container_pid_limit() -> u64 {
    512
}
fn docker_timezone() -> String {
    if let Ok(tz) = std::env::var("TZ") {
        return tz;
    } else if let Ok(tz) = File::open("/etc/timezone") {
        let mut buf = String::new();
        if std::io::BufReader::new(tz).read_line(&mut buf).is_ok() {
            let tz = buf.trim();
            if !tz.is_empty() {
                return tz.to_string();
            }
        }
    }

    chrono::Local::now().offset().to_string()
}
fn docker_registry_image_fetch_cache_enabled() -> bool {
    true
}
fn docker_registry_image_fetch_cache_duration() -> u64 {
    5 * 60
}
fn docker_cpu_period() -> u64 {
    100000
}
fn docker_cfs_burst_enabled() -> bool {
    true
}
fn docker_cfs_burst_multiple() -> f64 {
    1.0
}
fn docker_log_config_type() -> String {
    "local".to_string()
}
fn docker_log_config_config() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("max-size".to_string(), "5m".to_string()),
        ("max-file".to_string(), "1".to_string()),
        ("compress".to_string(), "false".to_string()),
    ])
}

/// Represents a size in Mebibytes (MiB). The inner value is the number of MiB (not bytes!!).
#[derive(
    ToSchema, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default,
)]
#[serde(transparent)]
#[repr(transparent)]
pub struct MiB(u64);

impl MiB {
    pub fn as_bytes(self) -> u64 {
        self.0 * 1024 * 1024
    }

    pub fn as_mib(self) -> u64 {
        self.0
    }
}

impl From<u64> for MiB {
    fn from(value: u64) -> Self {
        MiB(value)
    }
}

impl From<i64> for MiB {
    fn from(value: i64) -> Self {
        MiB(value as u64)
    }
}

#[derive(Clone, ToSchema, Deserialize, Serialize, DefaultFromSerde)]
#[serde(default)]
pub struct Tls {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub ktls_enabled: bool,
    #[serde(default = "tls_cert")]
    pub cert: String,
    #[serde(default = "tls_key")]
    pub key: String,
}

nestify::nest! {
    #[derive(ToSchema, Deserialize, Serialize, DefaultFromSerde)]
    pub struct InnerConfig {
        #[serde(default)]
        pub debug: bool,

        #[serde(default = "socket_dir")]
        pub socket_dir: String,
        #[serde(default = "data_dir")]
        pub data_dir: String,
        #[serde(default = "log_dir")]
        pub log_dir: String,

        #[serde(default = "disk_check_interval")]
        pub disk_check_interval: u64,
        #[serde(default = "disk_check_concurrency")]
        pub disk_check_concurrency: usize,

        #[serde(default = "boot_autostart_concurrency")]
        pub boot_autostart_concurrency: usize,

        #[serde(default = "websocket_log_count")]
        pub websocket_log_count: usize,

        #[serde(default = "tcp_congestion_control")]
        pub tcp_congestion_control: String,

        #[serde(default)]
        #[schema(inline)]
        pub postgres: #[derive(ToSchema, Deserialize, Serialize, DefaultFromSerde)] #[serde(default)] pub struct Postgres {
            #[serde(default = "postgres_enabled")]
            pub enabled: bool,
            #[serde(default = "postgres_bind")]
            #[schema(value_type = String)]
            pub bind: SocketAddr,
            #[serde(default)]
            pub tls: Tls,
        },

        #[serde(default)]
        #[schema(inline)]
        pub mariadb: #[derive(ToSchema, Deserialize, Serialize, DefaultFromSerde)] #[serde(default)] pub struct Mariadb {
            #[serde(default = "mariadb_enabled")]
            pub enabled: bool,
            #[serde(default = "mariadb_bind")]
            #[schema(value_type = String)]
            pub bind: SocketAddr,
            #[serde(default)]
            pub tls: Tls,
        },

        #[serde(default)]
        #[schema(inline)]
        pub mongodb: #[derive(ToSchema, Deserialize, Serialize, DefaultFromSerde)] #[serde(default)] pub struct Mongodb {
            #[serde(default = "mongodb_enabled")]
            pub enabled: bool,
            #[serde(default = "mongodb_bind")]
            #[schema(value_type = String)]
            pub bind: SocketAddr,
            #[serde(default)]
            pub tls: Tls,
        },

        #[serde(default)]
        #[schema(inline)]
        pub redis: #[derive(ToSchema, Deserialize, Serialize, DefaultFromSerde)] #[serde(default)] pub struct Redis {
            #[serde(default = "redis_enabled")]
            pub enabled: bool,
            #[serde(default = "redis_bind")]
            #[schema(value_type = String)]
            pub bind: SocketAddr,
            #[serde(default)]
            pub tls: Tls,
        },

        #[serde(default)]
        #[schema(inline)]
        pub database: #[derive(ToSchema, Deserialize, Serialize, DefaultFromSerde)] #[serde(default)] pub struct DatabaseConfig {
            #[serde(default = "database_url")]
            pub url: String,
            #[serde(default = "database_migrate")]
            pub migrate: bool,
        },

        #[serde(default)]
        #[schema(inline)]
        pub docker: #[derive(ToSchema, Deserialize, Serialize, DefaultFromSerde)] #[serde(default)] pub struct Docker {
            #[serde(default = "docker_socket")]
            pub socket: String,

            #[serde(default)]
            #[schema(inline)]
            pub registries: HashMap<String, #[derive(ToSchema, Deserialize, Serialize)] pub struct DockerRegistryConfiguration {
                pub username: String,
                pub password: String,
            }>,

            #[serde(default = "docker_tmpfs_size")]
            pub tmpfs_size: MiB,
            #[serde(default)]
            pub shm_size: MiB,
            #[serde(default = "docker_container_pid_limit")]
            pub container_pid_limit: u64,
            #[serde(default)]
            pub container_apparmor_profile: String,
            #[serde(default)]
            #[schema(inline)]
            pub container_ulimits: Vec<#[derive(Clone, ToSchema, Deserialize, Serialize)] pub struct DockerUlimit {
                pub name: String,
                pub soft: i64,
                pub hard: i64,
            }>,
            #[serde(default)]
            pub container_sysctls: HashMap<String, String>,
            #[serde(default = "docker_timezone")]
            pub timezone: String,
            #[serde(default)]
            pub userns_mode: String,

            #[serde(default = "docker_cpu_period")]
            pub cpu_period: u64,

            #[serde(default)]
            #[schema(inline)]
            pub cfs_burst: #[derive(Clone, Copy, ToSchema, Deserialize, Serialize, DefaultFromSerde)] #[serde(default)] pub struct DockerCfsBurst {
                #[serde(default = "docker_cfs_burst_enabled")]
                pub enabled: bool,
                #[serde(default = "docker_cfs_burst_multiple")]
                pub multiple: f64,
            },

            #[serde(default)]
            #[schema(inline)]
            pub registry_image_fetch_cache: #[derive(Clone, Copy, ToSchema, Deserialize, Serialize, DefaultFromSerde)] #[serde(default)] pub struct DockerRegistryImageFetchCache {
                #[serde(default = "docker_registry_image_fetch_cache_enabled")]
                pub enabled: bool,
                #[serde(default = "docker_registry_image_fetch_cache_duration")]
                pub duration: u64,
                #[serde(default)]
                pub background_refresh: bool,
            },

            #[serde(default)]
            #[schema(inline)]
            pub rootless: #[derive(ToSchema, Deserialize, Serialize, DefaultFromSerde)] #[serde(default)] pub struct DockerRootless {
                #[serde(default)]
                pub enabled: bool,
            },

            #[serde(default)]
            #[schema(inline)]
            pub log_config: #[derive(ToSchema, Deserialize, Serialize, DefaultFromSerde)] #[serde(default)] pub struct DockerLogConfig {
                #[serde(default = "docker_log_config_type")]
                pub r#type: String,
                #[serde(default = "docker_log_config_config")]
                pub config: BTreeMap<String, String>,
            },
        },

        #[serde(default)]
        #[schema(inline)]
        pub api: #[derive(ToSchema, Deserialize, Serialize, DefaultFromSerde)] #[serde(default)] pub struct Api {
            #[serde(default = "api_bind")]
            pub bind: String,

            #[serde(default)]
            pub tls: Tls,

            #[serde(default)]
            pub token: String,
            #[serde(default)]
            pub disable_openapi_docs: bool,
            #[serde(default)]
            pub disable_remote_import: bool,
            #[serde(default = "api_remote_import_blocked_cidrs")]
            #[schema(value_type = Vec<String>)]
            pub remote_import_blocked_cidrs: Vec<cidr::IpCidr>,

            #[serde(default)]
            #[schema(value_type = Vec<String>)]
            pub trusted_proxies: Vec<cidr::IpCidr>,
        },

        #[serde(default)]
        pub ignore_config_updates: bool,
        #[serde(default)]
        pub ignore_upgrades: bool,
    }
}

impl Docker {
    /// The configured CFS period in microseconds, clamped to what the kernel accepts.
    pub fn cpu_period_us(&self) -> i64 {
        self.cpu_period.clamp(1000, 1000000) as i64
    }
}

pub const FORBIDDEN_PATHS: &[&str] = &[
    "ignore_config_updates",
    "ignore_upgrades",
    "socket_dir",
    "data_dir",
    "log_dir",
    "docker.socket",
    "api.token",
    "api.bind",
    "api.tls",
    "api.trusted_proxies",
    "api.disable_remote_import",
    "api.remote_import_blocked_cidrs",
];

pub type ConfigSnapshot = arc_swap::Guard<Arc<InnerConfig>>;
type ReloadHandle =
    tracing_subscriber::reload::Handle<Targets, Layered<LevelFilter, tracing_subscriber::Registry>>;

fn log_filter(debug: bool) -> Targets {
    let crate_level = if debug {
        LevelFilter::DEBUG
    } else {
        LevelFilter::INFO
    };

    Targets::new()
        .with_default(LevelFilter::INFO)
        .with_target("db_agent", crate_level)
}

#[allow(dead_code)]
pub struct LogGuard(
    tracing_appender::non_blocking::WorkerGuard,
    tracing_appender::non_blocking::WorkerGuard,
);

pub struct Config {
    inner: ArcSwap<InnerConfig>,
    log_reload_handle: ReloadHandle,
    pub path: String,
    pub disk_check_concurrency_semaphore: ArcSwap<tokio::sync::Semaphore>,
}

impl Config {
    pub const DEFAULT_PATH: &'static str = "/etc/calagopus-db-agent/config.yml";

    pub fn find() -> Option<&'static str> {
        let paths = ["/etc/calagopus-db-agent/config.yml", "./config.yml"];

        paths
            .iter()
            .find(|path| std::path::Path::new(path).exists())
            .copied()
    }

    pub fn open(
        path: &str,
        debug: bool,
        ignore_debug: bool,
    ) -> anyhow::Result<(Arc<Self>, LogGuard)> {
        let file = File::open(path).context(format!("failed to open config file {path}"))?;
        let reader = std::io::BufReader::new(file);
        let inner: InnerConfig = serde_norway::from_reader(reader)
            .context(format!("failed to parse config file {path}"))?;

        Self::ensure_directories(&inner)?;

        let (stdout_writer, stdout_guard) = tracing_appender::non_blocking(std::io::stdout());

        let latest_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(Path::new(&inner.log_dir).join("db-agent.log"))
            .context("failed to open latest log file")?;

        let rolling = tracing_appender::rolling::Builder::new()
            .filename_prefix("db-agent")
            .filename_suffix("log")
            .max_log_files(30)
            .rotation(tracing_appender::rolling::Rotation::DAILY)
            .build(&inner.log_dir)
            .context("failed to create rolling log file appender")?;

        let (file_writer, file_guard) =
            tracing_appender::non_blocking::NonBlockingBuilder::default()
                .buffered_lines_limit(50)
                .finish(latest_file.and(rolling));

        Self::save_to(path, &inner)?;

        let (reload_layer, log_reload_handle) = tracing_subscriber::reload::Layer::new(log_filter(
            (inner.debug || debug) && !ignore_debug,
        ));

        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_timer(tracing_subscriber::fmt::time::ChronoLocal::new(
                "%Y-%m-%d %H:%M:%S %z".to_string(),
            ))
            .with_writer(stdout_writer.and(file_writer))
            .with_target(false)
            .with_level(true)
            .with_file(true)
            .with_line_number(true);

        tracing_subscriber::registry()
            .with(LevelFilter::DEBUG)
            .with(reload_layer)
            .with(fmt_layer)
            .try_init()
            .context("failed to install tracing subscriber")?;

        let disk_check_concurrency_semaphore = ArcSwap::from_pointee(tokio::sync::Semaphore::new(
            inner.disk_check_concurrency.max(1),
        ));

        let config = Arc::new(Self {
            inner: ArcSwap::from_pointee(inner),
            log_reload_handle,
            path: path.to_string(),
            disk_check_concurrency_semaphore,
        });

        Ok((config, LogGuard(file_guard, stdout_guard)))
    }

    fn ensure_directories(inner: &InnerConfig) -> std::io::Result<()> {
        for dir in [&inner.log_dir, &inner.socket_dir, &inner.data_dir] {
            let path = Path::new(dir);

            if !path.exists() {
                std::fs::create_dir_all(path)?;
            }

            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;

                let mode = std::fs::metadata(path)?.permissions().mode();
                if mode & 0o077 != 0 {
                    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode & !0o077))?;
                }
            }
        }

        Ok(())
    }

    #[inline]
    pub fn socket_path(&self, database_uuid: uuid::Uuid) -> PathBuf {
        Path::new(&self.load().socket_dir).join(database_uuid.to_string())
    }

    #[inline]
    pub fn data_path(&self, database_uuid: uuid::Uuid) -> PathBuf {
        Path::new(&self.load().data_dir).join(database_uuid.to_string())
    }

    #[inline]
    pub fn load(&self) -> ConfigSnapshot {
        self.inner.load()
    }

    pub fn find_ip(
        &self,
        headers: &HeaderMap,
        connect_info: ConnectInfo<std::net::SocketAddr>,
    ) -> std::net::IpAddr {
        let cfg = self.load();

        let trusted = headers
            .get("X-Real-Ip-Token")
            .and_then(|token| token.to_str().ok())
            .is_some_and(|token| {
                constant_time_eq::constant_time_eq(token.as_bytes(), cfg.api.token.as_bytes())
            })
            || cfg
                .api
                .trusted_proxies
                .iter()
                .any(|cidr| cidr.contains(&connect_info.ip()));

        if trusted {
            fn find_forwarded_ip(
                forwarded: &str,
                trusted_proxies: &[cidr::IpCidr],
            ) -> Option<std::net::IpAddr> {
                for entry in forwarded.rsplit(',') {
                    let ip: std::net::IpAddr = entry.trim().parse().ok()?;

                    if !trusted_proxies.iter().any(|cidr| cidr.contains(&ip)) {
                        return Some(ip);
                    }
                }

                None
            }

            if let Some(forwarded) = headers.get("X-Forwarded-For")
                && let Ok(forwarded) = forwarded.to_str()
                && let Some(ip) = find_forwarded_ip(forwarded, &cfg.api.trusted_proxies)
            {
                return ip;
            }

            if let Some(forwarded) = headers.get("X-Real-IP")
                && let Ok(forwarded) = forwarded.to_str()
                && let Ok(ip) = forwarded.trim().parse()
            {
                return ip;
            }
        }

        connect_info.ip()
    }

    pub fn replace(&self, new: InnerConfig) -> anyhow::Result<()> {
        Self::save_to(&self.path, &new)?;

        let old_debug = self.load().debug;
        let new_debug = new.debug;
        let old_concurrency = self.load().disk_check_concurrency.max(1);
        let new_concurrency = new.disk_check_concurrency.max(1);
        self.inner.store(Arc::new(new));

        if old_debug != new_debug {
            self.log_reload_handle
                .modify(|filter| *filter = log_filter(new_debug))
                .context("failed to reload tracing level filter")?;
        }

        if new_concurrency != old_concurrency {
            self.disk_check_concurrency_semaphore
                .store(Arc::new(tokio::sync::Semaphore::new(new_concurrency)));
        }

        Ok(())
    }

    fn save_to(path: &str, inner: &InnerConfig) -> anyhow::Result<()> {
        let mut options = std::fs::OpenOptions::new();
        options.create(true).write(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }

        let file = options
            .open(path)
            .context(format!("failed to create config file {path}"))?;
        let writer = std::io::BufWriter::new(file);
        serde_norway::to_writer(writer, inner)
            .context(format!("failed to write config file {path}"))?;

        Ok(())
    }

    pub fn save_new(path: &str, config: InnerConfig) -> anyhow::Result<()> {
        if let Some(parent) = Path::new(path).parent() {
            std::fs::create_dir_all(parent)
                .context(format!("failed to create config directory {path}"))?;
        }

        Self::save_to(path, &config)
    }
}
