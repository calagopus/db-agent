use anyhow::Context;
use arc_swap::ArcSwap;
use axum::{extract::ConnectInfo, http::HeaderMap};
use serde::{Deserialize, Serialize};
use serde_default::DefaultFromSerde;
use std::{
    fs::File,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};
use tracing_subscriber::fmt::writer::MakeWriterExt;
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

fn database_url() -> String {
    "sqlite://./data/database.db".to_string()
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

fn docker_socket() -> String {
    "/var/run/docker.sock".to_string()
}
fn docker_tmpfs_size() -> u64 {
    100
}
fn docker_container_pid_limit() -> i64 {
    512
}
fn docker_timezone() -> String {
    "UTC".to_string()
}
fn docker_log_config_type() -> String {
    "local".to_string()
}
fn docker_log_config_config() -> std::collections::BTreeMap<String, String> {
    std::collections::BTreeMap::from([
        ("max-size".to_string(), "5m".to_string()),
        ("max-file".to_string(), "1".to_string()),
        ("compress".to_string(), "false".to_string()),
    ])
}

#[derive(ToSchema, Deserialize, Serialize, DefaultFromSerde)]
pub struct Tls {
    #[serde(default)]
    pub enabled: bool,
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

        #[serde(default)]
        pub ignore_config_updates: bool,

        #[serde(default = "disk_check_interval")]
        pub disk_check_interval: u64,
        #[serde(default = "disk_check_concurrency")]
        pub disk_check_concurrency: usize,

        #[serde(default)]
        #[schema(inline)]
        pub postgres: #[derive(ToSchema, Deserialize, Serialize, DefaultFromSerde)] pub struct Postgres {
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
        pub mariadb: #[derive(ToSchema, Deserialize, Serialize, DefaultFromSerde)] pub struct Mariadb {
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
        pub mongodb: #[derive(ToSchema, Deserialize, Serialize, DefaultFromSerde)] pub struct Mongodb {
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
        pub redis: #[derive(ToSchema, Deserialize, Serialize, DefaultFromSerde)] pub struct Redis {
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
        pub database: #[derive(ToSchema, Deserialize, Serialize, DefaultFromSerde)] pub struct DatabaseConfig {
            #[serde(default = "database_url")]
            pub url: String,
            #[serde(default = "database_migrate")]
            pub migrate: bool,
        },

        #[serde(default)]
        #[schema(inline)]
        pub docker: #[derive(ToSchema, Deserialize, Serialize, DefaultFromSerde)] pub struct Docker {
            #[serde(default = "docker_socket")]
            pub socket: String,

            #[serde(default)]
            pub registries: std::collections::HashMap<String, #[derive(ToSchema, Deserialize, Serialize, Clone)] pub struct DockerRegistry {
                pub username: String,
                pub password: String,
            }>,

            #[serde(default = "docker_tmpfs_size")]
            pub tmpfs_size: u64,
            #[serde(default = "docker_container_pid_limit")]
            pub container_pid_limit: i64,
            #[serde(default = "docker_timezone")]
            pub timezone: String,
            #[serde(default)]
            pub userns_mode: String,

            #[serde(default)]
            #[schema(inline)]
            pub log_config: #[derive(ToSchema, Deserialize, Serialize, DefaultFromSerde)] pub struct DockerLogConfig {
                #[serde(default = "docker_log_config_type")]
                pub r#type: String,
                #[serde(default = "docker_log_config_config")]
                pub config: std::collections::BTreeMap<String, String>,
            },
        },

        #[serde(default)]
        #[schema(inline)]
        pub api: #[derive(ToSchema, Deserialize, Serialize, DefaultFromSerde)] pub struct Api {
            #[serde(default = "api_bind")]
            pub bind: String,
            #[serde(default)]
            pub token: String,
            #[serde(default)]
            pub disable_openapi_docs: bool,
            #[serde(default)]
            pub ignore_upgrades: bool,

            #[serde(default)]
            pub tls: Tls,

            #[serde(default)]
            #[schema(value_type = Vec<String>)]
            pub trusted_proxies: Vec<cidr::IpCidr>,
        },
    }
}

pub const FORBIDDEN_PATHS: &[&str] = &["api.token"];

pub type ConfigSnapshot = arc_swap::Guard<Arc<InnerConfig>>;

#[allow(dead_code)]
pub struct LogGuard(
    tracing_appender::non_blocking::WorkerGuard,
    tracing_appender::non_blocking::WorkerGuard,
);

pub struct Config {
    inner: ArcSwap<InnerConfig>,
    pub path: String,
    pub disk_check_semaphore: ArcSwap<tokio::sync::Semaphore>,
}

impl Config {
    pub fn open(path: &str) -> anyhow::Result<Arc<Self>> {
        let inner: InnerConfig = if Path::new(path).exists() {
            let file = File::open(path).context(format!("failed to open config file {path}"))?;
            serde_norway::from_reader(std::io::BufReader::new(file))
                .context(format!("failed to parse config file {path}"))?
        } else {
            tracing::warn!("config file {path} not found, writing defaults");
            InnerConfig::default()
        };

        Self::save_to(path, &inner)?;

        let disk_check_semaphore = ArcSwap::from_pointee(tokio::sync::Semaphore::new(
            inner.disk_check_concurrency.max(1),
        ));

        Ok(Arc::new(Self {
            inner: ArcSwap::from_pointee(inner),
            path: path.to_string(),
            disk_check_semaphore,
        }))
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
            if let Some(forwarded) = headers.get("X-Forwarded-For")
                && let Ok(forwarded) = forwarded.to_str()
                && let Some(ip) = forwarded.split(',').next()
            {
                return ip.trim().parse().unwrap_or_else(|_| connect_info.ip());
            }

            if let Some(forwarded) = headers.get("X-Real-IP")
                && let Ok(forwarded) = forwarded.to_str()
            {
                return forwarded
                    .trim()
                    .parse()
                    .unwrap_or_else(|_| connect_info.ip());
            }
        }

        connect_info.ip()
    }

    pub fn setup_logging(&self, debug: bool) -> anyhow::Result<LogGuard> {
        let debug = debug || self.load().debug;
        let log_dir = self.load().log_dir.clone();
        std::fs::create_dir_all(&log_dir)
            .context(format!("failed to create log directory {log_dir}"))?;

        let rolling = tracing_appender::rolling::Builder::new()
            .filename_prefix("db-agent")
            .filename_suffix("log")
            .max_log_files(30)
            .rotation(tracing_appender::rolling::Rotation::DAILY)
            .build(&log_dir)
            .context("failed to create rolling log file appender")?;

        let (stdout_writer, stdout_guard) = tracing_appender::non_blocking(std::io::stdout());
        let (file_writer, file_guard) = tracing_appender::non_blocking(rolling);

        tracing_subscriber::fmt()
            .with_max_level(if debug {
                tracing::Level::DEBUG
            } else {
                tracing::Level::INFO
            })
            .with_writer(stdout_writer.and(file_writer))
            .with_target(false)
            .init();

        Ok(LogGuard(file_guard, stdout_guard))
    }

    pub fn replace(&self, new: InnerConfig) -> anyhow::Result<()> {
        let old_concurrency = self.load().disk_check_concurrency.max(1);
        let new_concurrency = new.disk_check_concurrency.max(1);
        Self::save_to(&self.path, &new)?;
        self.inner.store(Arc::new(new));

        if new_concurrency != old_concurrency {
            self.disk_check_semaphore
                .store(Arc::new(tokio::sync::Semaphore::new(new_concurrency)));
        }

        Ok(())
    }

    pub fn save_new(path: &str, inner: &InnerConfig) -> anyhow::Result<()> {
        Self::save_to(path, inner)
    }

    fn save_to(path: &str, inner: &InnerConfig) -> anyhow::Result<()> {
        let file = File::create(path).context(format!("failed to create config file {path}"))?;
        serde_norway::to_writer(std::io::BufWriter::new(file), inner)
            .context(format!("failed to write config file {path}"))?;

        Ok(())
    }
}
