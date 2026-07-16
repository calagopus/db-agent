use crate::response::ApiResponse;
use anyhow::Context;
use axum::{
    body::Body,
    extract::{ConnectInfo, Request},
    http::{Response, StatusCode},
    middleware::Next,
    response::IntoResponse,
};
use std::{
    net::SocketAddr,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};
use utoipa::openapi::security::{ApiKey, ApiKeyValue, SecurityScheme};
use utoipa_axum::router::OpenApiRouter;

pub static CLAP_COMMAND: OnceLock<clap::Command> = OnceLock::new();

mod commands;
mod config;
mod database;
mod instance;
mod payload;
mod response;
mod routes;
mod stats;
mod subsystems;
mod tls;
mod utils;

pub use payload::Payload;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const GIT_COMMIT: &str = env!("CARGO_GIT_COMMIT");
const GIT_BRANCH: &str = env!("CARGO_GIT_BRANCH");
const TARGET: &str = env!("CARGO_TARGET");

const DEFAULT_CONFIG_PATH: &str = "/etc/calagopus-db-agent/config.yml";

fn full_version() -> String {
    if GIT_BRANCH == "unknown" {
        VERSION.to_string()
    } else {
        format!("{VERSION}:{GIT_COMMIT}@{GIT_BRANCH}")
    }
}

#[inline(always)]
#[cold]
fn cold_path() {}

#[allow(dead_code)]
#[inline(always)]
fn likely(b: bool) -> bool {
    if b {
        true
    } else {
        cold_path();
        false
    }
}

#[inline(always)]
fn unlikely(b: bool) -> bool {
    if b {
        cold_path();
        true
    } else {
        false
    }
}

macro_rules! exit_error {
    ($msg:expr) => {
        {
            use ::colored::Colorize;
            eprintln!("{}", $msg.red());
            std::process::exit(1);
        }
    };
    ($fmt:expr, $($arg:tt)*) => {
        {
            use ::colored::Colorize;
            eprintln!("{}", format!($fmt, $($arg)*).red());
            std::process::exit(1);
        }
    };
}

fn spawn_subsystem(
    name: &'static str,
    fut: impl std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(err) = fut.await {
            tracing::error!("{name} stopped: {err:?}");
        }
    })
}

fn handle_panic(err: Box<dyn std::any::Any + Send + 'static>) -> Response<Body> {
    let details = if let Some(s) = err.downcast_ref::<String>() {
        s.as_str()
    } else if let Some(s) = err.downcast_ref::<&str>() {
        s
    } else {
        "unknown panic"
    };

    tracing::error!("a panic occurred while handling a request: {}", details);

    ApiResponse::error("internal server error")
        .with_status(StatusCode::INTERNAL_SERVER_ERROR)
        .into_response()
}

async fn handle_request(
    state: axum::extract::State<Arc<crate::routes::AppState>>,
    req: Request<Body>,
    next: Next,
) -> Result<Response<Body>, StatusCode> {
    let ip = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ci| state.config.find_ip(req.headers(), ConnectInfo(ci.0)))
        .unwrap_or_else(|| std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));

    tracing::info!(
        ip = %ip,
        path = req.uri().path(),
        query = req.uri().query().unwrap_or_default(),
        "http {}",
        req.method().to_string().to_lowercase(),
    );

    Ok(crate::response::ACCEPT_HEADER
        .scope(crate::response::accept_from_headers(req.headers()), async {
            next.run(req).await
        })
        .await)
}

async fn main_rt() -> anyhow::Result<()> {
    let cli = commands::CliCommandGroupBuilder::new("db-agent", "Calagopus database agent.");
    let mut cli = commands::commands(cli);
    let mut matches = cli.get_matches();

    CLAP_COMMAND
        .set(cli.get_command())
        .expect("failed to set CLAP_COMMAND");

    let config_path = matches
        .get_one::<String>("config")
        .expect("config path is required")
        .to_string();
    let debug = *matches
        .get_one::<bool>("debug")
        .expect("debug flag is required");

    if let Some((command, arg_matches)) = matches.remove_subcommand() {
        if let Some((func, arg_matches)) = cli.match_command(command, arg_matches) {
            match func(None, arg_matches).await {
                Ok(exit_code) => std::process::exit(exit_code),
                Err(err) => exit_error!(format!(
                    "an error occurred while running cli command: {err:#?}"
                )),
            }
        } else {
            cli.print_help();
            std::process::exit(0);
        }
    }

    let config = config::Config::open(&config_path)?;
    let _log_guard = config.setup_logging(debug)?;

    if let Err(err) = rustls::crypto::aws_lc_rs::default_provider().install_default() {
        exit_error!("Failed to install rustls crypto provider: {:?}", err);
    }

    tracing::info!("db-agent {} loaded from {}", full_version(), config_path);

    spawn_subsystem("ntp clock drift check", async move {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let socket = tokio::net::UdpSocket::bind("0.0.0.0:0").await?;
        let socket = sntpc_net_tokio::UdpSocketWrapper::from(socket);
        let context = sntpc::NtpContext::new(sntpc::StdTimestampGen::default());

        let pool_ntp_addrs = tokio::net::lookup_host(("pool.ntp.org", 123))
            .await
            .context("failed to resolve pool.ntp.org")?;

        let get_pool_time = async |addr: std::net::SocketAddr| {
            tokio::time::timeout(
                std::time::Duration::from_secs(2),
                sntpc::get_time(addr, &socket, context),
            )
            .await?
            .map_err(|err| std::io::Error::other(format!("{err:?}")))
            .context("failed to get time from pool.ntp.org")
        };

        for pool_ntp_addr in pool_ntp_addrs {
            let pool_time = match get_pool_time(pool_ntp_addr).await {
                Ok(time) => time,
                Err(err) => {
                    tracing::warn!("failed to get time from {pool_ntp_addr:?}: {err:?}");
                    continue;
                }
            };

            let duration = std::time::Duration::from_micros(pool_time.offset().unsigned_abs());

            if duration > std::time::Duration::from_secs(5) {
                if pool_time.offset().is_negative() {
                    tracing::warn!(
                        "system clock is behind by {:.2}s according to {pool_ntp_addr:?}",
                        duration.as_secs_f64()
                    );
                } else {
                    tracing::warn!(
                        "system clock is ahead by {:.2}s according to {pool_ntp_addr:?}",
                        duration.as_secs_f64()
                    );
                }
            } else if pool_time.offset().is_negative() {
                tracing::info!(
                    "system clock is behind by {}ms according to {pool_ntp_addr:?}",
                    duration.as_millis()
                );
            } else {
                tracing::info!(
                    "system clock is ahead by {}ms according to {pool_ntp_addr:?}",
                    duration.as_millis()
                );
            }
        }

        Ok(())
    });

    let database = Arc::new(database::Database::new(Arc::clone(&config)).await?);
    let registry = Arc::new(subsystems::SubsystemRegistry::default());
    let database_route_manager = Arc::new(instance::manager::DatabaseRouteManager::default());

    if config.load().postgres.enabled {
        spawn_subsystem(
            "postgres",
            subsystems::postgres::run(
                Arc::clone(&config),
                Arc::clone(&registry.postgres),
                Arc::clone(&database_route_manager),
            ),
        );
    }
    if config.load().mariadb.enabled {
        spawn_subsystem(
            "mariadb",
            subsystems::mariadb::run(
                Arc::clone(&config),
                Arc::clone(&registry.mariadb),
                Arc::clone(&database_route_manager),
            ),
        );
    }
    if config.load().mongodb.enabled {
        spawn_subsystem(
            "mongodb",
            subsystems::mongodb::run(
                Arc::clone(&config),
                Arc::clone(&registry.mongodb),
                Arc::clone(&database_route_manager),
            ),
        );
    }
    if config.load().redis.enabled {
        spawn_subsystem(
            "redis",
            subsystems::redis::run(
                Arc::clone(&config),
                Arc::clone(&registry.redis),
                Arc::clone(&database_route_manager),
            ),
        );
    }

    tracing::info!("connecting to docker");
    let docker = {
        let socket = config.load().docker.socket.clone();
        let docker = if socket.starts_with("http://") || socket.starts_with("tcp://") {
            bollard::Docker::connect_with_http(&socket, 120, bollard::API_DEFAULT_VERSION)
        } else {
            bollard::Docker::connect_with_local(&socket, 120, bollard::API_DEFAULT_VERSION)
        };

        match docker {
            Ok(docker) => Arc::new(docker),
            Err(err) => exit_error!("failed to connect to docker: {:?}", err),
        }
    };

    let container_executor: Arc<dyn instance::executor::ContainerExecutor> = Arc::new(
        instance::executor::docker::DockerExecutor::new(Arc::clone(&docker), Arc::clone(&config)),
    );

    if let Err(err) = container_executor.boot().await {
        exit_error!("failed to boot server executor: {:?}", err);
    }

    let state = Arc::new(routes::AppState {
        start_time: Instant::now(),
        version: full_version(),
        container_type: match std::env::var("OCI_CONTAINER").as_deref() {
            Ok("official") => routes::AppContainerType::Official,
            Ok(_) => routes::AppContainerType::Unknown,
            Err(_) => routes::AppContainerType::None,
        },
        config: Arc::clone(&config),
        database: Arc::clone(&database),
        stats_manager: Arc::new(stats::StatsManager::default()),
        subsystem_registry: Arc::clone(&registry),
        instance_manager: Arc::new(instance::manager::InstanceManager::default()),
        database_route_manager: Arc::clone(&database_route_manager),
        container_executor,
    });

    if let Err(err) = state.instance_manager.initialize(state.clone()).await {
        exit_error!("failed to initialize database manager: {:?}", err);
    }

    let app = OpenApiRouter::new()
        .merge(routes::router(&state))
        .fallback(|| async {
            ApiResponse::error("route not found")
                .with_status(StatusCode::NOT_FOUND)
                .ok()
        })
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            handle_request,
        ))
        .layer(tower_http::catch_panic::CatchPanicLayer::custom(
            handle_panic,
        ))
        .with_state(state.clone());

    let (mut router, mut openapi) = app.split_for_parts();
    openapi.info.version = "1.0.0".into();
    openapi.info.description = None;
    openapi.info.title = "db-agent API".to_string();
    openapi.info.contact = None;
    openapi.info.license = None;
    if let Some(components) = openapi.components.as_mut() {
        components.add_security_scheme(
            "api_key",
            SecurityScheme::ApiKey(ApiKey::Header(ApiKeyValue::new("Authorization"))),
        );
    }

    for (path, item) in openapi.paths.paths.iter_mut() {
        let path = path
            .replace('/', "_")
            .replace(|c| ['{', '}'].contains(&c), "");

        let operations = [
            ("get", &mut item.get),
            ("post", &mut item.post),
            ("put", &mut item.put),
            ("patch", &mut item.patch),
            ("delete", &mut item.delete),
        ];

        for (method, operation) in operations {
            if let Some(operation) = operation {
                operation.operation_id = Some(format!("{method}{path}"));
            }
        }
    }

    if !state.config.load().api.disable_openapi_docs {
        router = router.route(
            "/openapi.json",
            axum::routing::get(|| async move { axum::Json(openapi) }),
        );
    }

    #[cfg(unix)]
    tokio::spawn(async move {
        let Ok(mut signal) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
        else {
            return;
        };

        loop {
            signal.recv().await;
            tracing::info!("received SIGHUP, ignoring");
        }
    });

    let bind = state.config.load().api.bind.clone();

    if let Ok(address) = bind.parse::<SocketAddr>() {
        if state.config.load().api.tls.enabled {
            tracing::info!("loading tls certs");

            let rustls_config = match axum_server::tls_rustls::RustlsConfig::from_pem_file(
                state.config.load().api.tls.cert.as_str(),
                state.config.load().api.tls.key.as_str(),
            )
            .await
            {
                Ok(config) => config,
                Err(err) => exit_error!("failed to load TLS certificate and key: {:?}", err),
            };

            tokio::spawn({
                let rustls_config = rustls_config.clone();
                let config = Arc::clone(&config);

                async move {
                    loop {
                        tokio::time::sleep(std::time::Duration::from_secs(24 * 60 * 60)).await;
                        tracing::info!("reloading tls certs");

                        if let Err(err) = rustls_config
                            .reload_from_pem_file(
                                config.load().api.tls.cert.as_str(),
                                config.load().api.tls.key.as_str(),
                            )
                            .await
                        {
                            tracing::error!("failed to reload TLS certificate and key: {:?}", err);
                        } else {
                            tracing::info!("tls certs reloaded successfully");
                        }
                    }
                }
            });

            tracing::info!(
                "https listening on {} (db-agent {}, {}ms)",
                address,
                state.version,
                state.start_time.elapsed().as_millis()
            );

            match axum_server::bind_rustls(address, rustls_config)
                .serve(router.into_make_service_with_connect_info::<SocketAddr>())
                .await
            {
                Ok(_) => {}
                Err(err) => {
                    if err.kind() == std::io::ErrorKind::AddrInUse {
                        exit_error!("failed to start https server ({} already in use)", address);
                    } else {
                        exit_error!("failed to start https server: {:?}", err);
                    }
                }
            }
        } else {
            tracing::info!(
                "http listening on {} (db-agent {}, {}ms)",
                address,
                state.version,
                state.start_time.elapsed().as_millis()
            );

            match axum::serve(
                match tokio::net::TcpListener::bind(address).await {
                    Ok(listener) => listener,
                    Err(err) => {
                        if err.kind() == std::io::ErrorKind::AddrInUse {
                            exit_error!("failed to start http server ({} already in use)", address);
                        } else {
                            exit_error!("failed to start http server: {:?}", err);
                        }
                    }
                },
                router.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            {
                Ok(_) => {}
                Err(err) => {
                    if err.kind() == std::io::ErrorKind::AddrInUse {
                        exit_error!("failed to start http server ({} already in use)", address);
                    } else {
                        exit_error!("failed to start http server: {:?}", err);
                    }
                }
            }
        }
    } else {
        #[cfg(unix)]
        {
            tracing::info!(
                "http listening on unix socket {} (db-agent {}, {}ms)",
                bind,
                state.version,
                state.start_time.elapsed().as_millis()
            );

            let router = router.layer(axum::middleware::from_fn(
                |mut req: Request<Body>, next: Next| async move {
                    req.extensions_mut()
                        .insert(axum::extract::ConnectInfo(SocketAddr::from((
                            std::net::IpAddr::from([127, 0, 0, 1]),
                            0,
                        ))));
                    next.run(req).await
                },
            ));

            let _ = tokio::fs::remove_file(&bind).await;
            let listener = match tokio::net::UnixListener::bind(&bind) {
                Ok(listener) => listener,
                Err(err) => exit_error!("failed to bind to unix socket ({}): {:?}", bind, err),
            };

            if let Err(err) = axum::serve(listener, router.into_make_service()).await {
                exit_error!("failed to start http server ({}): {:?}", bind, err);
            }
        }
        #[cfg(not(unix))]
        exit_error!("unix socket support is only available on unix systems");
    }

    Ok(())
}

fn main() {
    let thread_count = Arc::new(AtomicUsize::new(0));

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name_fn(move || {
            let count = thread_count.fetch_add(1, Ordering::SeqCst);
            format!("db-agent-rt-{count}")
        })
        .name("db-agent-rt")
        .build()
        .expect("failed to build Tokio runtime")
        .block_on(main_rt())
        .unwrap_or_else(|err: anyhow::Error| exit_error!("fatal: {:?}", err));
}
