use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod get {
    use crate::{
        instance::resources::ContainerState,
        response::{ApiResponse, ApiResponseResult},
        routes::GetState,
        subsystems::status::Connections,
    };
    use axum::http::{HeaderMap, StatusCode};
    use serde::Serialize;
    use utoipa::ToSchema;

    #[derive(ToSchema, Serialize, Clone, Copy, PartialEq, Eq)]
    #[serde(rename_all = "snake_case")]
    enum HealthStatus {
        Healthy,
        Degraded,
        Unhealthy,
    }

    #[derive(ToSchema, Serialize)]
    struct ResponseService {
        status: HealthStatus,

        #[serde(skip_serializing_if = "Option::is_none")]
        bind: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tls: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        connections: Option<Connections>,
    }

    #[derive(ToSchema, Serialize)]
    struct ResponseServices {
        #[serde(skip_serializing_if = "Option::is_none")]
        postgres: Option<ResponseService>,
        #[serde(skip_serializing_if = "Option::is_none")]
        mariadb: Option<ResponseService>,
        #[serde(skip_serializing_if = "Option::is_none")]
        mongodb: Option<ResponseService>,
        #[serde(skip_serializing_if = "Option::is_none")]
        redis: Option<ResponseService>,

        database: ResponseService,
    }

    #[derive(ToSchema, Serialize)]
    struct ResponseInstances {
        total: usize,
        online: usize,
        offline: usize,
    }

    #[derive(ToSchema, Serialize)]
    struct Response<'a> {
        status: HealthStatus,

        #[schema(inline)]
        services: ResponseServices,

        #[serde(skip_serializing_if = "Option::is_none")]
        version: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        uptime: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        container_type: Option<crate::routes::AppContainerType>,
        #[serde(skip_serializing_if = "Option::is_none")]
        #[schema(inline)]
        instances: Option<ResponseInstances>,
    }

    /// Builds the status of a single subsystem, or `None` if it is disabled in the config.
    ///
    /// `detailed` gates everything an unauthenticated caller must not see: the address the proxy
    /// listens on, whether it terminates TLS, and how many connections it is serving.
    fn service(
        enabled: bool,
        bind: &std::net::SocketAddr,
        tls: bool,
        status: crate::subsystems::status::SubsystemStatus,
        detailed: bool,
    ) -> Option<ResponseService> {
        if !enabled {
            return None;
        }

        Some(ResponseService {
            status: if status.running {
                HealthStatus::Healthy
            } else {
                HealthStatus::Unhealthy
            },
            bind: detailed.then(|| bind.to_string()),
            tls: detailed.then_some(tls),
            connections: detailed.then_some(status.connections),
        })
    }

    /// Unauthenticated health check. Reports whether the api and every subsystem enabled in the
    /// config is serving, and nothing more.
    ///
    /// Supplying a valid `Authorization: Bearer <token>` header additionally reports the agent
    /// version, uptime, instance counts and per-subsystem bind addresses and connection counts.
    /// Responds with `503` if any enabled subsystem is not healthy.
    #[utoipa::path(get, path = "/", responses(
        (status = OK, body = inline(Response)),
        (status = SERVICE_UNAVAILABLE, body = inline(Response)),
    ), security(
        (),
        ("api_key" = []),
    ))]
    pub async fn route(state: GetState, headers: HeaderMap) -> ApiResponseResult {
        let detailed = crate::routes::api::is_authenticated(&state, &headers);

        let database_healthy = sqlx::query("SELECT 1")
            .fetch_optional(state.database.read())
            .await
            .inspect_err(|err| tracing::error!("health check database probe failed: {err:#?}"))
            .is_ok();

        let mut instances = None;
        if detailed {
            let mut counts = ResponseInstances {
                total: 0,
                online: 0,
                offline: 0,
            };

            for instance in state.instance_manager.get_instances().await.iter() {
                counts.total += 1;
                if instance.resource_usage().await.state == ContainerState::Offline {
                    counts.offline += 1;
                } else {
                    counts.online += 1;
                }
            }

            instances = Some(counts);
        }

        // everything below is synchronous, the config guard is never held across an await
        let config = state.config.load();

        let services = ResponseServices {
            postgres: service(
                config.postgres.enabled,
                &config.postgres.bind,
                config.postgres.tls.enabled,
                state.subsystem_registry.postgres.snapshot(),
                detailed,
            ),
            mariadb: service(
                config.mariadb.enabled,
                &config.mariadb.bind,
                config.mariadb.tls.enabled,
                state.subsystem_registry.mariadb.snapshot(),
                detailed,
            ),
            mongodb: service(
                config.mongodb.enabled,
                &config.mongodb.bind,
                config.mongodb.tls.enabled,
                state.subsystem_registry.mongodb.snapshot(),
                detailed,
            ),
            redis: service(
                config.redis.enabled,
                &config.redis.bind,
                config.redis.tls.enabled,
                state.subsystem_registry.redis.snapshot(),
                detailed,
            ),
            database: ResponseService {
                status: if database_healthy {
                    HealthStatus::Healthy
                } else {
                    HealthStatus::Unhealthy
                },
                bind: None,
                tls: None,
                connections: None,
            },
        };

        // the api answered, so it is never fully unhealthy here, only degraded
        let degraded = [
            services.postgres.as_ref(),
            services.mariadb.as_ref(),
            services.mongodb.as_ref(),
            services.redis.as_ref(),
            Some(&services.database),
        ]
        .into_iter()
        .flatten()
        .any(|service| service.status != HealthStatus::Healthy);

        ApiResponse::new_serialized(Response {
            status: if degraded {
                HealthStatus::Degraded
            } else {
                HealthStatus::Healthy
            },
            services,
            version: detailed.then_some(state.version.as_str()),
            uptime: detailed.then_some(state.start_time.elapsed().as_secs()),
            container_type: detailed.then_some(state.container_type),
            instances,
        })
        .with_status(if degraded {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::OK
        })
        .ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .routes(routes!(get::route))
        .with_state(state.clone())
}
