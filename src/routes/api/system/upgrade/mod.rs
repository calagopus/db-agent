use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod check;

mod post {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::{ApiError, GetState},
        upgrade::{ReleaseProvider, VersionRequest},
    };
    use axum::http::StatusCode;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;
    use utoipa::ToSchema;

    static UPGRADE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[derive(ToSchema, Deserialize)]
    pub struct Payload {
        version: String,
        token: Option<String>,
    }

    #[derive(ToSchema, Serialize)]
    struct Response {
        applied: bool,
        version: Option<String>,
    }

    async fn busy_reason(state: &crate::routes::AppState) -> Option<String> {
        let manager = &state.instance_manager;
        if !manager.booted() {
            return Some("instances are still booting".to_string());
        }

        let autostarts = manager.autostarts();
        if autostarts > 0 {
            return Some(format!("{autostarts} instance(s) are still autostarting"));
        }

        let transfers = manager.transfers();
        if transfers > 0 {
            return Some(format!("{transfers} import(s) or export(s) are running"));
        }

        for instance in manager.get_instances().await.iter() {
            if instance.power_busy() {
                return Some(format!(
                    "instance {} is changing power state",
                    instance.uuid
                ));
            }
            if !instance.operations.operations().await.is_empty() {
                return Some(format!("instance {} has operations running", instance.uuid));
            }
        }

        None
    }

    #[utoipa::path(post, path = "/", responses(
        (status = OK, body = inline(Response)),
        (status = ACCEPTED, body = inline(Response)),
        (status = BAD_REQUEST, body = ApiError),
        (status = NOT_FOUND, body = ApiError),
        (status = CONFLICT, body = ApiError),
        (status = NOT_IMPLEMENTED, body = ApiError),
    ), request_body = inline(Payload))]
    pub async fn route(
        state: GetState,
        crate::Payload(data): crate::Payload<Payload>,
    ) -> ApiResponseResult {
        if !matches!(state.container_type, crate::routes::AppContainerType::None) {
            return ApiResponse::error(
                "upgrades are not supported in containerized environments, update the image instead",
            )
            .with_status(StatusCode::NOT_IMPLEMENTED)
            .ok();
        }

        if state.config.load().ignore_upgrades {
            return ApiResponse::new_serialized(Response {
                applied: false,
                version: None,
            })
            .ok();
        }

        let Some(provider) = ReleaseProvider::available() else {
            return ApiResponse::error("this build has no release provider for this platform")
                .with_status(StatusCode::NOT_IMPLEMENTED)
                .ok();
        };
        let Ok(request) = data.version.parse::<VersionRequest>() else {
            return ApiResponse::error("version must be a semver version or latest")
                .with_status(StatusCode::BAD_REQUEST)
                .ok();
        };

        let Ok(guard) = UPGRADE_LOCK.try_lock() else {
            return ApiResponse::error("an upgrade is already in progress")
                .with_status(StatusCode::CONFLICT)
                .ok();
        };

        if let Some(reason) = busy_reason(&state).await {
            return ApiResponse::error(&format!("unable to upgrade right now: {reason}"))
                .with_status(StatusCode::CONFLICT)
                .ok();
        }

        let executable = crate::upgrade::executable()?;
        let client = reqwest::Client::builder()
            .user_agent("Calagopus DB Agent (https://github.com/calagopus/db-agent)")
            .connect_timeout(std::time::Duration::from_secs(30))
            .build()?;
        let token = data.token.as_deref();

        let Some(release) = provider.resolve(&client, &request, token).await? else {
            return ApiResponse::error("release or release asset for this platform not found")
                .with_status(StatusCode::NOT_FOUND)
                .ok();
        };

        let current = crate::upgrade::current_version()?;
        if release.version < current {
            return ApiResponse::error(&format!(
                "refusing to downgrade from {current} to {}",
                release.version
            ))
            .with_status(StatusCode::CONFLICT)
            .ok();
        }
        if release.version == current && crate::GIT_BRANCH == "unknown" {
            return ApiResponse::error(&format!("already running {current}"))
                .with_status(StatusCode::CONFLICT)
                .ok();
        }

        let staged = crate::upgrade::download(&client, &release, token, &executable).await?;
        crate::upgrade::smoke_test(&staged, &release.version).await?;

        tokio::spawn({
            let state = Arc::clone(&state);
            let version = release.version.clone();

            async move {
                let _guard = guard;

                tokio::time::sleep(std::time::Duration::from_secs(1)).await;

                if let Some(reason) = busy_reason(&state).await {
                    tracing::error!("aborting upgrade to {}: {}", version, reason);
                    return;
                }
                if let Err(err) = state.instance_manager.persist(&state.database).await {
                    tracing::error!("aborting upgrade to {}: {:?}", version, err);
                    return;
                }

                tracing::info!(
                    "upgrading from {} to {}, restarting",
                    crate::VERSION,
                    version
                );
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;

                let err = crate::upgrade::replace_and_restart(staged, &executable);
                tracing::error!("failed to upgrade to {}: {:?}", version, err);
            }
        });

        ApiResponse::new_serialized(Response {
            applied: true,
            version: Some(release.version.to_string()),
        })
        .with_status(StatusCode::ACCEPTED)
        .ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .routes(routes!(post::route))
        .nest("/check", check::router(state))
        .with_state(state.clone())
}
