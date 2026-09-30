use super::{
    InnerInstance, Instance,
    resources::{ContainerState, ResourceUsage},
};
use std::{sync::Weak, time::Duration};

const PROBE_INTERVAL: Duration = Duration::from_secs(2);
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// records the mariadb version every time the container comes up, retrying until mysqld listens
pub async fn run(
    database: Weak<InnerInstance>,
    mut resource_usage: tokio::sync::watch::Receiver<ResourceUsage>,
) {
    let mut last_state = None;

    while resource_usage.changed().await.is_ok() {
        let state = resource_usage.borrow_and_update().state;
        if last_state == Some(state) {
            continue;
        }
        last_state = Some(state);

        while resource_usage.borrow().state == ContainerState::Running {
            let Some(database) = database.upgrade() else {
                return;
            };
            let database = Instance(database);

            let socket = database.get_socket_path().await;
            match tokio::time::timeout(
                PROBE_TIMEOUT,
                crate::subsystems::mariadb::read_server_version(&socket),
            )
            .await
            {
                Ok(Ok(version)) => {
                    let app_state = &database.app_state;

                    if app_state
                        .instance_manager
                        .get_instance(database.uuid)
                        .await
                        .is_none()
                    {
                        return;
                    }

                    if !app_state
                        .database_route_manager
                        .mariadb_versions
                        .set(database.uuid, &version)
                    {
                        tracing::debug!(instance = %database.uuid, "ignoring non-mariadb server version {version:?}");
                    }
                    break;
                }
                Ok(Err(err)) => {
                    tracing::debug!(instance = %database.uuid, "server version probe failed: {err}")
                }
                Err(_) => {
                    tracing::debug!(instance = %database.uuid, "server version probe timed out")
                }
            }

            drop(database);
            tokio::time::sleep(PROBE_INTERVAL).await;
        }
    }
}
