use super::{
    InnerInstance, Instance, executor,
    resources::{ContainerState, ResourceUsage},
};
use futures_util::StreamExt;
use std::{sync::Weak, time::Duration};

const PROBE_INTERVAL: Duration = Duration::from_secs(2);
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

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

            if database
                .app_state
                .instance_manager
                .get_instance(database.uuid)
                .await
                .is_none()
            {
                return;
            }

            let sweep = async {
                let dir = database.clickhouse_connection().await?.backup_dir().await?;
                let dir = crate::utils::shell_quote(&dir);
                let keep = crate::utils::shell_quote(&format!(
                    "calagopus-{}-*",
                    *super::connection::clickhouse::BACKUP_RUN_ID
                ));

                let data = database.data.read().await;
                let user = format!("{}:{}", data.image_uid, data.image_gid);
                drop(data);

                let mut stream = database
                    .exec(
                        executor::ExecOptions::new(vec![
                            "sh".to_string(),
                            "-c".to_string(),
                            format!(
                                "test ! -d {dir} || find {dir} -maxdepth 1 -name 'calagopus-*' ! -name {keep} -delete"
                            ),
                        ])
                        .with_user(user),
                    )
                    .await?;
                drop(stream.stdin);
                while let Some(chunk) = stream.output.next().await {
                    chunk?;
                }

                Ok::<_, anyhow::Error>(())
            };

            match tokio::time::timeout(PROBE_TIMEOUT, sweep).await {
                Ok(Ok(())) => break,
                Ok(Err(err)) => {
                    tracing::debug!(instance = %database.uuid, "staged backup sweep failed: {err:#}")
                }
                Err(_) => {
                    tracing::debug!(instance = %database.uuid, "staged backup sweep timed out")
                }
            }

            drop(database);
            tokio::time::sleep(PROBE_INTERVAL).await;
        }
    }
}
