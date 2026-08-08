use super::{
    InnerInstance, Instance,
    resources::{ContainerState, ResourceUsage, ResourceUsageWatchExt},
};
use cap_std::{
    ambient_authority,
    fs::{Dir, MetadataExt},
};
use std::{collections::HashSet, path::Path, sync::Weak, time::Duration};

pub async fn run(
    app_state: crate::routes::State,
    database: Weak<InnerInstance>,
    resource_usage: tokio::sync::watch::Sender<ResourceUsage>,
) {
    tokio::time::sleep(Duration::from_secs(5)).await;

    loop {
        let Some(database) = database.upgrade() else {
            break;
        };
        let database = Instance(database);

        {
            let semaphore = app_state.config.disk_check_concurrency_semaphore.load();
            let _permit = semaphore
                .acquire()
                .await
                .expect("failed to acquire disk check concurrency semaphore");
            database.check_disk_usage(&resource_usage).await;
        }

        let interval = app_state.config.load().disk_check_interval.max(1);
        tokio::time::sleep(Duration::from_secs(interval)).await;
    }
}

impl Instance {
    pub async fn check_disk_usage(
        &self,
        resource_usage: &tokio::sync::watch::Sender<ResourceUsage>,
    ) {
        let path = self.app_state.config.data_path(self.uuid);
        let usage = match tokio::task::spawn_blocking(move || scan_path(&path)).await {
            Ok(Ok(usage)) => usage,
            Ok(Err(err)) => {
                tracing::error!(instance = %self.uuid, "disk usage check failed: {err}");
                return;
            }
            Err(err) => {
                tracing::error!(instance = %self.uuid, "disk usage check panicked: {err}");
                return;
            }
        };

        resource_usage.publish_disk_usage(usage);

        if self.is_disk_full().await && self.resource_usage().state == ContainerState::Running {
            tracing::warn!(
                instance = %self.uuid,
                "instance is exceeding its disk limit ({usage} bytes), stopping",
            );

            if let Err(err) = self.stop().await {
                tracing::error!(instance = %self.uuid, "failed to stop instance over disk limit: {err}");
            }
        }
    }
}

fn scan_path(path: &Path) -> std::io::Result<u64> {
    let dir = match Dir::open_ambient_dir(path, ambient_authority()) {
        Ok(dir) => dir,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(err) => return Err(err),
    };

    let mut seen_inodes = HashSet::new();
    let mut total = 0;
    let mut stack = vec![dir];

    while let Some(dir) = stack.pop() {
        let Ok(entries) = dir.entries() else {
            continue;
        };

        for entry in entries.flatten() {
            let Ok(metadata) = entry.metadata() else {
                continue;
            };

            if metadata.is_dir() {
                total += metadata.blocks() * 512;
                if let Ok(sub) = entry.open_dir() {
                    stack.push(sub);
                }
            } else {
                if metadata.nlink() > 1 && !seen_inodes.insert(metadata.ino()) {
                    continue;
                }
                total += metadata.blocks() * 512;
            }
        }
    }

    Ok(total)
}
