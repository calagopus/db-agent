use super::{Database, InnerDatabase, resources::ContainerState};
use cap_std::{
    ambient_authority,
    fs::{Dir, MetadataExt},
};
use std::{
    collections::HashSet,
    path::Path,
    sync::{Weak, atomic::Ordering},
    time::Duration,
};

pub async fn run(app_state: crate::routes::State, database: Weak<InnerDatabase>) {
    tokio::time::sleep(Duration::from_secs(5)).await;

    loop {
        let Some(database) = database.upgrade() else {
            break;
        };
        let database = Database(database);

        {
            let _permit = app_state.config.disk_check_semaphore.acquire().await;
            database.check_disk_usage().await;
        }

        let interval = app_state.config.load().disk_check_interval.max(1);
        tokio::time::sleep(Duration::from_secs(interval)).await;
    }
}

impl Database {
    pub async fn check_disk_usage(&self) {
        let path = self.app_state.config.data_path(self.uuid);
        let usage = match tokio::task::spawn_blocking(move || scan_path(&path)).await {
            Ok(Ok(usage)) => usage,
            Ok(Err(err)) => {
                tracing::error!(database = %self.uuid, "disk usage scan failed: {err}");
                return;
            }
            Err(err) => {
                tracing::error!(database = %self.uuid, "disk usage scan panicked: {err}");
                return;
            }
        };

        self.disk_usage.store(usage, Ordering::Relaxed);

        let disk_limit = self.data.read().await.disk;
        if disk_limit > 0
            && usage >= disk_limit as u64 * 1024 * 1024
            && self.resource_usage().await.state == ContainerState::Running
        {
            tracing::warn!(
                database = %self.uuid,
                "database is exceeding its disk limit ({usage} bytes), stopping",
            );

            if let Err(err) = self.stop().await {
                tracing::error!(database = %self.uuid, "failed to stop database over disk limit: {err}");
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
