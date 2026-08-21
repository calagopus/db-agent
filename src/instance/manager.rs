use super::{Credentials, DatabaseType, identifier::UserIdentifier, resources::ContainerState};
use futures_util::TryStreamExt;
use parking_lot::RwLock;
use std::{
    collections::HashMap,
    sync::{Arc, Weak},
    time::Duration,
};
use tokio::sync::Semaphore;

#[derive(Default)]
pub struct InstanceManager {
    instances: tokio::sync::RwLock<Vec<super::Instance>>,
}

impl InstanceManager {
    pub async fn boot(&self, app_state: &crate::routes::State) {
        let mut states = Self::stored_states(app_state).await;

        let mut write = self.instances.write().await;

        let autostart_concurrency = app_state.config.load().boot_autostart_concurrency;
        let semaphore = Arc::new(Semaphore::new(autostart_concurrency));

        let mut instances_stream =
            sqlx::query_as::<_, crate::database::data::StoredInstance>("SELECT * FROM instances")
                .fetch(app_state.database.read());

        loop {
            let instance = match instances_stream.try_next().await {
                Ok(Some(instance)) => instance,
                Ok(None) => break,
                Err(err) => {
                    tracing::error!("failed to read stored instances: {err:#}");
                    break;
                }
            };

            let instance = super::Instance::new(instance, app_state.clone());
            let state = states.remove(&instance.uuid).unwrap_or_default();

            if let Err(err) = instance.resync_users().await {
                tracing::warn!(instance = %instance.uuid, "failed to resync users: {err}");
            }
            let attached = instance.attach_container().await;

            let autostart = !attached
                && autostart_concurrency > 0
                && matches!(state, ContainerState::Running | ContainerState::Starting)
                && instance.locked_state().is_none();

            if autostart {
                tokio::spawn({
                    let app_state = app_state.clone();
                    let semaphore = Arc::clone(&semaphore);
                    let instance = instance.clone();

                    async move {
                        tracing::info!(instance = %instance.uuid, "restoring instance state {state:?}");

                        tokio::time::sleep(Duration::from_secs(5)).await;
                        if instance.resource_usage().state != ContainerState::Offline {
                            return;
                        }

                        let Ok(_permit) = semaphore.acquire().await else {
                            return;
                        };

                        if app_state
                            .instance_manager
                            .get_instance(instance.uuid)
                            .await
                            .is_none()
                        {
                            return;
                        }

                        instance.check_disk_usage(&instance.resource_usage).await;

                        if let Err(err) = instance.start().await {
                            tracing::error!(instance = %instance.uuid, "failed to restore instance: {err:#}");
                        }
                    }
                });
            }

            write.push(instance);
        }

        drop(write);

        tokio::spawn({
            let app_state = app_state.clone();

            async move {
                let mut persisted: HashMap<uuid::Uuid, ContainerState> = HashMap::new();

                loop {
                    tokio::time::sleep(Duration::from_secs(10)).await;

                    let states: Vec<(uuid::Uuid, ContainerState)> = app_state
                        .instance_manager
                        .get_instances()
                        .await
                        .iter()
                        .map(|i| (i.uuid, i.resource_usage().state))
                        .collect();

                    persisted.retain(|uuid, _| states.iter().any(|(u, _)| u == uuid));

                    for (uuid, state) in states {
                        if persisted.get(&uuid) == Some(&state) {
                            continue;
                        }

                        match sqlx::query("UPDATE instances SET state = ? WHERE uuid = ?")
                            .bind(state.to_str())
                            .bind(uuid)
                            .execute(app_state.database.write())
                            .await
                        {
                            Ok(_) => {
                                persisted.insert(uuid, state);
                            }
                            Err(err) => {
                                tracing::error!(instance = %uuid, "failed to persist instance state: {err:#}");
                            }
                        }
                    }
                }
            }
        });
    }

    async fn stored_states(
        app_state: &crate::routes::State,
    ) -> HashMap<uuid::Uuid, ContainerState> {
        let rows =
            match sqlx::query_as::<_, (uuid::Uuid, String)>("SELECT uuid, state FROM instances")
                .fetch_all(app_state.database.read())
                .await
            {
                Ok(rows) => rows,
                Err(err) => {
                    tracing::error!("failed to read stored instance states: {err:#}");

                    return HashMap::new();
                }
            };

        rows.into_iter()
            .map(|(uuid, state)| {
                (
                    uuid,
                    ContainerState::from_db_str(&state).unwrap_or_default(),
                )
            })
            .collect()
    }

    #[inline]
    pub async fn get_instances(&self) -> tokio::sync::RwLockReadGuard<'_, Vec<super::Instance>> {
        self.instances.read().await
    }

    #[inline]
    pub async fn get_instance(&self, instance: uuid::Uuid) -> Option<super::Instance> {
        let instances = self.instances.read().await;

        instances.iter().find(|i| i.uuid == instance).cloned()
    }

    pub async fn create_instance(
        &self,
        app_state: &crate::routes::State,
        create: crate::database::data::StoredInstanceCreate,
    ) -> anyhow::Result<super::Instance> {
        let data = create.insert(&app_state.database).await?;

        let instance = super::Instance::new(data, app_state.clone());
        self.instances.write().await.push(instance.clone());

        Ok(instance)
    }

    pub async fn delete_instance(&self, instance: &super::Instance) -> anyhow::Result<()> {
        instance.destroy_container().await?;

        let config = &instance.app_state.config;
        for dir in [
            config.socket_path(instance.uuid),
            config.data_path(instance.uuid),
        ] {
            if let Err(err) = tokio::fs::remove_dir_all(&dir).await
                && err.kind() != std::io::ErrorKind::NotFound
            {
                tracing::warn!(instance = %instance.uuid, "failed to remove {}: {err}", dir.display());
            }
        }

        sqlx::query("DELETE FROM instances WHERE uuid = ?")
            .bind(instance.uuid)
            .execute(instance.app_state.database.write())
            .await?;

        self.instances
            .write()
            .await
            .retain(|i| i.uuid != instance.uuid);

        Ok(())
    }
}

type Table = RwLock<rustc_hash::FxHashMap<u32, Credentials>>;

#[derive(Default)]
pub struct DatabaseRouteManager {
    pub postgres: Table,
    pub mariadb: Table,
    pub mongodb: Table,
    pub redis: Table,
}

impl DatabaseRouteManager {
    pub fn table(&self, r#type: DatabaseType) -> &Table {
        match r#type {
            DatabaseType::Postgres => &self.postgres,
            DatabaseType::Mariadb => &self.mariadb,
            DatabaseType::Mongodb => &self.mongodb,
            DatabaseType::Redis => &self.redis,
        }
    }

    pub fn find(&self, r#type: DatabaseType, user: &UserIdentifier) -> Option<Credentials> {
        self.table(r#type).read().get(&user.short_uuid()).cloned()
    }

    pub fn inserter(
        self: &Arc<Self>,
        instance: Weak<super::InnerInstance>,
        r#type: DatabaseType,
    ) -> DatabaseRouteTableInserter {
        DatabaseRouteTableInserter::new(self.clone(), instance, r#type)
    }
}

pub struct DatabaseRouteTableInserter {
    manager: Arc<DatabaseRouteManager>,
    instance: Weak<super::InnerInstance>,
    r#type: DatabaseType,

    inserted_users: RwLock<rustc_hash::FxHashSet<u32>>,
}

impl DatabaseRouteTableInserter {
    fn new(
        manager: Arc<DatabaseRouteManager>,
        instance: Weak<super::InnerInstance>,
        r#type: DatabaseType,
    ) -> Self {
        Self {
            manager,
            instance,
            r#type,
            inserted_users: RwLock::new(rustc_hash::FxHashSet::default()),
        }
    }

    fn get_instance(&self) -> Option<super::Instance> {
        self.instance.upgrade().map(super::Instance)
    }

    pub fn insert(&self, user: UserIdentifier, password: impl Into<Arc<str>>) {
        if !self.inserted_users.write().insert(user.short_uuid()) {
            return;
        }
        let Some(instance) = self.get_instance() else {
            return;
        };

        self.manager
            .table(self.r#type)
            .write()
            .insert(user.short_uuid(), Credentials::new(instance, password));
    }

    pub fn remove(&self, user: &UserIdentifier) {
        if !self.inserted_users.write().remove(&user.short_uuid()) {
            return;
        }

        self.manager
            .table(self.r#type)
            .write()
            .remove(&user.short_uuid());
    }

    pub fn clear(&self) {
        let mut inserted = self.inserted_users.write();
        let table = self.manager.table(self.r#type);
        let mut write = table.write();
        for user in inserted.drain() {
            write.remove(&user);
        }
    }
}

impl Drop for DatabaseRouteTableInserter {
    fn drop(&mut self) {
        let table = self.manager.table(self.r#type);
        let mut write = table.write();
        for user in self.inserted_users.get_mut().iter() {
            write.remove(user);
        }
    }
}
