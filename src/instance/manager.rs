use super::{Credentials, DatabaseType, identifier::UserIdentifier};
use futures_util::TryStreamExt;
use parking_lot::RwLock;
use std::sync::{Arc, Weak};

#[derive(Default)]
pub struct InstanceManager {
    instances: tokio::sync::RwLock<Vec<super::Instance>>,
}

impl InstanceManager {
    pub async fn initialize(&self, app_state: crate::routes::State) -> anyhow::Result<()> {
        let mut write = self.instances.write().await;

        let mut instances_stream =
            sqlx::query_as::<_, crate::database::data::StoredInstance>("SELECT * FROM instances")
                .fetch(app_state.database.read());

        while let Some(instance) = instances_stream.try_next().await? {
            let instance = super::Instance::new(app_state.clone(), instance)?;
            instance.resync_users().await?;
            if let Err(err) = instance.attach_container().await {
                tracing::warn!(
                    "failed to attach to container for instance {}: {err}",
                    instance.uuid
                );
            }

            write.push(instance);
        }

        Ok(())
    }

    pub async fn get_instances(&self) -> tokio::sync::RwLockReadGuard<'_, Vec<super::Instance>> {
        self.instances.read().await
    }

    pub async fn get_instance(&self, uuid: uuid::Uuid) -> Option<super::Instance> {
        self.instances
            .read()
            .await
            .iter()
            .find(|i| i.uuid == uuid)
            .cloned()
    }

    pub async fn create_instance(
        &self,
        app_state: crate::routes::State,
        create: crate::database::data::StoredInstanceCreate,
    ) -> anyhow::Result<super::Instance> {
        let data = create.insert(&app_state.database).await?;
        let uuid = data.uuid;

        let instance = super::Instance::new(app_state.clone(), data)?;
        if let Err(err) = instance.create_container().await {
            sqlx::query("DELETE FROM instances WHERE uuid = ?")
                .bind(uuid)
                .execute(app_state.database.write())
                .await?;
            return Err(err);
        }

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
