use super::{Credentials, DatabaseType, identifier::UserIdentifier};
use futures_util::TryStreamExt;
use parking_lot::RwLock;
use std::sync::{Arc, Weak};

#[derive(Default)]
pub struct DatabaseManager {
    databases: tokio::sync::RwLock<Vec<super::Database>>,
}

impl DatabaseManager {
    pub async fn initialize(&self, app_state: crate::routes::State) -> anyhow::Result<()> {
        let mut write = self.databases.write().await;

        let mut databases_stream =
            sqlx::query_as::<_, crate::database::data::StoredDatabase>("SELECT * FROM databases")
                .fetch(app_state.database.read());

        while let Some(database) = databases_stream.try_next().await? {
            let database = super::Database::new(app_state.clone(), database)?;
            database.resync_users().await?;
            if let Err(err) = database.attach_container().await {
                tracing::warn!(
                    "failed to attach to container for database {}: {err}",
                    database.uuid
                );
            }

            write.push(database);
        }

        Ok(())
    }

    pub async fn get_databases(&self) -> tokio::sync::RwLockReadGuard<'_, Vec<super::Database>> {
        self.databases.read().await
    }

    pub async fn get_database(&self, uuid: uuid::Uuid) -> Option<super::Database> {
        self.databases
            .read()
            .await
            .iter()
            .find(|d| d.uuid == uuid)
            .cloned()
    }

    pub async fn create_database(
        &self,
        app_state: crate::routes::State,
        create: crate::database::data::StoredDatabaseCreate,
    ) -> anyhow::Result<super::Database> {
        let data = create.insert(&app_state.database).await?;
        let uuid = data.uuid;

        let database = super::Database::new(app_state.clone(), data)?;
        if let Err(err) = database.create_container().await {
            sqlx::query("DELETE FROM databases WHERE uuid = ?")
                .bind(uuid)
                .execute(app_state.database.write())
                .await?;
            return Err(err);
        }

        self.databases.write().await.push(database.clone());

        Ok(database)
    }

    pub async fn delete_database(&self, database: &super::Database) -> anyhow::Result<()> {
        database.destroy_container().await?;

        let config = &database.app_state.config;
        for dir in [
            config.socket_path(database.uuid),
            config.data_path(database.uuid),
        ] {
            if let Err(err) = tokio::fs::remove_dir_all(&dir).await
                && err.kind() != std::io::ErrorKind::NotFound
            {
                tracing::warn!(database = %database.uuid, "failed to remove {}: {err}", dir.display());
            }
        }

        sqlx::query("DELETE FROM databases WHERE uuid = ?")
            .bind(database.uuid)
            .execute(database.app_state.database.write())
            .await?;

        self.databases
            .write()
            .await
            .retain(|d| d.uuid != database.uuid);

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
        database: Weak<super::InnerDatabase>,
        r#type: DatabaseType,
    ) -> DatabaseRouteTableInserter {
        DatabaseRouteTableInserter::new(self.clone(), database, r#type)
    }
}

pub struct DatabaseRouteTableInserter {
    manager: Arc<DatabaseRouteManager>,
    database: Weak<super::InnerDatabase>,
    r#type: DatabaseType,

    inserted_users: RwLock<rustc_hash::FxHashSet<u32>>,
}

impl DatabaseRouteTableInserter {
    fn new(
        manager: Arc<DatabaseRouteManager>,
        database: Weak<super::InnerDatabase>,
        r#type: DatabaseType,
    ) -> Self {
        Self {
            manager,
            database,
            r#type,
            inserted_users: RwLock::new(rustc_hash::FxHashSet::default()),
        }
    }

    fn get_database(&self) -> Option<super::Database> {
        self.database.upgrade().map(super::Database)
    }

    pub fn insert(&self, user: UserIdentifier, password: impl Into<Arc<str>>) {
        if !self.inserted_users.write().insert(user.short_uuid()) {
            return;
        }
        let Some(database) = self.get_database() else {
            return;
        };

        self.manager
            .table(self.r#type)
            .write()
            .insert(user.short_uuid(), Credentials::new(database, password));
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
