use crate::{instance::identifier::UserIdentifier, io::SafeSliceExt};
use futures_util::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Row};
use std::{
    ops::Deref,
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::RwLock,
};
use utoipa::ToSchema;

pub mod connection;
pub mod disk_checker;
pub mod executor;
pub mod explorer;
pub mod identifier;
pub mod manager;
pub mod operations;
pub mod remote;
pub mod resources;
pub mod websocket;

pub const STDERR_CAPTURE_LIMIT: usize = 8 * 1024;

pub fn validate_database_name(value: &str, _ctx: &()) -> garde::Result {
    if !(1..=63).contains(&value.len()) {
        return Err(garde::Error::new("must be between 1 and 63 characters"));
    }
    if !value.bytes().all(|b| b.is_ascii_alphanumeric()) {
        return Err(garde::Error::new("must be ascii alphanumeric"));
    }

    Ok(())
}

pub fn validate_source_database_name(value: &str, _ctx: &()) -> garde::Result {
    if !(1..=64).contains(&value.len()) {
        return Err(garde::Error::new("must be between 1 and 64 characters"));
    }
    if value.starts_with('-') {
        return Err(garde::Error::new("must not start with '-'"));
    }
    if value.contains(char::is_control) {
        return Err(garde::Error::new("must not contain control characters"));
    }

    Ok(())
}

#[derive(Clone)]
pub struct Credentials {
    pub instance: Instance,
    pub password: Arc<str>,
}

impl Credentials {
    pub fn new(instance: Instance, password: impl Into<Arc<str>>) -> Self {
        Self {
            instance,
            password: password.into(),
        }
    }
}

#[derive(Clone, Copy, ToSchema, Deserialize, Serialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DatabaseType {
    Postgres,
    Mariadb,
    Mongodb,
    Redis,
}

impl DatabaseType {
    #[inline]
    pub fn to_str(self) -> &'static str {
        match self {
            DatabaseType::Postgres => "postgres",
            DatabaseType::Mariadb => "mariadb",
            DatabaseType::Mongodb => "mongodb",
            DatabaseType::Redis => "redis",
        }
    }

    #[inline]
    pub fn from_db_str(s: &str) -> Option<Self> {
        match s {
            "postgres" => Some(DatabaseType::Postgres),
            "mariadb" => Some(DatabaseType::Mariadb),
            "mongodb" => Some(DatabaseType::Mongodb),
            "redis" => Some(DatabaseType::Redis),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, ToSchema, Deserialize, Serialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DatabasePermission {
    None,
    ReadOnly,
    ReadWrite,
}

impl DatabasePermission {
    #[inline]
    pub fn to_db_str(self) -> Option<&'static str> {
        match self {
            DatabasePermission::None => None,
            DatabasePermission::ReadOnly => Some("read_only"),
            DatabasePermission::ReadWrite => Some("read_write"),
        }
    }

    #[inline]
    pub fn from_db_str(s: &str) -> Option<Self> {
        match s {
            "read_only" => Some(DatabasePermission::ReadOnly),
            "read_write" => Some(DatabasePermission::ReadWrite),
            _ => None,
        }
    }
}

fn duplicate_database_permission() -> anyhow::Error {
    crate::response::DisplayError::new("the same database appears twice in the permission list")
        .with_status(axum::http::StatusCode::BAD_REQUEST)
        .into()
}

pub struct InnerInstance {
    pub uuid: uuid::Uuid,
    pub app_state: crate::routes::State,

    route_inserter: manager::DatabaseRouteTableInserter,
    pub data: RwLock<crate::database::data::StoredInstance>,

    process_handle: RwLock<Option<Arc<dyn executor::ProcessHandle>>>,
    backend_auth_error: RwLock<Option<String>>,

    power_lock: tokio::sync::Mutex<()>,

    pub suspended: AtomicBool,

    pub websocket: tokio::sync::broadcast::Sender<websocket::WebsocketMessage>,
    pub operations: operations::OperationManager,

    resource_usage: tokio::sync::watch::Sender<resources::ResourceUsage>,
    disk_checker_task: tokio::task::JoinHandle<()>,
}

impl Drop for InnerInstance {
    fn drop(&mut self) {
        self.disk_checker_task.abort();
    }
}

#[derive(Clone)]
pub struct Instance(Arc<InnerInstance>);

impl Instance {
    pub fn new(
        data: crate::database::data::StoredInstance,
        app_state: crate::routes::State,
    ) -> Self {
        let suspended = AtomicBool::new(data.suspended);

        Self(Arc::new_cyclic(|weak| {
            let (resource_usage, _) =
                tokio::sync::watch::channel(resources::ResourceUsage::default());
            let (websocket, _) = tokio::sync::broadcast::channel(128);
            let disk_checker_task = tokio::spawn(disk_checker::run(
                app_state.clone(),
                weak.clone(),
                resource_usage.clone(),
            ));

            InnerInstance {
                uuid: data.uuid,
                app_state: app_state.clone(),
                route_inserter: app_state
                    .database_route_manager
                    .inserter(weak.clone(), data.database_type),
                data: RwLock::new(data),
                process_handle: RwLock::new(None),
                backend_auth_error: RwLock::new(None),
                power_lock: tokio::sync::Mutex::new(()),
                suspended,
                operations: operations::OperationManager::new(websocket.clone()),
                websocket,
                resource_usage,
                disk_checker_task,
            }
        }))
    }

    pub async fn get_socket_path(&self) -> PathBuf {
        self.app_state.config.socket_path(self.uuid).join(
            std::path::Path::new(&self.data.read().await.socket_path)
                .file_name()
                .unwrap_or_default(),
        )
    }

    pub fn log_daemon(&self, message: impl Into<String>) {
        self.websocket
            .send(
                websocket::WebsocketMessage::builder(
                    websocket::WebsocketEvent::InstanceDaemonMessage,
                )
                .arg(message)
                .build(),
            )
            .ok();
    }

    #[inline]
    pub fn locked_state(&self) -> Option<&'static str> {
        if self.suspended.load(Ordering::Relaxed) {
            tracing::debug!(instance = %self.uuid, "instance locked at state check: suspended");
            return Some("suspended");
        }

        None
    }

    pub async fn verify_mongodb_auth(&self) -> anyhow::Result<()> {
        let result = self.verify_mongodb_auth_inner().await;
        *self.backend_auth_error.write().await = result.as_ref().err().map(|e| e.to_string());
        result
    }

    async fn verify_mongodb_auth_inner(&self) -> anyhow::Result<()> {
        let conn = connection::mongodb::MongodbConnection::new(self.get_socket_path().await, None)?;
        let enforced = conn.auth_enforced().await?;

        if let Err(err) = self.ensure_mongodb_root().await {
            if enforced {
                anyhow::bail!(
                    "authorization is enforced but the agent has no root credentials: {err}"
                );
            }
            return Err(err);
        }

        if !enforced {
            anyhow::bail!("mongod does not enforce authorization, add --auth and restart");
        }

        Ok(())
    }

    pub async fn ensure_mongodb_root(&self) -> anyhow::Result<()> {
        let socket = self.get_socket_path().await;

        let mut data = self.data.write().await;
        if data.root_password.is_some() {
            return Ok(());
        }

        let conn = connection::mongodb::MongodbConnection::new(socket, None)?;
        let password = crate::utils::generate_password();
        conn.create_root(&password).await?;

        sqlx::query("UPDATE instances SET root_password = ? WHERE uuid = ?")
            .bind(&password)
            .bind(self.uuid)
            .execute(self.app_state.database.write())
            .await?;
        data.root_password = Some(password);

        Ok(())
    }

    pub async fn resync_users(&self) -> anyhow::Result<()> {
        let mut users_stream = sqlx::query_as::<_, crate::database::data::StoredUser>(
            "SELECT * FROM users WHERE instance_uuid = ?",
        )
        .bind(self.uuid)
        .fetch(self.app_state.database.read());

        self.route_inserter.clear();
        while let Some(user) = users_stream.try_next().await? {
            let identifier =
                match UserIdentifier::from_parts(user.uuid.as_fields().0, &user.username) {
                    Ok(identifier) => identifier,
                    Err(err) => {
                        tracing::warn!(
                            "failed to create user identifier for instance {} user {}: {err}",
                            self.uuid,
                            user.username
                        );
                        continue;
                    }
                };

            self.route_inserter.insert(identifier, user.password);
        }

        Ok(())
    }

    async fn ensure_online(&self, action: &str) -> anyhow::Result<()> {
        if self.resource_usage().state != resources::ContainerState::Running {
            return Err(crate::response::DisplayError::new(format!(
                "the instance must be online to {action}"
            ))
            .with_status(axum::http::StatusCode::CONFLICT)
            .into());
        }

        Ok(())
    }

    async fn ensure_acl_writable(&self, action: &str) -> anyhow::Result<()> {
        if self.data.read().await.database_type == DatabaseType::Redis {
            return Ok(());
        }

        self.ensure_online(action).await
    }

    pub async fn get_databases(
        &self,
    ) -> anyhow::Result<Vec<crate::database::data::StoredDatabase>> {
        Ok(sqlx::query_as::<_, crate::database::data::StoredDatabase>(
            "SELECT * FROM databases WHERE instance_uuid = ?",
        )
        .bind(self.uuid)
        .fetch_all(self.app_state.database.read())
        .await?)
    }

    async fn get_database_by_name(
        &self,
        name: &str,
    ) -> anyhow::Result<Option<crate::database::data::StoredDatabase>> {
        Ok(sqlx::query_as::<_, crate::database::data::StoredDatabase>(
            "SELECT * FROM databases WHERE instance_uuid = ? AND name = ?",
        )
        .bind(self.uuid)
        .bind(name)
        .fetch_optional(self.app_state.database.read())
        .await?)
    }

    pub async fn get_database(
        &self,
        uuid: uuid::Uuid,
    ) -> anyhow::Result<Option<crate::database::data::StoredDatabase>> {
        Ok(sqlx::query_as::<_, crate::database::data::StoredDatabase>(
            "SELECT * FROM databases WHERE instance_uuid = ? AND uuid = ?",
        )
        .bind(self.uuid)
        .bind(uuid)
        .fetch_optional(self.app_state.database.read())
        .await?)
    }

    pub async fn create_database(
        &self,
        name: &str,
    ) -> anyhow::Result<crate::database::data::StoredDatabase> {
        self.ensure_acl_writable("create a database").await?;

        self.acl_connection().await?.create_database(name).await?;

        let uuid = uuid::Uuid::new_v4();
        let created = chrono::Utc::now();
        if let Err(err) = sqlx::query(
            "INSERT INTO databases (uuid, instance_uuid, name, created) VALUES (?, ?, ?, ?)",
        )
        .bind(uuid)
        .bind(self.uuid)
        .bind(name)
        .bind(created.timestamp())
        .execute(self.app_state.database.write())
        .await
        {
            let _ = self.acl_connection().await?.delete_database(name).await;
            return Err(err.into());
        }

        Ok(crate::database::data::StoredDatabase {
            uuid,
            instance_uuid: self.uuid,
            name: name.to_string(),
            created,
        })
    }

    pub async fn delete_database(
        &self,
        database: &crate::database::data::StoredDatabase,
    ) -> anyhow::Result<()> {
        self.ensure_acl_writable("delete a database").await?;

        let connection = self.acl_connection().await?;
        for (user, _) in self.get_database_users(database.uuid).await? {
            let identifier = UserIdentifier::from_parts(user.uuid.as_fields().0, &user.username)?;
            connection
                .apply_permission(&identifier, &database.name, DatabasePermission::None)
                .await?;
        }

        connection.delete_database(&database.name).await?;

        sqlx::query("DELETE FROM databases WHERE uuid = ?")
            .bind(database.uuid)
            .execute(self.app_state.database.write())
            .await?;

        Ok(())
    }

    pub async fn recreate_database(
        &self,
        database: &crate::database::data::StoredDatabase,
    ) -> anyhow::Result<()> {
        self.ensure_acl_writable("recreate a database").await?;

        let connection = self.acl_connection().await?;
        connection.delete_database(&database.name).await?;
        connection.create_database(&database.name).await?;

        self.resync_database_acl(connection.as_ref(), database)
            .await
    }

    async fn resync_database_acl_by_name(&self, name: &str) -> anyhow::Result<()> {
        let Some(database) = self.get_database_by_name(name).await? else {
            return Ok(());
        };
        let connection = self.acl_connection().await?;

        self.resync_database_acl(connection.as_ref(), &database)
            .await
    }

    async fn resync_database_acl(
        &self,
        connection: &dyn connection::DatabaseConnection,
        database: &crate::database::data::StoredDatabase,
    ) -> anyhow::Result<()> {
        connection.bootstrap_database(&database.name).await?;

        for (user, permission) in self.get_database_users(database.uuid).await? {
            let identifier = UserIdentifier::from_parts(user.uuid.as_fields().0, &user.username)?;
            connection
                .apply_permission(&identifier, &database.name, permission)
                .await?;
        }

        Ok(())
    }

    async fn get_database_users(
        &self,
        database_uuid: uuid::Uuid,
    ) -> anyhow::Result<Vec<(crate::database::data::StoredUser, DatabasePermission)>> {
        let mut rows = sqlx::query(
            "SELECT users.*, user_databases.permission FROM users
             JOIN user_databases ON user_databases.user_uuid = users.uuid
             WHERE users.instance_uuid = ? AND user_databases.database_uuid = ?",
        )
        .bind(self.uuid)
        .bind(database_uuid)
        .fetch(self.app_state.database.read());

        let mut users = Vec::new();
        while let Some(row) = rows.try_next().await? {
            let permission = crate::database::data::decode_permission(&row)?;
            users.push((
                crate::database::data::StoredUser::from_row(&row)?,
                permission,
            ));
        }

        Ok(users)
    }

    pub async fn get_users(&self) -> anyhow::Result<Vec<crate::database::data::StoredUser>> {
        let mut users = sqlx::query_as::<_, crate::database::data::StoredUser>(
            "SELECT * FROM users WHERE instance_uuid = ?",
        )
        .bind(self.uuid)
        .fetch_all(self.app_state.database.read())
        .await?;

        let mut links = sqlx::query(
            "SELECT user_databases.* FROM user_databases
             JOIN users ON users.uuid = user_databases.user_uuid
             WHERE users.instance_uuid = ?",
        )
        .bind(self.uuid)
        .fetch(self.app_state.database.read());

        let mut by_user: rustc_hash::FxHashMap<uuid::Uuid, Vec<_>> = Default::default();
        while let Some(row) = links.try_next().await? {
            by_user
                .entry(row.try_get("user_uuid")?)
                .or_default()
                .push(crate::database::data::StoredUserDatabase::from_row(&row)?);
        }

        for user in &mut users {
            user.databases = by_user.remove(&user.uuid).unwrap_or_default();
        }

        Ok(users)
    }

    pub async fn get_user(
        &self,
        uuid: uuid::Uuid,
    ) -> anyhow::Result<Option<crate::database::data::StoredUser>> {
        let user = sqlx::query_as::<_, crate::database::data::StoredUser>(
            "SELECT * FROM users WHERE instance_uuid = ? AND uuid = ?",
        )
        .bind(self.uuid)
        .bind(uuid)
        .fetch_optional(self.app_state.database.read())
        .await?;

        let Some(mut user) = user else {
            return Ok(None);
        };

        user.databases = sqlx::query_as::<_, crate::database::data::StoredUserDatabase>(
            "SELECT * FROM user_databases WHERE user_uuid = ?",
        )
        .bind(user.uuid)
        .fetch_all(self.app_state.database.read())
        .await?;

        Ok(Some(user))
    }

    pub async fn create_user(
        &self,
        username: &str,
        databases: &[(uuid::Uuid, DatabasePermission)],
    ) -> anyhow::Result<crate::database::data::StoredUser> {
        self.ensure_acl_writable("create a user").await?;

        let mut named = rustc_hash::FxHashSet::default();
        let mut grants = Vec::with_capacity(databases.len());
        for (database_uuid, permission) in databases {
            if !named.insert(*database_uuid) {
                return Err(duplicate_database_permission());
            }
            if *permission == DatabasePermission::None {
                continue;
            }

            let database = self
                .get_database(*database_uuid)
                .await?
                .ok_or_else(|| crate::response::DisplayError::new("database not found"))?;
            grants.push((database, *permission));
        }

        let connection = self.acl_connection().await?;
        let password = crate::utils::generate_password();
        let password = password.as_str();

        let mut user = loop {
            let uuid = uuid::Uuid::new_v4();
            let uuid_short = uuid.as_fields().0;
            UserIdentifier::from_parts(uuid_short, username)?;
            let created = chrono::Utc::now();

            match sqlx::query(
                "INSERT INTO users (uuid, uuid_short, instance_uuid, username, password, created)
                 VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(uuid)
            .bind(uuid_short as i64)
            .bind(self.uuid)
            .bind(username)
            .bind(password)
            .bind(created.timestamp())
            .execute(self.app_state.database.write())
            .await
            {
                Ok(_) => {
                    break crate::database::data::StoredUser {
                        uuid,
                        uuid_short: uuid_short as i64,
                        instance_uuid: self.uuid,
                        username: username.to_string(),
                        password: password.to_string(),
                        databases: Vec::with_capacity(grants.len()),
                        created,
                    };
                }
                Err(sqlx::Error::Database(err)) if err.is_unique_violation() => continue,
                Err(err) => return Err(err.into()),
            }
        };

        let identifier = UserIdentifier::from_parts(user.uuid.as_fields().0, username)?;
        let mut backend_created = false;
        let result = async {
            connection.create_user(&identifier, password).await?;
            backend_created = true;

            for (database, permission) in &grants {
                user.databases
                    .push(self.link_user(&user, database, *permission).await?);
                connection
                    .apply_permission(&identifier, &database.name, *permission)
                    .await?;
            }

            Ok::<_, anyhow::Error>(())
        }
        .await;

        if let Err(err) = result {
            if backend_created {
                let _ = connection.delete_user(&identifier).await;
            }

            sqlx::query("DELETE FROM users WHERE uuid = ?")
                .bind(user.uuid)
                .execute(self.app_state.database.write())
                .await?;
            return Err(err);
        }

        self.route_inserter.insert(identifier, password);

        Ok(user)
    }

    pub async fn set_user_permission(
        &self,
        user: &crate::database::data::StoredUser,
        database: &crate::database::data::StoredDatabase,
        permission: DatabasePermission,
    ) -> anyhow::Result<Option<crate::database::data::StoredUserDatabase>> {
        self.ensure_acl_writable("change a user's permissions")
            .await?;

        let identifier = UserIdentifier::from_parts(user.uuid.as_fields().0, &user.username)?;
        let connection = self.acl_connection().await?;

        self.apply_permission(connection.as_ref(), &identifier, user, database, permission)
            .await
    }

    pub async fn set_user_permissions(
        &self,
        user: &crate::database::data::StoredUser,
        databases: &[(uuid::Uuid, DatabasePermission)],
    ) -> anyhow::Result<Vec<crate::database::data::StoredUserDatabase>> {
        self.ensure_acl_writable("change a user's permissions")
            .await?;

        let mut named = rustc_hash::FxHashSet::default();
        let mut changes = Vec::with_capacity(databases.len() + user.databases.len());

        for (database_uuid, permission) in databases {
            if !named.insert(*database_uuid) {
                return Err(duplicate_database_permission());
            }

            let database = self
                .get_database(*database_uuid)
                .await?
                .ok_or_else(|| crate::response::DisplayError::new("database not found"))?;
            changes.push((database, *permission));
        }

        for existing in &user.databases {
            if named.contains(&existing.database_uuid) {
                continue;
            }

            if let Some(database) = self.get_database(existing.database_uuid).await? {
                changes.push((database, DatabasePermission::None));
            }
        }

        let identifier = UserIdentifier::from_parts(user.uuid.as_fields().0, &user.username)?;
        let connection = self.acl_connection().await?;

        let mut links = Vec::with_capacity(changes.len());
        for (applied, (database, permission)) in changes.iter().enumerate() {
            let link = self
                .apply_permission(
                    connection.as_ref(),
                    &identifier,
                    user,
                    database,
                    *permission,
                )
                .await
                .inspect_err(|err| {
                    if applied > 0 {
                        tracing::warn!(
                            instance = %self.uuid,
                            user = %user.uuid,
                            database = %database.name,
                            "failed to change a permission with {applied} already applied: {err:#}"
                        );
                    }
                })?;

            if let Some(link) = link {
                links.push(link);
            }
        }

        Ok(links)
    }

    async fn apply_permission(
        &self,
        connection: &dyn connection::DatabaseConnection,
        identifier: &UserIdentifier,
        user: &crate::database::data::StoredUser,
        database: &crate::database::data::StoredDatabase,
        permission: DatabasePermission,
    ) -> anyhow::Result<Option<crate::database::data::StoredUserDatabase>> {
        connection
            .apply_permission(identifier, &database.name, permission)
            .await?;

        let stored = if permission == DatabasePermission::None {
            sqlx::query("DELETE FROM user_databases WHERE user_uuid = ? AND database_uuid = ?")
                .bind(user.uuid)
                .bind(database.uuid)
                .execute(self.app_state.database.write())
                .await
                .map(|_| None)
                .map_err(anyhow::Error::from)
        } else {
            self.link_user(user, database, permission).await.map(Some)
        };

        stored.inspect_err(|err| {
            tracing::warn!(
                instance = %self.uuid,
                user = %user.uuid,
                database = %database.name,
                "applied a permission on the backend but failed to store it: {err:#}"
            );
        })
    }

    async fn link_user(
        &self,
        user: &crate::database::data::StoredUser,
        database: &crate::database::data::StoredDatabase,
        permission: DatabasePermission,
    ) -> anyhow::Result<crate::database::data::StoredUserDatabase> {
        let created: i64 = sqlx::query_scalar(
            "INSERT INTO user_databases (user_uuid, database_uuid, permission, created)
             VALUES (?, ?, ?, ?)
             ON CONFLICT (user_uuid, database_uuid) DO UPDATE SET permission = excluded.permission
             RETURNING created",
        )
        .bind(user.uuid)
        .bind(database.uuid)
        .bind(permission.to_db_str())
        .bind(chrono::Utc::now().timestamp())
        .fetch_one(self.app_state.database.write())
        .await?;

        Ok(crate::database::data::StoredUserDatabase {
            database_uuid: database.uuid,
            permission,
            created: chrono::DateTime::from_timestamp(created, 0).unwrap_or_default(),
        })
    }

    pub async fn rotate_password(
        &self,
        user: &crate::database::data::StoredUser,
    ) -> anyhow::Result<String> {
        self.ensure_acl_writable("rotate a user's password").await?;

        let password = crate::utils::generate_password();
        let identifier = UserIdentifier::from_parts(user.uuid.as_fields().0, &user.username)?;

        self.acl_connection()
            .await?
            .update_user_password(&identifier, &password)
            .await?;

        sqlx::query("UPDATE users SET password = ? WHERE uuid = ?")
            .bind(&password)
            .bind(user.uuid)
            .execute(self.app_state.database.write())
            .await?;

        self.route_inserter.remove(&identifier);
        self.route_inserter.insert(identifier, password.as_str());

        Ok(password)
    }

    pub async fn delete_user(
        &self,
        user: &crate::database::data::StoredUser,
    ) -> anyhow::Result<()> {
        self.ensure_acl_writable("delete a user").await?;

        let identifier = UserIdentifier::from_parts(user.uuid.as_fields().0, &user.username)?;
        self.acl_connection()
            .await?
            .delete_user(&identifier)
            .await?;

        sqlx::query("DELETE FROM users WHERE uuid = ?")
            .bind(user.uuid)
            .execute(self.app_state.database.write())
            .await?;

        self.route_inserter.remove(&identifier);

        Ok(())
    }

    pub async fn setup_container(&self) -> anyhow::Result<()> {
        if self.process_handle.read().await.is_some() {
            return Ok(());
        }

        let handle = self
            .app_state
            .container_executor
            .setup_instance_process(self)
            .await?;
        *self.process_handle.write().await = Some(handle);

        Ok(())
    }

    pub async fn attach_container(&self) {
        if self.process_handle.read().await.is_some() {
            return;
        }

        tracing::info!(instance = %self.uuid, "attaching to container");

        match self
            .app_state
            .container_executor
            .attach_instance_process(self)
            .await
        {
            Ok(handle) => {
                *self.process_handle.write().await = Some(handle);
            }
            Err(err) => {
                tracing::debug!(instance = %self.uuid, "no running container to attach to: {}", err);
            }
        }
    }

    pub async fn destroy_container(&self) -> anyhow::Result<()> {
        self.app_state
            .container_executor
            .cleanup_instance_process(self)
            .await?;
        self.process_handle.write().await.take();

        explorer::close_pools(self.uuid).await;

        Ok(())
    }

    pub async fn is_disk_full(&self) -> bool {
        let disk_limit = self.data.read().await.disk;
        disk_limit != 0
            && self.resource_usage.borrow().disk_bytes >= disk_limit as u64 * 1024 * 1024
    }

    pub async fn start(&self) -> anyhow::Result<()> {
        if let Some(state) = self.locked_state() {
            anyhow::bail!("Instance is in a locked state ({state}), cannot start the instance.");
        }

        if self.is_disk_full().await {
            anyhow::bail!("Disk space is full, cannot start the instance.");
        }

        let _guard = self.power_lock.lock().await;

        self.destroy_container().await?;
        self.setup_container().await?;

        match self.process_handle.read().await.as_ref() {
            Some(handle) => handle.start().await,
            None => anyhow::bail!("instance has no active process"),
        }
    }

    pub async fn stop(&self) -> anyhow::Result<()> {
        let _guard = self.power_lock.lock().await;
        let result = match self.process_handle.read().await.as_ref() {
            Some(handle) => handle.stop().await,
            None => Ok(()),
        };

        explorer::close_pools(self.uuid).await;

        result
    }

    pub async fn kill(&self) -> anyhow::Result<()> {
        if self.resource_usage().state == resources::ContainerState::Offline {
            return Ok(());
        }

        let _guard = self.power_lock.lock().await;
        let result = match self.process_handle.read().await.as_ref() {
            Some(handle) => handle.kill().await,
            None => Ok(()),
        };

        explorer::close_pools(self.uuid).await;

        result
    }

    pub async fn exec(
        &self,
        options: executor::ExecOptions,
    ) -> anyhow::Result<executor::ExecStream> {
        self.ensure_online("run commands on the instance").await?;

        match self.process_handle.read().await.as_ref() {
            Some(handle) => handle.exec(options).await,
            None => anyhow::bail!("instance has no active process"),
        }
    }

    pub async fn export(
        &self,
        db: Option<&str>,
    ) -> anyhow::Result<Box<dyn tokio::io::AsyncRead + Send + Unpin>> {
        let data = self.data.read().await;
        let socket = &data.socket_path;

        let command = match data.database_type {
            DatabaseType::Postgres => {
                let dir =
                    crate::utils::shell_quote(socket.rsplit_once('/').map_or("", |(dir, _)| dir));
                match db {
                    Some(db) => format!(
                        "pg_dump --no-owner --no-privileges -h {dir} -U postgres {}",
                        crate::utils::shell_quote(db)
                    ),
                    None => format!("pg_dumpall --no-owner --no-privileges -h {dir} -U postgres"),
                }
            }
            DatabaseType::Mariadb => {
                let socket = crate::utils::shell_quote(socket);
                match db {
                    Some(db) => format!(
                        "mariadb-dump --socket={socket} -u root {}",
                        crate::utils::shell_quote(db)
                    ),
                    None => format!("mariadb-dump --socket={socket} -u root --all-databases"),
                }
            }
            DatabaseType::Mongodb => {
                let auth = mongodb_shell_auth(&data);
                let uri = crate::utils::shell_quote(&mongodb_socket_uri(socket));
                match db {
                    Some(db) => format!(
                        "mongodump --uri={uri}{auth} --archive -d {}",
                        crate::utils::shell_quote(db)
                    ),
                    None => format!("mongodump --uri={uri}{auth} --archive"),
                }
            }
            DatabaseType::Redis => {
                let socket = crate::utils::shell_quote(socket);
                let script = crate::utils::shell_quote(
                    r#"local out={"*2\r\n$6\r\nSELECT\r\n$"..#ARGV[1].."\r\n"..ARGV[1].."\r\n"}
local c="0"
repeat
  local s=redis.call("SCAN",c,"COUNT",512)
  c=s[1]
  for i=1,#s[2] do
    local k=s[2][i]
    local v=redis.call("DUMP",k)
    if v then
      local t=redis.call("PTTL",k)
      if t<0 then t=0 end
      t=string.format("%d",t)
      out[#out+1]="*5\r\n$7\r\nRESTORE\r\n$"..#k.."\r\n"..k.."\r\n$"..#t.."\r\n"..t.."\r\n$"..#v.."\r\n"..v.."\r\n$7\r\nREPLACE\r\n"
    end
  end
until c=="0"
return table.concat(out)"#,
                );

                format!(
                    r#"set -e; keyspace=$(redis-cli -s {socket} --raw INFO keyspace); for db in $(printf %s "$keyspace" | sed -n 's/^db\([0-9][0-9]*\):.*/\1/p'); do redis-cli -s {socket} -n "$db" --raw EVAL {script} 0 "$db"; done"#
                )
            }
        };
        let user = format!("{}:{}", data.image_uid, data.image_gid);
        drop(data);

        let stream = self
            .exec(
                executor::ExecOptions::new(vec!["sh".to_string(), "-c".to_string(), command])
                    .with_user(user),
            )
            .await?;

        let uuid = self.uuid;

        Ok(Box::new(tokio_util::io::StreamReader::new(
            stream
                .output
                .inspect_err(move |err| tracing::error!(instance = %uuid, "export failed: {err}"))
                .map_err(std::io::Error::other),
        )))
    }

    pub async fn import(
        &self,
        db: Option<&str>,
        source_db: Option<&str>,
        wipe: bool,
        reader: &mut (dyn tokio::io::AsyncRead + Send + Unpin),
    ) -> anyhow::Result<()> {
        if source_db.is_some() {
            let database_type = self.data.read().await.database_type;

            if db.is_none() {
                return Err(crate::response::DisplayError::new("source_db requires a db").into());
            }
            if database_type != DatabaseType::Mongodb {
                return Err(crate::response::DisplayError::new(format!(
                    "{} cannot import a single db out of a dump, omit source_db",
                    database_type.to_str()
                ))
                .into());
            }
        }

        self.import_inner(db, source_db, wipe, reader).await
    }

    pub async fn check_import(
        &self,
        db: Option<&str>,
        source_db: Option<&str>,
        wipe: bool,
    ) -> anyhow::Result<()> {
        check_import_args(self.data.read().await.database_type, db, source_db, wipe)?;

        self.ensure_online("import a database").await
    }

    async fn import_inner(
        &self,
        db: Option<&str>,
        source_db: Option<&str>,
        wipe: bool,
        reader: &mut (dyn tokio::io::AsyncRead + Send + Unpin),
    ) -> anyhow::Result<()> {
        let data = self.data.read().await;
        let socket = &data.socket_path;
        let database_type = data.database_type;

        check_import_args(database_type, db, source_db, wipe)?;

        let (wipe_command, command) = match database_type {
            DatabaseType::Postgres => {
                let dir =
                    crate::utils::shell_quote(socket.rsplit_once('/').map_or("", |(dir, _)| dir));
                match db {
                    Some(db) => {
                        // a dump taken with --create or pg_dumpall carries \connect for its own
                        // db, which lands everything there instead of here. the COPY range keeps
                        // the strips off row data, which can hold a line that reads like DDL
                        let strip = r"sed '/^COPY .*FROM stdin;$/,/^\\\.$/!{ s/^\\connect .*$//; s/^\\c .*$//; s/^CREATE DATABASE .*;$//; s/^DROP DATABASE .*;$//; s/^ALTER DATABASE .*;$//; }' |";
                        let base = format!(
                            "psql -q -v ON_ERROR_STOP=1 -h {dir} -U postgres -d {}",
                            crate::utils::shell_quote(db)
                        );
                        let wipe = wipe.then(|| {
                            format!("{base} -c 'DROP SCHEMA public CASCADE; CREATE SCHEMA public;'")
                        });
                        (wipe, format!("{strip} {base} -o /dev/null"))
                    }
                    // no ON_ERROR_STOP: pg_dumpall output recreates existing roles
                    None => (
                        None,
                        format!("psql -q -h {dir} -U postgres -d postgres -o /dev/null"),
                    ),
                }
            }
            DatabaseType::Mariadb => {
                let strip = r"sed -e 's/DEFINER=`[^`]*`@`[^`]*`//g'";
                let socket = crate::utils::shell_quote(socket);
                match db {
                    Some(db) => {
                        // a dump taken with --databases carries CREATE DATABASE/USE for its own
                        // name, which as root lands every table there instead of here, and
                        // --add-drop-database drops that database first. the DELIMITER range keeps
                        // the strips out of routine bodies, which are copied verbatim
                        let redirect = r" -e '/^DELIMITER ;;$/,/^DELIMITER ;$/!{ s/^USE `[^`]*`;$//; s/^CREATE DATABASE .*;$//; s/^DROP DATABASE .*;$//; s|^/\*![0-9]* DROP DATABASE .*;$||; }'";
                        let import = format!(
                            "{strip}{redirect} | mariadb --socket={socket} -u root {}",
                            crate::utils::shell_quote(db)
                        );
                        let wipe = wipe.then(|| {
                            format!(
                                "mariadb --socket={socket} -u root -e {}",
                                crate::utils::shell_quote(&format!(
                                    "DROP DATABASE IF EXISTS `{db}`; CREATE DATABASE `{db}`;"
                                ))
                            )
                        });
                        (wipe, import)
                    }
                    None => (None, format!("{strip} | mariadb --socket={socket} -u root")),
                }
            }
            DatabaseType::Mongodb => {
                let auth = mongodb_shell_auth(&data);
                let uri = crate::utils::shell_quote(&mongodb_socket_uri(socket));
                // the remap only renames, without nsInclude every other namespace
                // the archive carries is restored under its own name
                let import = match (db, source_db) {
                    (Some(db), Some(source_db)) => {
                        let source_ns = crate::utils::shell_quote(&format!("{source_db}.*"));

                        format!(
                            "mongorestore --uri={uri}{auth} --archive --nsInclude={source_ns} --nsExclude={} --nsExclude={} --nsFrom={source_ns} --nsTo={}",
                            crate::utils::shell_quote("admin.*"),
                            crate::utils::shell_quote("config.*"),
                            crate::utils::shell_quote(&format!("{db}.*"))
                        )
                    }
                    _ => format!("mongorestore --uri={uri}{auth} --archive"),
                };
                (None, import)
            }
            DatabaseType::Redis => {
                let socket = crate::utils::shell_quote(socket);
                let wipe = wipe.then(|| format!("redis-cli -s {socket} FLUSHALL"));
                (wipe, format!("redis-cli -s {socket} --pipe"))
            }
        };
        let user = format!("{}:{}", data.image_uid, data.image_gid);
        drop(data);

        // --drop only drops the collections the archive carries, stale ones would survive
        if wipe
            && database_type == DatabaseType::Mongodb
            && let Some(db) = db
        {
            self.connection().await?.delete_database(db).await?;
        }

        if let Some(wipe_command) = wipe_command {
            let mut stream = self
                .exec(
                    executor::ExecOptions::new(vec![
                        "sh".to_string(),
                        "-c".to_string(),
                        wipe_command,
                    ])
                    .with_user(user.clone()),
                )
                .await?;
            drop(stream.stdin);
            while let Some(chunk) = stream.output.next().await {
                chunk?;
            }
        }

        let executor::ExecStream {
            mut output,
            mut stdin,
        } = self
            .exec(
                executor::ExecOptions::new(vec!["sh".to_string(), "-c".to_string(), command])
                    .with_user(user),
            )
            .await?;

        let write = async {
            let copied = tokio::io::copy(reader, &mut stdin).await;
            let shutdown = stdin.shutdown().await;
            copied?;
            shutdown?;
            Ok::<_, anyhow::Error>(())
        };
        let drain = async {
            let mut buf = Vec::new();
            while let Some(chunk) = output.next().await {
                match chunk {
                    Ok(bytes) if buf.len() < STDERR_CAPTURE_LIMIT => buf.extend_from_slice(&bytes),
                    Ok(_) => {}
                    Err(err) => {
                        let msg = String::from_utf8_lossy(&buf);
                        let msg = msg.trim();
                        return Err(if msg.is_empty() {
                            err
                        } else {
                            anyhow::anyhow!("{err}: {msg}")
                        });
                    }
                }
            }
            Ok::<_, anyhow::Error>(())
        };

        let mut write = std::pin::pin!(write);
        let mut drain = std::pin::pin!(drain);
        let result = tokio::select! {
            // the side that failed first has the useful error, a dead source leaves
            // the target failing on a truncated dump
            drained = &mut drain => drained,
            written = &mut write => written.and(drain.await),
        };

        if wipe
            && let Some(db) = db
            && let Err(err) = self.resync_database_acl_by_name(db).await
        {
            tracing::warn!(
                instance = %self.uuid,
                database = %db,
                "failed to reapply permissions after a wiped import: {err:#}"
            );
        }

        result
    }

    pub async fn logs(&self, lines: Option<usize>) -> Box<dyn tokio::io::AsyncRead + Send + Unpin> {
        match self.process_handle.read().await.as_ref() {
            Some(handle) => handle
                .logs(lines)
                .await
                .unwrap_or_else(|_| Box::new(tokio::io::empty())),
            None => Box::new(tokio::io::empty()),
        }
    }

    pub async fn logs_lines(
        &self,
        lines: Option<usize>,
    ) -> Box<dyn futures_util::Stream<Item = Result<String, anyhow::Error>> + Unpin + Send> {
        let process_handle = match &*self.process_handle.read().await {
            Some(c) => Arc::clone(c),
            None => {
                return Box::new(futures_util::stream::empty())
                    as Box<
                        dyn futures_util::Stream<Item = Result<String, anyhow::Error>>
                            + Unpin
                            + Send,
                    >;
            }
        };

        let reader = match process_handle.logs(lines).await {
            Ok(reader) => reader,
            Err(_) => {
                return Box::new(futures_util::stream::empty())
                    as Box<
                        dyn futures_util::Stream<Item = Result<String, anyhow::Error>>
                            + Unpin
                            + Send,
                    >;
            }
        };

        struct LogsState {
            reader: Box<dyn tokio::io::AsyncRead + Send + Unpin>,
            line_buffer: crate::io::line_buffer::LineBuffer,
            read_buffer: Vec<u8>,
            eof: bool,
        }

        let stream = futures_util::stream::try_unfold(
            LogsState {
                reader,
                line_buffer: crate::io::line_buffer::LineBuffer::new(),
                read_buffer: vec![0; crate::BUFFER_SIZE],
                eof: false,
            },
            |mut state| async move {
                loop {
                    if let Some(line) = state
                        .line_buffer
                        .next_line()
                        .map(|line| String::from_utf8_lossy(line).into_owned())
                    {
                        state.line_buffer.compact();

                        return Ok(Some((line, state)));
                    }

                    if state.eof {
                        return Ok(None);
                    }

                    match state.reader.read(&mut state.read_buffer).await {
                        Ok(0) => {
                            state.eof = true;

                            let line = state
                                .line_buffer
                                .flush()
                                .map(|line| String::from_utf8_lossy(line).into_owned());

                            return Ok(line.map(|line| (line, state)));
                        }
                        Ok(bytes_read) => {
                            let chunk = state.read_buffer.get_slice(..bytes_read)?;
                            state.line_buffer.extend(chunk);
                        }
                        Err(err) => return Err(anyhow::Error::from(err)),
                    }
                }
            },
        );

        let pinned: Pin<
            Box<dyn futures_util::Stream<Item = Result<String, anyhow::Error>> + Send>,
        > = Box::pin(stream);
        Box::new(pinned)
    }

    pub async fn subscribe_stdout_lines(
        &self,
    ) -> Option<tokio::sync::broadcast::Receiver<Arc<String>>> {
        if let Some(process_handle) = self.process_handle.read().await.as_ref() {
            match process_handle.subscribe_stdout_lines().await {
                Ok(receiver) => return Some(receiver),
                Err(err) => {
                    tracing::error!(
                        instance = %self.uuid,
                        "failed to subscribe to container stdout: {err}"
                    );
                }
            }
        }

        None
    }

    pub fn resource_usage(&self) -> resources::ResourceUsage {
        *self.resource_usage.borrow()
    }

    #[inline]
    pub fn subscribe_resource_usage(
        &self,
    ) -> tokio::sync::watch::Receiver<resources::ResourceUsage> {
        self.resource_usage.subscribe()
    }

    pub async fn to_api_response(&self) -> ApiInstance {
        ApiInstance {
            data: self.data.read().await.clone(),
            backend_auth_error: self.backend_auth_error.read().await.clone(),
            utilization: self.resource_usage(),
        }
    }

    pub async fn sync_container(&self) -> anyhow::Result<()> {
        let data = self.data.read().await.clone();
        if let Some(handle) = self.process_handle.read().await.as_ref() {
            handle.update_resources(&data).await?;
        }

        Ok(())
    }
}

fn check_import_args(
    database_type: DatabaseType,
    db: Option<&str>,
    source_db: Option<&str>,
    wipe: bool,
) -> anyhow::Result<()> {
    if database_type == DatabaseType::Redis && db.is_some() {
        return Err(crate::response::DisplayError::new("redis has no named databases").into());
    }
    if wipe && db.is_none() && database_type != DatabaseType::Redis {
        return Err(crate::response::DisplayError::new("wipe requires a db").into());
    }
    // with --archive the archive names the database, restoring into another one is
    // a rename that needs the name it had
    if database_type == DatabaseType::Mongodb && db.is_some() && source_db.is_none() {
        return Err(crate::response::DisplayError::new("db requires a source_db").into());
    }

    Ok(())
}

/// the mongo shell tools reject a unix socket path given to --host, it has to be a
/// percent encoded uri host instead
fn mongodb_socket_uri(socket: &str) -> String {
    format!(
        "mongodb://{}",
        percent_encoding::utf8_percent_encode(socket, percent_encoding::NON_ALPHANUMERIC)
    )
}

fn mongodb_shell_auth(data: &crate::database::data::StoredInstance) -> String {
    data.root_password
        .as_deref()
        .map_or_else(String::new, |pw| {
            format!(
                " -u {} -p {} --authenticationDatabase admin",
                connection::mongodb::ROOT_USERNAME,
                crate::utils::shell_quote(pw)
            )
        })
}

#[derive(ToSchema, Serialize, Deserialize)]
pub struct ApiInstance {
    #[serde(flatten)]
    pub data: crate::database::data::StoredInstance,
    pub backend_auth_error: Option<String>,
    pub utilization: resources::ResourceUsage,
}

impl Deref for Instance {
    type Target = InnerInstance;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
