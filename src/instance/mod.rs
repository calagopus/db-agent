use crate::instance::identifier::UserIdentifier;
use futures_util::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::{
    ops::Deref,
    path::PathBuf,
    sync::{Arc, atomic::AtomicU64},
};
use tokio::{io::AsyncWriteExt, sync::RwLock};
use utoipa::ToSchema;

pub mod connection;
pub mod disk_checker;
pub mod executor;
pub mod identifier;
pub mod manager;
pub mod resources;

pub fn validate_database_name(value: &str, _ctx: &()) -> garde::Result {
    if !(1..=63).contains(&value.len()) {
        return Err(garde::Error::new("must be between 1 and 63 characters"));
    }
    if !value.bytes().all(|b| b.is_ascii_alphanumeric()) {
        return Err(garde::Error::new("must be ascii alphanumeric"));
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
    pub fn as_str(self) -> &'static str {
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

pub struct InnerInstance {
    pub uuid: uuid::Uuid,
    pub app_state: crate::routes::State,

    pub route_inserter: manager::DatabaseRouteTableInserter,
    pub data: RwLock<crate::database::data::StoredInstance>,

    pub process_handle: RwLock<Option<Arc<dyn executor::ProcessHandle>>>,
    pub backend_auth_error: RwLock<Option<String>>,

    power_lock: tokio::sync::Mutex<()>,

    pub disk_usage: AtomicU64,
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
        app_state: crate::routes::State,
        data: crate::database::data::StoredInstance,
    ) -> anyhow::Result<Self> {
        Ok(Self(Arc::new_cyclic(|weak| {
            let disk_checker_task =
                tokio::spawn(disk_checker::run(app_state.clone(), weak.clone()));

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
                disk_usage: AtomicU64::new(0),
                disk_checker_task,
            }
        })))
    }

    pub async fn get_socket_path(&self) -> PathBuf {
        self.app_state.config.socket_path(self.uuid).join(
            self.data
                .read()
                .await
                .socket_path
                .split('/')
                .rev()
                .take(1)
                .collect::<Vec<_>>()
                .join("/"),
        )
    }

    pub async fn is_suspended(&self) -> bool {
        self.data.read().await.suspended
    }

    /// verifies mongod enforces authorization and the agent holds root
    /// credentials, bootstrapping the root user when possible. sessions
    /// must not be relayed unless this succeeds (fail closed).
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

    /// creates and stores the agent's root user if missing. works while
    /// authorization is off, or on a fresh `--auth` instance via the
    /// localhost exception; must run before any other user is created
    pub async fn ensure_mongodb_root(&self) -> anyhow::Result<()> {
        let socket = self.get_socket_path().await;

        // write lock serializes concurrent bootstrap attempts
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

    async fn ensure_acl_writable(&self, action: &str) -> anyhow::Result<()> {
        if self.data.read().await.database_type != DatabaseType::Redis
            && self.resource_usage().await.state != resources::ContainerState::Running
        {
            return Err(crate::response::DisplayError::new(format!(
                "the instance must be online to {action}"
            ))
            .with_status(axum::http::StatusCode::CONFLICT)
            .into());
        }

        Ok(())
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

        self.connection().await?.create_database(name).await?;

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
            let _ = self.connection().await?.delete_database(name).await;
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

        for user in self.get_database_users(database.uuid).await? {
            self.delete_user(&user).await?;
        }

        self.connection()
            .await?
            .delete_database(&database.name)
            .await?;

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

        let users = self
            .get_database_users(database.uuid)
            .await?
            .into_iter()
            .map(|user| UserIdentifier::from_parts(user.uuid.as_fields().0, &user.username))
            .collect::<Result<Vec<_>, _>>()?;

        self.connection()
            .await?
            .recreate_database(&database.name, &users)
            .await?;

        Ok(())
    }

    async fn get_database_users(
        &self,
        database_uuid: uuid::Uuid,
    ) -> anyhow::Result<Vec<crate::database::data::StoredUser>> {
        Ok(sqlx::query_as::<_, crate::database::data::StoredUser>(
            "SELECT * FROM users WHERE instance_uuid = ? AND database_uuid = ?",
        )
        .bind(self.uuid)
        .bind(database_uuid)
        .fetch_all(self.app_state.database.read())
        .await?)
    }

    pub async fn get_users(&self) -> anyhow::Result<Vec<crate::database::data::StoredUser>> {
        Ok(sqlx::query_as::<_, crate::database::data::StoredUser>(
            "SELECT * FROM users WHERE instance_uuid = ?",
        )
        .bind(self.uuid)
        .fetch_all(self.app_state.database.read())
        .await?)
    }

    pub async fn get_user(
        &self,
        uuid: uuid::Uuid,
    ) -> anyhow::Result<Option<crate::database::data::StoredUser>> {
        Ok(sqlx::query_as::<_, crate::database::data::StoredUser>(
            "SELECT * FROM users WHERE instance_uuid = ? AND uuid = ?",
        )
        .bind(self.uuid)
        .bind(uuid)
        .fetch_optional(self.app_state.database.read())
        .await?)
    }

    pub async fn create_user(
        &self,
        username: &str,
        database_uuid: Option<uuid::Uuid>,
    ) -> anyhow::Result<crate::database::data::StoredUser> {
        self.ensure_acl_writable("create a user").await?;

        let database = match database_uuid {
            Some(database_uuid) => Some(
                self.get_database(database_uuid)
                    .await?
                    .ok_or_else(|| crate::response::DisplayError::new("database not found"))?,
            ),
            None => None,
        };

        let connection = self.connection().await?;
        let password = crate::utils::generate_password();
        let password = password.as_str();

        let user = loop {
            let uuid = uuid::Uuid::new_v4();
            let uuid_short = uuid.as_fields().0;
            UserIdentifier::from_parts(uuid_short, username)?;
            let created = chrono::Utc::now();

            match sqlx::query(
                "INSERT INTO users (uuid, uuid_short, instance_uuid, database_uuid, username, password, created)
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(uuid)
            .bind(uuid_short as i64)
            .bind(self.uuid)
            .bind(database_uuid)
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
                        database_uuid,
                        username: username.to_string(),
                        password: password.to_string(),
                        created,
                    };
                }
                Err(sqlx::Error::Database(err)) if err.is_unique_violation() => continue,
                Err(err) => return Err(err.into()),
            }
        };

        let identifier = UserIdentifier::from_parts(user.uuid.as_fields().0, username)?;
        if let Err(err) = connection.create_user(&identifier, password).await {
            sqlx::query("DELETE FROM users WHERE uuid = ?")
                .bind(user.uuid)
                .execute(self.app_state.database.write())
                .await?;
            return Err(err);
        }

        if let Some(database) = &database
            && let Err(err) = connection.grant_user(&identifier, &database.name).await
        {
            let _ = connection.delete_user(&identifier).await;
            sqlx::query("DELETE FROM users WHERE uuid = ?")
                .bind(user.uuid)
                .execute(self.app_state.database.write())
                .await?;
            return Err(err);
        }

        self.route_inserter.insert(identifier, password);

        Ok(user)
    }

    pub async fn rotate_password(
        &self,
        user: &crate::database::data::StoredUser,
    ) -> anyhow::Result<String> {
        self.ensure_acl_writable("rotate a user's password").await?;

        let password = crate::utils::generate_password();
        let identifier = UserIdentifier::from_parts(user.uuid.as_fields().0, &user.username)?;

        self.connection()
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
        self.connection().await?.delete_user(&identifier).await?;

        sqlx::query("DELETE FROM users WHERE uuid = ?")
            .bind(user.uuid)
            .execute(self.app_state.database.write())
            .await?;

        self.route_inserter.remove(&identifier);

        Ok(())
    }

    pub async fn create_container(&self) -> anyhow::Result<()> {
        if self.process_handle.read().await.is_some() {
            return Ok(());
        }

        let handle = self
            .app_state
            .container_executor
            .create_container(self)
            .await?;
        *self.process_handle.write().await = Some(handle);

        Ok(())
    }

    pub async fn attach_container(&self) -> anyhow::Result<()> {
        if self.process_handle.read().await.is_some() {
            return Ok(());
        }

        if let Some(handle) = self
            .app_state
            .container_executor
            .attach_container(self)
            .await?
        {
            *self.process_handle.write().await = Some(handle);
        }

        Ok(())
    }

    pub async fn destroy_container(&self) -> anyhow::Result<()> {
        self.app_state
            .container_executor
            .destroy_container(self)
            .await?;
        self.process_handle.write().await.take();

        Ok(())
    }

    pub async fn is_disk_full(&self) -> bool {
        let disk_limit = self.data.read().await.disk;
        disk_limit != 0
            && self.disk_usage.load(std::sync::atomic::Ordering::Relaxed)
                >= disk_limit as u64 * 1024 * 1024
    }

    pub async fn start(&self) -> anyhow::Result<()> {
        if self.is_disk_full().await {
            anyhow::bail!(
                "database {} is over its disk limit, cannot start",
                self.uuid
            );
        }

        let _guard = self.power_lock.lock().await;

        self.destroy_container().await?;
        self.create_container().await?;

        match self.process_handle.read().await.as_ref() {
            Some(handle) => handle.start().await,
            None => anyhow::bail!("no container handle for database {}", self.uuid),
        }
    }

    pub async fn stop(&self) -> anyhow::Result<()> {
        let _guard = self.power_lock.lock().await;
        match self.process_handle.read().await.as_ref() {
            Some(handle) => handle.stop().await,
            None => Ok(()),
        }
    }

    pub async fn kill(&self) -> anyhow::Result<()> {
        let _guard = self.power_lock.lock().await;
        match self.process_handle.read().await.as_ref() {
            Some(handle) => handle.kill().await,
            None => Ok(()),
        }
    }

    pub async fn exec(
        &self,
        options: executor::ExecOptions,
    ) -> anyhow::Result<executor::ExecStream> {
        match self.process_handle.read().await.as_ref() {
            Some(handle) => handle.exec(options).await,
            None => anyhow::bail!("no container handle for database {}", self.uuid),
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
                let dir = socket.rsplit_once('/').map_or("", |(dir, _)| dir);
                match db {
                    Some(db) => {
                        format!("pg_dump --no-owner --no-privileges -h '{dir}' -U postgres '{db}'")
                    }
                    None => format!("pg_dumpall --no-owner --no-privileges -h '{dir}' -U postgres"),
                }
            }
            DatabaseType::Mariadb => {
                let strip = r"| sed -e 's/DEFINER=`[^`]*`@`[^`]*`//g'";
                match db {
                    Some(db) => {
                        format!("mariadb-dump --socket='{socket}' -u root '{db}' {strip}")
                    }
                    None => {
                        format!("mariadb-dump --socket='{socket}' -u root --all-databases {strip}")
                    }
                }
            }
            DatabaseType::Mongodb => {
                let auth = mongodb_shell_auth(&data);
                match db {
                    Some(db) => format!("mongodump --host='{socket}'{auth} --archive -d '{db}'"),
                    None => format!("mongodump --host='{socket}'{auth} --archive"),
                }
            }
            DatabaseType::Redis => format!("redis-cli -s '{socket}' --rdb -"),
        };
        drop(data);

        let stream = self
            .exec(executor::ExecOptions::new(vec![
                "sh".to_string(),
                "-c".to_string(),
                format!("{command} 2>/dev/null"),
            ]))
            .await?;

        Ok(Box::new(tokio_util::io::StreamReader::new(
            stream.output.map_err(std::io::Error::other),
        )))
    }

    pub async fn import(
        &self,
        db: Option<&str>,
        wipe: bool,
        reader: &mut (dyn tokio::io::AsyncRead + Send + Unpin),
    ) -> anyhow::Result<()> {
        let data = self.data.read().await;
        let socket = &data.socket_path;

        if wipe && db.is_none() && !matches!(data.database_type, DatabaseType::Redis) {
            return Err(crate::response::DisplayError::new("wipe requires a db").into());
        }

        let (wipe_command, command) = match data.database_type {
            DatabaseType::Postgres => {
                let dir = socket.rsplit_once('/').map_or("", |(dir, _)| dir);
                match db {
                    Some(db) => {
                        let base =
                            format!("psql -q -v ON_ERROR_STOP=1 -h '{dir}' -U postgres -d '{db}'");
                        let wipe = wipe.then(|| {
                            format!("{base} -c 'DROP SCHEMA public CASCADE; CREATE SCHEMA public;'")
                        });
                        (wipe, format!("{base} -o /dev/null"))
                    }
                    // no ON_ERROR_STOP: pg_dumpall output recreates existing roles
                    None => (
                        None,
                        format!("psql -q -h '{dir}' -U postgres -d postgres -o /dev/null"),
                    ),
                }
            }
            DatabaseType::Mariadb => match db {
                Some(db) => {
                    let import = format!("mariadb --socket='{socket}' -u root '{db}'");
                    let wipe = wipe.then(|| {
                        format!(
                            "mariadb --socket='{socket}' -u root -e \
                             'DROP DATABASE IF EXISTS `{db}`; CREATE DATABASE `{db}`;'"
                        )
                    });
                    (wipe, import)
                }
                None => (None, format!("mariadb --socket='{socket}' -u root")),
            },
            DatabaseType::Mongodb => {
                let auth = mongodb_shell_auth(&data);
                let drop = if wipe { " --drop" } else { "" };
                let import = match db {
                    Some(db) => {
                        format!("mongorestore --host='{socket}'{auth} --archive{drop} -d '{db}'")
                    }
                    None => format!("mongorestore --host='{socket}'{auth} --archive{drop}"),
                };
                (None, import)
            }
            DatabaseType::Redis => {
                if db.is_some() {
                    return Err(
                        crate::response::DisplayError::new("redis has no named databases").into(),
                    );
                }
                let wipe = wipe.then(|| format!("redis-cli -s '{socket}' FLUSHALL"));
                (wipe, format!("redis-cli -s '{socket}' --pipe"))
            }
        };
        drop(data);

        if let Some(wipe_command) = wipe_command {
            let mut stream = self
                .exec(executor::ExecOptions::new(vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    wipe_command,
                ]))
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
            .exec(executor::ExecOptions::new(vec![
                "sh".to_string(),
                "-c".to_string(),
                command,
            ]))
            .await?;

        let write = async {
            tokio::io::copy(reader, &mut stdin).await?;
            stdin.shutdown().await?;
            Ok::<_, anyhow::Error>(())
        };
        let drain = async {
            let mut buf = Vec::new();
            while let Some(chunk) = output.next().await {
                match chunk {
                    Ok(bytes) if buf.len() < 8192 => buf.extend_from_slice(&bytes),
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
        tokio::select! {
            drained = &mut drain => drained,
            written = &mut write => drain.await.and(written),
        }
    }

    pub async fn logs(
        &self,
        lines: Option<usize>,
    ) -> futures_util::stream::BoxStream<'static, Result<bytes::Bytes, anyhow::Error>> {
        match self.process_handle.read().await.as_ref() {
            Some(handle) => handle
                .logs(lines)
                .await
                .unwrap_or_else(|_| futures_util::stream::empty().boxed()),
            None => futures_util::stream::empty().boxed(),
        }
    }

    pub async fn resource_usage(&self) -> resources::ResourceUsage {
        let mut usage = match self.process_handle.read().await.as_ref() {
            Some(handle) => handle.resource_usage().await.unwrap_or_default(),
            None => resources::ResourceUsage::default(),
        };
        usage.disk_bytes = self.disk_usage.load(std::sync::atomic::Ordering::Relaxed);

        usage
    }

    pub async fn to_api_response(&self) -> ApiInstance {
        ApiInstance {
            data: self.data.read().await.clone(),
            backend_auth_error: self.backend_auth_error.read().await.clone(),
            utilization: self.resource_usage().await,
        }
    }

    pub async fn sync_container_resources(&self) -> anyhow::Result<()> {
        let data = self.data.read().await.clone();
        if let Some(handle) = self.process_handle.read().await.as_ref() {
            handle.update_resources(&data).await?;
        }

        Ok(())
    }
}

fn mongodb_shell_auth(data: &crate::database::data::StoredInstance) -> String {
    data.root_password
        .as_deref()
        .map_or_else(String::new, |pw| {
            format!(
                " -u {} -p '{pw}' --authenticationDatabase admin",
                connection::mongodb::ROOT_USERNAME
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
