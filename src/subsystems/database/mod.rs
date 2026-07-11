use crate::subsystems::database::identifier::{DbIdentifier, UserIdentifier};
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

#[derive(Clone)]
pub struct Credentials {
    pub database: Database,
    pub password: Arc<str>,
}

impl Credentials {
    pub fn new(database: Database, password: impl Into<Arc<str>>) -> Self {
        Self {
            database,
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

pub struct InnerDatabase {
    pub uuid: uuid::Uuid,
    pub app_state: crate::routes::State,

    pub route_inserter: manager::DatabaseRouteTableInserter,
    pub data: RwLock<crate::database::data::StoredDatabase>,

    pub process_handle: RwLock<Option<Arc<dyn executor::ProcessHandle>>>,

    pub disk_usage: AtomicU64,
    disk_checker_task: tokio::task::JoinHandle<()>,
}

impl Drop for InnerDatabase {
    fn drop(&mut self) {
        self.disk_checker_task.abort();
    }
}

#[derive(Clone)]
pub struct Database(Arc<InnerDatabase>);

impl Database {
    pub fn new(
        app_state: crate::routes::State,
        data: crate::database::data::StoredDatabase,
    ) -> anyhow::Result<Self> {
        Ok(Self(Arc::new_cyclic(|weak| {
            let disk_checker_task =
                tokio::spawn(disk_checker::run(app_state.clone(), weak.clone()));

            InnerDatabase {
                uuid: data.uuid,
                app_state: app_state.clone(),
                route_inserter: app_state
                    .database_route_manager
                    .inserter(weak.clone(), data.database_type),
                data: RwLock::new(data),
                process_handle: RwLock::new(None),
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

    pub async fn resync_users(&self) -> anyhow::Result<()> {
        let mut users_stream = sqlx::query_as::<_, crate::database::data::StoredDatabaseUser>(
            "SELECT * FROM database_users WHERE database_uuid = ?",
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
                            "failed to create user identifier for database {} user {}: {err}",
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

    pub async fn get_users(
        &self,
    ) -> anyhow::Result<Vec<crate::database::data::StoredDatabaseUser>> {
        Ok(
            sqlx::query_as::<_, crate::database::data::StoredDatabaseUser>(
                "SELECT * FROM database_users WHERE database_uuid = ?",
            )
            .bind(self.uuid)
            .fetch_all(self.app_state.database.read())
            .await?,
        )
    }

    pub async fn get_user(
        &self,
        uuid: uuid::Uuid,
    ) -> anyhow::Result<Option<crate::database::data::StoredDatabaseUser>> {
        Ok(
            sqlx::query_as::<_, crate::database::data::StoredDatabaseUser>(
                "SELECT * FROM database_users WHERE database_uuid = ? AND uuid = ?",
            )
            .bind(self.uuid)
            .bind(uuid)
            .fetch_optional(self.app_state.database.read())
            .await?,
        )
    }

    pub async fn create_user(
        &self,
        username: &str,
    ) -> anyhow::Result<crate::database::data::StoredDatabaseUser> {
        let connection = self.connection().await?;
        let password = crate::utils::generate_password();
        let password = password.as_str();

        let user = loop {
            let uuid = uuid::Uuid::new_v4();
            let uuid_short = uuid.as_fields().0;
            UserIdentifier::from_parts(uuid_short, username)?;

            match sqlx::query(
                "INSERT INTO database_users (uuid, uuid_short, database_uuid, username, password)
                 VALUES (?, ?, ?, ?, ?)",
            )
            .bind(uuid)
            .bind(uuid_short as i64)
            .bind(self.uuid)
            .bind(username)
            .bind(password)
            .execute(self.app_state.database.write())
            .await
            {
                Ok(_) => {
                    break crate::database::data::StoredDatabaseUser {
                        uuid,
                        uuid_short: uuid_short as i64,
                        database_uuid: self.uuid,
                        username: username.to_string(),
                        password: password.to_string(),
                    };
                }
                Err(sqlx::Error::Database(err)) if err.is_unique_violation() => continue,
                Err(err) => return Err(err.into()),
            }
        };

        let identifier = UserIdentifier::from_parts(user.uuid.as_fields().0, username)?;
        if let Err(err) = connection.create_user(&identifier, password).await {
            sqlx::query("DELETE FROM database_users WHERE uuid = ?")
                .bind(user.uuid)
                .execute(self.app_state.database.write())
                .await?;
            return Err(err);
        }
        self.route_inserter.insert(identifier, password);

        Ok(user)
    }

    pub async fn create_db(
        &self,
        name: &str,
    ) -> anyhow::Result<(
        crate::database::data::StoredDatabaseUser,
        Option<DbIdentifier>,
    )> {
        let database_type = self.data.read().await.database_type;
        let user = self.create_user(name).await?;

        if database_type == DatabaseType::Redis {
            return Ok((user, None));
        }

        let short = user.uuid.as_fields().0;
        let owner = UserIdentifier::from_parts(short, name)?;
        let db = DbIdentifier::from_parts(short, name)?;
        self.connection()
            .await?
            .create_database(&db, &owner)
            .await?;

        Ok((user, Some(db)))
    }

    pub async fn rotate_user_password(
        &self,
        user: &crate::database::data::StoredDatabaseUser,
    ) -> anyhow::Result<String> {
        let password = crate::utils::generate_password();
        let identifier = UserIdentifier::from_parts(user.uuid.as_fields().0, &user.username)?;

        self.connection()
            .await?
            .update_user_password(&identifier, &password)
            .await?;

        sqlx::query("UPDATE database_users SET password = ? WHERE uuid = ?")
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
        user: &crate::database::data::StoredDatabaseUser,
    ) -> anyhow::Result<()> {
        let identifier = UserIdentifier::from_parts(user.uuid.as_fields().0, &user.username)?;
        let connection = self.connection().await?;

        if self.data.read().await.database_type != DatabaseType::Redis {
            let db = DbIdentifier::from_parts(user.uuid.as_fields().0, &user.username)?;
            connection.delete_database(&db).await?;
        }
        connection.delete_user(&identifier).await?;

        sqlx::query("DELETE FROM database_users WHERE uuid = ?")
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

        match self.process_handle.read().await.as_ref() {
            Some(handle) => handle.start().await,
            None => anyhow::bail!("no container handle for database {}", self.uuid),
        }
    }

    pub async fn stop(&self) -> anyhow::Result<()> {
        match self.process_handle.read().await.as_ref() {
            Some(handle) => handle.stop().await,
            None => anyhow::bail!("no container handle for database {}", self.uuid),
        }
    }

    pub async fn kill(&self) -> anyhow::Result<()> {
        match self.process_handle.read().await.as_ref() {
            Some(handle) => handle.kill().await,
            None => anyhow::bail!("no container handle for database {}", self.uuid),
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
        db: Option<&DbIdentifier>,
    ) -> anyhow::Result<Box<dyn tokio::io::AsyncRead + Send + Unpin>> {
        let data = self.data.read().await;
        let socket = &data.socket_path;

        let command = match data.database_type {
            DatabaseType::Postgres => {
                let dir = socket.rsplit_once('/').map_or("", |(dir, _)| dir);
                match db {
                    Some(db) => format!("pg_dump -h '{dir}' -U postgres '{db}'"),
                    None => format!("pg_dumpall -h '{dir}' -U postgres"),
                }
            }
            DatabaseType::Mariadb => match db {
                Some(db) => format!("mariadb-dump --socket='{socket}' -u root '{db}'"),
                None => format!("mariadb-dump --socket='{socket}' -u root --all-databases"),
            },
            DatabaseType::Mongodb => match db {
                Some(db) => format!("mongodump --host='{socket}' --archive -d '{db}'"),
                None => format!("mongodump --host='{socket}' --archive"),
            },
            DatabaseType::Redis => format!("redis-cli -s '{socket}' --rdb /dev/stdout"),
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
        db: Option<&DbIdentifier>,
        wipe: bool,
        reader: &mut (dyn tokio::io::AsyncRead + Send + Unpin),
    ) -> anyhow::Result<()> {
        let data = self.data.read().await;
        let socket = &data.socket_path;

        let (wipe_command, command) = match data.database_type {
            DatabaseType::Postgres => {
                let dir = socket.rsplit_once('/').map_or("", |(dir, _)| dir);
                let db = db.map_or("postgres".to_string(), |db| db.to_string());
                let base = format!("psql -q -v ON_ERROR_STOP=1 -h '{dir}' -U postgres -d '{db}'");
                let wipe = wipe.then(|| {
                    format!("{base} -c 'DROP SCHEMA public CASCADE; CREATE SCHEMA public;'")
                });
                (wipe, format!("{base} -o /dev/null"))
            }
            DatabaseType::Mariadb => {
                let import = match db {
                    Some(db) => format!("mariadb --socket='{socket}' -u root '{db}'"),
                    None => format!("mariadb --socket='{socket}' -u root"),
                };
                let wipe = if wipe {
                    let db = db.ok_or_else(|| anyhow::anyhow!("wipe requires a target db"))?;
                    Some(format!(
                        "mariadb --socket='{socket}' -u root -e \
                         'DROP DATABASE IF EXISTS `{db}`; CREATE DATABASE `{db}`;'"
                    ))
                } else {
                    None
                };
                (wipe, import)
            }
            DatabaseType::Mongodb => {
                let drop = if wipe { " --drop" } else { "" };
                let import = match db {
                    Some(db) => format!("mongorestore --host='{socket}' --archive{drop} -d '{db}'"),
                    None => format!("mongorestore --host='{socket}' --archive{drop}"),
                };
                (None, import)
            }
            DatabaseType::Redis => anyhow::bail!("import is not supported for redis"),
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
            while let Some(chunk) = output.next().await {
                chunk?;
            }
            Ok::<_, anyhow::Error>(())
        };

        let (write, drain) = tokio::join!(write, drain);
        drain.and(write)
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

    pub async fn to_api_response(&self) -> ApiDatabase {
        ApiDatabase {
            data: self.data.read().await.clone(),
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

#[derive(ToSchema, Serialize, Deserialize)]
pub struct ApiDatabase {
    #[serde(flatten)]
    pub data: crate::database::data::StoredDatabase,
    pub utilization: resources::ResourceUsage,
}

impl Deref for Database {
    type Target = InnerDatabase;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
