use super::{DatabasePermission, identifier::UserIdentifier};
use serde::Serialize;
use utoipa::ToSchema;

pub mod mariadb;
pub mod mongodb;
pub mod postgres;
pub mod redis;

#[derive(Debug, ToSchema, Default, Serialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub rows_affected: u64,
}

pub fn query_error(err: anyhow::Error) -> anyhow::Error {
    let message = if let Some(sqlx::Error::Database(err)) = err.downcast_ref::<sqlx::Error>() {
        Some(err.message().to_string())
    } else if let Some(err) = err.downcast_ref::<::redis::RedisError>() {
        matches!(
            err.kind(),
            ::redis::ErrorKind::Server(_) | ::redis::ErrorKind::Extension
        )
        .then(|| err.to_string())
    } else if let Some(::mongodb::error::ErrorKind::Command(err)) = err
        .downcast_ref::<::mongodb::error::Error>()
        .map(|err| &*err.kind)
    {
        Some(err.message.clone())
    } else {
        err.downcast_ref::<serde_json::Error>()
            .map(|err| err.to_string())
    };

    match message {
        Some(message) => crate::response::DisplayError::new(message)
            .with_status(axum::http::StatusCode::BAD_REQUEST)
            .into(),
        None => err,
    }
}

#[async_trait::async_trait]
pub trait DatabaseConnection: Send + Sync {
    async fn create_user(&self, user: &UserIdentifier, password: &str) -> anyhow::Result<()>;
    async fn update_user_password(
        &self,
        user: &UserIdentifier,
        password: &str,
    ) -> anyhow::Result<()>;
    async fn delete_user(&self, user: &UserIdentifier) -> anyhow::Result<()>;

    async fn apply_permission(
        &self,
        user: &UserIdentifier,
        database: &str,
        permission: DatabasePermission,
    ) -> anyhow::Result<()>;

    async fn create_database(&self, name: &str) -> anyhow::Result<()>;
    async fn delete_database(&self, name: &str) -> anyhow::Result<()>;
    async fn get_size(&self, name: &str) -> anyhow::Result<i64>;

    async fn bootstrap_database(&self, _name: &str) -> anyhow::Result<()> {
        Ok(())
    }

    async fn query(&self, db: Option<&str>, query: &str) -> anyhow::Result<QueryResult>;
}

impl super::Instance {
    pub async fn connection(&self) -> anyhow::Result<Box<dyn DatabaseConnection>> {
        self.ensure_online("reach the database").await?;

        self.acl_connection().await
    }

    /// for the acl paths, which gate on ensure_acl_writable instead because a
    /// redis user is ours to store and needs no backend
    pub(super) async fn acl_connection(&self) -> anyhow::Result<Box<dyn DatabaseConnection>> {
        let socket = self.get_socket_path().await;

        let database_type = self.data.read().await.database_type;
        Ok(match database_type {
            super::DatabaseType::Postgres => Box::new(postgres::PostgresConnection::new(socket)),
            super::DatabaseType::Mariadb => Box::new(mariadb::MariadbConnection::new(socket)),
            super::DatabaseType::Mongodb => {
                self.ensure_mongodb_root().await?;
                let root_password = self.data.read().await.root_password.clone();
                Box::new(mongodb::MongodbConnection::new(
                    socket,
                    root_password.as_deref(),
                )?)
            }
            super::DatabaseType::Redis => Box::new(redis::RedisConnection::new(socket)?),
        })
    }
}
