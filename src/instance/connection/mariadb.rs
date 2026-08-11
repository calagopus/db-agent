use super::{super::identifier::UserIdentifier, DatabaseConnection, QueryResult};
use futures_util::StreamExt;
use sqlx::{
    Column, Connection, Decode, Either, Executor, MySql, Row,
    mysql::{MySqlConnectOptions, MySqlConnection, MySqlRow},
};
use std::path::{Path, PathBuf};

pub const ADMIN_USER: &str = "root";

pub fn connect_options(socket: &Path, database: Option<&str>) -> MySqlConnectOptions {
    let options = MySqlConnectOptions::new()
        .socket(socket)
        .username(ADMIN_USER);

    match database {
        Some(database) => options.database(database),
        None => options,
    }
}

#[inline]
fn quote_ident(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

#[inline]
fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
}

fn duplicate_database(err: anyhow::Error) -> anyhow::Error {
    const ER_DB_CREATE_EXISTS: u16 = 1007;

    match err.downcast_ref::<sqlx::Error>() {
        Some(sqlx::Error::Database(server))
            if server
                .try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>()
                .is_some_and(|server| server.number() == ER_DB_CREATE_EXISTS) =>
        {
            crate::response::DisplayError::new("database already exists")
                .with_status(axum::http::StatusCode::CONFLICT)
                .into()
        }
        _ => err,
    }
}

fn row_values(row: &MySqlRow) -> Vec<serde_json::Value> {
    (0..row.columns().len())
        .map(|ordinal| {
            let value = match row.try_get_raw(ordinal) {
                Ok(value) => value,
                Err(_) => return serde_json::Value::Null,
            };

            match <&[u8] as Decode<MySql>>::decode(value) {
                Ok(bytes) => serde_json::Value::String(String::from_utf8_lossy(bytes).into_owned()),
                Err(_) => serde_json::Value::Null,
            }
        })
        .collect()
}

pub struct MariadbConnection {
    options: MySqlConnectOptions,
}

impl MariadbConnection {
    pub fn new(socket: PathBuf) -> Self {
        Self {
            options: connect_options(&socket, None),
        }
    }

    async fn conn(&self) -> anyhow::Result<MySqlConnection> {
        Ok(MySqlConnection::connect_with(&self.options).await?)
    }

    async fn execute(&self, sql: String) -> anyhow::Result<()> {
        let mut conn = self.conn().await?;
        conn.execute(sqlx::raw_sql(sqlx::AssertSqlSafe(sql)))
            .await?;
        let _ = conn.close().await;
        Ok(())
    }
}

#[async_trait::async_trait]
impl DatabaseConnection for MariadbConnection {
    async fn create_user(&self, user: &UserIdentifier, password: &str) -> anyhow::Result<()> {
        self.execute(format!(
            "CREATE USER {}@'%' IDENTIFIED BY {}",
            quote_literal(&user.to_string()),
            quote_literal(password)
        ))
        .await
    }

    async fn update_user_password(
        &self,
        user: &UserIdentifier,
        password: &str,
    ) -> anyhow::Result<()> {
        self.execute(format!(
            "ALTER USER {}@'%' IDENTIFIED BY {}",
            quote_literal(&user.to_string()),
            quote_literal(password)
        ))
        .await
    }

    async fn delete_user(&self, user: &UserIdentifier) -> anyhow::Result<()> {
        self.execute(format!(
            "DROP USER IF EXISTS {}@'%'",
            quote_literal(&user.to_string())
        ))
        .await
    }

    async fn grant_user(&self, user: &UserIdentifier, database: &str) -> anyhow::Result<()> {
        self.execute(format!(
            "GRANT ALL PRIVILEGES ON {}.* TO {}@'%'",
            quote_ident(database),
            quote_literal(&user.to_string())
        ))
        .await
    }

    async fn create_database(&self, name: &str) -> anyhow::Result<()> {
        self.execute(format!("CREATE DATABASE {}", quote_ident(name)))
            .await
            .map_err(duplicate_database)
    }

    async fn delete_database(&self, name: &str) -> anyhow::Result<()> {
        self.execute(format!("DROP DATABASE IF EXISTS {}", quote_ident(name)))
            .await
    }

    async fn recreate_database(&self, name: &str, _users: &[UserIdentifier]) -> anyhow::Result<()> {
        self.delete_database(name).await?;
        self.create_database(name).await
    }

    async fn get_size(&self, name: &str) -> anyhow::Result<i64> {
        let mut conn = self.conn().await?;
        let size: Option<i64> = sqlx::query_scalar(
            "SELECT CAST(SUM(data_length + index_length) AS SIGNED) FROM information_schema.tables WHERE table_schema = ?",
        )
        .bind(name)
        .fetch_one(&mut conn)
        .await?;
        let _ = conn.close().await;

        Ok(size.unwrap_or(0))
    }

    async fn query(&self, db: Option<&str>, query: &str) -> anyhow::Result<QueryResult> {
        let options = match db {
            Some(db) => self.options.clone().database(db),
            None => self.options.clone(),
        };
        let mut conn = MySqlConnection::connect_with(&options).await?;

        let mut result = QueryResult::default();
        let mut new_set = true;
        let mut stream = sqlx::raw_sql(sqlx::AssertSqlSafe(query.to_owned())).fetch_many(&mut conn);

        while let Some(item) = stream.next().await {
            match item? {
                Either::Left(done) => {
                    result.rows_affected += done.rows_affected();
                    new_set = true;
                }
                Either::Right(row) => {
                    if new_set {
                        result.columns = row
                            .columns()
                            .iter()
                            .map(|column| column.name().to_owned())
                            .collect();
                        new_set = false;
                    }

                    result.rows.push(row_values(&row));
                }
            }
        }

        drop(stream);
        let _ = conn.close().await;

        Ok(result)
    }
}
