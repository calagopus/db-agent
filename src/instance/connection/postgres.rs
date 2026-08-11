use super::{super::identifier::UserIdentifier, DatabaseConnection, QueryResult};
use futures_util::StreamExt;
use sqlx::{
    Column, Connection, Either, Executor, Row, ValueRef,
    postgres::{PgConnectOptions, PgConnection},
};
use std::path::{Path, PathBuf};

pub const ADMIN_USER: &str = "postgres";
pub const ADMIN_DATABASE: &str = "postgres";
pub const DEFAULT_PORT: u16 = 5432;

const SOCKET_PREFIX: &str = ".s.PGSQL.";

pub fn connect_options(socket: &Path, database: &str) -> PgConnectOptions {
    let port = socket
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix(SOCKET_PREFIX))
        .and_then(|port| port.parse().ok())
        .unwrap_or(DEFAULT_PORT);

    PgConnectOptions::new()
        .socket(socket.parent().unwrap_or(socket))
        .port(port)
        .username(ADMIN_USER)
        .database(database)
}

#[inline]
fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

#[inline]
fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn duplicate_database(err: anyhow::Error) -> anyhow::Error {
    const DUPLICATE_DATABASE: &str = "42P04";

    match err.downcast_ref::<sqlx::Error>() {
        Some(sqlx::Error::Database(server))
            if server.code().as_deref() == Some(DUPLICATE_DATABASE) =>
        {
            crate::response::DisplayError::new("database already exists")
                .with_status(axum::http::StatusCode::CONFLICT)
                .into()
        }
        _ => err,
    }
}

pub struct PostgresConnection {
    socket: PathBuf,
}

impl PostgresConnection {
    pub fn new(socket: PathBuf) -> Self {
        Self { socket }
    }

    async fn client(&self, database: &str) -> anyhow::Result<PgConnection> {
        Ok(PgConnection::connect_with(&connect_options(&self.socket, database)).await?)
    }

    async fn execute(&self, sql: &str) -> anyhow::Result<()> {
        let mut client = self.client(ADMIN_DATABASE).await?;
        client
            .execute(sqlx::raw_sql(sqlx::AssertSqlSafe(sql.to_owned())))
            .await?;
        let _ = client.close().await;
        Ok(())
    }
}

#[async_trait::async_trait]
impl DatabaseConnection for PostgresConnection {
    async fn create_user(&self, user: &UserIdentifier, password: &str) -> anyhow::Result<()> {
        self.execute(&format!(
            "CREATE ROLE {} LOGIN PASSWORD {}",
            quote_ident(&user.to_string()),
            quote_literal(password)
        ))
        .await
    }

    async fn update_user_password(
        &self,
        user: &UserIdentifier,
        password: &str,
    ) -> anyhow::Result<()> {
        self.execute(&format!(
            "ALTER ROLE {} PASSWORD {}",
            quote_ident(&user.to_string()),
            quote_literal(password)
        ))
        .await
    }

    async fn delete_user(&self, user: &UserIdentifier) -> anyhow::Result<()> {
        let name = user.to_string();
        let mut client = self.client(ADMIN_DATABASE).await?;

        if sqlx::query("SELECT 1 FROM pg_roles WHERE rolname = $1")
            .bind(&name)
            .fetch_optional(&mut client)
            .await?
            .is_none()
        {
            let _ = client.close().await;
            return Ok(());
        }

        let role = quote_ident(&name);
        let owner = quote_ident(ADMIN_USER);

        // DROP OWNED BY only covers the current database, so every database the role
        // holds privileges on needs its own connection
        let databases: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT d.datname FROM pg_database d
             CROSS JOIN LATERAL aclexplode(d.datacl) a
             JOIN pg_roles r ON r.oid = a.grantee
             WHERE r.rolname = $1 AND d.datallowconn AND d.datname <> $2",
        )
        .bind(&name)
        .bind(ADMIN_DATABASE)
        .fetch_all(&mut client)
        .await?;

        for database in &databases {
            let mut scoped = self.client(database).await?;
            scoped
                .execute(sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                    "REASSIGN OWNED BY {role} TO {owner}; DROP OWNED BY {role}"
                ))))
                .await?;
            let _ = scoped.close().await;

            client
                .execute(sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                    "REVOKE ALL PRIVILEGES ON DATABASE {} FROM {role}",
                    quote_ident(database)
                ))))
                .await?;
        }

        client
            .execute(sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                "REASSIGN OWNED BY {role} TO {owner}; DROP OWNED BY {role}; DROP ROLE IF EXISTS {role}"
            ))))
            .await?;
        let _ = client.close().await;

        Ok(())
    }

    async fn grant_user(&self, user: &UserIdentifier, database: &str) -> anyhow::Result<()> {
        let user = quote_ident(&user.to_string());
        self.execute(&format!(
            "GRANT ALL PRIVILEGES ON DATABASE {} TO {user}",
            quote_ident(database),
        ))
        .await?;

        let mut client = self.client(database).await?;
        client
            .execute(sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                "GRANT ALL ON SCHEMA public TO {user}"
            ))))
            .await?;
        let _ = client.close().await;

        Ok(())
    }

    async fn create_database(&self, name: &str) -> anyhow::Result<()> {
        let name = quote_ident(name);

        // CREATE DATABASE cannot share an implicit transaction with the revoke
        self.execute(&format!("CREATE DATABASE {name}"))
            .await
            .map_err(duplicate_database)?;
        self.execute(&format!(
            "REVOKE CONNECT, TEMPORARY ON DATABASE {name} FROM PUBLIC"
        ))
        .await
    }

    async fn delete_database(&self, name: &str) -> anyhow::Result<()> {
        self.execute(&format!(
            "DROP DATABASE IF EXISTS {} WITH (FORCE)",
            quote_ident(name)
        ))
        .await
    }

    async fn recreate_database(&self, name: &str, users: &[UserIdentifier]) -> anyhow::Result<()> {
        self.delete_database(name).await?;
        self.create_database(name).await?;

        for user in users {
            self.grant_user(user, name).await?;
        }

        Ok(())
    }

    async fn get_size(&self, name: &str) -> anyhow::Result<i64> {
        let mut client = self.client(ADMIN_DATABASE).await?;
        let size: i64 = sqlx::query_scalar("SELECT pg_database_size($1)")
            .bind(name)
            .fetch_one(&mut client)
            .await?;
        let _ = client.close().await;

        Ok(size)
    }

    async fn query(&self, db: Option<&str>, query: &str) -> anyhow::Result<QueryResult> {
        let mut client = self.client(db.unwrap_or(ADMIN_DATABASE)).await?;

        let mut result = QueryResult::default();
        let mut new_set = true;
        let mut stream =
            sqlx::raw_sql(sqlx::AssertSqlSafe(query.to_owned())).fetch_many(&mut client);

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

                    result.rows.push(
                        (0..row.columns().len())
                            .map(|ordinal| {
                                let value = match row.try_get_raw(ordinal) {
                                    Ok(value) => value,
                                    Err(_) => return serde_json::Value::Null,
                                };
                                if value.is_null() {
                                    return serde_json::Value::Null;
                                }

                                match value.as_bytes() {
                                    Ok(bytes) => serde_json::Value::String(
                                        String::from_utf8_lossy(bytes).into_owned(),
                                    ),
                                    Err(_) => serde_json::Value::Null,
                                }
                            })
                            .collect(),
                    );
                }
            }
        }

        drop(stream);
        let _ = client.close().await;

        Ok(result)
    }
}
