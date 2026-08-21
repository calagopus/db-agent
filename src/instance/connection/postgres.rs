use super::{
    super::{DatabasePermission, identifier::UserIdentifier},
    DatabaseConnection, QueryResult,
};
use futures_util::StreamExt;
use sha2::Digest;
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

fn group_role(database: &str, suffix: &str) -> String {
    let digest = sha2::Sha256::digest(database.as_bytes());

    quote_ident(&format!(
        "d{}_{suffix}",
        hex::encode(digest.get(..8).unwrap_or_default())
    ))
}

#[inline]
fn read_only_role(database: &str) -> String {
    group_role(database, "ro")
}

#[inline]
fn read_write_role(database: &str) -> String {
    group_role(database, "rw")
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
        self.execute_in(ADMIN_DATABASE, sql).await
    }

    async fn execute_in(&self, database: &str, sql: &str) -> anyhow::Result<()> {
        let mut client = self.client(database).await?;
        client
            .execute(sqlx::raw_sql(sqlx::AssertSqlSafe(sql.to_owned())))
            .await?;
        let _ = client.close().await;
        Ok(())
    }

    async fn ensure_roles(&self, database: &str) -> anyhow::Result<()> {
        let (read_only, read_write) = (read_only_role(database), read_write_role(database));

        self.execute(&format!(
            "DO $$ BEGIN CREATE ROLE {read_only} NOLOGIN; EXCEPTION WHEN duplicate_object THEN NULL; END $$;
             DO $$ BEGIN CREATE ROLE {read_write} NOLOGIN; EXCEPTION WHEN duplicate_object THEN NULL; END $$"
        ))
        .await
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

        let databases: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT d.datname FROM pg_shdepend s
             JOIN pg_roles r ON r.oid = s.refobjid
             JOIN pg_database d ON d.oid = CASE
                 WHEN s.dbid <> 0 THEN s.dbid
                 WHEN s.classid = 'pg_database'::regclass THEN s.objid
             END
             WHERE r.rolname = $1 AND d.datallowconn AND d.datname <> $2",
        )
        .bind(&name)
        .bind(ADMIN_DATABASE)
        .fetch_all(&mut client)
        .await?;
        let _ = client.close().await;

        let role = quote_ident(&name);

        for database in &databases {
            self.ensure_roles(database).await?;
            self.execute_in(
                database,
                &format!(
                    "REASSIGN OWNED BY {role} TO {}; DROP OWNED BY {role}",
                    read_write_role(database)
                ),
            )
            .await?;

            self.execute(&format!(
                "REVOKE ALL PRIVILEGES ON DATABASE {} FROM {role}",
                quote_ident(database)
            ))
            .await?;
        }

        self.execute(&format!(
            "REASSIGN OWNED BY {role} TO {}; DROP OWNED BY {role}; DROP ROLE IF EXISTS {role}",
            quote_ident(ADMIN_USER)
        ))
        .await
    }

    async fn apply_permission(
        &self,
        user: &UserIdentifier,
        database: &str,
        permission: DatabasePermission,
    ) -> anyhow::Result<()> {
        self.bootstrap_database(database).await?;

        let (read_only, read_write) = (read_only_role(database), read_write_role(database));
        let role = quote_ident(&user.to_string());

        self.execute(&format!(
            "DO $$ BEGIN
                 SET LOCAL client_min_messages = error;
                 REVOKE {read_only}, {read_write} FROM {role};
                 REVOKE ALL PRIVILEGES ON DATABASE {} FROM {role};
             END $$",
            quote_ident(database)
        ))
        .await?;

        let reassign = match permission {
            DatabasePermission::ReadWrite => String::new(),
            _ => format!("REASSIGN OWNED BY {role} TO {read_write};"),
        };

        self.execute_in(
            database,
            &format!(
                "ALTER DEFAULT PRIVILEGES FOR ROLE {role} IN SCHEMA public REVOKE ALL ON TABLES FROM {read_only}, {read_write};
                 ALTER DEFAULT PRIVILEGES FOR ROLE {role} IN SCHEMA public REVOKE ALL ON SEQUENCES FROM {read_only}, {read_write};
                 ALTER DEFAULT PRIVILEGES FOR ROLE {role} IN SCHEMA public REVOKE ALL ON FUNCTIONS FROM {read_only}, {read_write};
                 REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM {role};
                 REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM {role};
                 REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public FROM {role};
                 REVOKE ALL PRIVILEGES ON SCHEMA public FROM {role};
                 {reassign}"
            ),
        )
        .await?;

        let group = match permission {
            DatabasePermission::None => return Ok(()),
            DatabasePermission::ReadOnly => &read_only,
            DatabasePermission::ReadWrite => &read_write,
        };

        self.execute(&format!("GRANT {group} TO {role}")).await?;

        if permission == DatabasePermission::ReadWrite {
            self.execute_in(
                database,
                &format!(
                    "ALTER DEFAULT PRIVILEGES FOR ROLE {role} IN SCHEMA public GRANT SELECT ON TABLES TO {read_only};
                     ALTER DEFAULT PRIVILEGES FOR ROLE {role} IN SCHEMA public GRANT SELECT ON SEQUENCES TO {read_only};
                     ALTER DEFAULT PRIVILEGES FOR ROLE {role} IN SCHEMA public GRANT ALL ON TABLES TO {read_write};
                     ALTER DEFAULT PRIVILEGES FOR ROLE {role} IN SCHEMA public GRANT ALL ON SEQUENCES TO {read_write};
                     ALTER DEFAULT PRIVILEGES FOR ROLE {role} IN SCHEMA public GRANT EXECUTE ON FUNCTIONS TO {read_only}, {read_write}"
                ),
            )
            .await?;
        }

        Ok(())
    }

    async fn bootstrap_database(&self, name: &str) -> anyhow::Result<()> {
        self.ensure_roles(name).await?;

        let (read_only, read_write) = (read_only_role(name), read_write_role(name));

        self.execute(&format!(
            "REVOKE CONNECT, TEMPORARY ON DATABASE {name} FROM PUBLIC;
             GRANT CONNECT ON DATABASE {name} TO {read_only}, {read_write};
             GRANT CREATE, TEMPORARY ON DATABASE {name} TO {read_write}",
            name = quote_ident(name)
        ))
        .await?;

        self.execute_in(
            name,
            &format!(
                "GRANT USAGE ON SCHEMA public TO {read_only};
                 GRANT USAGE, CREATE ON SCHEMA public TO {read_write};
                 GRANT SELECT ON ALL TABLES IN SCHEMA public TO {read_only};
                 GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO {read_only};
                 GRANT ALL ON ALL TABLES IN SCHEMA public TO {read_write};
                 GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO {read_write};
                 GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO {read_only}, {read_write};
                 ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO {read_only};
                 ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON SEQUENCES TO {read_only};
                 ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO {read_write};
                 ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO {read_write};
                 ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT EXECUTE ON FUNCTIONS TO {read_only}, {read_write}"
            ),
        )
        .await
    }

    async fn create_database(&self, name: &str) -> anyhow::Result<()> {
        self.execute(&format!("CREATE DATABASE {}", quote_ident(name)))
            .await
            .map_err(duplicate_database)?;

        self.bootstrap_database(name).await
    }

    async fn delete_database(&self, name: &str) -> anyhow::Result<()> {
        self.execute(&format!(
            "DROP DATABASE IF EXISTS {} WITH (FORCE)",
            quote_ident(name)
        ))
        .await?;

        if let Err(err) = self
            .execute(&format!(
                "DROP ROLE IF EXISTS {}, {}",
                read_only_role(name),
                read_write_role(name)
            ))
            .await
        {
            tracing::warn!(database = %name, "failed to drop database group roles: {err:#}");
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
