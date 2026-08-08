use super::{super::identifier::UserIdentifier, DatabaseConnection, QueryResult};
use std::path::PathBuf;
use tokio_postgres::{NoTls, SimpleQueryMessage, error::SqlState};

const ADMIN_USER: &str = "postgres";
const ADMIN_DATABASE: &str = "postgres";

#[inline]
fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

#[inline]
fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn duplicate_database(err: anyhow::Error) -> anyhow::Error {
    match err
        .downcast_ref::<tokio_postgres::Error>()
        .and_then(|err| err.code())
    {
        Some(code) if *code == SqlState::DUPLICATE_DATABASE => {
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

    async fn client(&self, database: &str) -> anyhow::Result<tokio_postgres::Client> {
        let stream = tokio::net::UnixStream::connect(&self.socket).await?;
        let (client, connection) = tokio_postgres::Config::new()
            .user(ADMIN_USER)
            .dbname(database)
            .connect_raw(stream, NoTls)
            .await?;

        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::debug!("postgres admin connection error: {err}");
            }
        });

        Ok(client)
    }

    async fn execute(&self, sql: &str) -> anyhow::Result<()> {
        self.client(ADMIN_DATABASE).await?.simple_query(sql).await?;
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
        let client = self.client(ADMIN_DATABASE).await?;

        if client
            .query_opt("SELECT 1 FROM pg_roles WHERE rolname = $1", &[&name])
            .await?
            .is_none()
        {
            return Ok(());
        }

        let role = quote_ident(&name);
        let owner = quote_ident(ADMIN_USER);

        // DROP OWNED BY only covers the current database, so every database the role
        // holds privileges on needs its own connection
        let databases = client
            .query(
                "SELECT DISTINCT d.datname FROM pg_database d
                 CROSS JOIN LATERAL aclexplode(d.datacl) a
                 JOIN pg_roles r ON r.oid = a.grantee
                 WHERE r.rolname = $1 AND d.datallowconn AND d.datname <> $2",
                &[&name, &ADMIN_DATABASE],
            )
            .await?;

        for row in &databases {
            let database: &str = row.get(0);

            self.client(database)
                .await?
                .simple_query(&format!(
                    "REASSIGN OWNED BY {role} TO {owner}; DROP OWNED BY {role}"
                ))
                .await?;

            client
                .simple_query(&format!(
                    "REVOKE ALL PRIVILEGES ON DATABASE {} FROM {role}",
                    quote_ident(database)
                ))
                .await?;
        }

        client
            .simple_query(&format!(
                "REASSIGN OWNED BY {role} TO {owner}; DROP OWNED BY {role}; DROP ROLE IF EXISTS {role}"
            ))
            .await?;

        Ok(())
    }

    async fn grant_user(&self, user: &UserIdentifier, database: &str) -> anyhow::Result<()> {
        let user = quote_ident(&user.to_string());
        self.execute(&format!(
            "GRANT ALL PRIVILEGES ON DATABASE {} TO {user}",
            quote_ident(database),
        ))
        .await?;

        self.client(database)
            .await?
            .simple_query(&format!("GRANT ALL ON SCHEMA public TO {user}"))
            .await?;

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
        let row = self
            .client(ADMIN_DATABASE)
            .await?
            .query_one("SELECT pg_database_size($1)", &[&name])
            .await?;

        Ok(row.get::<_, i64>(0))
    }

    async fn query(&self, db: Option<&str>, query: &str) -> anyhow::Result<QueryResult> {
        let client = self.client(db.unwrap_or(ADMIN_DATABASE)).await?;

        let mut result = QueryResult::default();
        for message in client.simple_query(query).await? {
            match message {
                SimpleQueryMessage::RowDescription(columns) => {
                    result.columns = columns.iter().map(|c| c.name().to_string()).collect();
                }
                SimpleQueryMessage::Row(row) => {
                    result.rows.push(
                        (0..row.len())
                            .map(|i| match row.get(i) {
                                Some(value) => serde_json::Value::String(value.to_string()),
                                None => serde_json::Value::Null,
                            })
                            .collect(),
                    );
                }
                SimpleQueryMessage::CommandComplete(count) => {
                    result.rows_affected += count;
                }
                _ => {}
            }
        }

        Ok(result)
    }
}
