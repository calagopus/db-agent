use super::{super::identifier::UserIdentifier, DatabaseConnection, QueryResult};
use mysql_async::prelude::*;
use std::path::PathBuf;

const ADMIN_USER: &str = "root";

#[inline]
fn quote_ident(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

#[inline]
fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
}

fn value_to_json(value: mysql_async::Value) -> serde_json::Value {
    match value {
        mysql_async::Value::NULL => serde_json::Value::Null,
        mysql_async::Value::Bytes(bytes) => {
            serde_json::Value::String(String::from_utf8_lossy(&bytes).into_owned())
        }
        mysql_async::Value::Int(i) => i.into(),
        mysql_async::Value::UInt(u) => u.into(),
        mysql_async::Value::Float(f) => serde_json::json!(f),
        mysql_async::Value::Double(d) => serde_json::json!(d),
        date_or_time => serde_json::Value::String(date_or_time.as_sql(true)),
    }
}

pub struct MariadbConnection {
    opts: mysql_async::Opts,
}

impl MariadbConnection {
    pub fn new(socket: PathBuf) -> Self {
        Self {
            opts: mysql_async::OptsBuilder::default()
                .socket(Some(socket.to_string_lossy().into_owned()))
                .user(Some(ADMIN_USER))
                .into(),
        }
    }

    async fn conn(&self) -> anyhow::Result<mysql_async::Conn> {
        Ok(mysql_async::Conn::new(self.opts.clone()).await?)
    }

    async fn execute(&self, sql: String) -> anyhow::Result<()> {
        let mut conn = self.conn().await?;
        conn.query_drop(sql).await?;
        conn.disconnect().await?;
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
    }

    async fn delete_database(&self, name: &str) -> anyhow::Result<()> {
        self.execute(format!("DROP DATABASE IF EXISTS {}", quote_ident(name)))
            .await
    }

    async fn query(&self, db: Option<&str>, query: &str) -> anyhow::Result<QueryResult> {
        let mut conn = self.conn().await?;
        if let Some(db) = db {
            conn.query_drop(format!("USE {}", quote_ident(db))).await?;
        }

        let mut query_result = conn.query_iter(query).await?;
        let mut result = QueryResult {
            columns: query_result
                .columns()
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|c| c.name_str().into_owned())
                .collect(),
            rows_affected: query_result.affected_rows(),
            ..Default::default()
        };

        result.rows = query_result
            .collect::<mysql_async::Row>()
            .await?
            .into_iter()
            .map(|row| row.unwrap().into_iter().map(value_to_json).collect())
            .collect();

        drop(query_result);
        conn.disconnect().await?;

        Ok(result)
    }
}
