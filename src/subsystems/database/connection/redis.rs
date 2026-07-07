use super::{
    super::identifier::{DbIdentifier, UserIdentifier},
    DatabaseConnection, QueryResult,
};
use std::path::PathBuf;

fn value_to_json(value: redis::Value) -> serde_json::Value {
    match value {
        redis::Value::Nil => serde_json::Value::Null,
        redis::Value::Int(i) => i.into(),
        redis::Value::Double(d) => serde_json::json!(d),
        redis::Value::Boolean(b) => b.into(),
        redis::Value::BulkString(bytes) => {
            serde_json::Value::String(String::from_utf8_lossy(&bytes).into_owned())
        }
        redis::Value::SimpleString(s) => serde_json::Value::String(s),
        redis::Value::VerbatimString { text, .. } => serde_json::Value::String(text),
        redis::Value::BigNumber(n) => {
            serde_json::Value::String(String::from_utf8_lossy(&n).into_owned())
        }
        redis::Value::Okay => serde_json::Value::String("OK".to_string()),
        redis::Value::Array(values) | redis::Value::Set(values) => {
            serde_json::Value::Array(values.into_iter().map(value_to_json).collect())
        }
        redis::Value::Map(pairs) => serde_json::Value::Array(
            pairs
                .into_iter()
                .map(|(k, v)| serde_json::json!([value_to_json(k), value_to_json(v)]))
                .collect(),
        ),
        other => serde_json::Value::String(format!("{other:?}")),
    }
}

pub struct RedisConnection {
    client: redis::Client,
}

impl RedisConnection {
    pub fn new(socket: PathBuf) -> anyhow::Result<Self> {
        Ok(Self {
            client: redis::Client::open(format!("unix://{}", socket.display()))?,
        })
    }

    async fn conn(&self) -> anyhow::Result<redis::aio::MultiplexedConnection> {
        Ok(self.client.get_multiplexed_async_connection().await?)
    }
}

#[async_trait::async_trait]
impl DatabaseConnection for RedisConnection {
    async fn create_user(&self, _user: &UserIdentifier, _password: &str) -> anyhow::Result<()> {
        Ok(())
    }

    async fn update_user_password(
        &self,
        _user: &UserIdentifier,
        _password: &str,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn delete_user(&self, _user: &UserIdentifier) -> anyhow::Result<()> {
        Ok(())
    }

    async fn create_database(
        &self,
        _db: &DbIdentifier,
        _owner: &UserIdentifier,
    ) -> anyhow::Result<()> {
        anyhow::bail!("redis has no named databases");
    }

    async fn delete_database(&self, _db: &DbIdentifier) -> anyhow::Result<()> {
        anyhow::bail!("redis has no named databases");
    }

    async fn query(&self, _db: Option<&DbIdentifier>, query: &str) -> anyhow::Result<QueryResult> {
        let mut parts = query.split_whitespace();
        let Some(command) = parts.next() else {
            anyhow::bail!("empty command");
        };

        let mut cmd = redis::cmd(command);
        for arg in parts {
            cmd.arg(arg);
        }

        let value: redis::Value = cmd.query_async(&mut self.conn().await?).await?;
        let rows = match value_to_json(value) {
            serde_json::Value::Array(values) => values.into_iter().map(|v| vec![v]).collect(),
            value => vec![vec![value]],
        };

        Ok(QueryResult {
            columns: vec!["reply".to_string()],
            rows,
            rows_affected: 0,
        })
    }
}
