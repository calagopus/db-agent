use super::{super::identifier::UserIdentifier, DatabaseConnection, QueryResult};
use mongodb::bson::{Bson, Document, doc};
use std::path::PathBuf;

const ADMIN_DATABASE: &str = "admin";
pub const ROOT_USERNAME: &str = "calagopus_root";

pub struct MongodbConnection {
    client: mongodb::Client,
}

impl MongodbConnection {
    pub fn new(socket: PathBuf, root_password: Option<&str>) -> anyhow::Result<Self> {
        let options = mongodb::options::ClientOptions::builder()
            .hosts(vec![mongodb::options::ServerAddress::Unix { path: socket }])
            .direct_connection(true)
            .server_selection_timeout(std::time::Duration::from_secs(5))
            .credential(root_password.map(|password| {
                mongodb::options::Credential::builder()
                    .username(ROOT_USERNAME.to_string())
                    .password(password.to_string())
                    .source(ADMIN_DATABASE.to_string())
                    .build()
            }))
            .build();

        Ok(Self {
            client: mongodb::Client::with_options(options)?,
        })
    }

    async fn run_admin(&self, command: Document) -> anyhow::Result<()> {
        self.client
            .database(ADMIN_DATABASE)
            .run_command(command)
            .await?;
        Ok(())
    }

    pub async fn create_root(&self, password: &str) -> anyhow::Result<()> {
        self.run_admin(doc! {
            "createUser": ROOT_USERNAME,
            "pwd": password,
            "roles": [{ "role": "root", "db": ADMIN_DATABASE }],
        })
        .await
    }

    pub async fn auth_enforced(&self) -> anyhow::Result<bool> {
        match self
            .client
            .database(ADMIN_DATABASE)
            .run_command(doc! { "usersInfo": 1 })
            .await
        {
            Ok(_) => Ok(false),
            Err(err) if is_unauthorized(&err) => Ok(true),
            Err(err) => Err(err.into()),
        }
    }
}

fn is_unauthorized(err: &mongodb::error::Error) -> bool {
    const UNAUTHORIZED: i32 = 13;
    matches!(*err.kind, mongodb::error::ErrorKind::Command(ref c) if c.code == UNAUTHORIZED)
}

#[async_trait::async_trait]
impl DatabaseConnection for MongodbConnection {
    async fn create_user(&self, user: &UserIdentifier, password: &str) -> anyhow::Result<()> {
        self.run_admin(doc! {
            "createUser": user.to_string(),
            "pwd": password,
            "roles": [],
        })
        .await
    }

    async fn update_user_password(
        &self,
        user: &UserIdentifier,
        password: &str,
    ) -> anyhow::Result<()> {
        self.run_admin(doc! {
            "updateUser": user.to_string(),
            "pwd": password,
        })
        .await
    }

    async fn delete_user(&self, user: &UserIdentifier) -> anyhow::Result<()> {
        self.run_admin(doc! { "dropUser": user.to_string() }).await
    }

    async fn grant_user(&self, user: &UserIdentifier, database: &str) -> anyhow::Result<()> {
        self.run_admin(doc! {
            "grantRolesToUser": user.to_string(),
            "roles": [{ "role": "dbOwner", "db": database }],
        })
        .await
    }

    async fn create_database(&self, _name: &str) -> anyhow::Result<()> {
        Ok(())
    }

    async fn delete_database(&self, name: &str) -> anyhow::Result<()> {
        self.client.database(name).drop().await?;
        Ok(())
    }

    async fn recreate_database(&self, name: &str, _users: &[UserIdentifier]) -> anyhow::Result<()> {
        self.delete_database(name).await
    }

    async fn get_size(&self, name: &str) -> anyhow::Result<i64> {
        let stats = self
            .client
            .database(name)
            .run_command(doc! { "dbStats": 1, "scale": 1 })
            .await?;

        Ok(match stats.get("dataSize") {
            Some(Bson::Int32(i)) => *i as i64,
            Some(Bson::Int64(i)) => *i,
            Some(Bson::Double(f)) => *f as i64,
            _ => 0,
        })
    }

    async fn query(&self, db: Option<&str>, query: &str) -> anyhow::Result<QueryResult> {
        let command: Document = serde_json::from_str(query)?;

        let reply = self
            .client
            .database(db.unwrap_or(ADMIN_DATABASE))
            .run_command(command)
            .await?;

        Ok(QueryResult {
            columns: vec!["reply".to_string()],
            rows: vec![vec![Bson::Document(reply).into_relaxed_extjson()]],
            rows_affected: 0,
        })
    }
}
