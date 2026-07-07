use super::{
    super::identifier::{DbIdentifier, UserIdentifier},
    DatabaseConnection, QueryResult,
};
use mongodb::bson::{Bson, Document, doc};
use std::path::PathBuf;

const ADMIN_DATABASE: &str = "admin";

pub struct MongodbConnection {
    client: mongodb::Client,
}

impl MongodbConnection {
    pub fn new(socket: PathBuf) -> anyhow::Result<Self> {
        let options = mongodb::options::ClientOptions::builder()
            .hosts(vec![mongodb::options::ServerAddress::Unix { path: socket }])
            .direct_connection(true)
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

    async fn create_database(
        &self,
        db: &DbIdentifier,
        owner: &UserIdentifier,
    ) -> anyhow::Result<()> {
        self.run_admin(doc! {
            "grantRolesToUser": owner.to_string(),
            "roles": [{ "role": "dbOwner", "db": db.to_string() }],
        })
        .await
    }

    async fn delete_database(&self, db: &DbIdentifier) -> anyhow::Result<()> {
        self.client.database(&db.to_string()).drop().await?;
        Ok(())
    }

    async fn query(&self, db: Option<&DbIdentifier>, query: &str) -> anyhow::Result<QueryResult> {
        let command: Document = serde_json::from_str(query)?;
        let database = db.map(|d| d.to_string());

        let reply = self
            .client
            .database(database.as_deref().unwrap_or(ADMIN_DATABASE))
            .run_command(command)
            .await?;

        Ok(QueryResult {
            columns: vec!["reply".to_string()],
            rows: vec![vec![Bson::Document(reply).into_relaxed_extjson()]],
            rows_affected: 0,
        })
    }
}
