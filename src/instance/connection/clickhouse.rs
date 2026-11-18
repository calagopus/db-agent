use super::{
    super::{DatabasePermission, identifier::UserIdentifier},
    DatabaseConnection, QueryResult,
};
use serde::Deserialize;
use std::path::PathBuf;

pub const ROOT_USERNAME: &str = "calagopus_root";
pub static BACKUP_RUN_ID: std::sync::LazyLock<uuid::Uuid> =
    std::sync::LazyLock::new(uuid::Uuid::new_v4);

const SYSTEM_DATABASES: &str = "system, information_schema, INFORMATION_SCHEMA";
const SYSTEM_DATABASE_NAMES: [&str; 3] = ["system", "information_schema", "INFORMATION_SCHEMA"];

/// the `(database, table)` pairs of a `metadata/<db>/<table>.sql` backup listing
pub fn parse_backup_tables(listing: &str) -> Vec<(String, String)> {
    let decode = |name: &str| {
        percent_encoding::percent_decode_str(name)
            .decode_utf8()
            .ok()
            .map(|name| name.into_owned())
    };

    listing
        .lines()
        .filter_map(|line| {
            let (database, table) = line
                .strip_prefix("metadata/")?
                .strip_suffix(".sql")?
                .split_once('/')?;
            Some((decode(database)?, decode(table)?))
        })
        .collect()
}

fn ident(name: &str) -> String {
    format!("`{}`", name.replace('\\', "\\\\").replace('`', "\\`"))
}

#[derive(Deserialize)]
struct JsonCompact {
    meta: Vec<Column>,
    data: Vec<Vec<serde_json::Value>>,
}

#[derive(Deserialize)]
struct Column {
    name: String,
}

#[derive(Deserialize)]
struct Exception {
    exception: String,
}

#[derive(Deserialize, Default)]
struct Summary {
    #[serde(default)]
    written_rows: String,
}

pub struct ClickhouseConnection {
    client: reqwest::Client,
    user: &'static str,
    password: String,
}

impl ClickhouseConnection {
    pub fn new(socket: PathBuf, root_password: Option<&str>) -> anyhow::Result<Self> {
        Ok(Self {
            client: reqwest::Client::builder().unix_socket(socket).build()?,
            user: if root_password.is_some() {
                ROOT_USERNAME
            } else {
                "default"
            },
            password: root_password.unwrap_or_default().to_string(),
        })
    }

    async fn run(&self, query: &str, params: &[(&str, &str)]) -> anyhow::Result<reqwest::Response> {
        let url = url::Url::parse_with_params("http://localhost/", params)?;
        let response = self
            .client
            .post(url)
            .header("X-ClickHouse-User", self.user)
            .header("X-ClickHouse-Key", &self.password)
            .body(query.to_string())
            .send()
            .await?;

        if !response.status().is_success() {
            let body = response.text().await?;
            let message = serde_json::from_str::<Exception>(&body)
                .map(|e| e.exception)
                .unwrap_or(body);
            return Err(
                crate::response::DisplayError::new(message.trim().to_string())
                    .with_status(axum::http::StatusCode::BAD_REQUEST)
                    .into(),
            );
        }

        Ok(response)
    }

    pub async fn create_root(&self, password: &str) -> anyhow::Result<()> {
        let root = ident(ROOT_USERNAME);
        self.run(
            &format!(
                "CREATE USER OR REPLACE {root} IDENTIFIED WITH sha256_password BY {{p:String}}"
            ),
            &[("param_p", password)],
        )
        .await?;
        self.run(
            &format!("GRANT CURRENT GRANTS ON *.* TO {root} WITH GRANT OPTION"),
            &[],
        )
        .await?;

        Ok(())
    }

    pub async fn backup_dir(&self) -> anyhow::Result<String> {
        let path = self
            .run("SELECT path FROM system.disks WHERE name = 'default'", &[])
            .await?
            .text()
            .await?;

        Ok(format!("{}/backups", path.trim().trim_end_matches('/')))
    }

    pub async fn backup_file(&self) -> anyhow::Result<String> {
        Ok(format!(
            "{}/calagopus-{}-{}.zip",
            self.backup_dir().await?,
            *BACKUP_RUN_ID,
            uuid::Uuid::new_v4()
        ))
    }

    pub async fn backup(&self, db: Option<&str>, file: &str) -> anyhow::Result<()> {
        let target = match db {
            Some(db) => format!("DATABASE {}", ident(db)),
            None => format!("ALL EXCEPT DATABASES {SYSTEM_DATABASES}"),
        };

        self.run(
            &format!("BACKUP {target} TO File({{f:String}})"),
            &[("param_f", file)],
        )
        .await
        .map(|_| ())
    }

    /// drops the tables a restore of `tables` would recreate
    pub async fn drop_restore_targets(
        &self,
        target: Option<(&str, &str)>,
        tables: &[(String, String)],
    ) -> anyhow::Result<()> {
        for (database, table) in tables {
            let database = match target {
                Some((db, source_db)) if database == source_db => db,
                Some(_) => continue,
                None if SYSTEM_DATABASE_NAMES.contains(&database.as_str()) => continue,
                None => database.as_str(),
            };

            let engine = self
                .run(
                    "SELECT engine FROM system.tables WHERE database = {d:String} AND name = {t:String}",
                    &[("param_d", database), ("param_t", table)],
                )
                .await?
                .text()
                .await?;
            let kind = match engine.trim() {
                "" => continue,
                "Dictionary" => "DICTIONARY",
                _ => "TABLE",
            };

            self.run(
                &format!(
                    "DROP {kind} IF EXISTS {}.{} SYNC",
                    ident(database),
                    ident(table)
                ),
                &[],
            )
            .await?;
        }

        Ok(())
    }

    pub async fn restore(&self, target: Option<(&str, &str)>, file: &str) -> anyhow::Result<()> {
        let target = match target {
            Some((db, source_db)) => format!("DATABASE {} AS {}", ident(source_db), ident(db)),
            None => format!("ALL EXCEPT DATABASES {SYSTEM_DATABASES}"),
        };

        self.run(
            &format!("RESTORE {target} FROM File({{f:String}})"),
            &[("param_f", file)],
        )
        .await
        .map(|_| ())
    }
}

#[async_trait::async_trait]
impl DatabaseConnection for ClickhouseConnection {
    async fn create_user(&self, user: &UserIdentifier, password: &str) -> anyhow::Result<()> {
        self.run(
            &format!(
                "CREATE USER {} IDENTIFIED WITH sha256_password BY {{p:String}}",
                ident(&user.to_string())
            ),
            &[("param_p", password)],
        )
        .await
        .map(|_| ())
    }

    async fn update_user_password(
        &self,
        user: &UserIdentifier,
        password: &str,
    ) -> anyhow::Result<()> {
        self.run(
            &format!(
                "ALTER USER {} IDENTIFIED WITH sha256_password BY {{p:String}}",
                ident(&user.to_string())
            ),
            &[("param_p", password)],
        )
        .await
        .map(|_| ())
    }

    async fn delete_user(&self, user: &UserIdentifier) -> anyhow::Result<()> {
        self.run(
            &format!("DROP USER IF EXISTS {}", ident(&user.to_string())),
            &[],
        )
        .await
        .map(|_| ())
    }

    async fn apply_permission(
        &self,
        user: &UserIdentifier,
        database: &str,
        permission: DatabasePermission,
    ) -> anyhow::Result<()> {
        let (user, database) = (ident(&user.to_string()), ident(database));

        self.run(&format!("REVOKE ALL ON {database}.* FROM {user}"), &[])
            .await?;
        let privileges = match permission {
            DatabasePermission::None => return Ok(()),
            DatabasePermission::ReadOnly => "SELECT, SHOW",
            DatabasePermission::ReadWrite => "ALL",
        };
        self.run(
            &format!("GRANT {privileges} ON {database}.* TO {user}"),
            &[],
        )
        .await
        .map(|_| ())
    }

    async fn create_database(&self, name: &str) -> anyhow::Result<()> {
        self.run(
            &format!("CREATE DATABASE IF NOT EXISTS {}", ident(name)),
            &[],
        )
        .await
        .map(|_| ())
    }

    async fn delete_database(&self, name: &str) -> anyhow::Result<()> {
        self.run(
            &format!("DROP DATABASE IF EXISTS {} SYNC", ident(name)),
            &[],
        )
        .await
        .map(|_| ())
    }

    async fn get_size(&self, name: &str) -> anyhow::Result<i64> {
        let size = self
            .run(
                "SELECT sum(bytes_on_disk) FROM system.parts WHERE database = {d:String} AND active",
                &[("param_d", name)],
            )
            .await?
            .text()
            .await?;

        Ok(size.trim().parse().unwrap_or(0))
    }

    async fn query(&self, db: Option<&str>, query: &str) -> anyhow::Result<QueryResult> {
        let mut params = vec![
            ("default_format", "JSONCompact"),
            ("output_format_json_quote_64bit_integers", "0"),
        ];
        if let Some(db) = db {
            params.push(("database", db));
        }

        let response = self.run(query, &params).await?;
        let rows_affected = response
            .headers()
            .get("X-ClickHouse-Summary")
            .and_then(|summary| serde_json::from_slice::<Summary>(summary.as_bytes()).ok())
            .unwrap_or_default()
            .written_rows
            .parse()
            .unwrap_or(0);

        let body = response.bytes().await?;
        if body.iter().all(u8::is_ascii_whitespace) {
            return Ok(QueryResult {
                rows_affected,
                ..Default::default()
            });
        }

        let result: JsonCompact = serde_json::from_slice(&body)?;
        Ok(QueryResult {
            columns: result.meta.into_iter().map(|c| c.name).collect(),
            rows: result.data,
            rows_affected,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ident

    #[test]
    fn ident_quotes_plain_name() {
        assert_eq!(ident("my_db"), "`my_db`");
    }

    #[test]
    fn ident_escapes_backticks_and_backslashes() {
        assert_eq!(ident("a`b"), r"`a\`b`");
        assert_eq!(ident(r"a\b"), r"`a\\b`");
        assert_eq!(
            ident(r"x\`; DROP DATABASE y; --"),
            r"`x\\\`; DROP DATABASE y; --`"
        );
        assert_eq!(ident(r"trailing\"), r"`trailing\\`");
    }
}
