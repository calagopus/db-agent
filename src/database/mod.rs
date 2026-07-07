use anyhow::Context;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool};
use std::{path::Path, str::FromStr, sync::Arc, time::Instant};

pub mod data;

pub struct Database {
    sqlite: SqlitePool,
}

impl Database {
    pub async fn new(config: Arc<crate::config::Config>) -> anyhow::Result<Self> {
        let (url, migrate) = {
            let config = config.load();
            (config.database.url.clone(), config.database.migrate)
        };

        let path = url
            .trim_start_matches("sqlite://")
            .trim_start_matches("sqlite:");
        if let Some(parent) = Path::new(path).parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create database directory {parent:?}"))?;
        }

        let sqlite = SqlitePool::connect_with(
            SqliteConnectOptions::from_str(&url)
                .context("invalid database url")?
                .create_if_missing(true)
                .journal_mode(SqliteJournalMode::Wal),
        )
        .await
        .context("failed to connect to the database")?;

        if migrate {
            let start = Instant::now();
            sqlx::migrate!("./database/migrations")
                .run(&sqlite)
                .await
                .context("failed to run database migrations")?;
            tracing::info!("database migrated ({}ms)", start.elapsed().as_millis());
        }

        Ok(Self { sqlite })
    }

    #[inline]
    pub fn write(&self) -> &SqlitePool {
        &self.sqlite
    }

    #[inline]
    pub fn read(&self) -> &SqlitePool {
        &self.sqlite
    }
}
