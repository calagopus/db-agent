use crate::subsystems::database::DatabaseType;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Row, sqlite::SqliteRow};
use std::collections::BTreeMap;
use utoipa::ToSchema;

pub struct VolumeMappingEntry<'a> {
    host_name: &'a str,
    container_path: &'a str,
}

impl<'a> VolumeMappingEntry<'a> {
    pub fn host_path(
        &self,
        config: &crate::config::Config,
        database_uuid: uuid::Uuid,
    ) -> std::path::PathBuf {
        config
            .data_path(database_uuid)
            .join("volumes")
            .join(self.host_name)
    }

    pub fn container_path(&self) -> &std::path::Path {
        std::path::Path::new(self.container_path)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(transparent)]
pub struct VolumeMapping(BTreeMap<String, String>);

pub struct VolumeMappingIter<'a>(std::collections::btree_map::Iter<'a, String, String>);

impl<'a> Iterator for VolumeMappingIter<'a> {
    type Item = VolumeMappingEntry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .next()
            .map(|(host_name, container_path)| VolumeMappingEntry {
                host_name,
                container_path,
            })
    }
}

impl<'a> IntoIterator for &'a VolumeMapping {
    type Item = VolumeMappingEntry<'a>;
    type IntoIter = VolumeMappingIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        VolumeMappingIter(self.0.iter())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct StoredDatabase {
    pub uuid: uuid::Uuid,
    pub uuid_short: i64,
    pub database_type: DatabaseType,
    pub suspended: bool,
    pub memory: i64,
    pub swap: i64,
    pub disk: i64,
    pub io_weight: Option<i64>,
    pub cpu: i64,
    pub image: String,
    pub image_uid: u32,
    pub image_gid: u32,
    pub volumes: VolumeMapping,
    pub socket_path: String,
    pub timezone: Option<String>,
    pub env: BTreeMap<String, String>,
    pub cmd: Option<Vec<String>>,
}

impl FromRow<'_, SqliteRow> for StoredDatabase {
    fn from_row(row: &SqliteRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            uuid: row.try_get("uuid")?,
            uuid_short: row.try_get("uuid_short")?,
            database_type: {
                let raw = row.try_get::<String, _>("database_type")?;
                DatabaseType::from_db_str(&raw).ok_or_else(|| sqlx::Error::ColumnDecode {
                    index: "database_type".into(),
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("invalid database type: {raw}"),
                    )),
                })?
            },
            suspended: row.try_get("suspended")?,
            memory: row.try_get("memory")?,
            swap: row.try_get("swap")?,
            disk: row.try_get("disk")?,
            io_weight: row.try_get("io_weight")?,
            cpu: row.try_get("cpu")?,
            image: row.try_get("image")?,
            image_uid: row.try_get("image_uid")?,
            image_gid: row.try_get("image_gid")?,
            volumes: {
                let raw = row.try_get::<String, _>("volumes")?;
                serde_json::from_str(&raw).map_err(|e| sqlx::Error::ColumnDecode {
                    index: "volumes".into(),
                    source: Box::new(e),
                })?
            },
            socket_path: row.try_get("socket_path")?,
            timezone: row.try_get("timezone")?,
            env: {
                let raw = row.try_get::<String, _>("env")?;
                serde_json::from_str(&raw).map_err(|e| sqlx::Error::ColumnDecode {
                    index: "env".into(),
                    source: Box::new(e),
                })?
            },
            cmd: {
                let raw = row.try_get::<Option<String>, _>("cmd")?;
                raw.as_deref()
                    .map(serde_json::from_str)
                    .transpose()
                    .map_err(|e| sqlx::Error::ColumnDecode {
                        index: "cmd".into(),
                        source: Box::new(e),
                    })?
            },
        })
    }
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct StoredDatabaseCreate {
    pub database_type: DatabaseType,
    #[serde(default)]
    pub suspended: bool,
    pub memory: i64,
    pub swap: i64,
    pub disk: i64,
    pub io_weight: Option<i64>,
    pub cpu: i64,
    pub image: String,
    pub image_uid: u32,
    pub image_gid: u32,
    pub volumes: VolumeMapping,
    pub socket_path: String,
    pub timezone: Option<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    pub cmd: Option<Vec<String>>,
}

impl StoredDatabaseCreate {
    pub async fn insert(
        self,
        database: &crate::database::Database,
    ) -> anyhow::Result<StoredDatabase> {
        loop {
            let uuid = uuid::Uuid::new_v4();
            let uuid_short = uuid.as_fields().0 as i64;

            match sqlx::query(
                "INSERT INTO databases (uuid, uuid_short, database_type, suspended, memory, swap, disk, io_weight, cpu, image, image_uid, image_gid, volumes, socket_path, timezone, env, cmd)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(uuid)
            .bind(uuid_short)
            .bind(self.database_type.as_str())
            .bind(self.suspended)
            .bind(self.memory)
            .bind(self.swap)
            .bind(self.disk)
            .bind(self.io_weight)
            .bind(self.cpu)
            .bind(&self.image)
            .bind(self.image_uid)
            .bind(self.image_gid)
            .bind(serde_json::to_string(&self.volumes)?)
            .bind(&self.socket_path)
            .bind(&self.timezone)
            .bind(serde_json::to_string(&self.env)?)
            .bind(self.cmd.as_ref().map(serde_json::to_string).transpose()?)
            .execute(database.write())
            .await
            {
                Ok(_) => {
                    return Ok(StoredDatabase {
                        uuid,
                        uuid_short,
                        database_type: self.database_type,
                        suspended: self.suspended,
                        memory: self.memory,
                        swap: self.swap,
                        disk: self.disk,
                        io_weight: self.io_weight,
                        cpu: self.cpu,
                        image: self.image,
                        image_uid: self.image_uid,
                        image_gid: self.image_gid,
                        volumes: self.volumes,
                        socket_path: self.socket_path,
                        timezone: self.timezone,
                        env: self.env,
                        cmd: self.cmd,
                    });
                }
                Err(sqlx::Error::Database(err)) if err.is_unique_violation() => continue,
                Err(err) => return Err(err.into()),
            }
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, ToSchema)]
#[serde(default)]
pub struct StoredDatabaseUpdate {
    pub suspended: Option<bool>,
    pub memory: Option<i64>,
    pub swap: Option<i64>,
    pub disk: Option<i64>,
    pub io_weight: Option<Option<i64>>,
    pub cpu: Option<i64>,
    pub image: Option<String>,
    pub image_uid: Option<u32>,
    pub image_gid: Option<u32>,
    pub volumes: Option<VolumeMapping>,
    pub socket_path: Option<String>,
    pub timezone: Option<Option<String>>,
    pub env: Option<BTreeMap<String, String>>,
    pub cmd: Option<Option<Vec<String>>>,
}

impl StoredDatabaseUpdate {
    pub async fn apply(
        self,
        database: &crate::database::Database,
        data: &mut StoredDatabase,
    ) -> anyhow::Result<()> {
        if let Some(suspended) = self.suspended {
            data.suspended = suspended;
        }
        if let Some(memory) = self.memory {
            data.memory = memory;
        }
        if let Some(swap) = self.swap {
            data.swap = swap;
        }
        if let Some(disk) = self.disk {
            data.disk = disk;
        }
        if let Some(io_weight) = self.io_weight {
            data.io_weight = io_weight;
        }
        if let Some(cpu) = self.cpu {
            data.cpu = cpu;
        }
        if let Some(image) = self.image {
            data.image = image;
        }
        if let Some(image_uid) = self.image_uid {
            data.image_uid = image_uid;
        }
        if let Some(image_gid) = self.image_gid {
            data.image_gid = image_gid;
        }
        if let Some(volumes) = self.volumes {
            data.volumes = volumes;
        }
        if let Some(socket_path) = self.socket_path {
            data.socket_path = socket_path;
        }
        if let Some(timezone) = self.timezone {
            data.timezone = timezone;
        }
        if let Some(env) = self.env {
            data.env = env;
        }
        if let Some(cmd) = self.cmd {
            data.cmd = cmd;
        }

        sqlx::query(
            "UPDATE databases SET suspended = ?, memory = ?, swap = ?,
            disk = ?, io_weight = ?, cpu = ?, image = ?, image_uid = ?,
            image_gid = ?, volumes = ?, socket_path = ?, timezone = ?,
            env = ?, cmd = ? WHERE uuid = ?",
        )
        .bind(data.suspended)
        .bind(data.memory)
        .bind(data.swap)
        .bind(data.disk)
        .bind(data.io_weight)
        .bind(data.cpu)
        .bind(&data.image)
        .bind(data.image_uid)
        .bind(data.image_gid)
        .bind(serde_json::to_string(&data.volumes)?)
        .bind(&data.socket_path)
        .bind(&data.timezone)
        .bind(serde_json::to_string(&data.env)?)
        .bind(data.cmd.as_ref().map(serde_json::to_string).transpose()?)
        .bind(data.uuid)
        .execute(database.write())
        .await?;

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct StoredDatabaseUser {
    pub uuid: uuid::Uuid,
    pub uuid_short: i64,
    pub database_uuid: uuid::Uuid,
    pub username: String,
    pub password: String,
}

impl FromRow<'_, SqliteRow> for StoredDatabaseUser {
    fn from_row(row: &SqliteRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            uuid: row.try_get("uuid")?,
            uuid_short: row.try_get("uuid_short")?,
            database_uuid: row.try_get("database_uuid")?,
            username: row.try_get("username")?,
            password: row.try_get("password")?,
        })
    }
}
