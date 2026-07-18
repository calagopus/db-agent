use crate::instance::DatabaseType;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Row, sqlite::SqliteRow};
use std::collections::BTreeMap;
use utoipa::ToSchema;

fn decode_created(row: &SqliteRow) -> Result<chrono::DateTime<chrono::Utc>, sqlx::Error> {
    let secs = row.try_get::<i64, _>("created")?;
    chrono::DateTime::from_timestamp(secs, 0).ok_or_else(|| sqlx::Error::ColumnDecode {
        index: "created".into(),
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("invalid created timestamp: {secs}"),
        )),
    })
}

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
        let mut path = config.data_path(database_uuid).join("volumes");
        path.extend(
            std::path::Path::new(self.host_name)
                .components()
                .filter_map(|c| match c {
                    std::path::Component::Normal(p) => Some(p),
                    _ => None,
                }),
        );
        path
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
pub struct StoredInstance {
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
    #[serde(skip)]
    pub root_password: Option<String>,
    pub created: chrono::DateTime<chrono::Utc>,
}

impl FromRow<'_, SqliteRow> for StoredInstance {
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
            root_password: row.try_get("root_password")?,
            created: decode_created(row)?,
        })
    }
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct StoredInstanceCreate {
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

impl StoredInstanceCreate {
    pub async fn insert(
        self,
        database: &crate::database::Database,
    ) -> anyhow::Result<StoredInstance> {
        loop {
            let uuid = uuid::Uuid::new_v4();
            let uuid_short = uuid.as_fields().0 as i64;
            let created = chrono::Utc::now();

            match sqlx::query(
                "INSERT INTO instances (uuid, uuid_short, database_type, suspended, memory, swap, disk, io_weight, cpu, image, image_uid, image_gid, volumes, socket_path, timezone, env, cmd, created)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
            .bind(created.timestamp())
            .execute(database.write())
            .await
            {
                Ok(_) => {
                    return Ok(StoredInstance {
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
                        root_password: None,
                        created,
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
pub struct StoredInstanceUpdate {
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

impl StoredInstanceUpdate {
    pub async fn apply(
        self,
        database: &crate::database::Database,
        data: &mut StoredInstance,
    ) -> anyhow::Result<()> {
        let mut new_data = data.clone();

        if let Some(suspended) = self.suspended {
            new_data.suspended = suspended;
        }
        if let Some(memory) = self.memory {
            new_data.memory = memory;
        }
        if let Some(swap) = self.swap {
            new_data.swap = swap;
        }
        if let Some(disk) = self.disk {
            new_data.disk = disk;
        }
        if let Some(io_weight) = self.io_weight {
            new_data.io_weight = io_weight;
        }
        if let Some(cpu) = self.cpu {
            new_data.cpu = cpu;
        }
        if let Some(image) = self.image {
            new_data.image = image;
        }
        if let Some(image_uid) = self.image_uid {
            new_data.image_uid = image_uid;
        }
        if let Some(image_gid) = self.image_gid {
            new_data.image_gid = image_gid;
        }
        if let Some(volumes) = self.volumes {
            new_data.volumes = volumes;
        }
        if let Some(socket_path) = self.socket_path {
            new_data.socket_path = socket_path;
        }
        if let Some(timezone) = self.timezone {
            new_data.timezone = timezone;
        }
        if let Some(env) = self.env {
            new_data.env = env;
        }
        if let Some(cmd) = self.cmd {
            new_data.cmd = cmd;
        }

        sqlx::query(
            "UPDATE instances SET suspended = ?, memory = ?, swap = ?,
            disk = ?, io_weight = ?, cpu = ?, image = ?, image_uid = ?,
            image_gid = ?, volumes = ?, socket_path = ?, timezone = ?,
            env = ?, cmd = ? WHERE uuid = ?",
        )
        .bind(new_data.suspended)
        .bind(new_data.memory)
        .bind(new_data.swap)
        .bind(new_data.disk)
        .bind(new_data.io_weight)
        .bind(new_data.cpu)
        .bind(&new_data.image)
        .bind(new_data.image_uid)
        .bind(new_data.image_gid)
        .bind(serde_json::to_string(&new_data.volumes)?)
        .bind(&new_data.socket_path)
        .bind(&new_data.timezone)
        .bind(serde_json::to_string(&new_data.env)?)
        .bind(
            new_data
                .cmd
                .as_ref()
                .map(serde_json::to_string)
                .transpose()?,
        )
        .bind(new_data.uuid)
        .execute(database.write())
        .await?;

        *data = new_data;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct StoredDatabase {
    pub uuid: uuid::Uuid,
    pub instance_uuid: uuid::Uuid,
    pub name: String,
    pub created: chrono::DateTime<chrono::Utc>,
}

impl FromRow<'_, SqliteRow> for StoredDatabase {
    fn from_row(row: &SqliteRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            uuid: row.try_get("uuid")?,
            instance_uuid: row.try_get("instance_uuid")?,
            name: row.try_get("name")?,
            created: decode_created(row)?,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct StoredUser {
    pub uuid: uuid::Uuid,
    pub uuid_short: i64,
    pub instance_uuid: uuid::Uuid,
    pub database_uuid: Option<uuid::Uuid>,
    pub username: String,
    pub password: String,
    pub created: chrono::DateTime<chrono::Utc>,
}

impl FromRow<'_, SqliteRow> for StoredUser {
    fn from_row(row: &SqliteRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            uuid: row.try_get("uuid")?,
            uuid_short: row.try_get("uuid_short")?,
            instance_uuid: row.try_get("instance_uuid")?,
            database_uuid: row.try_get("database_uuid")?,
            username: row.try_get("username")?,
            password: row.try_get("password")?,
            created: decode_created(row)?,
        })
    }
}
