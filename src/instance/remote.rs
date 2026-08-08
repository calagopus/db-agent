use super::{DatabaseType, executor::NetworkedContainerOptions};
use futures_util::TryStreamExt;
use mongodb::options::{ConnectionString, HostInfo, ServerAddress};
use url::Url;

/// libpq reads the real endpoint from these instead of the uri host, which would
/// make any check against the host meaningless
const POSTGRES_HOST_OVERRIDES: &[&str] = &["host", "hostaddr", "service", "servicefile"];

fn parse_url(url: &str, database_type: DatabaseType) -> anyhow::Result<Url> {
    let schemes: &[&str] = match database_type {
        DatabaseType::Postgres => &["postgres", "postgresql"],
        _ => &["mysql", "mariadb"],
    };

    let url = Url::parse(url).map_err(|err| {
        crate::response::DisplayError::new(format!("invalid connection string: {err}"))
    })?;

    if !schemes.contains(&url.scheme()) {
        return Err(crate::response::DisplayError::new(format!(
            "connection string for {} must use one of the schemes: {}",
            database_type.as_str(),
            schemes.join(", ")
        ))
        .into());
    }

    if url.host_str().unwrap_or_default().is_empty() {
        return Err(
            crate::response::DisplayError::new("connection string must contain a host").into(),
        );
    }

    if database_type == DatabaseType::Postgres
        && let Some((key, _)) = url
            .query_pairs()
            .find(|(key, _)| POSTGRES_HOST_OVERRIDES.contains(&key.as_ref()))
    {
        return Err(crate::response::DisplayError::new(format!(
            "connection string must not set '{key}'"
        ))
        .into());
    }

    Ok(url)
}

fn decode(value: &str) -> String {
    percent_encoding::percent_decode_str(value)
        .decode_utf8_lossy()
        .into_owned()
}

/// the database the connection string points at, if any
fn url_database(url: &Url) -> Option<String> {
    Some(decode(url.path().trim_start_matches('/'))).filter(|db| !db.is_empty())
}

/// the name may come from the connection string instead of the payload, where garde
/// never saw it
fn checked_source_db(source_db: Option<String>) -> anyhow::Result<Option<String>> {
    if let Some(source_db) = &source_db {
        crate::instance::validate_source_database_name(source_db, &())
            .map_err(|err| crate::response::DisplayError::new(format!("source db {err}")))?;
    }

    Ok(source_db)
}

fn url_hosts(url: &Url) -> Vec<String> {
    url.host_str()
        .unwrap_or_default()
        .split(',')
        .map(str::to_string)
        .collect()
}

fn mariadb_uses_tls(url: &Url) -> bool {
    url.query_pairs().any(|(key, value)| {
        matches!(key.as_ref(), "ssl-mode" | "sslmode" | "ssl")
            && !matches!(
                value.to_ascii_lowercase().as_str(),
                "disabled" | "disable" | "false" | "0"
            )
    })
}

/// rejects the import if any host of the connection string resolves into a blocked
/// range. the vetted addresses are returned to be pinned, since whoever connects
/// resolves the names again and could otherwise get a different answer
async fn vetted_hosts(
    hosts: &[String],
    blocked: &[cidr::IpCidr],
) -> anyhow::Result<Vec<(String, std::net::IpAddr)>> {
    if blocked.is_empty() {
        return Ok(Vec::new());
    }

    let refuse = |host: &str, ip: std::net::IpAddr| {
        tracing::warn!("blocking internal IP address in remote import: {host} -> {ip}");

        crate::response::DisplayError::new("the requested address is blocked")
    };

    let mut pinned = Vec::new();
    for host in hosts {
        if let Some(ip) = crate::utils::host_to_ip(host) {
            if crate::utils::is_blocked_ip(blocked, &ip) {
                return Err(refuse(host, ip).into());
            }

            continue;
        }

        // fail closed, a name the agent cannot resolve cannot be vetted either
        let addresses: Vec<_> = tokio::net::lookup_host((host.as_str(), 0))
            .await
            .map_err(|err| {
                crate::response::DisplayError::new(format!("failed to resolve '{host}': {err}"))
            })?
            .collect();

        for address in &addresses {
            if crate::utils::is_blocked_ip(blocked, &address.ip()) {
                return Err(refuse(host, address.ip()).into());
            }
        }

        let Some(first) = addresses.first() else {
            return Err(crate::response::DisplayError::new(format!(
                "'{host}' resolved to no address"
            ))
            .into());
        };

        pinned.push((host.clone(), first.ip()));
    }

    Ok(pinned)
}

impl super::Instance {
    /// dumps a remote database of this instance's own type and imports it, see
    /// [`super::Instance::import`] for the meaning of `db` and `wipe`. the dump runs
    /// in a script runner container since instance containers have no networking
    pub async fn import_remote(
        &self,
        url: &str,
        source_db: Option<&str>,
        db: Option<&str>,
        wipe: bool,
    ) -> anyhow::Result<()> {
        let blocked = self
            .app_state
            .config
            .load()
            .api
            .remote_import_blocked_cidrs
            .clone();

        let database_type = self.data.read().await.database_type;
        let (command, env, hosts, source_db) = match database_type {
            DatabaseType::Postgres => {
                let mut url = parse_url(url, database_type)?;
                let hosts = url_hosts(&url);
                let source_db = checked_source_db(
                    source_db.map(str::to_string).or_else(|| url_database(&url)),
                )?;
                if let Some(source_db) = &source_db {
                    url.set_path(source_db);
                }

                // pg_dumpall does not hand a password in the connection string down to
                // the pg_dump children it spawns
                let password = url.password().map(decode);
                let _ = url.set_password(None);
                let quoted = crate::utils::shell_quote(url.as_str());

                let command = match &source_db {
                    Some(_) => format!("pg_dump --no-owner --no-privileges -d {quoted}"),
                    None => format!("pg_dumpall --no-owner --no-privileges -d {quoted}"),
                };

                (
                    command,
                    password
                        .map(|password| vec![format!("PGPASSWORD={password}")])
                        .unwrap_or_default(),
                    hosts,
                    source_db,
                )
            }
            DatabaseType::Mariadb => {
                let url = parse_url(url, database_type)?;
                let source_db = checked_source_db(
                    source_db.map(str::to_string).or_else(|| url_database(&url)),
                )?;

                // the source is live and not ours to lock, hence --single-transaction
                let mut flags = format!(
                    "-h {} -P {} --single-transaction",
                    crate::utils::shell_quote(url.host_str().unwrap_or_default()),
                    url.port().unwrap_or(3306)
                );
                if !url.username().is_empty() {
                    flags.push_str(&format!(
                        " -u {}",
                        crate::utils::shell_quote(&decode(url.username()))
                    ));
                }
                if mariadb_uses_tls(&url) {
                    flags.push_str(" --ssl");
                }

                let command = match &source_db {
                    Some(source_db) => format!(
                        "mariadb-dump {flags} {}",
                        crate::utils::shell_quote(source_db)
                    ),
                    None => format!("mariadb-dump {flags} --all-databases"),
                };

                (
                    command,
                    url.password()
                        .map(|password| vec![format!("MYSQL_PWD={}", decode(password))])
                        .unwrap_or_default(),
                    url_hosts(&url),
                    source_db,
                )
            }
            DatabaseType::Mongodb => {
                // the url crate cannot hold a seed list, every host past the first
                // carries its own port
                let connection = ConnectionString::parse(url).map_err(|err| {
                    crate::response::DisplayError::new(format!("invalid connection string: {err}"))
                })?;

                let hosts = match &connection.host_info {
                    HostInfo::HostIdentifiers(addresses) => addresses
                        .iter()
                        .filter_map(|address| match address {
                            ServerAddress::Tcp { host, .. } => Some(host.clone()),
                            _ => None,
                        })
                        .collect(),
                    HostInfo::DnsRecord(_) if blocked.is_empty() => Vec::new(),
                    // srv resolves to hosts the agent never sees, and its txt record
                    // can set connection options on top
                    _ => {
                        return Err(crate::response::DisplayError::new(
                            "mongodb+srv connection strings are not supported, list the hosts instead",
                        )
                        .into());
                    }
                };

                let source_db = checked_source_db(
                    source_db
                        .map(str::to_string)
                        .or_else(|| connection.default_database.clone()),
                )?;
                let select = match &source_db {
                    Some(source_db) => format!(" -d {}", crate::utils::shell_quote(source_db)),
                    None => String::new(),
                };

                (
                    format!(
                        "mongodump --uri={} --archive{select}",
                        crate::utils::shell_quote(url)
                    ),
                    Vec::new(),
                    hosts,
                    source_db,
                )
            }
            DatabaseType::Redis => {
                return Err(crate::response::DisplayError::new(
                    "remote imports are not supported for redis",
                )
                .into());
            }
        };

        let pinned = vetted_hosts(&hosts, &blocked).await?;
        let stream = self
            .app_state
            .container_executor
            .run_networked_container(
                self,
                NetworkedContainerOptions::new(vec!["sh".to_string(), "-c".to_string(), command])
                    .with_env(env)
                    .with_extra_hosts(
                        pinned
                            .into_iter()
                            .map(|(host, ip)| format!("{host}:{ip}"))
                            .collect(),
                    ),
            )
            .await?;

        let mut reader = tokio_util::io::StreamReader::new(stream.map_err(std::io::Error::other));

        self.import(db, source_db.as_deref(), wipe, &mut reader)
            .await
    }
}
