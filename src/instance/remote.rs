use super::{DatabaseType, executor::NetworkedContainerOptions};
use crate::net::{host_to_ip, is_blocked_ip};
use futures_util::{StreamExt, TryStreamExt};
use mongodb::options::{ConnectionString, HostInfo, ServerAddress};
use std::{
    sync::{Arc, atomic::AtomicU64},
    time::Duration,
};
use url::Url;

/// libpq reads the real endpoint from these instead of the uri host, which would
/// make any check against the host meaningless
const POSTGRES_HOST_OVERRIDES: &[&str] = &["host", "hostaddr", "service", "servicefile"];

/// how long a dump tool may spend reaching the source before it gives up
const CONNECT_TIMEOUT: u64 = 10;

/// a source that connects and then stalls holds the dump container, the operation
/// and the target's import open for as long as it likes, so the dump is cut off
/// once it goes quiet for this long
const IDLE_TIMEOUT: Duration = Duration::from_secs(120);

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
            database_type.to_str(),
            schemes.join(", ")
        ))
        .into());
    }

    if url.host_str().unwrap_or_default().is_empty() {
        return Err(
            crate::response::DisplayError::new("connection string must contain a host").into(),
        );
    }

    // no scheme here has a fragment, but the url crate keeps everything past '#' out of
    // query_pairs() and host_str() while as_str() still emits it, and libpq then reads
    // the tail as more options. refusing beats rewriting what the user asked for
    if url.fragment().is_some() {
        return Err(crate::response::DisplayError::new(
            "connection string must not contain a fragment",
        )
        .into());
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
        if let Some(ip) = host_to_ip(host) {
            if is_blocked_ip(blocked, &ip) {
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
            if is_blocked_ip(blocked, &address.ip()) {
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

/// fails the dump once the source goes quiet for `timeout`, dropping the container
/// stream with it
fn with_idle_timeout<S>(
    stream: S,
    timeout: Duration,
) -> impl futures_util::Stream<Item = Result<bytes::Bytes, anyhow::Error>> + Send
where
    S: futures_util::Stream<Item = Result<bytes::Bytes, anyhow::Error>> + Send + Unpin + 'static,
{
    futures_util::stream::unfold(Some(stream), move |state| async move {
        let mut stream = state?;

        match tokio::time::timeout(timeout, stream.next()).await {
            Ok(Some(chunk)) => Some((chunk, Some(stream))),
            Ok(None) => None,
            Err(_) => Some((
                Err(crate::response::DisplayError::new(format!(
                    "the source sent no data for {} seconds",
                    timeout.as_secs()
                ))
                .into()),
                None,
            )),
        }
    })
}

/// a validated remote import, ready to be run in the background. it holds the dump
/// command, which carries the source's password
pub struct RemoteImport {
    pub source_host: String,
    pub source_db: Option<String>,

    command: String,
    env: Vec<String>,
    extra_hosts: Vec<String>,
}

impl super::Instance {
    /// validates a remote import of this instance's own type, resolving and vetting
    /// the source. see [`super::Instance::import`] for the meaning of `db` and `wipe`
    pub async fn prepare_remote_import(
        &self,
        url: &str,
        source_db: Option<&str>,
    ) -> anyhow::Result<RemoteImport> {
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

                // an explicit one is the caller's choice, and libpq takes the last
                // occurrence of a parameter
                if !url
                    .query_pairs()
                    .any(|(key, _)| key.as_ref() == "connect_timeout")
                {
                    url.query_pairs_mut()
                        .append_pair("connect_timeout", &CONNECT_TIMEOUT.to_string());
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

                let mut flags = format!(
                    "-h {} -P {}",
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

                // the source is live and not ours to lock, hence --single-transaction
                let dump = match &source_db {
                    Some(source_db) => format!(
                        "mariadb-dump {flags} --single-transaction {}",
                        crate::utils::shell_quote(source_db)
                    ),
                    None => format!("mariadb-dump {flags} --single-transaction --all-databases"),
                };

                // mariadb-dump has no connect timeout of its own, the client it ships
                // with does, so reaching the source at all is bounded by a probe
                let command = format!(
                    "mariadb {flags} --connect-timeout={CONNECT_TIMEOUT} -e 'SELECT 1' > /dev/null && {dump}"
                );

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

                // mongodump refuses a flag the uri already sets
                let mut timeouts = String::new();
                if connection.connect_timeout.is_none() {
                    timeouts.push_str(&format!(" --dialTimeout={CONNECT_TIMEOUT}"));
                }
                if connection.server_selection_timeout.is_none() {
                    timeouts.push_str(&format!(" --serverSelectionTimeout={CONNECT_TIMEOUT}"));
                }

                (
                    format!(
                        "mongodump --uri={}{timeouts} --archive{select}",
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

        Ok(RemoteImport {
            source_host: hosts.join(","),
            source_db,
            command,
            env,
            extra_hosts: pinned
                .into_iter()
                .map(|(host, ip)| format!("{host}:{ip}"))
                .collect(),
        })
    }

    /// dumps the prepared source and imports it. the dump runs in a script runner
    /// container since instance containers have no networking
    pub async fn run_remote_import(
        &self,
        import: RemoteImport,
        db: Option<&str>,
        wipe: bool,
        bytes_processed: Arc<AtomicU64>,
    ) -> anyhow::Result<()> {
        let stream = self
            .app_state
            .container_executor
            .run_networked_container(
                self,
                NetworkedContainerOptions::new(vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    import.command,
                ])
                .with_env(import.env)
                .with_extra_hosts(import.extra_hosts),
            )
            .await?;

        let mut reader = tokio_util::io::StreamReader::new(
            with_idle_timeout(stream, IDLE_TIMEOUT)
                .boxed()
                .inspect_ok(move |chunk| {
                    bytes_processed
                        .fetch_add(chunk.len() as u64, std::sync::atomic::Ordering::Relaxed);
                })
                .map_err(std::io::Error::other),
        );

        self.import_inner(db, import.source_db.as_deref(), wipe, &mut reader)
            .await
    }
}
