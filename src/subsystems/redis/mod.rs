use crate::{
    config::Config,
    instance::{DatabaseType, identifier::UserIdentifier, manager::DatabaseRouteManager},
    subsystems::status::SubsystemConnections,
    tls::{MaybeKtlsStream, ReloadableAcceptor},
};
use std::{io, net::SocketAddr, sync::Arc};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream, UnixStream},
};

mod resp;

enum Conn {
    Plain(TcpStream),
    Tls(MaybeKtlsStream),
}

pub async fn run(
    config: Arc<Config>,
    status: Arc<SubsystemConnections>,
    routes: Arc<DatabaseRouteManager>,
) -> anyhow::Result<()> {
    let tls = config.load().redis.tls.clone();
    let acceptor = if tls.enabled {
        Some(crate::tls::build_acceptor(&tls, &[]).await?)
    } else {
        None
    };
    if let Some(acceptor) = &acceptor {
        let config = Arc::clone(&config);
        acceptor.spawn_reloader("redis", move || {
            let config = config.load();
            (config.redis.tls.cert.clone(), config.redis.tls.key.clone())
        });
    }
    let bind = config.load().redis.bind;

    let listener = TcpListener::bind(bind).await?;
    crate::net::apply_socket_congestion_control(&listener, &config);
    status.mark_running();
    tracing::info!(
        "redis listening on {bind} (client TLS: {})",
        acceptor.as_ref().map_or("off", ReloadableAcceptor::mode)
    );

    crate::utils::accept_loop(&listener, "redis", |tcp, peer| {
        let status = Arc::clone(&status);
        let routes = Arc::clone(&routes);
        let acceptor = acceptor.clone();
        async move { handle(tcp, &status, &routes, acceptor, peer).await }
    })
    .await
}

async fn handle(
    tcp: TcpStream,
    status: &Arc<SubsystemConnections>,
    routes: &DatabaseRouteManager,
    acceptor: Option<ReloadableAcceptor>,
    peer: SocketAddr,
) -> io::Result<()> {
    match negotiate(tcp, &acceptor).await? {
        Conn::Plain(s) => {
            tracing::debug!(%peer, "connection (plain)");
            session(s, status, routes, peer).await
        }
        Conn::Tls(s) => {
            tracing::debug!(%peer, "connection (tls)");
            session(s, status, routes, peer).await
        }
    }
}

async fn negotiate(tcp: TcpStream, acceptor: &Option<ReloadableAcceptor>) -> io::Result<Conn> {
    let mut b = [0; 1];
    let n = crate::utils::handshake_step(tcp.peek(&mut b)).await?;
    match acceptor {
        Some(acc) if n == 1 && b[0] == 0x16 => Ok(Conn::Tls(
            crate::utils::handshake_step(acc.accept(tcp)).await?,
        )),
        _ => Ok(Conn::Plain(tcp)),
    }
}

async fn session<S: AsyncRead + AsyncWrite + Unpin>(
    mut client: S,
    status: &Arc<SubsystemConnections>,
    routes: &DatabaseRouteManager,
    peer: SocketAddr,
) -> io::Result<()> {
    let Some((args, _raw)) = resp::read_command(&mut client)
        .await?
        .filter(|(a, _)| !a.is_empty())
    else {
        tracing::debug!(%peer, "no parseable command");
        return Ok(());
    };

    let Some(first_arg) = args.first() else {
        return Ok(());
    };
    let cmd = String::from_utf8_lossy(first_arg).to_ascii_uppercase();
    let (user, password, forward): (String, Vec<u8>, Option<Vec<u8>>) = match cmd.as_str() {
        "AUTH" => {
            let (user, password) = match args.as_slice() {
                [_, user, password, ..] => {
                    (String::from_utf8_lossy(user).into_owned(), password.clone())
                }
                [_, password] => ("default".to_string(), password.clone()),
                _ => {
                    client
                        .write_all(b"-ERR wrong number of arguments for 'auth' command\r\n")
                        .await?;
                    return Ok(());
                }
            };
            (user, password, None)
        }
        "HELLO" => {
            let user = resp::extract_hello_user(&args).unwrap_or_else(|| "default".to_string());
            let password = resp::extract_hello_password(&args).unwrap_or_default();
            (
                user,
                password,
                Some(resp::encode_command(&resp::strip_hello_auth(&args))),
            )
        }
        other => {
            tracing::debug!(%peer, command = %other, "first command carries no identity");
            return Ok(());
        }
    };

    let user_id = user.parse::<UserIdentifier>().ok();
    let creds = user_id.and_then(|id| routes.find(DatabaseType::Redis, &id));
    let Some(creds) =
        creds.filter(|c| constant_time_eq::constant_time_eq(&password, c.password.as_bytes()))
    else {
        tracing::debug!(%peer, %user, "rejected auth");
        client
            .write_all(b"-WRONGPASS invalid username-password pair or user is disabled.\r\n")
            .await?;
        return Ok(());
    };

    if let Some(state) = creds.instance.locked_state() {
        tracing::debug!(
            %peer,
            instance = %creds.instance.uuid,
            state = %state,
            "rejected: instance locked"
        );
        client.write_all(b"-ERR database is locked\r\n").await?;
        return Ok(());
    }
    if let Some(state) = creds.instance.write_locked_state() {
        tracing::debug!(
            %peer,
            instance = %creds.instance.uuid,
            state = %state,
            "rejected: instance write locked"
        );
        client
            .write_all(b"-ERR database is write locked\r\n")
            .await?;
        return Ok(());
    }

    let mut backend = match UnixStream::connect(&creds.instance.get_socket_path().await).await {
        Ok(backend) => backend,
        Err(err) => {
            tracing::debug!(
                %peer,
                instance = %creds.instance.uuid,
                "rejected: backend unreachable: {err}"
            );
            client.write_all(b"-ERR database is offline\r\n").await?;
            return Ok(());
        }
    };

    match forward {
        Some(bytes) => backend.write_all(&bytes).await?,
        None => client.write_all(b"+OK\r\n").await?,
    }

    let _guard = user_id.map(|id| status.connect(id, None));
    let (c2b, b2c) = tokio::select! {
        copied = tokio::io::copy_bidirectional(&mut client, &mut backend) => copied?,
        _ = creds.instance.write_locked() => {
            tracing::debug!(%peer, "closed: instance write locked");
            return Ok(());
        }
    };
    tracing::debug!(%peer, "closed (c->b {c2b} B, b->c {b2c} B)");
    Ok(())
}
