use crate::{
    config::Config,
    instance::{DatabaseType, identifier::UserIdentifier, manager::DatabaseRouteManager},
    subsystems::status::SubsystemConnections,
    tls::ReloadableAcceptor,
};
use std::{io, net::SocketAddr, sync::Arc};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream, UnixStream},
};
use tokio_rustls::server::TlsStream;

mod resp;

enum Conn {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

pub async fn run(
    config: Arc<Config>,
    status: Arc<SubsystemConnections>,
    routes: Arc<DatabaseRouteManager>,
) -> anyhow::Result<()> {
    let acceptor = {
        let config = config.load();
        if config.redis.tls.enabled {
            crate::tls::build_acceptor(&config.redis.tls.cert, &config.redis.tls.key)?
        } else {
            None
        }
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
    status.mark_running();
    tracing::info!(
        "redis listening on {bind} (client TLS: {})",
        if acceptor.is_some() { "on" } else { "off" }
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
            tracing::debug!("[{peer}] connection (plain)");
            session(s, status, routes, peer).await
        }
        Conn::Tls(s) => {
            tracing::debug!("[{peer}] connection (tls)");
            session(*s, status, routes, peer).await
        }
    }
}

async fn negotiate(tcp: TcpStream, acceptor: &Option<ReloadableAcceptor>) -> io::Result<Conn> {
    let mut b = [0; 1];
    let n = tcp.peek(&mut b).await?;
    match acceptor {
        Some(acc) if n == 1 && b[0] == 0x16 => Ok(Conn::Tls(Box::new(acc.accept(tcp).await?))),
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
        tracing::debug!("[{peer}] no parseable command");
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
            tracing::debug!("[{peer}] first command {other:?} carries no identity");
            return Ok(());
        }
    };

    let user_id = user.parse::<UserIdentifier>().ok();
    let creds = user_id.and_then(|id| routes.find(DatabaseType::Redis, &id));
    let Some(creds) =
        creds.filter(|c| constant_time_eq::constant_time_eq(&password, c.password.as_bytes()))
    else {
        tracing::debug!("[{peer}] rejected auth for user {user:?}");
        client
            .write_all(b"-WRONGPASS invalid username-password pair or user is disabled.\r\n")
            .await?;
        return Ok(());
    };

    if creds.instance.is_suspended().await {
        tracing::debug!(
            "[{peer}] rejected: database {} suspended",
            creds.instance.uuid
        );
        client.write_all(b"-ERR database is suspended\r\n").await?;
        return Ok(());
    }

    let mut backend = UnixStream::connect(&creds.instance.get_socket_path().await).await?;

    match forward {
        Some(bytes) => backend.write_all(&bytes).await?,
        None => client.write_all(b"+OK\r\n").await?,
    }

    let _guard = user_id.map(|id| status.connect(id, None));
    let (c2b, b2c) = tokio::io::copy_bidirectional(&mut client, &mut backend).await?;
    tracing::debug!("[{peer}] closed (c->b {c2b} B, b->c {b2c} B)");
    Ok(())
}
