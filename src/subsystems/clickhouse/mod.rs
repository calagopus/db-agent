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

mod protocol;

enum Conn {
    Plain(TcpStream),
    Tls(MaybeKtlsStream),
}

pub async fn run(
    config: Arc<Config>,
    status: Arc<SubsystemConnections>,
    routes: Arc<DatabaseRouteManager>,
) -> anyhow::Result<()> {
    let tls = config.load().clickhouse.tls.clone();
    let acceptor = if tls.enabled {
        Some(crate::tls::build_acceptor(&tls, &[]).await?)
    } else {
        None
    };
    if let Some(acceptor) = &acceptor {
        let config = Arc::clone(&config);
        acceptor.spawn_reloader("clickhouse", move || {
            let config = config.load();
            (
                config.clickhouse.tls.cert.clone(),
                config.clickhouse.tls.key.clone(),
            )
        });
    }
    let bind = config.load().clickhouse.bind;

    let listener = TcpListener::bind(bind).await?;
    crate::net::apply_socket_congestion_control(&listener, &config);
    status.mark_running();
    tracing::info!(
        "clickhouse listening on {bind} (client TLS: {})",
        acceptor.as_ref().map_or("off", ReloadableAcceptor::mode)
    );

    crate::utils::accept_loop(&listener, "clickhouse", |tcp, peer| {
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
    let hello = protocol::read_hello(&mut client).await?;

    let user_id = hello.user.parse::<UserIdentifier>().ok();
    let creds = user_id.and_then(|id| routes.find(DatabaseType::Clickhouse, &id));
    let Some(creds) = creds
        .filter(|c| constant_time_eq::constant_time_eq(&hello.password, c.password.as_bytes()))
    else {
        tracing::debug!(%peer, user = %hello.user, "rejected auth");
        client
            .write_all(&protocol::exception(
                protocol::AUTHENTICATION_FAILED,
                "Authentication failed: password is incorrect, or there is no user with such name",
            ))
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
        client
            .write_all(&protocol::exception(
                protocol::DATABASE_ACCESS_DENIED,
                "database is locked",
            ))
            .await?;
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
            .write_all(&protocol::exception(
                protocol::READONLY,
                "database is write locked",
            ))
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
            client
                .write_all(&protocol::exception(
                    protocol::NETWORK_ERROR,
                    "database is offline",
                ))
                .await?;
            return Ok(());
        }
    };

    backend.write_all(&hello.raw).await?;

    let _guard = user_id
        .map(|id| status.connect(id, Some(hello.database.clone()).filter(|s| !s.is_empty())));
    let (c2b, b2c) = tokio::select! {
        copied = tokio::io::copy_bidirectional(&mut client, &mut backend) => copied?,
        _ = creds.instance.write_locked() => {
            tracing::info!(%peer, "closed: instance write locked");
            return Ok(());
        }
    };
    tracing::debug!(%peer, "closed (c->b {c2b} B, b->c {b2c} B)");
    Ok(())
}
