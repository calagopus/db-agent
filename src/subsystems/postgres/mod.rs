use super::database::{
    DatabaseType,
    identifier::{DbIdentifier, UserIdentifier},
    manager::DatabaseRouteManager,
};
use crate::{
    config::Config,
    subsystems::status::SubsystemConnections,
    utils::{SafeSliceExt, bad},
};
use protocol::Params;
use std::{net::SocketAddr, sync::Arc};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt, copy_bidirectional},
    net::{TcpListener, TcpStream, UnixStream},
};
use tokio_rustls::{TlsAcceptor, server::TlsStream};

mod protocol;
mod scram;

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
        if config.postgres.tls.enabled {
            crate::tls::build_acceptor(&config.postgres.tls.cert, &config.postgres.tls.key)?
        } else {
            None
        }
    };
    let bind = config.load().postgres.bind;

    let listener = TcpListener::bind(bind).await?;
    status.mark_running();
    tracing::info!(
        "postgres listening on {bind} (client TLS: {})",
        if acceptor.is_some() { "on" } else { "off" }
    );

    crate::utils::accept_loop(&listener, "postgres", |tcp, peer| {
        let status = Arc::clone(&status);
        let routes = Arc::clone(&routes);
        let acceptor = acceptor.clone();
        async move { handle(tcp, status, routes, acceptor, peer).await }
    })
    .await
}

async fn handle(
    tcp: TcpStream,
    status: Arc<SubsystemConnections>,
    routes: Arc<DatabaseRouteManager>,
    acceptor: Option<TlsAcceptor>,
    peer: SocketAddr,
) -> std::io::Result<()> {
    let (conn, preread) = negotiate(tcp, &acceptor).await?;
    match conn {
        Conn::Plain(s) => {
            tracing::debug!("[{peer}] connection (plain)");
            session(s, preread, &status, &routes, peer).await
        }
        Conn::Tls(s) => {
            tracing::debug!("[{peer}] connection (tls)");
            session(*s, preread, &status, &routes, peer).await
        }
    }
}

async fn negotiate(
    mut tcp: TcpStream,
    acceptor: &Option<TlsAcceptor>,
) -> std::io::Result<(Conn, Option<Params>)> {
    loop {
        let body = protocol::read_startup_body(&mut tcp).await?;
        match protocol::startup_code(&body) {
            protocol::SSL_REQUEST => match acceptor {
                Some(acc) => {
                    tcp.write_all(b"S").await?;
                    return Ok((Conn::Tls(Box::new(acc.accept(tcp).await?)), None));
                }
                None => tcp.write_all(b"N").await?,
            },
            protocol::GSS_REQUEST => tcp.write_all(b"N").await?,
            protocol::PROTOCOL_30 => {
                return Ok((
                    Conn::Plain(tcp),
                    Some(protocol::parse_params(body.get_slice(4..)?)),
                ));
            }
            other => return Err(bad(&format!("unsupported startup code {other}"))),
        }
    }
}

async fn session<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    preread: Option<Params>,
    status: &Arc<SubsystemConnections>,
    routes: &DatabaseRouteManager,
    peer: SocketAddr,
) -> std::io::Result<()> {
    let params = match preread {
        Some(p) => p,
        None => protocol::read_startup_message(&mut stream).await?,
    };
    let user = params.get("user").cloned().unwrap_or_default();
    let database = params
        .get("database")
        .cloned()
        .unwrap_or_else(|| user.clone());
    tracing::debug!("[{peer}] startup: user={user:?} database={database:?}");

    let user_id = user.parse::<UserIdentifier>().ok();
    let creds = user_id.and_then(|id| routes.find(DatabaseType::Postgres, &id));
    let Some(creds) = creds else {
        protocol::send_error(
            &mut stream,
            "28P01",
            &format!("no credential for user {user:?}"),
        )
        .await?;
        return Ok(());
    };

    if creds.database.is_suspended().await {
        protocol::send_error(&mut stream, "28P01", "database is suspended").await?;
        tracing::debug!(
            "[{peer}] rejected: database {} suspended",
            creds.database.uuid
        );
        return Ok(());
    }

    if !scram::authenticate_client(&mut stream, &creds.password).await? {
        protocol::send_error(&mut stream, "28P01", "authentication failed").await?;
        return Ok(());
    }
    protocol::write_msg(&mut stream, b'R', &0i32.to_be_bytes()).await?; // AuthenticationOk
    tracing::info!("[{peer}] {user:?}@{database:?} authenticated");

    let mut backend = UnixStream::connect(&creds.database.get_socket_path().await).await?;
    protocol::send_startup(&mut backend, &params).await?;
    scram::authenticate_backend(&mut backend, &mut stream, &creds.password).await?;
    tracing::debug!("[{peer}] backend ready, relaying");

    let _guard = user_id.map(|id| status.connect(id, database.parse::<DbIdentifier>().ok()));
    let (c2b, b2c) = copy_bidirectional(&mut stream, &mut backend).await?;
    tracing::debug!("[{peer}] closed (c->b {c2b} B, b->c {b2c} B)");
    Ok(())
}
