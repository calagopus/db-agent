use crate::{
    config::Config,
    instance::{DatabaseType, identifier::UserIdentifier, manager::DatabaseRouteManager},
    subsystems::status::SubsystemConnections,
    tls::{MaybeKtlsStream, ReloadableAcceptor},
};
use protocol::Params;
use std::{net::SocketAddr, sync::Arc};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt, copy_bidirectional},
    net::{TcpListener, TcpStream, UnixStream},
};

mod protocol;
mod scram;

enum Conn {
    Plain(TcpStream),
    Tls(MaybeKtlsStream),
}

pub async fn run(
    config: Arc<Config>,
    status: Arc<SubsystemConnections>,
    routes: Arc<DatabaseRouteManager>,
) -> anyhow::Result<()> {
    let tls = config.load().postgres.tls.clone();
    let acceptor = if tls.enabled {
        Some(crate::tls::build_acceptor(&tls, &[]).await?)
    } else {
        None
    };
    if let Some(acceptor) = &acceptor {
        let config = Arc::clone(&config);
        acceptor.spawn_reloader("postgres", move || {
            let config = config.load();
            (
                config.postgres.tls.cert.clone(),
                config.postgres.tls.key.clone(),
            )
        });
    }
    let bind = config.load().postgres.bind;

    let listener = TcpListener::bind(bind).await?;
    crate::net::apply_socket_congestion_control(&listener, &config);
    status.mark_running();
    tracing::info!(
        "postgres listening on {bind} (client TLS: {})",
        acceptor.as_ref().map_or("off", ReloadableAcceptor::mode)
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
    acceptor: Option<ReloadableAcceptor>,
    peer: SocketAddr,
) -> std::io::Result<()> {
    let (conn, preread) = negotiate(tcp, &acceptor).await?;
    match conn {
        Conn::Plain(s) => {
            tracing::debug!(%peer, "connection (plain)");
            session(s, preread, &status, &routes, peer).await
        }
        Conn::Tls(s) => {
            tracing::debug!(%peer, "connection (tls)");
            session(s, preread, &status, &routes, peer).await
        }
    }
}

async fn negotiate(
    mut tcp: TcpStream,
    acceptor: &Option<ReloadableAcceptor>,
) -> std::io::Result<(Conn, Option<Params>)> {
    loop {
        let body = protocol::read_startup_body(&mut tcp).await?;
        match protocol::startup_code(&body) {
            protocol::SSL_REQUEST => match acceptor {
                Some(acc) => {
                    tcp.write_all(b"S").await?;
                    let tls = crate::utils::handshake_step(acc.accept(tcp)).await?;
                    return Ok((Conn::Tls(tls), None));
                }
                None => tcp.write_all(b"N").await?,
            },
            protocol::GSS_REQUEST => tcp.write_all(b"N").await?,
            _ => {
                let params = protocol::accept_startup(&mut tcp, &body).await?;
                return Ok((Conn::Plain(tcp), Some(params)));
            }
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
    tracing::debug!(%peer, %user, %database, "startup received");

    let user_id = user.parse::<UserIdentifier>().ok();
    let creds = user_id.and_then(|id| routes.find(DatabaseType::Postgres, &id));
    let Some(creds) = creds else {
        protocol::send_error(
            &mut stream,
            "28P01",
            &format!("no credential for user {user}"),
        )
        .await?;
        return Ok(());
    };

    if let Some(state) = creds.instance.locked_state() {
        protocol::send_error(&mut stream, "28P01", "database is locked").await?;
        tracing::debug!(
            %peer,
            instance = %creds.instance.uuid,
            state = %state,
            "rejected: instance locked"
        );
        return Ok(());
    }

    if !scram::authenticate_client(&mut stream, &creds.password).await? {
        protocol::send_error(&mut stream, "28P01", "authentication failed").await?;
        return Ok(());
    }
    // the backend socket has to be reachable before the client is told it is in, an offline
    // instance would otherwise reach the client as a dropped connection instead of an error.
    // AuthenticationOk still precedes authenticate_backend, which relays the backend's
    // ParameterStatus/BackendKeyData/ReadyForQuery straight to the client
    let mut backend = match UnixStream::connect(&creds.instance.get_socket_path().await).await {
        Ok(backend) => backend,
        Err(err) => {
            protocol::send_error(&mut stream, "57P03", "database is offline").await?;
            tracing::debug!(
                %peer,
                instance = %creds.instance.uuid,
                "rejected: backend unreachable: {err}"
            );
            return Ok(());
        }
    };
    protocol::send_startup(&mut backend, &params).await?;

    protocol::write_msg(&mut stream, b'R', &0i32.to_be_bytes()).await?; // AuthenticationOk
    tracing::info!(%peer, %user, %database, "client authenticated");

    scram::authenticate_backend(&mut backend, &mut stream, &creds.password).await?;
    tracing::debug!(%peer, "backend ready, relaying");

    let _guard =
        user_id.map(|id| status.connect(id, Some(database.to_string()).filter(|s| !s.is_empty())));
    let (c2b, b2c) = copy_bidirectional(&mut stream, &mut backend).await?;
    tracing::debug!(%peer, "closed (c->b {c2b} B, b->c {b2c} B)");
    Ok(())
}
