use crate::instance::{DatabaseType, identifier::UserIdentifier, manager::DatabaseRouteManager};
use crate::{
    config::Config,
    subsystems::status::SubsystemConnections,
    utils::{SafeSliceExt, SafeSliceMutExt, bad, get_array},
};
use protocol::{read_packet, write_packet};
use rand::Rng;
use std::{net::SocketAddr, path::Path, sync::Arc};
use tokio::{
    io::{AsyncRead, AsyncWrite, copy_bidirectional},
    net::{TcpListener, TcpStream, UnixStream},
};
use tokio_rustls::TlsAcceptor;

mod auth;
mod protocol;

pub async fn run(
    config: Arc<Config>,
    status: Arc<SubsystemConnections>,
    routes: Arc<DatabaseRouteManager>,
) -> anyhow::Result<()> {
    let acceptor = {
        let config = config.load();
        if config.mariadb.tls.enabled {
            crate::tls::build_acceptor(&config.mariadb.tls.cert, &config.mariadb.tls.key)?
        } else {
            None
        }
    };
    let bind = config.load().mariadb.bind;

    let listener = TcpListener::bind(bind).await?;
    status.mark_running();
    tracing::info!(
        "mariadb listening on {bind} (client TLS: {})",
        if acceptor.is_some() { "on" } else { "off" }
    );

    crate::utils::accept_loop(&listener, "mariadb", |tcp, peer| {
        let status = Arc::clone(&status);
        let routes = Arc::clone(&routes);
        let acceptor = acceptor.clone();
        async move { handle(tcp, status, routes, acceptor, peer).await }
    })
    .await
}

async fn handle(
    mut tcp: TcpStream,
    status: Arc<SubsystemConnections>,
    routes: Arc<DatabaseRouteManager>,
    acceptor: Option<TlsAcceptor>,
    peer: SocketAddr,
) -> std::io::Result<()> {
    let mut scramble = [0; 20];
    rand::rng().fill_bytes(&mut scramble);
    let ssl_offered = acceptor.is_some();
    write_packet(
        &mut tcp,
        0,
        &protocol::server_handshake(&scramble, ssl_offered),
    )
    .await?;

    let (seq, first) = read_packet(&mut tcp).await?;
    let caps = u32::from_le_bytes(get_array(&first, 0)?);

    if ssl_offered && caps & protocol::CLIENT_SSL != 0 {
        tracing::debug!("[{peer}] connection (tls)");
        let tls = acceptor
            .ok_or_else(|| bad("ssl requested without acceptor"))?
            .accept(tcp)
            .await?;
        session(tls, &status, &routes, scramble, None, peer).await
    } else {
        tracing::debug!("[{peer}] connection (plain)");
        session(tcp, &status, &routes, scramble, Some((seq, first)), peer).await
    }
}

async fn session<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    status: &Arc<SubsystemConnections>,
    routes: &DatabaseRouteManager,
    scramble: [u8; 20],
    preread: Option<(u8, Vec<u8>)>,
    peer: SocketAddr,
) -> std::io::Result<()> {
    let (cseq, resp) = match preread {
        Some(x) => x,
        None => read_packet(&mut stream).await?,
    };
    let hr = protocol::parse_handshake_response(&resp)?;
    tracing::debug!(
        "[{peer}] handshake: user={:?} database={:?} plugin={:?}",
        hr.user,
        hr.database,
        hr.plugin
    );

    let user_id = hr.user.parse::<UserIdentifier>().ok();
    let Some(creds) = user_id.and_then(|id| routes.find(DatabaseType::Mariadb, &id)) else {
        write_packet(
            &mut stream,
            cseq + 1,
            &protocol::err_packet(
                1045,
                "28000",
                &format!("no credential for user {:?}", hr.user),
            ),
        )
        .await?;
        return Ok(());
    };

    if creds.instance.is_suspended().await {
        write_packet(
            &mut stream,
            cseq + 1,
            &protocol::err_packet(1045, "28000", "database is suspended"),
        )
        .await?;
        tracing::debug!(
            "[{peer}] rejected: database {} suspended",
            creds.instance.uuid
        );
        return Ok(());
    }

    let (mut token, mut seq) = (hr.auth_response, cseq);
    if hr.plugin != protocol::NATIVE {
        write_packet(
            &mut stream,
            seq + 1,
            &protocol::auth_switch_request(&scramble),
        )
        .await?;
        let (s2, t2) = read_packet(&mut stream).await?;
        token = t2;
        seq = s2;
    }

    if !constant_time_eq::constant_time_eq(
        &token,
        &auth::native_token(&scramble, creds.password.as_bytes()),
    ) {
        write_packet(
            &mut stream,
            seq + 1,
            &protocol::err_packet(1045, "28000", "access denied"),
        )
        .await?;
        return Ok(());
    }
    write_packet(&mut stream, seq + 1, &protocol::ok_packet()).await?;
    tracing::info!("[{peer}] {:?}@{:?} authenticated", hr.user, hr.database);

    let mut backend = backend_auth(
        &creds.instance.get_socket_path().await,
        &hr.user,
        &creds.password,
        &hr.database,
        hr.caps,
    )
    .await?;
    tracing::debug!("[{peer}] backend ready, relaying");

    let _guard = user_id
        .map(|id| status.connect(id, Some(hr.database.to_string()).filter(|s| !s.is_empty())));
    let (c2b, b2c) = copy_bidirectional(&mut stream, &mut backend).await?;
    tracing::debug!("[{peer}] closed (c->b {c2b} B, b->c {b2c} B)");
    Ok(())
}

async fn backend_auth(
    socket: &Path,
    user: &str,
    password: &str,
    database: &str,
    client_caps: u32,
) -> std::io::Result<UnixStream> {
    let mut be = UnixStream::connect(socket).await?;
    let (seq, hs) = read_packet(&mut be).await?;
    let (scramble, _plugin) = protocol::parse_server_handshake(&hs)?;
    let token = auth::native_token(&scramble, password.as_bytes());
    write_packet(
        &mut be,
        seq + 1,
        &protocol::handshake_response(user, &token, database, client_caps),
    )
    .await?;

    loop {
        let (rseq, r) = read_packet(&mut be).await?;
        match r.first() {
            Some(0x00) => return Ok(be),
            Some(0xff) => {
                let msg = if r.len() > 9 {
                    String::from_utf8_lossy(r.get_slice(9..)?).into_owned()
                } else {
                    "backend error".into()
                };
                return Err(bad(&format!("backend refused auth: {msg}")));
            }
            Some(0xfe) => {
                // AuthSwitchRequest: 0xfe, plugin CString, auth data
                let mut i = 1usize;
                let _plugin = protocol::read_cstr(&r, &mut i)?;
                let mut new_scramble = [0; 20];
                let avail = r.len().saturating_sub(i).min(20);
                new_scramble
                    .get_slice_mut(..avail)?
                    .copy_from_slice(r.get_slice(i..i + avail)?);
                let token = auth::native_token(&new_scramble, password.as_bytes());
                write_packet(&mut be, rseq + 1, &token).await?;
            }
            _ => return Err(bad("unexpected backend auth packet")),
        }
    }
}
