use crate::{
    config::Config,
    instance::{DatabaseType, identifier::UserIdentifier, manager::DatabaseRouteManager},
    io::{SafeSliceExt, SafeSliceMutExt},
    subsystems::status::SubsystemConnections,
    tls::ReloadableAcceptor,
    utils::{bad, get_array},
};
use protocol::{read_packet, write_packet};
use std::{net::SocketAddr, sync::Arc};
use tokio::{
    io::{AsyncRead, AsyncWrite, copy_bidirectional},
    net::{TcpListener, TcpStream, UnixStream},
};

mod auth;
mod protocol;

pub async fn run(
    config: Arc<Config>,
    status: Arc<SubsystemConnections>,
    routes: Arc<DatabaseRouteManager>,
) -> anyhow::Result<()> {
    let tls = config.load().mariadb.tls.clone();
    let acceptor = if tls.enabled {
        Some(crate::tls::build_acceptor(&tls, &[]).await?)
    } else {
        None
    };
    if let Some(acceptor) = &acceptor {
        let config = Arc::clone(&config);
        acceptor.spawn_reloader("mariadb", move || {
            let config = config.load();
            (
                config.mariadb.tls.cert.clone(),
                config.mariadb.tls.key.clone(),
            )
        });
    }
    let bind = config.load().mariadb.bind;

    let listener = TcpListener::bind(bind).await?;
    crate::net::apply_socket_congestion_control(&listener, &config);
    status.mark_running();
    tracing::info!(
        "mariadb listening on {bind} (client TLS: {})",
        acceptor.as_ref().map_or("off", ReloadableAcceptor::mode)
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
    acceptor: Option<ReloadableAcceptor>,
    peer: SocketAddr,
) -> std::io::Result<()> {
    let scramble = protocol::random_scramble();
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
        tracing::debug!(%peer, "connection (tls)");
        let acceptor = acceptor.ok_or_else(|| bad("ssl requested without acceptor"))?;
        let tls = crate::utils::handshake_step(acceptor.accept(tcp)).await?;
        session(tls, &status, &routes, scramble, None, peer).await
    } else {
        tracing::debug!(%peer, "connection (plain)");
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
        %peer,
        user = %hr.user,
        database = %hr.database,
        plugin = %hr.plugin,
        "handshake received"
    );

    let user_id = hr.user.parse::<UserIdentifier>().ok();
    let Some(creds) = user_id.and_then(|id| routes.find(DatabaseType::Mariadb, &id)) else {
        write_packet(
            &mut stream,
            cseq + 1,
            &protocol::err_packet(
                1045,
                "28000",
                &format!("no credential for user {}", hr.user),
            ),
        )
        .await?;
        return Ok(());
    };

    if let Some(state) = creds.instance.locked_state() {
        write_packet(
            &mut stream,
            cseq + 1,
            &protocol::err_packet(1045, "28000", "database is locked"),
        )
        .await?;
        tracing::debug!(
            %peer,
            instance = %creds.instance.uuid,
            state = %state,
            "rejected: instance locked"
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
    // the backend answers before the client is told it is in, an offline instance or a refused
    // database would otherwise reach the client as a dropped connection instead of an error
    let mut backend = match UnixStream::connect(&creds.instance.get_socket_path().await).await {
        Ok(backend) => backend,
        Err(err) => {
            write_packet(
                &mut stream,
                seq + 1,
                &protocol::err_packet(1053, "08S01", "database is offline"),
            )
            .await?;
            tracing::debug!(
                %peer,
                instance = %creds.instance.uuid,
                "rejected: backend unreachable: {err}"
            );
            return Ok(());
        }
    };

    if let Some(refusal) = backend_auth(
        &mut backend,
        &hr.user,
        &creds.password,
        &hr.database,
        hr.caps,
    )
    .await?
    {
        write_packet(&mut stream, seq + 1, &refusal).await?;
        tracing::debug!(
            %peer,
            instance = %creds.instance.uuid,
            "rejected: backend refused the relayed credentials"
        );
        return Ok(());
    }

    write_packet(&mut stream, seq + 1, &protocol::ok_packet()).await?;
    tracing::info!(%peer, user = %hr.user, database = %hr.database, "client authenticated");
    tracing::debug!(%peer, "backend ready, relaying");

    let _guard = user_id
        .map(|id| status.connect(id, Some(hr.database.to_string()).filter(|s| !s.is_empty())));
    let (c2b, b2c) = copy_bidirectional(&mut stream, &mut backend).await?;
    tracing::debug!(%peer, "closed (c->b {c2b} B, b->c {b2c} B)");
    Ok(())
}

/// yields the backend's err packet when it refuses the relayed credentials, so the session can
/// hand it to the client verbatim
async fn backend_auth(
    be: &mut UnixStream,
    user: &str,
    password: &str,
    database: &str,
    client_caps: u32,
) -> std::io::Result<Option<Vec<u8>>> {
    let (seq, hs) = read_packet(be).await?;
    let (scramble, _plugin) = protocol::parse_server_handshake(&hs)?;
    let token = auth::native_token(&scramble, password.as_bytes());
    write_packet(
        be,
        seq + 1,
        &protocol::handshake_response(user, &token, database, client_caps),
    )
    .await?;

    loop {
        let (rseq, r) = read_packet(be).await?;
        match r.first() {
            Some(0x00) => return Ok(None),
            Some(0xff) => return Ok(Some(r)),
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
                write_packet(be, rseq + 1, &token).await?;
            }
            _ => return Err(bad("unexpected backend auth packet")),
        }
    }
}
