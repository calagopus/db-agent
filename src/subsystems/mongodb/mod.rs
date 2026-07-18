use crate::instance::{DatabaseType, identifier::UserIdentifier, manager::DatabaseRouteManager};
use crate::{config::Config, subsystems::status::SubsystemConnections, utils::bad};
use bson::doc;
use protocol::{
    OP_MSG, OP_QUERY, binary, hello_doc, op_msg_doc, read_message, sasl_error, write_op_msg,
    write_op_reply,
};
use scram::Scram;
use std::{net::SocketAddr, sync::Arc};
use tokio::{
    io::{AsyncRead, AsyncWrite, copy_bidirectional},
    net::{TcpListener, TcpStream},
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
        if config.mongodb.tls.enabled {
            crate::tls::build_acceptor(&config.mongodb.tls.cert, &config.mongodb.tls.key)?
        } else {
            None
        }
    };
    let bind = config.load().mongodb.bind;

    let listener = TcpListener::bind(bind).await?;
    status.mark_running();
    tracing::info!(
        "mongodb listening on {bind} (client TLS: {})",
        if acceptor.is_some() { "on" } else { "off" }
    );

    crate::utils::accept_loop(&listener, "mongodb", |tcp, peer| {
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
    match negotiate(tcp, &acceptor).await? {
        Conn::Plain(s) => {
            tracing::debug!("[{peer}] connection (plain)");
            session(s, &status, &routes, peer).await
        }
        Conn::Tls(s) => {
            tracing::debug!("[{peer}] connection (tls)");
            session(*s, &status, &routes, peer).await
        }
    }
}

async fn negotiate(tcp: TcpStream, acceptor: &Option<TlsAcceptor>) -> std::io::Result<Conn> {
    let mut b = [0; 1];
    let n = tcp.peek(&mut b).await?;
    match acceptor {
        Some(acc) if n == 1 && b[0] == 0x16 => Ok(Conn::Tls(Box::new(acc.accept(tcp).await?))),
        _ => Ok(Conn::Plain(tcp)),
    }
}

async fn session<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    status: &Arc<SubsystemConnections>,
    routes: &DatabaseRouteManager,
    peer: SocketAddr,
) -> std::io::Result<()> {
    let mut scram: Option<Scram> = None;

    loop {
        let (reqid, opcode, body) = read_message(&mut stream).await?;

        if opcode == OP_QUERY {
            write_op_reply(&mut stream, reqid, &hello_doc()).await?;
            continue;
        }
        if opcode != OP_MSG {
            return Err(bad("unsupported opcode"));
        }
        let msg = op_msg_doc(&body).ok_or_else(|| bad("bad OP_MSG"))?;
        let cmd = msg.keys().next().ok_or_else(|| bad("empty command"))?;

        match cmd.as_str() {
            "ismaster" | "isMaster" | "hello" => {
                write_op_msg(&mut stream, reqid, &hello_doc()).await?;
            }
            "saslStart" => {
                let payload = msg
                    .get_binary_generic("payload")
                    .map_err(|_| bad("no payload"))?;
                let db = msg.get_str("$db").unwrap_or_default().to_string();
                let client_first = String::from_utf8_lossy(payload).into_owned();
                let (bare, cnonce, user) = scram::parse_client_first(&client_first)
                    .ok_or_else(|| bad("bad client-first"))?;
                tracing::debug!("[{peer}] saslStart user={user:?} db={db:?}");

                let creds = user
                    .parse::<UserIdentifier>()
                    .ok()
                    .and_then(|id| routes.find(DatabaseType::Mongodb, &id));
                let Some(creds) = creds else {
                    write_op_msg(&mut stream, reqid, &sasl_error("authentication failed")).await?;
                    return Ok(());
                };

                if creds.instance.is_suspended().await {
                    tracing::debug!(
                        "[{peer}] rejected: database {} suspended",
                        creds.instance.uuid
                    );
                    write_op_msg(&mut stream, reqid, &sasl_error("database is suspended")).await?;
                    return Ok(());
                }

                if let Err(err) = creds.instance.verify_mongodb_auth().await {
                    tracing::error!("[{peer}] rejected: instance {}: {err}", creds.instance.uuid);
                    write_op_msg(
                        &mut stream,
                        reqid,
                        &sasl_error("backend authorization not verified"),
                    )
                    .await?;
                    return Ok(());
                }

                let (st, server_first) = Scram::start(
                    &creds.password,
                    &creds.instance.get_socket_path().await,
                    bare,
                    &cnonce,
                    user,
                    db,
                );

                let reply = doc! {
                    "conversationId": 1,
                    "done": false,
                    "payload": binary(server_first.as_bytes()),
                    "ok": 1.0,
                };
                write_op_msg(&mut stream, reqid, &reply).await?;

                scram = Some(st);
            }
            "saslContinue" => {
                let st = scram
                    .as_ref()
                    .ok_or_else(|| bad("saslContinue before saslStart"))?;
                let payload = msg
                    .get_binary_generic("payload")
                    .map_err(|_| bad("no payload"))?;
                let client_final = String::from_utf8_lossy(payload).into_owned();
                let Some(server_final) = st.verify(&client_final) else {
                    write_op_msg(&mut stream, reqid, &sasl_error("authentication failed")).await?;
                    return Ok(());
                };
                let reply = doc! {
                    "conversationId": 1,
                    "done": true,
                    "payload": binary(server_final.as_bytes()),
                    "ok": 1.0,
                };
                write_op_msg(&mut stream, reqid, &reply).await?;
                break;
            }
            _ => {
                write_op_msg(&mut stream, reqid, &doc! { "ok": 1.0 }).await?;
            }
        }
    }

    let st = scram.ok_or_else(|| bad("not authenticated"))?;
    tracing::info!("[{peer}] {:?}@{:?} authenticated", st.user, st.db);
    let mut backend = scram::backend_auth(&st.socket, &st.user, &st.password).await?;
    tracing::debug!("[{peer}] backend ready, relaying");

    let _guard = st
        .user
        .parse::<UserIdentifier>()
        .ok()
        .map(|id| status.connect(id, Some(st.db.to_string()).filter(|s| !s.is_empty())));
    let (c2b, b2c) = copy_bidirectional(&mut stream, &mut backend).await?;
    tracing::debug!("[{peer}] closed (c->b {c2b} B, b->c {b2c} B)");

    Ok(())
}
