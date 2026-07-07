use rand::{RngExt, distr::SampleString};
use std::{future::Future, net::SocketAddr, time::Duration};
use tokio::net::{TcpListener, TcpStream};

pub fn generate_password() -> String {
    const PASSWORD_SPECIAL_CHARS: &[u8] = b"!@#$%^&*()<>-_";

    let mut rng = rand::rng();
    let mut password = rand::distr::Alphanumeric
        .sample_string(&mut rng, 24)
        .into_bytes();

    for _ in 0..rng.random_range(1..=5) {
        let pos = rng.random_range(0..password.len());
        password[pos] = PASSWORD_SPECIAL_CHARS[rng.random_range(0..PASSWORD_SPECIAL_CHARS.len())];
    }

    String::from_utf8_lossy(&password).into_owned()
}

pub fn bad(msg: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, msg.to_string())
}

pub fn is_silent_error(err: &std::io::Error) -> bool {
    matches!(
        err.kind(),
        std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::UnexpectedEof
    )
}

pub async fn accept_loop<
    F: FnMut(TcpStream, SocketAddr) -> Fut,
    Fut: Future<Output = std::io::Result<()>> + Send + 'static,
>(
    listener: &TcpListener,
    name: &'static str,
    mut on_accept: F,
) -> Result<(), anyhow::Error> {
    loop {
        match listener.accept().await {
            Ok((tcp, peer)) => {
                let fut = on_accept(tcp, peer);
                tokio::spawn(async move {
                    if let Err(err) = fut.await
                        && !is_silent_error(&err)
                    {
                        tracing::error!("[{peer}] error: {err}");
                    }
                });
            }
            Err(err) => {
                const EMFILE: i32 = 24;
                const ENFILE: i32 = 23;

                let backoff = match err.raw_os_error() {
                    Some(EMFILE) | Some(ENFILE) => Duration::from_millis(500),
                    _ => Duration::from_millis(50),
                };

                tracing::error!("{name} accept error: {err}; backing off {backoff:?}");
                tokio::time::sleep(backoff).await;
            }
        }
    }
}

pub fn strip_paths(value: &mut serde_json::Value, paths: &[&str]) {
    for path in paths {
        let mut cursor = &mut *value;
        let mut parts = path.split('.').peekable();

        while let Some(part) = parts.next() {
            let serde_json::Value::Object(map) = cursor else {
                break;
            };

            if parts.peek().is_none() {
                map.remove(part);
                break;
            }

            match map.get_mut(part) {
                Some(next) => cursor = next,
                None => break,
            }
        }
    }
}
