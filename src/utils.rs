use rand::{RngExt, distr::SampleString};
use std::{future::Future, net::SocketAddr, path::Path, time::Duration};
use tokio::net::{TcpListener, TcpStream};

#[inline]
pub fn slice_up_to(s: &str, max_len: usize) -> &str {
    if max_len >= s.len() || s.is_empty() {
        return s;
    }

    let mut idx = max_len;
    while !s.is_char_boundary(idx) {
        idx -= 1;
    }

    s.get(..idx).unwrap_or(s)
}

pub fn is_single_component_file_name(name: &str) -> bool {
    let mut components = Path::new(name).components();

    match (components.next(), components.next()) {
        (Some(std::path::Component::Normal(component)), None) => component.to_str() == Some(name),
        _ => false,
    }
}

pub fn validate_data<T: garde::Validate<Context = ()>>(data: &T) -> Result<(), Vec<String>> {
    data.validate().map_err(|report| {
        report
            .iter()
            .map(|(path, error)| format!("{path}: {error}"))
            .collect()
    })
}

pub fn generate_password() -> String {
    const PASSWORD_SPECIAL_CHARS: &[u8] = b"!@#$%^&*()<>-_";

    let mut rng = rand::rng();
    let mut password = rand::distr::Alphanumeric
        .sample_string(&mut rng, 24)
        .into_bytes();

    for _ in 0..rng.random_range(1..=5) {
        let pos = rng.random_range(0..password.len());
        let special = rng.random_range(0..PASSWORD_SPECIAL_CHARS.len());
        if let (Some(slot), Some(&ch)) =
            (password.get_mut(pos), PASSWORD_SPECIAL_CHARS.get(special))
        {
            *slot = ch;
        }
    }

    String::from_utf8_lossy(&password).into_owned()
}

/// quotes a value for interpolation into a `sh -c` command line
pub fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', r"'\''"))
}

pub fn bad(msg: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, msg.to_string())
}

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

pub async fn handshake_step<T>(
    fut: impl Future<Output = std::io::Result<T>>,
) -> std::io::Result<T> {
    tokio::time::timeout(HANDSHAKE_TIMEOUT, fut)
        .await
        .unwrap_or_else(|_| Err(std::io::ErrorKind::TimedOut.into()))
}

pub fn is_silent_error(err: &std::io::Error) -> bool {
    matches!(
        err.kind(),
        std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::UnexpectedEof
            // kTLS reports a peer close_notify as a zero-length write, which the copy loop
            // turns into WriteZero
            | std::io::ErrorKind::WriteZero
            | std::io::ErrorKind::TimedOut
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
                    match fut.await {
                        Ok(()) => {}
                        Err(err) if err.kind() == std::io::ErrorKind::InvalidData => {
                            tracing::debug!(%peer, "protocol error: {err}");
                        }
                        Err(err) if is_silent_error(&err) => {}
                        Err(err) => tracing::error!(%peer, "connection error: {err}"),
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

            if map.get(part).is_some_and(|next| !next.is_object()) {
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

const SENSITIVE_QUERY_KEY_PARTS: [&str; 6] = [
    "token",
    "secret",
    "password",
    "key",
    "signature",
    "credential",
];
const SENSITIVE_QUERY_KEYS: [&str; 2] = ["code", "data"];

fn contains_ignore_ascii_case(haystack: &str, needle: &str) -> bool {
    haystack
        .as_bytes()
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle.as_bytes()))
}

fn is_sensitive_query_key(key: &str) -> bool {
    SENSITIVE_QUERY_KEYS
        .iter()
        .any(|sensitive| key.eq_ignore_ascii_case(sensitive))
        || SENSITIVE_QUERY_KEY_PARTS
            .iter()
            .any(|part| contains_ignore_ascii_case(key, part))
}

pub fn redact_query(query: &str) -> std::borrow::Cow<'_, str> {
    fn is_sensitive_pair(pair: &str) -> bool {
        pair.split_once('=')
            .is_some_and(|(key, _)| is_sensitive_query_key(key))
    }

    if !query.split('&').any(is_sensitive_pair) {
        return std::borrow::Cow::Borrowed(query);
    }

    let mut redacted = String::with_capacity(query.len());
    for (index, pair) in query.split('&').enumerate() {
        if index > 0 {
            redacted.push('&');
        }

        match pair.split_once('=') {
            Some((key, _)) if is_sensitive_query_key(key) => {
                redacted.push_str(key);
                redacted.push_str("=<redacted>");
            }
            _ => redacted.push_str(pair),
        }
    }

    std::borrow::Cow::Owned(redacted)
}

pub fn get_array<const N: usize>(slice: &[u8], start: usize) -> std::io::Result<[u8; N]> {
    slice
        .get(start..start.saturating_add(N))
        .and_then(|s| s.try_into().ok())
        .ok_or_else(|| bad("unexpected end of buffer"))
}

#[cfg(test)]
mod tests {
    use super::*;

    // redact_query

    #[test]
    fn redact_query_leaves_harmless_queries_borrowed() {
        let query = "directory=%2F&ignored=&per_page=100&page=1";

        assert!(matches!(redact_query(query), std::borrow::Cow::Borrowed(_)));
        assert_eq!(redact_query(query), query);
        assert_eq!(redact_query(""), "");
    }

    #[test]
    fn redact_query_redacts_jwt_values() {
        assert_eq!(
            redact_query(
                "token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzY29wZSI6ImJhY2t1cC1kb3dubG9hZCJ9.sig"
            ),
            "token=<redacted>"
        );
        assert_eq!(
            redact_query("token=eyJ0eXAi.eyJzY29wZSJ9.sig&archive_format=tar_gz"),
            "token=<redacted>&archive_format=tar_gz"
        );
    }

    #[test]
    fn redact_query_redacts_opaque_token_code_and_data_values() {
        assert_eq!(
            redact_query("token=6RnJvbTNoZVNoYWRvd3NPZlRoZURlZXBXZUNhbGxUb1RoZWU"),
            "token=<redacted>"
        );
        assert_eq!(
            redact_query("code=abc123&state=xyz"),
            "code=<redacted>&state=xyz"
        );
        assert_eq!(redact_query("data=eyJ1c2VyIjp7fX0="), "data=<redacted>");
    }

    #[test]
    fn redact_query_matches_keys_case_insensitively_and_by_substring() {
        assert_eq!(redact_query("Token=abc"), "Token=<redacted>");
        assert_eq!(redact_query("access_token=abc"), "access_token=<redacted>");
        assert_eq!(redact_query("api_key=abc"), "api_key=<redacted>");
        assert_eq!(
            redact_query("X-Amz-Signature=abc&X-Amz-Credential=def&X-Amz-Date=20260825T000000Z"),
            "X-Amz-Signature=<redacted>&X-Amz-Credential=<redacted>&X-Amz-Date=20260825T000000Z"
        );
    }

    #[test]
    fn redact_query_keeps_valueless_and_malformed_pairs_intact() {
        assert_eq!(redact_query("token"), "token");
        assert_eq!(redact_query("token="), "token=<redacted>");
        assert_eq!(redact_query("&&"), "&&");
        assert_eq!(redact_query("token=a=b&x=1"), "token=<redacted>&x=1");
    }
}
