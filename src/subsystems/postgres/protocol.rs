use crate::utils::{SafeSliceExt, bad};
use std::collections::HashMap;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const PROTOCOL_30: i32 = 0x30000;
pub const SSL_REQUEST: i32 = 80877103;
pub const GSS_REQUEST: i32 = 80877104;

const MAX_MSG_LEN: i32 = 16 * 1024 * 1024;

pub type Params = HashMap<String, String>;

pub async fn read_startup_body<S: AsyncRead + Unpin>(stream: &mut S) -> std::io::Result<Vec<u8>> {
    let len = stream.read_i32().await?;
    if !(8..=1 << 20).contains(&len) {
        return Err(bad("implausible startup length"));
    }
    let mut body = vec![0; (len - 4) as usize];
    stream.read_exact(&mut body).await?;
    Ok(body)
}

pub async fn read_startup_message<S: AsyncRead + Unpin>(stream: &mut S) -> std::io::Result<Params> {
    let body = read_startup_body(stream).await?;
    if startup_code(&body) != PROTOCOL_30 {
        return Err(bad("expected StartupMessage after TLS"));
    }
    Ok(parse_params(body.get_slice(4..)?))
}

pub fn startup_code(body: &[u8]) -> i32 {
    match body {
        [a, b, c, d, ..] => i32::from_be_bytes([*a, *b, *c, *d]),
        _ => 0,
    }
}

pub fn parse_params(mut buf: &[u8]) -> Params {
    let mut map = HashMap::new();
    while let Some(key) = next_cstr(&mut buf) {
        if key.is_empty() {
            break;
        }
        map.insert(key, next_cstr(&mut buf).unwrap_or_default());
    }

    map
}

fn next_cstr(buf: &mut &[u8]) -> Option<String> {
    let end = buf.iter().position(|&b| b == 0)?;
    let s = String::from_utf8_lossy(buf.get(..end)?).into_owned();
    *buf = buf.get(end + 1..).unwrap_or_default();
    Some(s)
}

pub async fn read_msg<S: AsyncRead + Unpin>(s: &mut S) -> std::io::Result<(u8, Vec<u8>)> {
    let tag = s.read_u8().await?;
    let len = s.read_i32().await?;
    if len < 4 {
        return Err(bad("short message"));
    }
    if len > MAX_MSG_LEN {
        return Err(bad("message too large"));
    }
    let mut body = vec![0; (len - 4) as usize];
    s.read_exact(&mut body).await?;
    Ok((tag, body))
}

pub async fn write_msg<S: AsyncWrite + Unpin>(
    s: &mut S,
    tag: u8,
    body: &[u8],
) -> std::io::Result<()> {
    s.write_u8(tag).await?;
    s.write_i32((body.len() + 4) as i32).await?;
    s.write_all(body).await
}

pub async fn send_error<S: AsyncWrite + Unpin>(
    s: &mut S,
    code: &str,
    msg: &str,
) -> std::io::Result<()> {
    let mut body = vec![b'S'];
    body.extend_from_slice(b"FATAL\0");
    body.push(b'C');
    body.extend_from_slice(code.as_bytes());
    body.push(0);
    body.push(b'M');
    body.extend_from_slice(msg.as_bytes());
    body.push(0);
    body.push(0);
    write_msg(s, b'E', &body).await
}

pub async fn send_startup<S: AsyncWrite + Unpin>(
    backend: &mut S,
    params: &Params,
) -> std::io::Result<()> {
    let mut buf = bytes::BytesMut::new();
    postgres_protocol::message::frontend::startup_message(
        params.iter().map(|(k, v)| (k.as_str(), v.as_str())),
        &mut buf,
    )
    .map_err(|err| bad(&err.to_string()))?;
    backend.write_all(&buf).await
}
