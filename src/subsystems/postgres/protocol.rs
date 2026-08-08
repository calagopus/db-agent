use crate::{
    io::SafeSliceExt,
    utils::{bad, handshake_step},
};
use std::collections::HashMap;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const PROTOCOL_30: i32 = 0x30000;
pub const SSL_REQUEST: i32 = 80877103;
pub const GSS_REQUEST: i32 = 80877104;

const MAX_MSG_LEN: i32 = 1024 * 1024;
const PROTOCOL_OPTION_PREFIX: &str = "_pq_.";

pub type Params = HashMap<String, String>;

pub async fn read_startup_body<S: AsyncRead + Unpin>(stream: &mut S) -> std::io::Result<Vec<u8>> {
    handshake_step(async {
        let len = stream.read_i32().await?;
        if !(8..=MAX_MSG_LEN).contains(&len) {
            return Err(bad("implausible startup length"));
        }
        let mut body = vec![0; (len - 4) as usize];
        stream.read_exact(&mut body).await?;
        Ok(body)
    })
    .await
}

pub async fn read_startup_message<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
) -> std::io::Result<Params> {
    let body = read_startup_body(stream).await?;
    accept_startup(stream, &body).await
}

pub async fn accept_startup<S: AsyncWrite + Unpin>(
    stream: &mut S,
    body: &[u8],
) -> std::io::Result<Params> {
    let code = startup_code(body);
    if code >> 16 != PROTOCOL_30 >> 16 {
        return Err(bad(&format!("unsupported startup code {code}")));
    }

    let mut params = parse_params(body.get_slice(4..)?);
    let options: Vec<String> = params
        .keys()
        .filter(|key| key.starts_with(PROTOCOL_OPTION_PREFIX))
        .cloned()
        .collect();

    if code != PROTOCOL_30 || !options.is_empty() {
        for key in &options {
            params.remove(key);
        }

        let mut msg = PROTOCOL_30.to_be_bytes().to_vec();
        msg.extend_from_slice(&(options.len() as i32).to_be_bytes());
        for key in &options {
            msg.extend_from_slice(key.as_bytes());
            msg.push(0);
        }
        write_msg(stream, b'v', &msg).await?;
    }

    Ok(params)
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

pub async fn read_msg<S: AsyncRead + Unpin>(stream: &mut S) -> std::io::Result<(u8, Vec<u8>)> {
    handshake_step(async {
        let tag = stream.read_u8().await?;
        let len = stream.read_i32().await?;
        if len < 4 {
            return Err(bad("short message"));
        }
        if len > MAX_MSG_LEN {
            return Err(bad("message too large"));
        }
        let mut body = vec![0; (len - 4) as usize];
        stream.read_exact(&mut body).await?;
        Ok((tag, body))
    })
    .await
}

pub async fn write_msg<S: AsyncWrite + Unpin>(
    stream: &mut S,
    tag: u8,
    body: &[u8],
) -> std::io::Result<()> {
    stream.write_u8(tag).await?;
    stream.write_i32((body.len() + 4) as i32).await?;
    stream.write_all(body).await
}

pub async fn send_error<S: AsyncWrite + Unpin>(
    stream: &mut S,
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
    write_msg(stream, b'E', &body).await
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
