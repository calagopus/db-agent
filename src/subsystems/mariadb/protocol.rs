use crate::{
    io::SafeSliceExt,
    utils::{bad, get_array, handshake_step},
};
use rand::RngExt;
use std::{
    pin::Pin,
    sync::atomic::{AtomicU32, Ordering},
    task::{Context, Poll, ready},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

pub const CLIENT_LONG_PASSWORD: u32 = 0x0000_0001;
pub const CLIENT_FOUND_ROWS: u32 = 0x0000_0002;
pub const CLIENT_LONG_FLAG: u32 = 0x0000_0004;
pub const CLIENT_CONNECT_WITH_DB: u32 = 0x0000_0008;
pub const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
pub const CLIENT_SSL: u32 = 0x0000_0800;
pub const CLIENT_TRANSACTIONS: u32 = 0x0000_2000;
pub const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
pub const CLIENT_PLUGIN_AUTH: u32 = 0x0008_0000;
pub const CLIENT_PLUGIN_AUTH_LENENC: u32 = 0x0020_0000;
pub const CLIENT_MULTI_STATEMENTS: u32 = 0x0001_0000;
pub const CLIENT_MULTI_RESULTS: u32 = 0x0002_0000;

pub const NATIVE: &str = "mysql_native_password";

const MAX_PACKET_LEN: usize = 1024 * 1024;
const MAX_PAYLOAD_LEN: usize = 0xff_ffff;

const COM_SLEEP: u8 = 0x00;
const COM_CHANGE_USER: u8 = 0x11;
const COM_MULTI: u8 = 0xfe;

const CAPS: u32 = CLIENT_FOUND_ROWS
    | CLIENT_LONG_FLAG
    | CLIENT_PROTOCOL_41
    | CLIENT_SECURE_CONNECTION
    | CLIENT_PLUGIN_AUTH
    | CLIENT_CONNECT_WITH_DB
    | CLIENT_TRANSACTIONS
    | CLIENT_MULTI_STATEMENTS
    | CLIENT_MULTI_RESULTS;

// Caps whose bit must match the fixed packet layout handshake_response() writes.
const BACKEND_REQUIRED_CAPS: u32 =
    CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_CONNECT_WITH_DB | CLIENT_PLUGIN_AUTH;
// Caps safe to pass through from the client as-is.
const BACKEND_FORWARDABLE_CAPS: u32 = CLIENT_LONG_PASSWORD
    | CLIENT_FOUND_ROWS
    | CLIENT_LONG_FLAG
    | CLIENT_TRANSACTIONS
    | CLIENT_MULTI_STATEMENTS
    | CLIENT_MULTI_RESULTS;

// Must be printable ASCII, matching upstream's create_random_string(): Connector/J reads the
// seed as a NUL-terminated string and re-encodes it with the connection charset, so a 0x00
// truncates it and any byte >= 0x80 is mangled, yielding a token that never matches.
pub fn random_scramble() -> [u8; 20] {
    let mut rng = rand::rng();
    std::array::from_fn(|_| rng.random_range(33..=126))
}

pub fn next_connection_id() -> u32 {
    static NEXT: AtomicU32 = AtomicU32::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

pub const FALLBACK_SERVER_VERSION: &str = "5.5.5-10.6.0-MariaDB";

pub fn server_handshake(
    version: &str,
    scramble: &[u8; 20],
    connection_id: u32,
    ssl: bool,
) -> Vec<u8> {
    let mut caps = CAPS;
    if ssl {
        caps |= CLIENT_SSL;
    }
    let mut p = vec![10]; // protocol version
    p.extend_from_slice(version.as_bytes());
    p.push(0);
    p.extend_from_slice(&connection_id.to_le_bytes());
    p.extend_from_slice(&scramble[..8]); // auth-plugin-data-1
    p.push(0); // filler
    p.extend_from_slice(&(caps as u16).to_le_bytes()); // capabilities lower
    p.push(45); // charset utf8mb4_general_ci
    p.extend_from_slice(&0x0002u16.to_le_bytes()); // status: autocommit
    p.extend_from_slice(&((caps >> 16) as u16).to_le_bytes()); // capabilities upper
    p.push(21); // length of auth-plugin-data (20 + null)
    p.extend_from_slice(&[0; 10]); // reserved, zero mariadb caps: handshake_response() forwards none
    p.extend_from_slice(&scramble[8..20]); // auth-plugin-data-2 (12 bytes)
    p.push(0); // null terminator of part 2
    p.extend_from_slice(NATIVE.as_bytes());
    p.push(0);
    p
}

pub fn handshake_response(user: &str, token: &[u8], database: &str, client_caps: u32) -> Vec<u8> {
    let caps = BACKEND_REQUIRED_CAPS | (client_caps & BACKEND_FORWARDABLE_CAPS);
    let mut p = caps.to_le_bytes().to_vec();
    p.extend_from_slice(&(16 * 1024 * 1024u32).to_le_bytes()); // max packet
    p.push(45); // charset
    p.extend_from_slice(&[0; 23]); // reserved
    p.extend_from_slice(user.as_bytes());
    p.push(0);
    p.push(token.len() as u8); // CLIENT_SECURE_CONNECTION: 1-byte length
    p.extend_from_slice(token);
    p.extend_from_slice(database.as_bytes());
    p.push(0);
    p.extend_from_slice(NATIVE.as_bytes());
    p.push(0);
    p
}

pub fn auth_switch_request(scramble: &[u8; 20]) -> Vec<u8> {
    let mut p = vec![0xfe];
    p.extend_from_slice(NATIVE.as_bytes());
    p.push(0);
    p.extend_from_slice(scramble);
    p.push(0);
    p
}

pub fn ok_packet() -> Vec<u8> {
    let mut p = vec![0x00]; // OK header
    p.push(0x00); // affected rows (lenenc 0)
    p.push(0x00); // last insert id (lenenc 0)
    p.extend_from_slice(&0x0002u16.to_le_bytes()); // status: autocommit
    p.extend_from_slice(&0u16.to_le_bytes()); // warnings
    p
}

pub fn err_packet(code: u16, sqlstate: &str, msg: &str) -> Vec<u8> {
    let mut p = vec![0xff];
    p.extend_from_slice(&code.to_le_bytes());
    p.push(b'#');
    p.extend_from_slice(sqlstate.as_bytes());
    p.extend_from_slice(msg.as_bytes());
    p
}

pub struct HandshakeResponse {
    pub caps: u32,
    pub user: String,
    pub auth_response: Vec<u8>,
    pub database: String,
    pub plugin: String,
}

pub fn parse_handshake_response(p: &[u8]) -> std::io::Result<HandshakeResponse> {
    if p.len() < 32 {
        return Err(bad("short handshake response"));
    }
    let caps = u32::from_le_bytes(get_array(p, 0)?);
    let mut i = 32;
    let user = read_cstr(p, &mut i)?;
    let auth_response = if caps & CLIENT_PLUGIN_AUTH_LENENC != 0 {
        let n = read_lenenc(p, &mut i)? as usize;
        read_n(p, &mut i, n)?
    } else if caps & CLIENT_SECURE_CONNECTION != 0 {
        let n = *p.get(i).ok_or_else(|| bad("eof"))? as usize;
        i += 1;
        read_n(p, &mut i, n)?
    } else {
        read_cstr_bytes(p, &mut i)?
    };
    let database = if caps & CLIENT_CONNECT_WITH_DB != 0 {
        read_cstr(p, &mut i)?
    } else {
        String::new()
    };
    let plugin = if caps & CLIENT_PLUGIN_AUTH != 0 {
        read_cstr(p, &mut i).unwrap_or_default()
    } else {
        String::new()
    };
    Ok(HandshakeResponse {
        caps,
        user,
        auth_response,
        database,
        plugin,
    })
}

/// yields (scramble, auth plugin, server version)
pub fn parse_server_handshake(p: &[u8]) -> std::io::Result<([u8; 20], String, String)> {
    if p.first() != Some(&10) {
        return Err(bad("not a v10 server handshake"));
    }
    let mut i = 1;
    let version = read_cstr(p, &mut i)?;
    i += 4; // connection id
    let mut scramble = [0; 20];
    scramble[..8].copy_from_slice(p.get(i..i + 8).ok_or_else(|| bad("eof"))?);
    i += 8 + 1; // part1 + filler
    let cap_lo = u16::from_le_bytes(get_array(p, i)?);
    i += 2 + 1 + 2; // caps_lo + charset + status
    let cap_hi = u16::from_le_bytes(get_array(p, i)?);
    i += 2;
    let caps = ((cap_hi as u32) << 16) | cap_lo as u32;
    let adlen = *p.get(i).ok_or_else(|| bad("eof"))? as usize;
    i += 1 + 10; // auth-data-len + reserved
    scramble[8..20].copy_from_slice(p.get(i..i + 12).ok_or_else(|| bad("eof"))?);
    i += if adlen > 8 { adlen - 8 } else { 13 };
    let plugin = if caps & CLIENT_PLUGIN_AUTH != 0 {
        read_cstr(p, &mut i).unwrap_or_default()
    } else {
        String::new()
    };
    Ok((scramble, plugin, version))
}

pub fn read_cstr(p: &[u8], i: &mut usize) -> std::io::Result<String> {
    let start = *i;
    while p.get(*i).is_some_and(|&b| b != 0) {
        *i += 1;
    }
    if *i >= p.len() {
        return Err(bad("unterminated string"));
    }
    let s = String::from_utf8_lossy(p.get_slice(start..*i)?).into_owned();
    *i += 1;
    Ok(s)
}

fn read_cstr_bytes(p: &[u8], i: &mut usize) -> std::io::Result<Vec<u8>> {
    let start = *i;
    while p.get(*i).is_some_and(|&b| b != 0) {
        *i += 1;
    }
    let v = p.get_slice(start..*i)?.to_vec();
    *i += 1;
    Ok(v)
}

fn read_n(p: &[u8], i: &mut usize, n: usize) -> std::io::Result<Vec<u8>> {
    let end = i.checked_add(n).ok_or_else(|| bad("eof"))?;
    let v = p.get(*i..end).ok_or_else(|| bad("eof"))?.to_vec();
    *i = end;
    Ok(v)
}

fn read_lenenc(p: &[u8], i: &mut usize) -> std::io::Result<u64> {
    let first = *p.get(*i).ok_or_else(|| bad("eof"))?;
    *i += 1;
    Ok(match first {
        n if n < 0xfb => n as u64,
        0xfc => {
            let v = u16::from_le_bytes(get_array(p, *i)?) as u64;
            *i += 2;
            v
        }
        0xfd => {
            let [b0, b1, b2] = get_array(p, *i)?;
            *i += 3;
            u32::from_le_bytes([b0, b1, b2, 0]) as u64
        }
        _ => {
            let b: [u8; 8] = get_array(p, *i)?;
            *i += 8;
            u64::from_le_bytes(b)
        }
    })
}

pub async fn read_packet<S: AsyncRead + Unpin>(stream: &mut S) -> std::io::Result<(u8, Vec<u8>)> {
    handshake_step(async {
        let mut hdr = [0; 4];
        stream.read_exact(&mut hdr).await?;
        let len = u32::from_le_bytes([hdr[0], hdr[1], hdr[2], 0]) as usize;
        if len > MAX_PACKET_LEN {
            return Err(bad("packet too large"));
        }

        let seq = hdr[3];
        let mut payload = vec![0; len];
        stream.read_exact(&mut payload).await?;

        Ok((seq, payload))
    })
    .await
}

pub async fn write_packet<S: AsyncWrite + Unpin>(
    stream: &mut S,
    seq: u8,
    payload: &[u8],
) -> std::io::Result<()> {
    let len = payload.len();
    stream
        .write_all(&[len as u8, (len >> 8) as u8, (len >> 16) as u8, seq])
        .await?;
    stream.write_all(payload).await
}

/// Rewrites COM_CHANGE_USER, and COM_MULTI which can nest one, to COM_SLEEP in the client's
/// command stream, so mariadbd answers ER_UNKNOWN_COM_ERROR in order instead of re-authenticating
/// over a socket that trusts root.
pub struct CommandFilter<S> {
    inner: S,
    header: [u8; 4],
    header_len: usize,
    remaining: usize,
    continued: bool,
    command: bool,
}

impl<S> CommandFilter<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            header: [0; 4],
            header_len: 0,
            remaining: 0,
            continued: false,
            command: false,
        }
    }

    fn filter(&mut self, buf: &mut [u8]) {
        let mut pos = 0;
        while pos < buf.len() {
            if self.remaining == 0 {
                let n = (self.header.len() - self.header_len).min(buf.len() - pos);
                if let (Some(dst), Some(src)) = (
                    self.header.get_mut(self.header_len..self.header_len + n),
                    buf.get(pos..pos + n),
                ) {
                    dst.copy_from_slice(src);
                }
                self.header_len += n;
                pos += n;

                if self.header_len == self.header.len() {
                    let [b0, b1, b2, _] = self.header;
                    let len = u32::from_le_bytes([b0, b1, b2, 0]) as usize;
                    self.header_len = 0;
                    self.remaining = len;
                    self.command = !self.continued && len > 0;
                    self.continued = len == MAX_PAYLOAD_LEN;
                }
                continue;
            }

            if std::mem::take(&mut self.command)
                && let Some(cmd) = buf.get_mut(pos)
                && matches!(*cmd, COM_CHANGE_USER | COM_MULTI)
            {
                *cmd = COM_SLEEP;
            }

            let n = self.remaining.min(buf.len() - pos);
            self.remaining -= n;
            pos += n;
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for CommandFilter<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let start = buf.filled().len();
        ready!(Pin::new(&mut this.inner).poll_read(cx, buf))?;
        if let Some(read) = buf.filled_mut().get_mut(start..) {
            this.filter(read);
        }

        Poll::Ready(Ok(()))
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for CommandFilter<S> {
    #[inline]
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    #[inline]
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write_vectored(cx, bufs)
    }

    #[inline]
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    #[inline]
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    #[inline]
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const COM_QUIT: u8 = 0x01;
    const COM_QUERY: u8 = 0x03;
    const COM_RESET_CONNECTION: u8 = 0x1f;

    fn packet(seq: u8, payload: &[u8]) -> Vec<u8> {
        let len = payload.len();
        let mut out = vec![len as u8, (len >> 8) as u8, (len >> 16) as u8, seq];
        out.extend_from_slice(payload);
        out
    }

    fn command(cmd: u8, len: usize) -> Vec<u8> {
        let mut out = vec![COM_CHANGE_USER; len];
        if let Some(first) = out.first_mut() {
            *first = cmd;
        }
        out
    }

    async fn filtered(input: &[u8], chunk: usize) -> Vec<u8> {
        let mut filter = CommandFilter::new(input);
        let mut buf = vec![0; chunk];
        let mut out = Vec::with_capacity(input.len());
        loop {
            let n = filter.read(&mut buf).await.expect("read through filter");
            if n == 0 {
                return out;
            }
            out.extend_from_slice(buf.get(..n).expect("n within buffer"));
        }
    }

    async fn assert_filtered(input: &[u8], expected: &[u8]) {
        for chunk in [input.len().max(1), 1, 3] {
            assert_eq!(filtered(input, chunk).await, expected, "chunk size {chunk}");
        }
    }

    // CommandFilter

    #[tokio::test]
    async fn change_user_becomes_sleep() {
        let input = packet(0, &command(COM_CHANGE_USER, 6));
        let expected = packet(0, &command(COM_SLEEP, 6));
        assert_filtered(&input, &expected).await;
    }

    #[tokio::test]
    async fn multi_becomes_sleep() {
        let input = packet(0, &command(COM_MULTI, 6));
        let expected = packet(0, &command(COM_SLEEP, 6));
        assert_filtered(&input, &expected).await;
    }

    #[tokio::test]
    async fn other_commands_and_headers_untouched() {
        let mut input = packet(COM_MULTI, &command(COM_QUERY, 0x11));
        input.extend(packet(COM_CHANGE_USER, &command(COM_QUIT, 0xfe)));
        input.extend(packet(0, &command(COM_RESET_CONNECTION, 1)));
        assert_filtered(&input, &input).await;
    }

    #[tokio::test]
    async fn pipelined_packets_filtered_independently() {
        let mut input = packet(0, &command(COM_QUERY, 5));
        input.extend(packet(0, &command(COM_CHANGE_USER, 7)));
        input.extend(packet(0, &command(COM_MULTI, 2)));
        input.extend(packet(0, &command(COM_QUIT, 1)));
        input.extend(packet(0, &command(COM_CHANGE_USER, 1)));

        let mut expected = packet(0, &command(COM_QUERY, 5));
        expected.extend(packet(0, &command(COM_SLEEP, 7)));
        expected.extend(packet(0, &command(COM_SLEEP, 2)));
        expected.extend(packet(0, &command(COM_QUIT, 1)));
        expected.extend(packet(0, &command(COM_SLEEP, 1)));

        assert_filtered(&input, &expected).await;
    }

    #[tokio::test]
    async fn continuation_payload_untouched_and_next_command_filtered() {
        let mut input = packet(0, &command(COM_QUERY, MAX_PAYLOAD_LEN));
        input.extend(packet(1, &command(COM_CHANGE_USER, 4)));
        input.extend(packet(0, &command(COM_CHANGE_USER, 4)));

        let mut expected = packet(0, &command(COM_QUERY, MAX_PAYLOAD_LEN));
        expected.extend(packet(1, &command(COM_CHANGE_USER, 4)));
        expected.extend(packet(0, &command(COM_SLEEP, 4)));

        assert_filtered(&input, &expected).await;
    }

    #[tokio::test]
    async fn continuation_ended_by_empty_packet() {
        let mut input = packet(0, &command(COM_MULTI, MAX_PAYLOAD_LEN));
        input.extend(packet(1, &[]));
        input.extend(packet(0, &command(COM_MULTI, 3)));

        let mut expected = packet(0, &command(COM_SLEEP, MAX_PAYLOAD_LEN));
        expected.extend(packet(1, &[]));
        expected.extend(packet(0, &command(COM_SLEEP, 3)));

        assert_filtered(&input, &expected).await;
    }

    #[tokio::test]
    async fn empty_command_then_change_user() {
        let mut input = packet(0, &[]);
        input.extend(packet(0, &command(COM_CHANGE_USER, 3)));

        let mut expected = packet(0, &[]);
        expected.extend(packet(0, &command(COM_SLEEP, 3)));

        assert_filtered(&input, &expected).await;
    }

    #[tokio::test]
    async fn truncated_trailing_header_passed_through() {
        let mut input = packet(0, &command(COM_QUERY, 3));
        input.extend([0x05, 0x00]);
        assert_filtered(&input, &input).await;
    }
}
