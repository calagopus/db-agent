use crate::utils::{bad, handshake_step};
use tokio::io::{AsyncRead, AsyncReadExt};

const MAX_HELLO_STRING_LEN: u64 = 64 * 1024;

pub const AUTHENTICATION_FAILED: i32 = 516;
pub const DATABASE_ACCESS_DENIED: i32 = 291;
pub const READONLY: i32 = 164;
pub const NETWORK_ERROR: i32 = 210;

pub struct ClientHello {
    pub database: String,
    pub user: String,
    pub password: Vec<u8>,
    pub raw: Vec<u8>,
}

pub async fn read_hello<S: AsyncRead + Unpin>(s: &mut S) -> std::io::Result<ClientHello> {
    handshake_step(async {
        let mut raw = Vec::new();
        if read_varuint(s, &mut raw).await? != 0 {
            return Err(bad("expected client hello"));
        }
        read_string(s, &mut raw).await?;
        for _ in 0..3 {
            read_varuint(s, &mut raw).await?;
        }
        let database = String::from_utf8_lossy(&read_string(s, &mut raw).await?).into_owned();
        let user = String::from_utf8_lossy(&read_string(s, &mut raw).await?).into_owned();
        let password = read_string(s, &mut raw).await?;

        Ok(ClientHello {
            database,
            user,
            password,
            raw,
        })
    })
    .await
}

async fn read_varuint<S: AsyncRead + Unpin>(s: &mut S, raw: &mut Vec<u8>) -> std::io::Result<u64> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let b = s.read_u8().await?;
        raw.push(b);
        value |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(bad("varuint too long"))
}

async fn read_string<S: AsyncRead + Unpin>(
    s: &mut S,
    raw: &mut Vec<u8>,
) -> std::io::Result<Vec<u8>> {
    let len = read_varuint(s, raw).await?;
    if len > MAX_HELLO_STRING_LEN {
        return Err(bad("hello string too long"));
    }
    let mut buf = vec![0; len as usize];
    s.read_exact(&mut buf).await?;
    raw.extend_from_slice(&buf);
    Ok(buf)
}

fn write_varuint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn write_string(out: &mut Vec<u8>, s: &str) {
    write_varuint(out, s.len() as u64);
    out.extend_from_slice(s.as_bytes());
}

pub fn exception(code: i32, message: &str) -> Vec<u8> {
    let mut out = Vec::new();
    write_varuint(&mut out, 2);
    out.extend_from_slice(&code.to_le_bytes());
    write_string(&mut out, "DB::Exception");
    write_string(&mut out, message);
    write_string(&mut out, "");
    out.push(0);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::ErrorKind;

    fn varuint(out: &mut Vec<u8>, mut value: u64) {
        loop {
            let byte = (value & 0x7f) as u8;
            value >>= 7;
            if value == 0 {
                out.push(byte);
                return;
            }
            out.push(byte | 0x80);
        }
    }

    fn string(out: &mut Vec<u8>, s: &[u8]) {
        varuint(out, s.len() as u64);
        out.extend_from_slice(s);
    }

    fn hello(database: &[u8], user: &[u8], password: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        varuint(&mut out, 0);
        string(&mut out, b"clickhouse-client");
        varuint(&mut out, 24);
        varuint(&mut out, 8);
        varuint(&mut out, 54478);
        string(&mut out, database);
        string(&mut out, user);
        string(&mut out, password);
        out
    }

    // read_hello

    #[tokio::test]
    async fn read_hello_decodes_fields_and_leaves_trailing_bytes() {
        let database = "d".repeat(200);
        let encoded = hello(database.as_bytes(), b"alice", b"p\xffss");
        let mut input = encoded.clone();
        input.extend_from_slice(b"\x01\x02\x03");

        let mut reader = input.as_slice();
        let parsed = read_hello(&mut reader).await.expect("valid hello");

        assert_eq!(parsed.database, database);
        assert_eq!(parsed.user, "alice");
        assert_eq!(parsed.password, b"p\xffss");
        assert_eq!(parsed.raw, encoded);
        assert_eq!(reader, b"\x01\x02\x03");
    }

    #[tokio::test]
    async fn read_hello_accepts_string_at_length_limit() {
        let password = vec![b'x'; 65536];
        let mut reader = &*hello(b"db", b"u", &password);
        let parsed = read_hello(&mut reader).await.expect("limit is inclusive");
        assert_eq!(parsed.password, password);
    }

    #[tokio::test]
    async fn read_hello_rejects_oversized_string_length_without_reading_it() {
        let mut input = Vec::new();
        varuint(&mut input, 0);
        varuint(&mut input, 65537);

        let err = read_hello(&mut input.as_slice()).await.err();
        assert_eq!(err.map(|e| e.kind()), Some(ErrorKind::InvalidData));
    }

    #[tokio::test]
    async fn read_hello_rejects_non_hello_packet() {
        let mut input = hello(b"db", b"u", b"p");
        if let Some(first) = input.first_mut() {
            *first = 1;
        }

        let err = read_hello(&mut input.as_slice()).await.err();
        assert_eq!(err.map(|e| e.kind()), Some(ErrorKind::InvalidData));
    }

    // exception

    #[test]
    fn exception_layout() {
        let mut expected = vec![0x02];
        expected.extend_from_slice(&AUTHENTICATION_FAILED.to_le_bytes());
        string(&mut expected, b"DB::Exception");
        string(&mut expected, b"bad password");
        string(&mut expected, b"");
        expected.push(0x00);

        assert_eq!(exception(AUTHENTICATION_FAILED, "bad password"), expected);
    }
}
