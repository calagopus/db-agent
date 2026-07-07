use crate::utils::{SafeSliceExt, bad, get_array};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const CLIENT_LONG_PASSWORD: u32 = 0x0000_0001;
pub const CLIENT_LONG_FLAG: u32 = 0x0000_0004;
pub const CLIENT_CONNECT_WITH_DB: u32 = 0x0000_0008;
pub const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
pub const CLIENT_SSL: u32 = 0x0000_0800;
pub const CLIENT_TRANSACTIONS: u32 = 0x0000_2000;
pub const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
pub const CLIENT_PLUGIN_AUTH: u32 = 0x0008_0000;
pub const CLIENT_PLUGIN_AUTH_LENENC: u32 = 0x0020_0000;

pub const NATIVE: &str = "mysql_native_password";

const CAPS: u32 = CLIENT_LONG_PASSWORD
    | CLIENT_LONG_FLAG
    | CLIENT_PROTOCOL_41
    | CLIENT_SECURE_CONNECTION
    | CLIENT_PLUGIN_AUTH
    | CLIENT_CONNECT_WITH_DB
    | CLIENT_TRANSACTIONS;

pub fn server_handshake(scramble: &[u8; 20], ssl: bool) -> Vec<u8> {
    let mut caps = CAPS;
    if ssl {
        caps |= CLIENT_SSL;
    }
    let mut p = vec![10]; // protocol version
    p.extend_from_slice(b"8.0.30"); // a plain MySQL version: standard capability negotiation
    p.push(0);
    p.extend_from_slice(&1u32.to_le_bytes()); // connection id
    p.extend_from_slice(&scramble[..8]); // auth-plugin-data-1
    p.push(0); // filler
    p.extend_from_slice(&(caps as u16).to_le_bytes()); // capabilities lower
    p.push(45); // charset utf8mb4_general_ci
    p.extend_from_slice(&0x0002u16.to_le_bytes()); // status: autocommit
    p.extend_from_slice(&((caps >> 16) as u16).to_le_bytes()); // capabilities upper
    p.push(21); // length of auth-plugin-data (20 + null)
    p.extend_from_slice(&[0; 10]); // reserved
    p.extend_from_slice(&scramble[8..20]); // auth-plugin-data-2 (12 bytes)
    p.push(0); // null terminator of part 2
    p.extend_from_slice(NATIVE.as_bytes());
    p.push(0);
    p
}

pub fn handshake_response(user: &str, token: &[u8], database: &str) -> Vec<u8> {
    let mut p = CAPS.to_le_bytes().to_vec();
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
        user,
        auth_response,
        database,
        plugin,
    })
}

pub fn parse_server_handshake(p: &[u8]) -> std::io::Result<([u8; 20], String)> {
    let mut i = 1; // skip protocol version
    let _ver = read_cstr(p, &mut i)?;
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
    Ok((scramble, plugin))
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
    let v = p.get(*i..*i + n).ok_or_else(|| bad("eof"))?.to_vec();
    *i += n;
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

pub async fn read_packet<S: AsyncRead + Unpin>(s: &mut S) -> std::io::Result<(u8, Vec<u8>)> {
    let mut hdr = [0; 4];
    s.read_exact(&mut hdr).await?;
    let len = u32::from_le_bytes([hdr[0], hdr[1], hdr[2], 0]) as usize;
    let seq = hdr[3];
    let mut payload = vec![0; len];
    s.read_exact(&mut payload).await?;

    Ok((seq, payload))
}

pub async fn write_packet<S: AsyncWrite + Unpin>(
    s: &mut S,
    seq: u8,
    payload: &[u8],
) -> std::io::Result<()> {
    let len = payload.len();
    s.write_all(&[len as u8, (len >> 8) as u8, (len >> 16) as u8, seq])
        .await?;
    s.write_all(payload).await
}
