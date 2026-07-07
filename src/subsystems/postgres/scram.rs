use super::protocol::{read_msg, write_msg};
use crate::utils::bad;
use base64::{Engine, engine::general_purpose::STANDARD as B64};
use hmac::{Hmac, KeyInit, Mac};
use postgres_protocol::{
    authentication::sasl::{ChannelBinding, ScramSha256},
    message::frontend::{password_message, sasl_initial_response, sasl_response},
};
use rand::Rng;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

const SCRAM_ITERATIONS: u32 = 4096;

pub async fn authenticate_client<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    password: &str,
) -> std::io::Result<bool> {
    let mut advert = 10i32.to_be_bytes().to_vec();
    advert.extend_from_slice(b"SCRAM-SHA-256\0\0");
    write_msg(stream, b'R', &advert).await?;

    let (tag, body) = read_msg(stream).await?;
    if tag != b'p' {
        return Err(bad("expected SASLInitialResponse"));
    }

    let mech_end = body
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| bad("no mechanism"))?;
    let client_first = String::from_utf8_lossy(&body[mech_end + 5..]).into_owned();
    let client_first_bare = bare_after_gs2(&client_first);
    let client_nonce = field(&client_first_bare, "r=").ok_or_else(|| bad("no client nonce"))?;

    let server_nonce = B64.encode(random_bytes::<18>());
    let combined = format!("{client_nonce}{server_nonce}");
    let salt = random_bytes::<16>();
    let server_first = format!("r={combined},s={},i={SCRAM_ITERATIONS}", B64.encode(salt));

    let mut cont = 11i32.to_be_bytes().to_vec();
    cont.extend_from_slice(server_first.as_bytes());
    write_msg(stream, b'R', &cont).await?;

    let (tag, body) = read_msg(stream).await?;
    if tag != b'p' {
        return Err(bad("expected SASLResponse"));
    }
    let client_final = String::from_utf8_lossy(&body).into_owned();
    let Some(proof_b64) = field(&client_final, "p=") else {
        return Err(bad("no client proof"));
    };
    let Some(without_proof) = client_final.rsplit_once(",p=").map(|(l, _)| l) else {
        return Err(bad("malformed client-final"));
    };

    let salted = pbkdf2_sha256(password.as_bytes(), &salt, SCRAM_ITERATIONS);
    let client_key = hmac(&salted, b"Client Key");
    let stored_key = sha256(&client_key);
    let auth_message = format!("{client_first_bare},{server_first},{without_proof}");
    let client_sig = hmac(&stored_key, auth_message.as_bytes());
    let expected: Vec<u8> = client_key
        .iter()
        .zip(client_sig)
        .map(|(a, b)| a ^ b)
        .collect();
    let Ok(given) = B64.decode(proof_b64) else {
        return Err(bad("bad proof base64"));
    };
    if expected != given {
        return Ok(false);
    }

    let server_key = hmac(&salted, b"Server Key");
    let server_sig = hmac(&server_key, auth_message.as_bytes());
    let mut fin = 12i32.to_be_bytes().to_vec();
    fin.extend_from_slice(format!("v={}", B64.encode(server_sig)).as_bytes());
    write_msg(stream, b'R', &fin).await?;

    Ok(true)
}

fn bare_after_gs2(client_first: &str) -> String {
    let mut commas = 0;
    for (i, c) in client_first.char_indices() {
        if c == ',' {
            commas += 1;
            if commas == 2 {
                return client_first[i + 1..].to_string();
            }
        }
    }
    client_first.to_string()
}

fn field(s: &str, prefix: &str) -> Option<String> {
    s.split(',')
        .find(|p| p.starts_with(prefix))
        .map(|p| p[prefix.len()..].to_string())
}

fn random_bytes<const N: usize>() -> [u8; N] {
    let mut b = [0; N];
    rand::rng().fill_bytes(&mut b);
    b
}

fn hmac(key: &[u8], msg: &[u8]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("hmac key");
    mac.update(msg);
    mac.finalize().into_bytes().into()
}

fn sha256(data: &[u8]) -> [u8; 32] {
    Sha256::digest(data).into()
}

fn pbkdf2_sha256(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(password).expect("pbkdf2 key");
    mac.update(salt);
    mac.update(&1u32.to_be_bytes());
    let mut u: [u8; 32] = mac.finalize().into_bytes().into();
    let mut result = u;
    for _ in 1..iterations {
        u = hmac(password, &u);
        for (r, x) in result.iter_mut().zip(u) {
            *r ^= x;
        }
    }
    result
}

pub async fn authenticate_backend<
    B: AsyncRead + AsyncWrite + Unpin,
    S: AsyncRead + AsyncWrite + Unpin,
>(
    backend: &mut B,
    client: &mut S,
    password: &str,
) -> std::io::Result<()> {
    let mut scram: Option<ScramSha256> = None;
    loop {
        let (tag, body) = read_msg(backend).await?;
        match tag {
            b'R' => match i32::from_be_bytes([body[0], body[1], body[2], body[3]]) {
                0 => {}
                10 => {
                    let s = ScramSha256::new(password.as_bytes(), ChannelBinding::unsupported());
                    let mut buf = bytes::BytesMut::new();
                    sasl_initial_response("SCRAM-SHA-256", s.message(), &mut buf)?;
                    backend.write_all(&buf).await?;
                    scram = Some(s);
                }
                11 => {
                    let s = scram
                        .as_mut()
                        .ok_or_else(|| bad("unexpected SASLContinue"))?;
                    s.update(&body[4..])?;
                    let mut buf = bytes::BytesMut::new();
                    sasl_response(s.message(), &mut buf)?;
                    backend.write_all(&buf).await?;
                }
                12 => scram
                    .as_mut()
                    .ok_or_else(|| bad("unexpected SASLFinal"))?
                    .finish(&body[4..])?,
                3 => {
                    let mut buf = bytes::BytesMut::new();
                    password_message(password.as_bytes(), &mut buf)?;
                    backend.write_all(&buf).await?;
                }
                5 => return Err(bad("backend asked for md5; use scram-sha-256")),
                other => return Err(bad(&format!("unsupported backend auth {other}"))),
            },
            b'E' => {
                write_msg(client, b'E', &body).await?;
                return Err(bad("backend rejected startup"));
            }
            other => {
                write_msg(client, other, &body).await?;
                if other == b'Z' {
                    return Ok(());
                }
            }
        }
    }
}
