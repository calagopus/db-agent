use super::protocol::{binary, op_msg_doc, read_message, write_op_msg_request};
use crate::utils::bad;
use base64::{Engine, engine::general_purpose::STANDARD as B64};
use bson::doc;
use hmac::{Hmac, KeyInit, Mac};
use rand::Rng;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tokio::net::UnixStream;

const SCRAM_ITERATIONS: u32 = 15000;

pub struct Scram {
    bare: String,
    server_first: String,
    salt: [u8; 16],
    pub password: String,
    pub socket: PathBuf,
    pub user: String,
    pub db: String,
}

impl Scram {
    pub fn start(
        password: &str,
        socket: &Path,
        bare: String,
        cnonce: &str,
        user: String,
        db: String,
    ) -> (Self, String) {
        let salt = random_bytes::<16>();
        let sn = random_bytes::<18>();
        let combined = format!("{cnonce}{}", B64.encode(sn));
        let server_first = format!("r={combined},s={},i={SCRAM_ITERATIONS}", B64.encode(salt));
        let scram = Scram {
            bare,
            server_first: server_first.clone(),
            salt,
            password: password.to_string(),
            socket: socket.to_path_buf(),
            user,
            db,
        };
        (scram, server_first)
    }

    pub fn verify(&self, client_final: &str) -> Option<String> {
        let proof_b64 = field(client_final, "p=")?;
        let without = client_final.rsplit_once(",p=").map(|(l, _)| l)?;
        let salted = pbkdf2_sha256(self.password.as_bytes(), &self.salt, SCRAM_ITERATIONS);
        let client_key = hmac(&salted, b"Client Key");
        let stored = sha256(&client_key);
        let auth_message = format!("{},{},{}", self.bare, self.server_first, without);
        let client_sig = hmac(&stored, auth_message.as_bytes());
        let expected: Vec<u8> = client_key
            .iter()
            .zip(client_sig)
            .map(|(a, b)| a ^ b)
            .collect();
        let given = B64.decode(proof_b64).ok()?;
        if !constant_time_eq::constant_time_eq(&expected, &given) {
            return None;
        }
        let server_key = hmac(&salted, b"Server Key");
        let server_sig = hmac(&server_key, auth_message.as_bytes());
        Some(format!("v={}", B64.encode(server_sig)))
    }
}

pub fn parse_client_first(cf: &str) -> Option<(String, String, String)> {
    let mut commas = 0;
    let mut bare_start = None;
    for (i, c) in cf.char_indices() {
        if c == ',' {
            commas += 1;
            if commas == 2 {
                bare_start = Some(i + 1);
                break;
            }
        }
    }
    let bare = cf.get(bare_start?..)?.to_string();
    let user = unescape(&field(&bare, "n=")?);
    let cnonce = field(&bare, "r=")?;
    Some((bare, cnonce, user))
}

pub async fn backend_auth(
    socket: &Path,
    user: &str,
    password: &str,
    db: &str,
) -> std::io::Result<UnixStream> {
    let mut be = UnixStream::connect(socket).await?;

    let hello = doc! { "ismaster": 1, "$db": "admin" };
    write_op_msg_request(&mut be, 1, &hello).await?;
    let _ = read_message(&mut be).await?;

    let cnonce = B64.encode(random_bytes::<18>());
    let bare = format!("n={},r={cnonce}", escape(user));
    let client_first = format!("n,,{bare}");
    let start = doc! {
        "saslStart": 1,
        "mechanism": "SCRAM-SHA-256",
        "payload": binary(client_first.as_bytes()),
        "$db": db,
    };
    write_op_msg_request(&mut be, 2, &start).await?;

    let (_, _, body) = read_message(&mut be).await?;
    let reply = op_msg_doc(&body).ok_or_else(|| bad("bad backend reply"))?;
    let conversation_id = reply.get_i32("conversationId").unwrap_or(1);
    let server_first = String::from_utf8(
        reply
            .get_binary_generic("payload")
            .map_err(|_| bad("no server-first"))?
            .clone(),
    )
    .map_err(|_| bad("non-utf8 server-first"))?;

    let combined = field(&server_first, "r=").ok_or_else(|| bad("no nonce"))?;
    let salt = B64
        .decode(field(&server_first, "s=").ok_or_else(|| bad("no salt"))?)
        .map_err(|_| bad("bad salt"))?;
    let iters: u32 = field(&server_first, "i=")
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| bad("bad iters"))?;

    let salted = pbkdf2_sha256(password.as_bytes(), &salt, iters);
    let client_key = hmac(&salted, b"Client Key");
    let stored = sha256(&client_key);
    let without = format!("c=biws,r={combined}");
    let auth_message = format!("{bare},{server_first},{without}");
    let client_sig = hmac(&stored, auth_message.as_bytes());
    let proof: Vec<u8> = client_key
        .iter()
        .zip(client_sig)
        .map(|(a, b)| a ^ b)
        .collect();
    let client_final = format!("{without},p={}", B64.encode(&proof));

    let cont = doc! {
        "saslContinue": 1,
        "conversationId": conversation_id,
        "payload": binary(client_final.as_bytes()),
        "$db": db,
    };
    write_op_msg_request(&mut be, 3, &cont).await?;

    let (_, _, body) = read_message(&mut be).await?;
    let reply = op_msg_doc(&body).ok_or_else(|| bad("bad backend reply"))?;
    if reply.get_f64("ok").ok() != Some(1.0) {
        return Err(bad("backend rejected auth"));
    }
    Ok(be)
}

fn field(s: &str, prefix: &str) -> Option<String> {
    s.split(',')
        .find(|p| p.starts_with(prefix))
        .and_then(|p| p.get(prefix.len()..))
        .map(str::to_string)
}

fn escape(s: &str) -> String {
    s.replace('=', "=3D").replace(',', "=2C")
}
fn unescape(s: &str) -> String {
    s.replace("=2C", ",").replace("=3D", "=")
}

fn random_bytes<const N: usize>() -> [u8; N] {
    let mut b = [0; N];
    rand::rng().fill_bytes(&mut b);
    b
}

fn hmac(key: &[u8], msg: &[u8]) -> [u8; 32] {
    let mut m = Hmac::<Sha256>::new_from_slice(key).expect("hmac key");
    m.update(msg);
    m.finalize().into_bytes().into()
}
fn sha256(d: &[u8]) -> [u8; 32] {
    Sha256::digest(d).into()
}
fn pbkdf2_sha256(password: &[u8], salt: &[u8], iters: u32) -> [u8; 32] {
    let mut m = Hmac::<Sha256>::new_from_slice(password).expect("pbkdf2 key");
    m.update(salt);
    m.update(&1u32.to_be_bytes());
    let mut u: [u8; 32] = m.finalize().into_bytes().into();
    let mut out = u;
    for _ in 1..iters {
        u = hmac(password, &u);
        for (o, x) in out.iter_mut().zip(u) {
            *o ^= x;
        }
    }
    out
}
