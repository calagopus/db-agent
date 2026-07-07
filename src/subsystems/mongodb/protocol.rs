use crate::utils::bad;
use bson::{Bson, Document, doc, spec::BinarySubtype};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const MAX_MSG: usize = 48 * 1000 * 1000;
pub const OP_REPLY: i32 = 1;
pub const OP_QUERY: i32 = 2004;
pub const OP_MSG: i32 = 2013;

pub fn binary(bytes: impl Into<Vec<u8>>) -> Bson {
    Bson::Binary(bson::Binary {
        subtype: BinarySubtype::Generic,
        bytes: bytes.into(),
    })
}

pub fn hello_doc() -> Document {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);

    doc! {
        "ismaster": true,
        "isWritablePrimary": true,
        "maxBsonObjectSize": 16 * 1024 * 1024,
        "maxMessageSizeBytes": MAX_MSG as i32,
        "maxWriteBatchSize": 100000,
        "localTime": bson::DateTime::from_millis(now),
        "logicalSessionTimeoutMinutes": 30,
        "connectionId": 1,
        "minWireVersion": 0,
        "maxWireVersion": 9,
        "readOnly": false,
        "saslSupportedMechs": ["SCRAM-SHA-256"],
        "ok": 1.0,
    }
}

pub fn sasl_error(msg: &str) -> Document {
    doc! {
        "ok": 0.0,
        "errmsg": msg,
        "code": 18,
        "codeName": "AuthenticationFailed",
    }
}

pub async fn read_message<S: AsyncRead + AsyncWrite + Unpin>(
    s: &mut S,
) -> std::io::Result<(i32, i32, Vec<u8>)> {
    let mut hdr = [0; 16];
    s.read_exact(&mut hdr).await?;
    let mlen = i32::from_le_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
    let reqid = i32::from_le_bytes([hdr[4], hdr[5], hdr[6], hdr[7]]);
    let opcode = i32::from_le_bytes([hdr[12], hdr[13], hdr[14], hdr[15]]);
    if !(16..=MAX_MSG).contains(&mlen) {
        return Err(bad("implausible message length"));
    }
    let mut body = vec![0; mlen - 16];
    s.read_exact(&mut body).await?;
    Ok((reqid, opcode, body))
}

pub fn op_msg_doc(body: &[u8]) -> Option<Document> {
    if *body.get(4)? != 0 {
        return None;
    }

    Document::from_reader(body.get(5..)?).ok()
}

pub async fn write_op_msg<S: AsyncRead + AsyncWrite + Unpin>(
    s: &mut S,
    response_to: i32,
    doc: &Document,
) -> std::io::Result<()> {
    let body = encode_op_msg(doc)?;
    write_header(s, 0, response_to, OP_MSG, &body).await
}

pub async fn write_op_msg_request<S: AsyncRead + AsyncWrite + Unpin>(
    s: &mut S,
    request_id: i32,
    doc: &Document,
) -> std::io::Result<()> {
    let body = encode_op_msg(doc)?;
    write_header(s, request_id, 0, OP_MSG, &body).await
}

fn encode_op_msg(doc: &Document) -> std::io::Result<Vec<u8>> {
    let mut body = Vec::new();
    body.extend_from_slice(&0u32.to_le_bytes()); // flagBits
    body.push(0x00); // section kind
    doc.to_writer(&mut body).map_err(|_| bad("bson encode"))?;
    Ok(body)
}

pub async fn write_op_reply<S: AsyncRead + AsyncWrite + Unpin>(
    s: &mut S,
    response_to: i32,
    doc: &Document,
) -> std::io::Result<()> {
    let mut body = Vec::new();
    body.extend_from_slice(&0i32.to_le_bytes()); // responseFlags
    body.extend_from_slice(&0i64.to_le_bytes()); // cursorId
    body.extend_from_slice(&0i32.to_le_bytes()); // startingFrom
    body.extend_from_slice(&1i32.to_le_bytes()); // numberReturned
    doc.to_writer(&mut body).map_err(|_| bad("bson encode"))?;
    write_header(s, 0, response_to, OP_REPLY, &body).await
}

async fn write_header<S: AsyncRead + AsyncWrite + Unpin>(
    s: &mut S,
    request_id: i32,
    response_to: i32,
    opcode: i32,
    body: &[u8],
) -> std::io::Result<()> {
    let mlen = (16 + body.len()) as i32;
    let mut hdr = Vec::with_capacity(16);
    hdr.extend_from_slice(&mlen.to_le_bytes());
    hdr.extend_from_slice(&request_id.to_le_bytes());
    hdr.extend_from_slice(&response_to.to_le_bytes());
    hdr.extend_from_slice(&opcode.to_le_bytes());
    s.write_all(&hdr).await?;
    s.write_all(body).await
}
