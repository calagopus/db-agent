use crate::utils::bad;
use tokio::io::{AsyncRead, AsyncReadExt};

const MAX_ARGS: i64 = 1024;
const MAX_COMMAND_LEN: usize = 1024 * 1024;
const MAX_LINE_LEN: usize = 64 * 1024;

pub async fn read_command<S: AsyncRead + Unpin>(
    s: &mut S,
) -> std::io::Result<Option<(Vec<Vec<u8>>, Vec<u8>)>> {
    let mut raw = Vec::new();

    let first = match s.read_u8().await {
        Ok(b) => b,
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err),
    };
    raw.push(first);

    if first != b'*' {
        read_line(s, &mut raw).await?;
        return Ok(Some((Vec::new(), raw)));
    }

    let count = parse_int(&read_line(s, &mut raw).await?)?;
    if count < 0 {
        return Ok(Some((Vec::new(), raw)));
    }
    if count > MAX_ARGS {
        return Err(bad("multibulk count too large"));
    }

    let mut args = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let marker = s.read_u8().await?;
        raw.push(marker);
        if marker != b'$' {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "expected bulk string",
            ));
        }
        let len = parse_int(&read_line(s, &mut raw).await?)?;
        if len < 0 {
            args.push(Vec::new());
            continue;
        }
        if len as usize > MAX_COMMAND_LEN.saturating_sub(raw.len()) {
            return Err(bad("command too large"));
        }
        let mut buf = vec![0; len as usize];
        s.read_exact(&mut buf).await?;
        raw.extend_from_slice(&buf);

        raw.push(s.read_u8().await?);
        raw.push(s.read_u8().await?);
        args.push(buf);
    }

    Ok(Some((args, raw)))
}

async fn read_line<S: AsyncRead + Unpin>(s: &mut S, raw: &mut Vec<u8>) -> std::io::Result<Vec<u8>> {
    let mut line = Vec::new();
    loop {
        if line.len() > MAX_LINE_LEN {
            return Err(bad("protocol line too long"));
        }
        let b = s.read_u8().await?;
        raw.push(b);
        if b == b'\r' {
            let n = s.read_u8().await?;
            raw.push(n);
            if n == b'\n' {
                break;
            }
            line.push(b'\r');
            line.push(n);
        } else {
            line.push(b);
        }
    }
    Ok(line)
}

fn parse_int(bytes: &[u8]) -> std::io::Result<i64> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "bad integer"))
}

pub fn encode_command(args: &[Vec<u8>]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    out
}

pub fn extract_hello_user(args: &[Vec<u8>]) -> Option<String> {
    let pos = args
        .iter()
        .position(|a| a.as_slice().eq_ignore_ascii_case(b"AUTH"))?;
    args.get(pos + 1)
        .map(|u| String::from_utf8_lossy(u).into_owned())
}

pub fn extract_hello_password(args: &[Vec<u8>]) -> Option<Vec<u8>> {
    let pos = args
        .iter()
        .position(|a| a.as_slice().eq_ignore_ascii_case(b"AUTH"))?;
    args.get(pos + 2).cloned()
}

pub fn strip_hello_auth(args: &[Vec<u8>]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < args.len() {
        if args[i].as_slice().eq_ignore_ascii_case(b"AUTH") {
            i += 3; // skip AUTH + username + password
        } else {
            out.push(args[i].clone());
            i += 1;
        }
    }
    out
}
