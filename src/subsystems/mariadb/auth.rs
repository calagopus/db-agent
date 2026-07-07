use sha1::{Digest, Sha1};

fn sha1d(data: &[u8]) -> [u8; 20] {
    Sha1::digest(data).into()
}

pub fn native_token(scramble: &[u8], password: &[u8]) -> Vec<u8> {
    if password.is_empty() {
        return Vec::new();
    }

    let stage1 = sha1d(password);
    let stage2 = sha1d(&stage1);
    let mut h = Sha1::new();
    h.update(scramble);
    h.update(stage2);
    let m: [u8; 20] = h.finalize().into();
    stage1.iter().zip(m).map(|(a, b)| a ^ b).collect()
}
