use std::{io::Read, path::PathBuf};

pub static RELAY_BIN: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/relay.gz"));
pub const RELAY_CONTAINER_PATH: &str = "/usr/local/libexec/calagopus-relay";

pub async fn get_relay_bin_path(
    config: &crate::config::Config,
) -> Result<Option<PathBuf>, std::io::Error> {
    static EXTRACTED: tokio::sync::Mutex<Option<PathBuf>> = tokio::sync::Mutex::const_new(None);

    if RELAY_BIN.is_empty() {
        return Ok(None);
    }

    let bin_dir = PathBuf::from(&config.load().data_dir).join(".bin");
    let bin_path = bin_dir.join("relay");

    let mut extracted = EXTRACTED.lock().await;
    if extracted.as_ref() == Some(&bin_path) {
        return Ok(Some(bin_path));
    }

    let decompressed = tokio::task::spawn_blocking(|| {
        let mut out = Vec::new();
        flate2::read::GzDecoder::new(RELAY_BIN).read_to_end(&mut out)?;
        Ok::<_, std::io::Error>(out)
    })
    .await??;

    if tokio::fs::read(&bin_path).await.ok().as_ref() != Some(&decompressed) {
        tokio::fs::create_dir_all(&bin_dir).await?;
        let tmp_path = bin_dir.join("relay.tmp");
        tokio::fs::write(&tmp_path, &decompressed).await?;

        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(&tmp_path, std::fs::Permissions::from_mode(0o755)).await?;
        tokio::fs::rename(&tmp_path, &bin_path).await?;
    }

    *extracted = Some(bin_path.clone());
    Ok(Some(bin_path))
}
