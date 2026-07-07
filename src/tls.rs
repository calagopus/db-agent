use std::sync::Arc;
use tokio_rustls::{
    TlsAcceptor,
    rustls::{
        ServerConfig,
        pki_types::{CertificateDer, PrivateKeyDer},
    },
};

pub fn build_acceptor(cert: &str, key: &str) -> anyhow::Result<Option<TlsAcceptor>> {
    let (Ok(cert_pem), Ok(key_pem)) = (std::fs::read(cert), std::fs::read(key)) else {
        let err = (std::fs::metadata(cert).err(), std::fs::metadata(key).err());
        return Err(anyhow::anyhow!(
            "failed to read TLS cert/key (cert={cert}, key={key}): {:?}",
            err
        ));
    };

    let certs: Vec<CertificateDer> =
        rustls_pemfile::certs(&mut &cert_pem[..]).collect::<Result<_, _>>()?;
    let key: PrivateKeyDer = rustls_pemfile::private_key(&mut &key_pem[..])?
        .ok_or_else(|| anyhow::anyhow!("no private key in {key}"))?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;

    Ok(Some(TlsAcceptor::from(Arc::new(config))))
}
