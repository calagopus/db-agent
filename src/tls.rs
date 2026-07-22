use arc_swap::ArcSwap;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::{
    Accept, TlsAcceptor,
    rustls::{
        ServerConfig,
        pki_types::{CertificateDer, PrivateKeyDer},
    },
};

fn load_config(cert: &str, key: &str) -> anyhow::Result<Arc<ServerConfig>> {
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

    Ok(Arc::new(
        ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)?,
    ))
}

#[derive(Clone)]
pub struct ReloadableAcceptor {
    config: Arc<ArcSwap<ServerConfig>>,
}

impl ReloadableAcceptor {
    pub fn accept<IO>(&self, stream: IO) -> Accept<IO>
    where
        IO: AsyncRead + AsyncWrite + Unpin,
    {
        TlsAcceptor::from(self.config.load_full()).accept(stream)
    }

    fn reload(&self, cert: &str, key: &str) -> anyhow::Result<()> {
        self.config.store(load_config(cert, key)?);
        Ok(())
    }

    pub fn spawn_reloader<F: Fn() -> (String, String) + Send + 'static>(
        &self,
        name: &'static str,
        paths: F,
    ) {
        let acceptor = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_hours(24)).await;
                tracing::info!("reloading {name} tls certs");

                let (cert, key) = paths();
                match acceptor.reload(&cert, &key) {
                    Ok(()) => tracing::info!("{name} tls certs reloaded successfully"),
                    Err(err) => tracing::error!("failed to reload {name} tls certs: {err:?}"),
                }
            }
        });
    }
}

pub fn build_acceptor(cert: &str, key: &str) -> anyhow::Result<Option<ReloadableAcceptor>> {
    Ok(Some(ReloadableAcceptor {
        config: Arc::new(ArcSwap::from(load_config(cert, key)?)),
    }))
}
