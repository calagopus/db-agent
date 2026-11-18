use crate::config::Tls;
use arc_swap::ArcSwap;
use axum_server::accept::Accept;
use ktls::{CompatibleCiphers, CorkStream, KtlsStream};
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
    sync::OnceCell,
};
use tokio_rustls::{
    TlsAcceptor,
    rustls::{
        ServerConfig,
        pki_types::{CertificateDer, PrivateKeyDer},
    },
    server::TlsStream,
};

pub const API_ALPN: &[&[u8]] = &[b"h2", b"http/1.1"];

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

static KTLS_CIPHERS: OnceCell<Option<Arc<CompatibleCiphers>>> = OnceCell::const_new();

fn load_config(
    cert: &str,
    key: &str,
    ktls: bool,
    alpn: &[&[u8]],
) -> anyhow::Result<Arc<ServerConfig>> {
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

    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;

    config.alpn_protocols = alpn.iter().map(|proto| proto.to_vec()).collect();
    config.enable_secret_extraction = ktls;

    Ok(Arc::new(config))
}

async fn ktls_ciphers() -> Option<Arc<CompatibleCiphers>> {
    KTLS_CIPHERS.get_or_init(detect_ktls_support).await.clone()
}

async fn detect_ktls_support() -> Option<Arc<CompatibleCiphers>> {
    let ciphers = match tokio::time::timeout(PROBE_TIMEOUT, CompatibleCiphers::new()).await {
        Ok(Ok(ciphers)) => ciphers,
        Ok(Err(err)) => {
            tracing::warn!("failed to probe kernel tls support: {:#?}", err);

            return None;
        }
        Err(_) => {
            tracing::warn!("timed out probing kernel tls support");

            return None;
        }
    };

    let supported = [
        ("TLS1.2 AES-GCM-128", ciphers.tls12.aes_gcm_128),
        ("TLS1.2 AES-GCM-256", ciphers.tls12.aes_gcm_256),
        ("TLS1.2 CHACHA20-POLY1305", ciphers.tls12.chacha20_poly1305),
        ("TLS1.3 AES-GCM-128", ciphers.tls13.aes_gcm_128),
        ("TLS1.3 AES-GCM-256", ciphers.tls13.aes_gcm_256),
        ("TLS1.3 CHACHA20-POLY1305", ciphers.tls13.chacha20_poly1305),
    ];

    let names: Vec<&str> = supported
        .iter()
        .filter(|(_, ok)| *ok)
        .map(|(name, _)| *name)
        .collect();

    if names.is_empty() {
        tracing::warn!("kernel tls is not supported by this kernel, using userspace tls");

        return None;
    }

    tracing::info!("kernel tls enabled (ciphers: {})", names.join(", "));

    Some(Arc::new(ciphers))
}

fn timed_out(_: tokio::time::error::Elapsed) -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "tls handshake timed out")
}

#[derive(Clone)]
pub struct ReloadableAcceptor {
    config: Arc<ArcSwap<ServerConfig>>,
    ciphers: Option<Arc<CompatibleCiphers>>,
    alpn: &'static [&'static [u8]],
}

impl ReloadableAcceptor {
    pub async fn accept(&self, tcp: TcpStream) -> io::Result<MaybeKtlsStream> {
        let acceptor = TlsAcceptor::from(self.config.load_full());

        let Some(ciphers) = self.ciphers.clone() else {
            return tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(tcp))
                .await
                .map_err(timed_out)?
                .map(|stream| MaybeKtlsStream::Rustls(Box::new(stream)));
        };

        tokio::time::timeout(HANDSHAKE_TIMEOUT, async move {
            let stream = acceptor.accept(CorkStream::new(tcp)).await?;

            let suite = stream.get_ref().1.negotiated_cipher_suite();
            if !suite.is_some_and(|suite| ciphers.is_compatible(suite)) {
                tracing::debug!(
                    "negotiated cipher suite ({:?}) is not kernel tls compatible, using userspace tls",
                    suite.map(|suite| suite.suite())
                );

                return Ok(MaybeKtlsStream::Corked(Box::new(stream)));
            }

            let (drained, tcp) = ktls::config_ktls_server(stream)
                .await
                .map_err(|err| {
                    io::Error::other(format!("failed to hand connection off to kernel tls: {err}"))
                })?
                .into_raw();

            Ok(MaybeKtlsStream::Ktls {
                stream: KtlsStream::new(tcp, None),
                drained: drained.filter(|d| !d.is_empty()).map(|d| (0, d)),
            })
        })
        .await
        .map_err(timed_out)?
    }

    pub fn mode(&self) -> &'static str {
        if self.ciphers.is_some() {
            "on (kernel)"
        } else {
            "on"
        }
    }

    fn reload(&self, cert: &str, key: &str) -> anyhow::Result<()> {
        self.config
            .store(load_config(cert, key, self.ciphers.is_some(), self.alpn)?);
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

impl<S: Send + 'static> Accept<TcpStream, S> for ReloadableAcceptor {
    type Stream = MaybeKtlsStream;
    type Service = S;
    type Future = Pin<Box<dyn Future<Output = io::Result<(Self::Stream, Self::Service)>> + Send>>;

    fn accept(&self, tcp: TcpStream, service: S) -> Self::Future {
        let acceptor = self.clone();

        Box::pin(async move {
            let stream = ReloadableAcceptor::accept(&acceptor, tcp)
                .await
                .inspect_err(|err| {
                    tracing::debug!("failed to accept https connection: {:#?}", err)
                })?;

            Ok((stream, service))
        })
    }
}

pub async fn build_acceptor(
    tls: &Tls,
    alpn: &'static [&'static [u8]],
) -> anyhow::Result<ReloadableAcceptor> {
    let ciphers = if tls.ktls_enabled {
        ktls_ciphers().await
    } else {
        None
    };

    Ok(ReloadableAcceptor {
        config: Arc::new(ArcSwap::from(load_config(
            &tls.cert,
            &tls.key,
            ciphers.is_some(),
            alpn,
        )?)),
        ciphers,
        alpn,
    })
}

pub enum MaybeKtlsStream {
    Ktls {
        stream: KtlsStream<TcpStream>,
        drained: Option<(usize, Vec<u8>)>,
    },
    Rustls(Box<TlsStream<TcpStream>>),
    Corked(Box<TlsStream<CorkStream<TcpStream>>>),
}

impl AsyncRead for MaybeKtlsStream {
    #[inline]
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Ktls { stream, drained } => {
                if let Some((offset, bytes)) = drained
                    && buf.remaining() > 0
                {
                    let rest = bytes.get(*offset..).unwrap_or_default();
                    let len = rest.len().min(buf.remaining());
                    buf.put_slice(rest.get(..len).unwrap_or_default());
                    *offset += len;
                    if *offset >= bytes.len() {
                        *drained = None;
                    }
                    return Poll::Ready(Ok(()));
                }

                Pin::new(stream).poll_read(cx, buf)
            }
            Self::Rustls(stream) => Pin::new(&mut **stream).poll_read(cx, buf),
            Self::Corked(stream) => Pin::new(&mut **stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeKtlsStream {
    #[inline]
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Ktls { stream, .. } => Pin::new(stream).poll_write(cx, buf),
            Self::Rustls(stream) => Pin::new(&mut **stream).poll_write(cx, buf),
            Self::Corked(stream) => Pin::new(&mut **stream).poll_write(cx, buf),
        }
    }

    #[inline]
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Ktls { stream, .. } => Pin::new(stream.get_mut()).poll_write_vectored(cx, bufs),
            Self::Rustls(stream) => Pin::new(&mut **stream).poll_write_vectored(cx, bufs),
            Self::Corked(stream) => Pin::new(&mut **stream).poll_write_vectored(cx, bufs),
        }
    }

    #[inline]
    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Ktls { stream, .. } => stream.get_ref().is_write_vectored(),
            Self::Rustls(stream) => stream.is_write_vectored(),
            Self::Corked(stream) => stream.is_write_vectored(),
        }
    }

    #[inline]
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Ktls { stream, .. } => Pin::new(stream).poll_flush(cx),
            Self::Rustls(stream) => Pin::new(&mut **stream).poll_flush(cx),
            Self::Corked(stream) => Pin::new(&mut **stream).poll_flush(cx),
        }
    }

    #[inline]
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Ktls { stream, .. } => Pin::new(stream).poll_shutdown(cx),
            Self::Rustls(stream) => Pin::new(&mut **stream).poll_shutdown(cx),
            Self::Corked(stream) => Pin::new(&mut **stream).poll_shutdown(cx),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    const DRAINED: &[u8] = b"0123456789abcde";
    const SOCKET: &[u8] = b"from-the-socket";

    async fn ktls_with_drained() -> MaybeKtlsStream {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let mut peer = TcpStream::connect(addr).await.expect("connect");
        let (tcp, _) = listener.accept().await.expect("accept");

        peer.write_all(SOCKET).await.expect("peer write");
        peer.shutdown().await.expect("peer shutdown");

        MaybeKtlsStream::Ktls {
            stream: KtlsStream::new(tcp, None),
            drained: Some((0, DRAINED.to_vec())),
        }
    }

    fn expected() -> Vec<u8> {
        [DRAINED, SOCKET].concat()
    }

    // MaybeKtlsStream

    #[tokio::test]
    async fn ktls_serves_drained_bytes_first_when_read_bytewise() {
        let mut stream = ktls_with_drained().await;
        let mut out = Vec::new();
        for _ in 0..expected().len() {
            out.push(stream.read_u8().await.expect("read byte"));
        }
        assert_eq!(out, expected());
        assert_eq!(
            stream.read_u8().await.err().map(|e| e.kind()),
            Some(io::ErrorKind::UnexpectedEof)
        );
    }

    #[tokio::test]
    async fn ktls_serves_drained_bytes_first_with_large_read() {
        let mut stream = ktls_with_drained().await;
        let mut out = vec![0; expected().len()];
        stream.read_exact(&mut out).await.expect("read_exact");
        assert_eq!(out, expected());
    }
}
