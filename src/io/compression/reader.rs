use super::CompressionType;
use std::{
    io::Read,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, ReadBuf};

pub enum CompressionReader<R: Read> {
    None(R),
    Gz(flate2::read::MultiGzDecoder<R>),
}

impl<R: Read> CompressionReader<R> {
    pub fn new(reader: R, compression_type: CompressionType) -> Self {
        match compression_type {
            CompressionType::None => CompressionReader::None(reader),
            CompressionType::Gz => CompressionReader::Gz(flate2::read::MultiGzDecoder::new(reader)),
        }
    }
}

impl<R: Read> Read for CompressionReader<R> {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            CompressionReader::None(reader) => reader.read(buf),
            CompressionReader::Gz(decoder) => decoder.read(buf),
        }
    }
}

pub struct AsyncCompressionReader {
    inner_error_receiver: tokio::sync::oneshot::Receiver<std::io::Error>,
    inner_reader: tokio::io::ReadHalf<tokio::io::SimplexStream>,
}

impl AsyncCompressionReader {
    pub fn new(reader: impl Read + Send + 'static, compression_type: CompressionType) -> Self {
        let (inner_reader, inner_writer) = tokio::io::simplex(crate::BUFFER_SIZE * 2);
        let (inner_error_sender, inner_error_receiver) = tokio::sync::oneshot::channel();

        tokio::task::spawn_blocking(move || {
            let mut writer = tokio_util::io::SyncIoBridge::new(inner_writer);
            let mut stream = CompressionReader::new(reader, compression_type);

            if let Err(err) = std::io::copy(&mut stream, &mut writer) {
                let _ = inner_error_sender.send(err);
                return;
            }

            if let Err(err) = writer.shutdown() {
                let _ = inner_error_sender.send(err);
            }
        });

        Self {
            inner_error_receiver,
            inner_reader,
        }
    }
}

impl AsyncRead for AsyncCompressionReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if !self.inner_error_receiver.is_terminated()
            && let Poll::Ready(result) = Pin::new(&mut self.inner_error_receiver).poll(cx)
            && let Ok(err) = result
        {
            return Poll::Ready(Err(err));
        }

        Pin::new(&mut self.inner_reader).poll_read(cx, buf)
    }
}
