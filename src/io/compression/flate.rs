use crate::io::SafeSliceExt;
use std::{
    io::Write,
    pin::Pin,
    task::{Context, Poll, ready},
};
use tokio::io::{AsyncRead, ReadBuf};

pub trait FlateCodec: Write {
    fn try_finish(&mut self) -> std::io::Result<()>;
    fn output(&mut self) -> &mut Vec<u8>;
}

impl FlateCodec for flate2::write::GzEncoder<Vec<u8>> {
    #[inline]
    fn try_finish(&mut self) -> std::io::Result<()> {
        flate2::write::GzEncoder::try_finish(self)
    }

    #[inline]
    fn output(&mut self) -> &mut Vec<u8> {
        self.get_mut()
    }
}

impl FlateCodec for flate2::write::MultiGzDecoder<Vec<u8>> {
    #[inline]
    fn try_finish(&mut self) -> std::io::Result<()> {
        flate2::write::MultiGzDecoder::try_finish(self)
    }

    #[inline]
    fn output(&mut self) -> &mut Vec<u8> {
        self.get_mut()
    }
}

pub struct AsyncFlateReader<R, C> {
    inner: R,
    codec: C,
    pos: usize,
    finished: bool,
    scratch: Box<[u8]>,
}

impl<R, C> AsyncFlateReader<R, C> {
    fn new(inner: R, codec: C) -> Self {
        Self {
            inner,
            codec,
            pos: 0,
            finished: false,
            scratch: vec![0; crate::BUFFER_SIZE].into_boxed_slice(),
        }
    }
}

impl<R> AsyncFlateReader<R, flate2::write::GzEncoder<Vec<u8>>> {
    pub fn gzip_encode(inner: R) -> Self {
        Self::new(
            inner,
            flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default()),
        )
    }
}

impl<R> AsyncFlateReader<R, flate2::write::MultiGzDecoder<Vec<u8>>> {
    pub fn gzip_decode(inner: R) -> Self {
        Self::new(inner, flate2::write::MultiGzDecoder::new(Vec::new()))
    }
}

impl<R: AsyncRead + Unpin, C: FlateCodec + Unpin> AsyncRead for AsyncFlateReader<R, C> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = &mut *self;

        loop {
            let output = this.codec.output();
            if this.pos < output.len() {
                let n = buf.remaining().min(output.len() - this.pos);
                buf.put_slice(output.get_slice(this.pos..this.pos + n)?);
                this.pos += n;

                if this.pos == output.len() {
                    output.clear();
                    this.pos = 0;
                }

                return Poll::Ready(Ok(()));
            }

            if this.finished {
                return Poll::Ready(Ok(()));
            }

            let mut scratch = ReadBuf::new(&mut this.scratch);
            ready!(Pin::new(&mut this.inner).poll_read(cx, &mut scratch))?;

            if scratch.filled().is_empty() {
                this.codec.try_finish()?;
                this.finished = true;
            } else {
                this.codec.write_all(scratch.filled())?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{io::Read, time::Duration};
    use tokio::io::AsyncReadExt;

    fn pseudo_random(len: usize) -> Vec<u8> {
        let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 32) as u8
            })
            .collect()
    }

    fn reference_gzip(data: &[u8]) -> Vec<u8> {
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        enc.write_all(data).unwrap();
        enc.finish().unwrap()
    }

    fn reference_gunzip(gz: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        flate2::read::MultiGzDecoder::new(gz)
            .read_to_end(&mut out)
            .unwrap();
        out
    }

    async fn read_all<R: AsyncRead + Unpin>(mut r: R) -> std::io::Result<Vec<u8>> {
        let mut out = Vec::new();
        r.read_to_end(&mut out).await?;
        Ok(out)
    }

    // AsyncFlateReader

    #[test]
    fn round_trip_multi_chunk_data() {
        tokio_test::block_on(async {
            let data = pseudo_random(crate::BUFFER_SIZE * 5 + 123);
            let encoded = AsyncFlateReader::gzip_encode(data.as_slice());
            let decoded = read_all(AsyncFlateReader::gzip_decode(encoded))
                .await
                .unwrap();
            assert_eq!(decoded, data);
        });
    }

    #[test]
    fn encoder_output_is_valid_gzip() {
        tokio_test::block_on(async {
            let data = pseudo_random(crate::BUFFER_SIZE * 3 + 7);
            let gz = read_all(AsyncFlateReader::gzip_encode(data.as_slice()))
                .await
                .unwrap();
            assert_eq!(reference_gunzip(&gz), data);
        });
    }

    #[test]
    fn encoder_empty_input_is_valid_gzip() {
        tokio_test::block_on(async {
            let gz = read_all(AsyncFlateReader::gzip_encode(&b""[..]))
                .await
                .unwrap();
            assert!(!gz.is_empty());
            assert_eq!(reference_gunzip(&gz), b"");
        });
    }

    #[test]
    fn decoder_joins_concatenated_members() {
        tokio_test::block_on(async {
            let a = pseudo_random(crate::BUFFER_SIZE + 11);
            let b = b"INSERT INTO t VALUES (1);\n".to_vec();
            let mut gz = reference_gzip(&a);
            gz.extend(reference_gzip(&b));

            let decoded = read_all(AsyncFlateReader::gzip_decode(gz.as_slice()))
                .await
                .unwrap();
            assert_eq!(decoded, [a, b].concat());
        });
    }

    #[test]
    fn decoder_expands_highly_compressible_input() {
        tokio_test::block_on(async {
            let line = b"INSERT INTO `users` VALUES (1,'alice','alice@example.com');\n";
            let data = line.repeat(8 * 1024 * 1024 / line.len());
            let gz = reference_gzip(&data);
            assert!(data.len() > gz.len() * 100);

            let decoded = read_all(AsyncFlateReader::gzip_decode(gz.as_slice()))
                .await
                .unwrap();
            assert_eq!(decoded.len(), data.len());
            assert!(decoded == data);
        });
    }

    #[test]
    fn decoder_handles_fragmented_pending_inner() {
        tokio_test::block_on(async {
            let data = pseudo_random(crate::BUFFER_SIZE + 4096);
            let gz = reference_gzip(&data);

            let mut builder = tokio_test::io::Builder::new();
            for (i, chunk) in gz.chunks(7).enumerate() {
                if i % 1000 == 0 {
                    builder.wait(Duration::from_millis(1));
                }
                builder.read(chunk);
            }

            let decoded = read_all(AsyncFlateReader::gzip_decode(builder.build()))
                .await
                .unwrap();
            assert_eq!(decoded, data);
        });
    }

    #[test]
    fn tiny_caller_buffer_yields_full_output() {
        tokio_test::block_on(async {
            let data = pseudo_random(crate::BUFFER_SIZE * 2 + 3);
            let gz = reference_gzip(&data);

            let mut reader = AsyncFlateReader::gzip_decode(gz.as_slice());
            let mut decoded = Vec::new();
            let mut buf = [0u8; 7];
            loop {
                let n = reader.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                decoded.extend(buf.iter().take(n).copied());
            }
            assert_eq!(decoded, data);
        });
    }

    #[test]
    fn decoder_rejects_truncated_or_corrupted_input() {
        tokio_test::block_on(async {
            let data = pseudo_random(crate::BUFFER_SIZE * 2);
            let gz = reference_gzip(&data);

            let mut cut_trailer = gz.clone();
            cut_trailer.truncate(gz.len() - 4);

            let mut cut_mid = gz.clone();
            cut_mid.truncate(gz.len() / 2);

            let mut corrupted = gz.clone();
            *corrupted.get_mut(gz.len() / 2).unwrap() ^= 0xff;

            for input in [cut_trailer, cut_mid, corrupted] {
                let result = read_all(AsyncFlateReader::gzip_decode(input.as_slice())).await;
                assert!(
                    result.is_err(),
                    "got Ok with {} bytes",
                    result.unwrap().len()
                );
            }
        });
    }

    #[test]
    fn reads_after_eof_return_zero() {
        tokio_test::block_on(async {
            let gz = reference_gzip(b"select 1;");
            let mut decoder = AsyncFlateReader::gzip_decode(gz.as_slice());
            assert_eq!(read_all(&mut decoder).await.unwrap(), b"select 1;");

            let mut encoder = AsyncFlateReader::gzip_encode(&b"select 1;"[..]);
            read_all(&mut encoder).await.unwrap();

            let mut buf = [0u8; 16];
            for _ in 0..3 {
                assert_eq!(decoder.read(&mut buf).await.unwrap(), 0);
                assert_eq!(encoder.read(&mut buf).await.unwrap(), 0);
            }
        });
    }
}
