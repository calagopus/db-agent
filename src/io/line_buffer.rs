pub struct LineBuffer {
    buffer: Vec<u8>,
    line_start: usize,
}

impl LineBuffer {
    const INITIAL_CAPACITY: usize = 10240; // 10 KiB
    const MAX_LINE_LENGTH: usize = 5120; // 5 KiB
    const COMPACT_THRESHOLD: usize = 10240; // 10 KiB

    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(Self::INITIAL_CAPACITY),
            line_start: 0,
        }
    }

    pub fn extend(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    pub fn next_line(&mut self) -> Option<&[u8]> {
        let newline = self
            .buffer
            .get(self.line_start..)?
            .iter()
            .position(|&b| b == b'\n');
        let available = self.buffer.len() - self.line_start;

        let (len, advance) = match newline {
            Some(pos) if pos <= Self::MAX_LINE_LENGTH => (pos, pos + 1),
            Some(_) => (Self::MAX_LINE_LENGTH, Self::MAX_LINE_LENGTH),
            None if available > Self::MAX_LINE_LENGTH => {
                (Self::MAX_LINE_LENGTH, Self::MAX_LINE_LENGTH)
            }
            None => return None,
        };

        let start = self.line_start;
        self.line_start += advance;

        self.buffer
            .get(start..start + len)
            .map(|slice| slice.trim_ascii())
    }

    pub fn compact(&mut self) {
        if self.line_start > Self::COMPACT_THRESHOLD && self.line_start > self.buffer.len() / 2 {
            self.buffer.drain(..self.line_start);
            self.line_start = 0;
        }
    }

    pub fn flush(&self) -> Option<&[u8]> {
        let rest = self.buffer.get(self.line_start..)?;
        (!rest.is_empty()).then(|| rest.trim_ascii())
    }
}
