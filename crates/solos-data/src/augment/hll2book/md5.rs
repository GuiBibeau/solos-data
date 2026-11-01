//! MD5, used only as a gate against the downloader TSV's `etag` column (ADR-0021).
//!
//! The S3 etag of a non-multipart object is the MD5 of its bytes, and the archive is never
//! uploaded multipart. The manifest builder compares that etag to the local file to prove the
//! local bytes are the object the downloader recorded; the manifest's checksum itself is
//! SHA-256 of the same local bytes. MD5 is a gate, never a checksum, and is not a security
//! boundary here. The crate has no MD5 dependency, and this is one small well-tested file.

const SHIFTS: [u32; 64] = [
    7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9,
    14, 20, 5, 9, 14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 6, 10, 15,
    21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
];

const SINES: [u32; 64] = [
    0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee, 0xf57c0faf, 0x4787c62a, 0xa8304613, 0xfd469501,
    0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be, 0x6b901122, 0xfd987193, 0xa679438e, 0x49b40821,
    0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa, 0xd62f105d, 0x02441453, 0xd8a1e681, 0xe7d3fbc8,
    0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed, 0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a,
    0xfffa3942, 0x8771f681, 0x6d9d6122, 0xfde5380c, 0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70,
    0x289b7ec6, 0xeaa127fa, 0xd4ef3085, 0x04881d05, 0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665,
    0xf4292244, 0x432aff97, 0xab9423a7, 0xfc93a039, 0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1,
    0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1, 0xf7537e82, 0xbd3af235, 0x2ad7d2bb, 0xeb86d391,
];

/// Streaming MD5 (RFC 1321).
#[derive(Clone, Debug)]
pub struct Md5 {
    state: [u32; 4],
    buffer: [u8; 64],
    buffered: usize,
    length: u64,
}

impl Default for Md5 {
    fn default() -> Self {
        Self::new()
    }
}

impl Md5 {
    /// The initial state.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            state: [0x6745_2301, 0xefcd_ab89, 0x98ba_dcfe, 0x1032_5476],
            buffer: [0; 64],
            buffered: 0,
            length: 0,
        }
    }

    /// Absorb bytes.
    pub fn update(&mut self, mut bytes: &[u8]) {
        self.length = self
            .length
            .saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        if self.buffered > 0 {
            let want = 64 - self.buffered;
            let take = want.min(bytes.len());
            self.buffer[self.buffered..self.buffered + take].copy_from_slice(&bytes[..take]);
            self.buffered += take;
            bytes = &bytes[take..];
            if self.buffered == 64 {
                let block = self.buffer;
                self.compress(&block);
                self.buffered = 0;
            }
        }
        while bytes.len() >= 64 {
            let mut block = [0u8; 64];
            block.copy_from_slice(&bytes[..64]);
            self.compress(&block);
            bytes = &bytes[64..];
        }
        if !bytes.is_empty() {
            self.buffer[..bytes.len()].copy_from_slice(bytes);
            self.buffered = bytes.len();
        }
    }

    /// Finish: the 16-byte digest.
    #[must_use]
    pub fn finalize(mut self) -> [u8; 16] {
        let bits = self.length.saturating_mul(8);
        self.update(&[0x80]);
        while self.buffered != 56 {
            self.update(&[0]);
        }
        let mut block = self.buffer;
        block[56..64].copy_from_slice(&bits.to_le_bytes());
        self.compress(&block);
        let mut out = [0u8; 16];
        for (index, word) in self.state.iter().enumerate() {
            out[index * 4..index * 4 + 4].copy_from_slice(&word.to_le_bytes());
        }
        out
    }

    fn compress(&mut self, block: &[u8; 64]) {
        let mut words = [0u32; 16];
        for (index, word) in words.iter_mut().enumerate() {
            *word = u32::from_le_bytes(
                block[index * 4..index * 4 + 4]
                    .try_into()
                    .expect("slice is four bytes"),
            );
        }
        let [mut a, mut b, mut c, mut d] = self.state;
        for round in 0..64 {
            let (f, target) = match round / 16 {
                0 => ((b & c) | (!b & d), round),
                1 => ((d & b) | (!d & c), (5 * round + 1) % 16),
                2 => (b ^ c ^ d, (3 * round + 5) % 16),
                _ => (c ^ (b | !d), (7 * round) % 16),
            };
            let temp = d;
            d = c;
            c = b;
            let sum = a
                .wrapping_add(f)
                .wrapping_add(SINES[round])
                .wrapping_add(words[target]);
            b = b.wrapping_add(sum.rotate_left(SHIFTS[round]));
            a = temp;
        }
        self.state[0] = self.state[0].wrapping_add(a);
        self.state[1] = self.state[1].wrapping_add(b);
        self.state[2] = self.state[2].wrapping_add(c);
        self.state[3] = self.state[3].wrapping_add(d);
    }
}

/// Lowercase MD5 hex of the bytes.
#[must_use]
pub fn md5_hex(bytes: &[u8]) -> String {
    let mut hasher = Md5::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut out = String::with_capacity(32);
    for byte in digest {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc_1321_and_public_vectors() {
        assert_eq!(md5_hex(b""), "d41d8cd98f00b204e9800998ecf8427e");
        assert_eq!(md5_hex(b"abc"), "900150983cd24fb0d6963f7d28e17f72");
        assert_eq!(
            md5_hex(b"The quick brown fox jumps over the lazy dog"),
            "9e107d9d372bb6826bd81d3542a419d6"
        );
        // Exercises the multi-block and buffering paths.
        let as_ = vec![b'a'; 1_000_000];
        assert_eq!(md5_hex(&as_), "7707d6ae4e027c70eea2a935c2296f21");
        // Streaming in odd chunk sizes lands on the same digest as one update.
        let data: Vec<u8> = (0..1000u32).map(|n| n as u8).collect();
        let mut streamed = Md5::new();
        streamed.update(&data[..7]);
        streamed.update(&data[7..33]);
        streamed.update(&data[33..500]);
        streamed.update(&data[500..]);
        let mut whole = Md5::new();
        whole.update(&data);
        assert_eq!(streamed.finalize(), whole.finalize());
    }

    /// DuckDB's own `md5` is an independent implementation in-process.
    #[test]
    fn duckdb_agrees() {
        let conn = crate::store::memory_connection("1", "256MB").unwrap();
        let rows =
            crate::store::query_rows(&conn, "SELECT md5('the money is gone') AS digest", &[])
                .unwrap();
        assert_eq!(
            md5_hex(b"the money is gone"),
            rows[0].str("digest").unwrap_or_default()
        );
    }
}
