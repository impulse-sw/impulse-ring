//! Self-describing wire frame wrapping an Avro body.
//!
//! Layout (little-endian), exactly as specified in `SPEC/wire-format.md` — this
//! is the byte-for-byte contract that native connectors in other languages will
//! reimplement:
//!
//! ```text
//! offset  size  field
//! 0       2     magic       = 0x5249 ("IR")
//! 2       1     wire_ver    = 1
//! 3       1     flags
//! 4       8     schema_fp   (CRC-64-AVRO Rabin fingerprint, LE)
//! 12      4     body_len    (LE)
//! 16      N     body        (Avro binary datum)
//! ```

use crate::avro::Fingerprint;
use std::io;

/// Magic marker `"IR"` (Impulse Ring) in little-endian.
pub const FRAME_MAGIC: u16 = 0x5249;
/// Current wire format version.
pub const WIRE_VERSION: u8 = 1;
/// Size of the fixed frame header preceding the Avro body.
pub const FRAME_HEADER: usize = 16;

/// A decoded frame: schema fingerprint + Avro body, plus flags.
#[derive(Clone, Debug, PartialEq)]
pub struct Frame {
  pub flags: u8,
  pub schema_fp: Fingerprint,
  pub body: Vec<u8>,
}

impl Frame {
  pub fn new(schema_fp: Fingerprint, body: Vec<u8>) -> Self {
    Frame {
      flags: 0,
      schema_fp,
      body,
    }
  }

  /// Serialize the frame to its on-wire byte form.
  pub fn encode(&self) -> Vec<u8> {
    let mut out = Vec::with_capacity(FRAME_HEADER + self.body.len());
    out.extend_from_slice(&FRAME_MAGIC.to_le_bytes());
    out.push(WIRE_VERSION);
    out.push(self.flags);
    out.extend_from_slice(&self.schema_fp.to_le_bytes());
    out.extend_from_slice(&(self.body.len() as u32).to_le_bytes());
    out.extend_from_slice(&self.body);
    out
  }

  /// Parse a frame from its on-wire byte form.
  pub fn decode(buf: &[u8]) -> io::Result<Frame> {
    if buf.len() < FRAME_HEADER {
      return Err(io::Error::other("frame shorter than header"));
    }
    let magic = u16::from_le_bytes([buf[0], buf[1]]);
    if magic != FRAME_MAGIC {
      return Err(io::Error::other("bad frame magic"));
    }
    let ver = buf[2];
    if ver != WIRE_VERSION {
      return Err(io::Error::other(format!("unsupported wire version {ver}")));
    }
    let flags = buf[3];
    let schema_fp = u64::from_le_bytes(buf[4..12].try_into().unwrap());
    let body_len = u32::from_le_bytes(buf[12..16].try_into().unwrap()) as usize;
    if buf.len() < FRAME_HEADER + body_len {
      return Err(io::Error::other("frame body truncated"));
    }
    let body = buf[FRAME_HEADER..FRAME_HEADER + body_len].to_vec();
    Ok(Frame { flags, schema_fp, body })
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn frame_roundtrip() {
    let f = Frame::new(0xDEAD_BEEF_CAFE_F00D, vec![1, 2, 3, 4, 5]);
    let bytes = f.encode();
    assert_eq!(bytes.len(), FRAME_HEADER + 5);
    let back = Frame::decode(&bytes).unwrap();
    assert_eq!(f, back);
  }

  #[test]
  fn golden_header_bytes() {
    // Locks the byte layout so native connectors can match it exactly.
    let f = Frame {
      flags: 0,
      schema_fp: 0x0102_0304_0506_0708,
      body: vec![0xAA],
    };
    let b = f.encode();
    assert_eq!(&b[0..2], &[0x49, 0x52]); // magic "IR" LE
    assert_eq!(b[2], 1); // wire_ver
    assert_eq!(b[3], 0); // flags
    assert_eq!(&b[4..12], &[0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]);
    assert_eq!(&b[12..16], &[1, 0, 0, 0]); // body_len = 1
    assert_eq!(b[16], 0xAA);
  }

  #[test]
  fn rejects_bad_magic() {
    let mut b = Frame::new(1, vec![]).encode();
    b[0] = 0xFF;
    assert!(Frame::decode(&b).is_err());
  }
}
