use std::convert::From;
use snap;

/// Enum which represents a type of compression. Only non-startup frame's body can be compressen.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Compression {
    /// [lz4](https://code.google.com/p/lz4/) compression
    Lz4,
    /// [snappy](https://code.google.com/p/snappy/) compression
    Snappy,
    /// Non compression
    None
}

impl Compression {
    /// It encodes `bytes` basing on type of compression.
    pub fn encode(&self, bytes: Vec<u8>) -> snap::Result<Vec<u8>> {
        return match self {
            &Compression::Lz4 => unimplemented!(),
            &Compression::Snappy => Compression::encode_snappy(bytes),
            &Compression::None => Ok(bytes)
        };
    }

    /// It decodes `bytes` basing on type of compression.
    pub fn decode(&self, bytes: Vec<u8>) -> snap::Result<Vec<u8>> {
        return match self {
            &Compression::Lz4 => unimplemented!(),
            &Compression::Snappy => Compression::decode_snappy(bytes),
            &Compression::None => Ok(bytes)
        };
    }

    /// It transforms compression method into a string.
    pub fn into_string(&self) -> Option<String> {
        return match self {
            &Compression::Lz4 => Some(String::from("lz4")),
            &Compression::Snappy => Some(String::from("snappy")),
            &Compression::None => None
        };
    }

    fn encode_snappy(bytes: Vec<u8>) -> snap::Result<Vec<u8>> {
        let mut encoder = snap::Encoder::new();
        return encoder.compress_vec(bytes.as_slice());
    }

    fn decode_snappy(bytes: Vec<u8>) -> snap::Result<Vec<u8>> {
        let mut decoder = snap::Decoder::new();
        return decoder.decompress_vec(bytes.as_slice());
    }
}

impl From<String> for Compression {
    /// It converts `String` into `Compression`. If string is neither `lz4` nor `snappy` then
    /// `Compression::None` will be returned
    fn from(compression_string: String) -> Compression {
        return Compression::from(compression_string.as_str());
    }
}

impl<'a> From<&'a str> for Compression {
    /// It converts `str` into `Compression`. If string is neither `lz4` nor `snappy` then
    /// `Compression::None` will be returned
    fn from(compression_str: &'a str) -> Compression {
        return match compression_str {
            "lz4" => Compression::Lz4,
            "snappy" => Compression::Snappy,
            _ => Compression::None
        };
    }
}
