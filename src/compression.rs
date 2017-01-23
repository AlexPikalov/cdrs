use std::convert::From;
use std::error::Error;
use std::result;
use std::fmt;
use snap;
use lz4_compress as lz4;

type Result<T> = result::Result<T, CompressionError>;

/// It's an error which may occure during encoding or deconding
/// frame body. As there are only two types of compressors it
/// contains two related enum options.
#[derive(Debug)]
pub enum CompressionError {
    /// Snappy error.
    Snappy(Box<Error>),
    /// Lz4 error.
    Lz4(String)
}

impl fmt::Display for CompressionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &CompressionError::Snappy(ref err) => write!(f, "Snappy Error: {:?}", err),
            &CompressionError::Lz4(ref s) => write!(f, "Lz4 Error: {:?}", s)
        }
    }
}

impl Error for CompressionError {
    fn description(&self) -> &str {
        let desc = match self {
            &CompressionError::Snappy(ref err) => err.description(),
            &CompressionError::Lz4(ref s) => s.as_str()
        };

        return desc;
    }
}

/// Compressor trait that defines functionality
/// which should be provided by typical compressor.
pub trait Compressor {
    /// Encodes given bytes and returns `Result` that contains either
    /// encoded data or an error which occures during the transformation.
    fn encode(&self, bytes: Vec<u8>) -> Result<Vec<u8>>;
    /// Encodes given encoded data and returns `Result` that contains either
    /// encoded bytes or an error which occures during the transformation.
    fn decode(&self, bytes: Vec<u8>) -> Result<Vec<u8>>;
    /// Returns a string which is a name of a compressor. This name should be
    /// exactly the same as one which returns a server in a response to
    /// `Options` request.
    fn into_string(&self) -> Option<String>;
}

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
    pub fn encode(&self, bytes: Vec<u8>) -> Result<Vec<u8>> {
        return match self {
            &Compression::Lz4 => Compression::encode_lz4(bytes),
            &Compression::Snappy => Compression::encode_snappy(bytes),
            &Compression::None => Ok(bytes)
        };
    }

    /// It decodes `bytes` basing on type of compression.
    pub fn decode(&self, bytes: Vec<u8>) -> Result<Vec<u8>> {
        return match self {
            &Compression::Lz4 => Compression::decode_lz4(bytes),
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

    fn encode_snappy(bytes: Vec<u8>) -> Result<Vec<u8>> {
        let mut encoder = snap::Encoder::new();
        return encoder
            .compress_vec(bytes.as_slice())
            .map_err(|err| CompressionError::Snappy(Box::new(err)));
    }

    fn decode_snappy(bytes: Vec<u8>) -> Result<Vec<u8>> {
        let mut decoder = snap::Decoder::new();
        return decoder
            .decompress_vec(bytes.as_slice())
            .map_err(|err| CompressionError::Snappy(Box::new(err)));
    }

    fn encode_lz4(bytes: Vec<u8>) -> Result<Vec<u8>> {
        return Ok(lz4::compress(bytes.as_slice()));
    }

    fn decode_lz4(bytes: Vec<u8>) -> Result<Vec<u8>> {
        // skip first 4 bytes in accordance to
        // https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L805
        return lz4::decompress(&bytes[4..]).map_err(|err| CompressionError::Lz4(err.description().to_string()));
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
