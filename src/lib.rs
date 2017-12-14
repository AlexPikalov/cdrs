//! **cdrs** is a native Cassandra DB client written in Rust.

extern crate byteorder;
extern crate snap;
#[macro_use]
pub mod macros;

#[macro_use]
extern crate log;
extern crate lz4_compress;
#[cfg(feature = "ssl")]
extern crate openssl;
extern crate r2d2;
extern crate rand;
extern crate time;
extern crate uuid;

use std::io::Cursor;

pub mod frame;
pub mod types;

pub mod authenticators;
pub mod client;
pub mod cluster;
pub mod compression;
pub mod connection_manager;
pub mod consistency;
pub mod error;
pub mod events;
pub mod query;
pub mod transport;

/// `IntoBytes` should be used to convert a structure into array of bytes.
pub trait IntoBytes {
    /// It should convert a struct into an array of bytes.
    fn into_cbytes(&self) -> Vec<u8>;
}

/// `FromBytes` should be used to parse an array of bytes into a structure.
pub trait FromBytes {
    /// It gets and array of bytes and should return an implementor struct.
    fn from_bytes(&[u8]) -> error::Result<Self>
        where Self: Sized;
}

/// `AsBytes` should be used to convert a value into a single byte.
pub trait AsByte {
    /// It should represent a struct as a single byte.
    fn as_byte(&self) -> u8;
}

/// `FromSingleByte` should be used to convert a single byte into a value.
/// It is opposite to `AsByte`.
pub trait FromSingleByte {
    /// It should convert a single byte into an implementor struct.
    fn from_byte(u8) -> Self;
}

/// `FromCursor` should be used to get parsed structure from an `io:Cursor`
/// wich bound to an array of bytes.
pub trait FromCursor {
    /// It should return an implementor from an `io::Cursor` over an array of bytes.
    fn from_cursor(&mut Cursor<&[u8]>) -> error::Result<Self>
        where Self: Sized;
}

/// The trait that allows transformation of `Self` to `types::value::Value`.
pub trait IntoCDRSValue {
    /// It converts `Self` to `types::value::Value`.
    fn into_cdrs_value(self) -> types::value::Value;
}

impl<T: Into<types::value::Bytes>> IntoCDRSValue for T {
    fn into_cdrs_value(self) -> types::value::Value {
        let bytes: types::value::Bytes = self.into();
        bytes.into()
    }
}
