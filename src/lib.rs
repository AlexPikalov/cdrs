//! **cdrs** is a native Cassandra DB client written in Rust. It's under hard development as of now.
extern crate byteorder;
extern crate futures;
#[macro_use]
extern crate log;
extern crate tokio_core;

use std::io::Cursor;

pub mod client;
pub mod consistency;
pub mod frame;
pub mod frame_query;
pub mod frame_ready;
pub mod frame_response_result;
pub mod frame_response;
pub mod frame_startup;
pub mod parser;
pub mod types;
pub mod value;

/// `IntoBytes` should be used to convert a structure into array of bytes.
pub trait IntoBytes {
    fn into_cbytes(&self) -> Vec<u8>;
}

/// `FromBytes` should be used to parse an array of bytes into a structure.
pub trait FromBytes {
    fn from_bytes(Vec<u8>) -> Self;
}

/// `AsBytes` should be used to convert a value into a single byte.
pub trait AsByte {
    fn as_byte(&self) -> u8;
}

/// `FromSingleByte` should be used to convert a single byte into a value. It is opposite to `AsByte`.
pub trait FromSingleByte {
    fn from_byte(u8) -> Self;
}

/// `FromCursor` should be used to get parsed structure from a cursor wich bound to an array of bytes.
pub trait FromCursor {
    fn from_cursor(&mut Cursor<Vec<u8>>) -> Self;
}
