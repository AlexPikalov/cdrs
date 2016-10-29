extern crate byteorder;
#[macro_use]
extern crate log;

pub mod consistency;
pub mod frame;
pub mod frame_query;
pub mod frame_ready;
pub mod frame_response_rows;
pub mod frame_response_set_keyspace;
pub mod frame_response_void;
pub mod frame_response;
pub mod frame_startup;
pub mod parser;
pub mod types;
pub mod value;

pub trait IntoBytes {
    fn into_cbytes(&self) -> Vec<u8>;
}

pub trait FromBytes {
    fn from_bytes(Vec<u8>) -> Self;
}

pub trait AsByte {
    fn as_byte(&self) -> u8;
}

pub trait FromSingleByte {
    fn from_byte(u8) -> Self;
}
