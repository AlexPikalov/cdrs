extern crate byteorder;
#[macro_use]
extern crate log;

pub mod consistency;
pub mod frame;
pub mod frame_query;
pub mod frame_ready;
pub mod frame_startup;
pub mod parser;
pub mod types;

pub trait IntoBytes {
    fn into_bytes(&self) -> Vec<u8>;
}

pub trait FromBytes {
    fn from_bytes(Vec<u8>) -> Self;
}

pub trait AsByte {
    fn as_byte(&self) -> u8;
}
