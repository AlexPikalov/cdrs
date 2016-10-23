extern crate byteorder;
#[macro_use]
extern crate log;

pub mod frame;
pub mod frame_query;
pub mod frame_ready;
pub mod frame_startup;
pub mod parser;

pub trait IntoBytes {
    fn into_bytes(&self) -> Vec<u8>;
}

pub trait AsByte {
    fn as_byte(&self) -> u8;
}

// common utils
use std::io::Cursor;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
/// Convert u64 numerical value into array of n bytes
pub fn to_n_bytes(int: u64, n: usize) -> Vec<u8> {
    let mut bytes = vec![];
    bytes.write_uint::<BigEndian>(int, n).unwrap();
    return bytes;
}

pub fn from_bytes(bytes: Vec<u8>) -> u64 {
    let mut c = Cursor::new(bytes.clone());
    return c.read_uint::<BigEndian>(bytes.len()).unwrap()
}
