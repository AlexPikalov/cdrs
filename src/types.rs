use super::IntoBytes;

/// Cassandra types

pub const LONG_STR_LEN: usize = 4;
pub const SHORT_LEN: usize = 2;
pub const INT_LEN: usize = 4;

use std::io::Cursor;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

/// Converts u64 numerical value into array of n bytes
pub fn to_n_bytes(int: u64, n: usize) -> Vec<u8> {
    let mut bytes = vec![];
    bytes.write_uint::<BigEndian>(int, n).unwrap();
    return bytes;
}

pub fn i_to_n_bytes(int: i64, n: usize) -> Vec<u8> {
    let mut bytes = vec![];
    bytes.write_int::<BigEndian>(int, n).unwrap();
    return bytes;
}

/// Converts byte-array into u64
pub fn from_bytes(bytes: Vec<u8>) -> u64 {
    let mut c = Cursor::new(bytes.clone());
    return c.read_uint::<BigEndian>(bytes.len()).unwrap()
}

/// Converts number u64 into Cassandra's [short].
pub fn to_short(int: u64) -> Vec<u8> {
    return to_n_bytes(int, SHORT_LEN);
}

/// Convers integer into Cassandra's [int]
pub fn to_int(int: i64) -> Vec<u8> {
    return i_to_n_bytes(int, INT_LEN);
}

// Implementation for Rust std types

impl IntoBytes for String {
    fn into_bytes(&self) -> Vec<u8> {
        let mut v: Vec<u8> = vec![];
        let l = self.len() as u64;
        v.extend_from_slice(to_short(l).as_slice());
        v.extend_from_slice(self.as_bytes());
        return v;
    }
}
