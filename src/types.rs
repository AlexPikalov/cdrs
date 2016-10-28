use super::{FromBytes, IntoBytes};

// TODO: create Cassandra types

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

// Use extended Rust string as Cassandra [string]
impl IntoBytes for String {
    /// Converts into Cassandra byte representation of [string]
    fn into_cbytes(&self) -> Vec<u8> {
        let mut v: Vec<u8> = vec![];
        let l = self.len() as i64;
        v.extend_from_slice(to_int(l).as_slice());
        v.extend_from_slice(self.as_bytes());
        return v;
    }
}

impl FromBytes for String {
    fn from_bytes(bytes: Vec<u8>) -> String {
        let len: usize = from_bytes(bytes[..SHORT_LEN].to_vec()) as usize;
        return match String::from_utf8(bytes[(SHORT_LEN + 1)..len].to_vec()) {
            Ok(string) => string,
            Err(err) => {
                error!("Parsing string error {:?}", err);
                panic!("Parsing string error {:?}", err);
            }
        };
    }
}

/**/
// Use extended Rust Vec<u8> as Cassandra [bytes]
impl IntoBytes for Vec<u8> {
    fn into_cbytes(&self) -> Vec<u8> {
        let mut v: Vec<u8> = vec![];
        let l = self.len() as u64;
        v.extend_from_slice(to_short(l).as_slice());
        v.extend_from_slice(self.as_slice());
        return v;
    }
}
