// TODO: create Cassandra types
/// Cassandra types

pub const LONG_STR_LEN: usize = 4;
pub const SHORT_LEN: usize = 2;
pub const INT_LEN: usize = 4;

use std::io::{Cursor, Read};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use super::{FromBytes, IntoBytes, FromCursor};

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

pub type CString = String;

// Implementation for Rust std types
// Use extended Rust string as Cassandra [string]
impl IntoBytes for CString {
    /// Converts into Cassandra byte representation of [string]
    fn into_cbytes(&self) -> Vec<u8> {
        let mut v: Vec<u8> = vec![];
        let l = self.len() as i64;
        v.extend_from_slice(to_int(l).as_slice());
        v.extend_from_slice(self.as_bytes());
        return v;
    }
}

impl FromBytes for CString {
    fn from_bytes(bytes: Vec<u8>) -> CString {
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

impl FromCursor for CString {
    /// from_cursor gets Cursor who's position is set such that it should be a start of a [string].
    /// It reads required number of bytes and returns a String
    fn from_cursor(mut cursor: &mut Cursor<Vec<u8>>) -> CString {
        let len_bytes = cursor_next_value(&mut cursor, SHORT_LEN as u64);
        let len: u64 = from_bytes(len_bytes.to_vec());
        let body_bytes = cursor_next_value(&mut cursor, len);

        return String::from_utf8(body_bytes).unwrap();
    }
}

pub type CStringList = Vec<CString>;

impl FromCursor for CStringList {
    fn from_cursor(mut cursor: &mut Cursor<Vec<u8>>) -> CStringList {
        let mut len_bytes = [0; SHORT_LEN];
        if let Err(err) = cursor.read(&mut len_bytes) {
            error!("Read Cassandra bytes error: {}", err);
            panic!(err);
        }
        let len: u64 = from_bytes(len_bytes.to_vec());
        return (0..len).map(|_| CString::from_cursor(&mut cursor)).collect();
    }
}

/**/

pub type CBytes = Vec<u8>;

impl FromCursor for CBytes {
    /// from_cursor gets Cursor who's position is set such that it should be a start of a [bytes].
    /// It reads required number of bytes and returns a CBytes
    fn from_cursor(mut cursor: &mut Cursor<Vec<u8>>) -> CBytes {
        let mut len_bytes = [0; SHORT_LEN];
        if let Err(err) = cursor.read(&mut len_bytes) {
            error!("Read Cassandra bytes error: {}", err);
            panic!(err);
        }
        let len: u64 = from_bytes(len_bytes.to_vec());
        return cursor_next_value(&mut cursor, len);
    }
}

// Use extended Rust Vec<u8> as Cassandra [bytes]
impl IntoBytes for CBytes {
    fn into_cbytes(&self) -> Vec<u8> {
        let mut v: Vec<u8> = vec![];
        let l = self.len() as u64;
        v.extend_from_slice(to_short(l).as_slice());
        v.extend_from_slice(self.as_slice());
        return v;
    }
}

/// Cassandra int type.
pub type CInt = i32;

impl FromCursor for CInt {
    fn from_cursor(mut cursor: &mut Cursor<Vec<u8>>) -> CInt {
        let bytes = cursor_next_value(&mut cursor, INT_LEN as u64);
        return from_bytes(bytes) as CInt;
    }
}

// Use extended Rust Vec<u8> as Cassandra [bytes]
impl FromBytes for Vec<u8> {
    fn from_bytes(bytes: Vec<u8>) -> Vec<u8> {
        let mut cursor = Cursor::new(bytes);
        let len_bytes = cursor_next_value(&mut cursor, SHORT_LEN as u64);
        let len: u64 = from_bytes(len_bytes);
        return cursor_next_value(&mut cursor, len);
    }
}

pub fn cursor_next_value(mut cursor: &mut Cursor<Vec<u8>>, len: u64) -> Vec<u8> {
    let l = len as usize;
    let current_position = cursor.position();
    let mut buff: Vec<u8> = Vec::with_capacity(l);
    unsafe {
        buff.set_len(l);
    }
    if let Err(err) = cursor.read(&mut buff) {
        error!("Read from cursor error: {}", err);
        panic!(err);
    }
    cursor.set_position(current_position + len);
    return buff;
}
