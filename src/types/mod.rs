// TODO: create Cassandra types
/// Cassandra types

pub const LONG_STR_LEN: usize = 4;
pub const SHORT_LEN: usize = 2;
pub const INT_LEN: usize = 4;

use std::io;
use std::io::{Cursor, Read};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use super::{FromBytes, IntoBytes, FromCursor};

pub mod data_serialization_types;
pub mod generic_types;
pub mod value;

/// Tries to converts u64 numerical value into array of n bytes.
pub fn try_to_n_bytes(int: u64, n: usize) -> io::Result<Vec<u8>> {
    let mut bytes = vec![];
    try!(bytes.write_uint::<BigEndian>(int, n));

    return Ok(bytes);
}

/// Converts u64 numerical value into array of n bytes
pub fn to_n_bytes(int: u64, n: usize) -> Vec<u8> {
    return try_to_n_bytes(int, n).unwrap();
}

pub fn try_i_to_n_bytes(int: i64, n: usize) -> io::Result<Vec<u8>> {
    let mut bytes = vec![];
    try!(bytes.write_int::<BigEndian>(int, n));

    return Ok(bytes);
}

pub fn i_to_n_bytes(int: i64, n: usize) -> Vec<u8> {
    return try_i_to_n_bytes(int, n).unwrap();
}

///
pub fn try_from_bytes(bytes: Vec<u8>) -> Result<u64, io::Error> {
    let mut c = Cursor::new(bytes.clone());
    return c.read_uint::<BigEndian>(bytes.len());
}

///
pub fn try_u16_from_bytes(bytes: Vec<u8>) -> Result<u16, io::Error> {
    let mut c = Cursor::new(bytes.clone());
    return c.read_u16::<BigEndian>();
}

///
pub fn try_i_from_bytes(bytes: Vec<u8>) -> Result<i64, io::Error> {
    let mut c = Cursor::new(bytes.clone());
    return c.read_int::<BigEndian>(bytes.len());
}

///
pub fn try_i32_from_bytes(bytes: Vec<u8>) -> Result<i32, io::Error> {
    let mut c = Cursor::new(bytes.clone());
    return c.read_i32::<BigEndian>();
}

///
pub fn try_f32_from_bytes(bytes: Vec<u8>) -> Result<f32, io::Error> {
    let mut c = Cursor::new(bytes.clone());
    return c.read_f32::<BigEndian>();
}

///
pub fn try_f64_from_bytes(bytes: Vec<u8>) -> Result<f64, io::Error> {
    let mut c = Cursor::new(bytes.clone());
    return c.read_f64::<BigEndian>();
}

/// Converts byte-array into u64
pub fn from_bytes(bytes: Vec<u8>) -> u64 {
    return try_from_bytes(bytes).unwrap();
}

/// Converts byte-array into i64
pub fn from_i_bytes(bytes: Vec<u8>) -> i64 {
    return try_i_from_bytes(bytes).unwrap();
}

/// Converts byte-array into u16
pub fn from_u16_bytes(bytes: Vec<u8>) -> u16 {
    return try_u16_from_bytes(bytes).unwrap();
}

/// Converts number u64 into Cassandra's [short].
pub fn to_short(int: u64) -> Vec<u8> {
    return to_n_bytes(int, SHORT_LEN);
}

/// Convers integer into Cassandra's [int]
pub fn to_int(int: i64) -> Vec<u8> {
    return i_to_n_bytes(int, INT_LEN);
}

#[derive(Debug, Clone)]
pub struct CString {
    string: String
}

impl CString {
    pub fn new(string: String) -> CString {
        return CString { string: string };
    }

    /// Converts internal value into pointer of `str`.
    pub fn as_str<'a>(&'a self) -> &'a str {
        return self.string.as_str();
    }

    /// Converts internal value into a plain `String`.
    pub fn into_plain(self) -> String {
        return self.string;
    }
}

// Implementation for Rust std types
// Use extended Rust string as Cassandra [string]
impl IntoBytes for CString {
    /// Converts into Cassandra byte representation of [string]
    fn into_cbytes(&self) -> Vec<u8> {
        let mut v: Vec<u8> = vec![];
        let l = self.string.len() as u64;
        v.extend_from_slice(to_short(l).as_slice());
        v.extend_from_slice(self.string.as_bytes());
        return v;
    }
}

impl FromCursor for CString {
    /// from_cursor gets Cursor who's position is set such that it should be a start of a [string].
    /// It reads required number of bytes and returns a String
    fn from_cursor(mut cursor: &mut Cursor<Vec<u8>>) -> CString {
        let len_bytes = cursor_next_value(&mut cursor, SHORT_LEN as u64);
        let len: u64 = from_bytes(len_bytes.to_vec());
        let body_bytes = cursor_next_value(&mut cursor, len);

        return CString { string: String::from_utf8(body_bytes).unwrap() };
    }
}

#[derive(Debug, Clone)]
pub struct CStringLong {
    string: String
}

impl CStringLong {
    pub fn new(string: String) -> CStringLong {
        return CStringLong { string: string };
    }

    /// Converts internal value into pointer of `str`.
    pub fn as_str<'a>(&'a self) -> &'a str {
        return self.string.as_str();
    }

    /// Converts internal value into a plain `String`.
    pub fn into_plain(self) -> String {
        return self.string;
    }
}

// Implementation for Rust std types
// Use extended Rust string as Cassandra [string]
impl IntoBytes for CStringLong {
    /// Converts into Cassandra byte representation of [string]
    fn into_cbytes(&self) -> Vec<u8> {
        let mut v: Vec<u8> = vec![];
        let l = self.string.len() as i64;
        v.extend_from_slice(to_int(l).as_slice());
        v.extend_from_slice(self.string.as_bytes());
        return v;
    }
}

impl FromCursor for CStringLong {
    /// from_cursor gets Cursor who's position is set such that it should be a start of a [string].
    /// It reads required number of bytes and returns a String
    fn from_cursor(mut cursor: &mut Cursor<Vec<u8>>) -> CStringLong {
        let len_bytes = cursor_next_value(&mut cursor, INT_LEN as u64);
        let len: u64 = from_bytes(len_bytes.to_vec());
        let body_bytes = cursor_next_value(&mut cursor, len);

        return CStringLong { string: String::from_utf8(body_bytes).unwrap() };
    }
}

#[derive(Debug, Clone)]
pub struct CStringList {
    list: Vec<CString>
}

impl CStringList {
    pub fn into_plain(self) -> Vec<String> {
        return self.list
            .iter()
            .map(|string| string.clone().into_plain())
            .collect();
    }
}

impl FromCursor for CStringList {
    fn from_cursor(mut cursor: &mut Cursor<Vec<u8>>) -> CStringList {
        let mut len_bytes = [0; SHORT_LEN];
        if let Err(err) = cursor.read(&mut len_bytes) {
            error!("Read Cassandra bytes error: {}", err);
            panic!(err);
        }
        let len: u64 = from_bytes(len_bytes.to_vec());
        let list = (0..len).map(|_| CString::from_cursor(&mut cursor)).collect();
        return CStringList { list: list };
    }
}

/**/

#[derive(Debug, Clone)]
/// The structure that represents Cassandra byte type
pub struct CBytes {
    bytes: Vec<u8>
}

impl CBytes {
    pub fn new(bytes: Vec<u8>) -> CBytes {
        return CBytes { bytes: bytes };
    }
    /// Converts `CBytes` into a plain array of bytes
    pub fn into_plain(self) -> Vec<u8> {
        return self.bytes;
    }
}

impl FromCursor for CBytes {
    /// from_cursor gets Cursor who's position is set such that it should be a start of a [bytes].
    /// It reads required number of bytes and returns a CBytes
    fn from_cursor(mut cursor: &mut Cursor<Vec<u8>>) -> CBytes {
        let len: u64 = CInt::from_cursor(&mut cursor) as u64;
        return CBytes { bytes: cursor_next_value(&mut cursor, len) };
    }
}

// Use extended Rust Vec<u8> as Cassandra [bytes]
impl IntoBytes for CBytes {
    fn into_cbytes(&self) -> Vec<u8> {
        let mut v: Vec<u8> = vec![];
        let l = self.bytes.len() as i64;
        v.extend_from_slice(to_int(l).as_slice());
        v.extend_from_slice(self.bytes.as_slice());
        return v;
    }
}

/// Cassandra short bytes
#[derive(Debug, Clone)]
pub struct CBytesShort {
    bytes: Vec<u8>
}

impl CBytesShort {
    pub fn new(bytes: Vec<u8>) -> CBytesShort {
        return CBytesShort { bytes: bytes };
    }
    /// Converts `CBytesShort` into plain vector of bytes;
    pub fn into_plain(self) -> Vec<u8> {
        return self.bytes;
    }
}

impl FromCursor for CBytesShort {
    /// from_cursor gets Cursor who's position is set such that it should be a start of a [bytes].
    /// It reads required number of bytes and returns a CBytes
    fn from_cursor(mut cursor: &mut Cursor<Vec<u8>>) -> CBytesShort {
        let len: u64 = CIntShort::from_cursor(&mut cursor) as u64;
        return CBytesShort { bytes: cursor_next_value(&mut cursor, len) };
    }
}

// Use extended Rust Vec<u8> as Cassandra [bytes]
impl IntoBytes for CBytesShort {
    fn into_cbytes(&self) -> Vec<u8> {
        let mut v: Vec<u8> = vec![];
        let l = self.bytes.len() as u64;
        v.extend_from_slice(to_short(l).as_slice());
        v.extend_from_slice(self.bytes.as_slice());
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

/// Cassandra int short type.
pub type CIntShort = i16;

impl FromCursor for CIntShort {
    fn from_cursor(mut cursor: &mut Cursor<Vec<u8>>) -> CIntShort {
        let bytes = cursor_next_value(&mut cursor, SHORT_LEN as u64);
        return from_bytes(bytes) as CIntShort;
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
