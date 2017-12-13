/// Cassandra types

use std::io;
use std::io::{Cursor, Read};
use std::net::SocketAddr;

use byteorder::{BigEndian, ByteOrder, ReadBytesExt, WriteBytesExt};
use error::{column_is_empty_err, Error as CDRSError, Result as CDRSResult};
use types::data_serialization_types::decode_inet;
use {FromBytes, FromCursor, IntoBytes};

pub const LONG_STR_LEN: usize = 4;
pub const SHORT_LEN: usize = 2;
pub const INT_LEN: usize = 4;
pub const UUID_LEN: usize = 16;

#[macro_use]
pub mod blob;
pub mod data_serialization_types;
pub mod list;
pub mod map;
pub mod rows;
pub mod udt;
pub mod value;
pub mod tuple;

/// Should be used to represent a single column as a Rust value.
pub trait AsRustType<T> {
    fn as_rust_type(&self) -> CDRSResult<Option<T>>;

    fn as_r_type(&self) -> CDRSResult<T> {
        self.as_rust_type()
            .and_then(|op| op.ok_or(CDRSError::from("Value is null or non-set")))
    }
}

pub trait AsRust {
    fn as_rust<R>(&self) -> CDRSResult<Option<R>>
        where Self: AsRustType<R>
    {
        self.as_rust_type()
    }

    fn as_r_rust<T>(&self) -> CDRSResult<T>
        where Self: AsRustType<T>
    {
        self.as_rust()
            .and_then(|op| op.ok_or("Value is null or non-set".into()))
    }
}

/// Should be used to return a single column as Rust value by its name.
pub trait IntoRustByName<R> {
    fn get_by_name(&self, name: &str) -> CDRSResult<Option<R>>;

    fn get_r_by_name(&self, name: &str) -> CDRSResult<R> {
        self.get_by_name(name)
            .and_then(|op| op.ok_or(column_is_empty_err()))
    }
}

pub trait ByName {
    fn by_name<R>(&self, name: &str) -> CDRSResult<Option<R>>
        where Self: IntoRustByName<R>
    {
        self.get_by_name(name)
    }

    fn r_by_name<R>(&self, name: &str) -> CDRSResult<R>
        where Self: IntoRustByName<R>
    {
        self.by_name(name)
            .and_then(|op| op.ok_or(column_is_empty_err()))
    }
}

/// Should be used to return a single column as Rust value by its name.
pub trait IntoRustByIndex<R> {
    fn get_by_index(&self, index: usize) -> CDRSResult<Option<R>>;

    fn get_r_by_index(&self, index: usize) -> CDRSResult<R> {
        self.get_by_index(index)
            .and_then(|op| op.ok_or(column_is_empty_err()))
    }
}

pub trait ByIndex {
    fn by_index<R>(&self, index: usize) -> CDRSResult<Option<R>>
        where Self: IntoRustByIndex<R>
    {
        self.get_by_index(index)
    }

    fn r_by_index<R>(&self, index: usize) -> CDRSResult<R>
        where Self: IntoRustByIndex<R>
    {
        self.by_index(index)
            .and_then(|op| op.ok_or(column_is_empty_err()))
    }
}

/// Tries to converts u64 numerical value into array of n bytes.
pub fn try_to_n_bytes(int: u64, n: usize) -> io::Result<Vec<u8>> {
    let mut bytes = vec![];
    try!(bytes.write_uint::<BigEndian>(int, n));

    Ok(bytes)
}

/// Converts u64 numerical value into array of n bytes
///
/// # Panics
///
/// It panics if given unisigned integer could not be converted in an array of n bytes
pub fn to_n_bytes(int: u64, n: usize) -> Vec<u8> {
    try_to_n_bytes(int, n).unwrap()
}

pub fn try_i_to_n_bytes(int: i64, n: usize) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(n);
    unsafe {
        bytes.set_len(n);
    }
    BigEndian::write_int(&mut bytes, int, n);

    Ok(bytes)
}

/// Converts u64 numerical value into array of n bytes
///
/// # Panics
///
/// It panics if given integer could not be converted in an array of n bytes
pub fn i_to_n_bytes(int: i64, n: usize) -> Vec<u8> {
    try_i_to_n_bytes(int, n).unwrap()
}

/// Tryies to decode bytes array into `u64`.
pub fn try_from_bytes(bytes: &[u8]) -> Result<u64, io::Error> {
    let l = bytes.len();
    let mut c = Cursor::new(bytes);
    c.read_uint::<BigEndian>(l)
}

/// Tryies to decode bytes array into `u16`.
pub fn try_u16_from_bytes(bytes: &[u8]) -> Result<u16, io::Error> {
    let mut c = Cursor::new(bytes);
    c.read_u16::<BigEndian>()
}

/// Tries to decode bytes array into `i64`.
pub fn try_i_from_bytes(bytes: &[u8]) -> Result<i64, io::Error> {
    let l = bytes.len();
    let mut c = Cursor::new(bytes);
    c.read_int::<BigEndian>(l)
}

/// Tries to decode bytes array into `i32`.
pub fn try_i32_from_bytes(bytes: &[u8]) -> Result<i32, io::Error> {
    let mut c = Cursor::new(bytes);
    c.read_i32::<BigEndian>()
}

/// Tries to decode bytes array into `i16`.
pub fn try_i16_from_bytes(bytes: &[u8]) -> Result<i16, io::Error> {
    let mut c = Cursor::new(bytes);
    c.read_i16::<BigEndian>()
}

/// Tries to decode bytes array into `f32`.
pub fn try_f32_from_bytes(bytes: &[u8]) -> Result<f32, io::Error> {
    let mut c = Cursor::new(bytes);
    c.read_f32::<BigEndian>()
}

/// Tries to decode bytes array into `f64`.
pub fn try_f64_from_bytes(bytes: &[u8]) -> Result<f64, io::Error> {
    let mut c = Cursor::new(bytes);
    c.read_f64::<BigEndian>()
}

/// Converts byte-array into u64
///
/// # Panics
///
/// It panics if given bytes could not be converted into `u64`
pub fn from_bytes(bytes: &[u8]) -> u64 {
    try_from_bytes(bytes).unwrap()
}

/// Converts byte-array into i64
///
/// # Panics
///
/// It panics if given bytes could not be converted into `i64`
pub fn from_i_bytes(bytes: &[u8]) -> i64 {
    try_i_from_bytes(bytes).unwrap()
}

/// Converts byte-array into u16
///
/// # Panics
///
/// It panics if given bytes could not be converted into `u16`
pub fn from_u16_bytes(bytes: &[u8]) -> u16 {
    try_u16_from_bytes(bytes).unwrap()
}

/// Converts number i16 into Cassandra's [short].
///
/// # Panics
///
/// It panics if given `i16` could not be converted into bytes
pub fn to_short(int: i16) -> Vec<u8> {
    let mut bytes = vec![];
    // should not panic as input is i16
    let _ = bytes.write_i16::<BigEndian>(int).unwrap();

    bytes
}

/// Convers integer into Cassandra's [int]
///
/// # Panics
///
/// It panics if given `i32` could not be converted into bytes
pub fn to_int(int: i32) -> Vec<u8> {
    let mut bytes = vec![];
    // should not panic as input is i16
    let _ = bytes.write_i32::<BigEndian>(int).unwrap();

    bytes
}

/// Convers integer into Cassandra's [int]
///
/// # Panics
///
/// It panics if given `i64` could not be converted into bytes
pub fn to_bigint(int: i64) -> Vec<u8> {
    let mut bytes = vec![];
    // should not panic as input is i16
    let _ = bytes.write_i64::<BigEndian>(int).unwrap();

    bytes
}

/// Converts number i16 into Cassandra's `short`.
///
/// # Panics
///
/// It panics if given `u16` could not be converted into bytes
pub fn to_u_short(int: u16) -> Vec<u8> {
    let mut bytes = vec![];
    // should not panic as input is i16
    let _ = bytes.write_u16::<BigEndian>(int).unwrap();

    bytes
}

/// Convers integer into Cassandra's [int]
///
/// # Panics
///
/// It panics if given `u32` could not be converted into bytes
pub fn to_u(int: u32) -> Vec<u8> {
    let mut bytes = vec![];
    // should not panic as input is u64
    let _ = bytes.write_u32::<BigEndian>(int).unwrap();

    bytes
}

/// Convers integer into Cassandra's `int`
///
/// # Panics
///
/// It panics if given `u64` could not be converted into `u64`
pub fn to_u_big(int: u64) -> Vec<u8> {
    let mut bytes = vec![];
    // should not panic as input is u64
    let _ = bytes.write_u64::<BigEndian>(int).unwrap();

    bytes
}

/// Converts `f32` into bytes
///
/// # Panics
///
/// It panics if given `f32` could not be converted into bytes
pub fn to_float(f: f32) -> Vec<u8> {
    let mut bytes = vec![];
    // should not panic as input is f32
    let _ = bytes.write_f32::<BigEndian>(f).unwrap();

    bytes
}

/// Converts `f64` into array of bytes
///
/// # Panics
///
/// It panics if given `f63` could not be converted into bytes
pub fn to_float_big(f: f64) -> Vec<u8> {
    let mut bytes = vec![];
    // should not panic as input is f64
    let _ = bytes.write_f64::<BigEndian>(f).unwrap();

    bytes
}

#[derive(Debug, Clone)]
pub struct CString {
    string: String,
}

impl CString {
    pub fn new(string: String) -> CString {
        CString { string: string }
    }

    /// Converts internal value into pointer of `str`.
    pub fn as_str<'a>(&'a self) -> &'a str {
        self.string.as_str()
    }

    /// Converts internal value into a plain `String`.
    pub fn into_plain(self) -> String {
        self.string
    }

    /// Represents internal value as a `String`.
    pub fn as_plain(&self) -> String {
        self.string.clone()
    }
}

// Implementation for Rust std types
// Use extended Rust string as Cassandra [string]
impl IntoBytes for CString {
    /// Converts into Cassandra byte representation of [string]
    fn into_cbytes(&self) -> Vec<u8> {
        let mut v: Vec<u8> = vec![];
        let l = self.string.len() as i16;
        v.extend_from_slice(to_short(l).as_slice());
        v.extend_from_slice(self.string.as_bytes());
        v
    }
}

impl FromCursor for CString {
    /// from_cursor gets Cursor who's position is set such that it should be a start of a [string].
    /// It reads required number of bytes and returns a String
    fn from_cursor(mut cursor: &mut Cursor<&[u8]>) -> CDRSResult<CString> {
        let len_bytes = try!(cursor_next_value(&mut cursor, SHORT_LEN as u64));
        let len: u64 = try_from_bytes(len_bytes.as_slice())?;
        let body_bytes = try!(cursor_next_value(&mut cursor, len));

        String::from_utf8(body_bytes)
            .map_err(Into::into)
            .map(CString::new)
    }
}

#[derive(Debug, Clone)]
pub struct CStringLong {
    string: String,
}

impl CStringLong {
    pub fn new(string: String) -> CStringLong {
        CStringLong { string: string }
    }

    /// Converts internal value into pointer of `str`.
    pub fn as_str<'a>(&'a self) -> &'a str {
        self.string.as_str()
    }

    /// Converts internal value into a plain `String`.
    pub fn into_plain(self) -> String {
        self.string
    }
}

// Implementation for Rust std types
// Use extended Rust string as Cassandra [string]
impl IntoBytes for CStringLong {
    /// Converts into Cassandra byte representation of [string]
    fn into_cbytes(&self) -> Vec<u8> {
        let mut v: Vec<u8> = vec![];
        let l = self.string.len() as i32;
        v.extend_from_slice(to_int(l).as_slice());
        v.extend_from_slice(self.string.as_bytes());
        v
    }
}

impl FromCursor for CStringLong {
    /// from_cursor gets Cursor who's position is set such that it should be a start of a [string].
    /// It reads required number of bytes and returns a String
    fn from_cursor(mut cursor: &mut Cursor<&[u8]>) -> CDRSResult<CStringLong> {
        let len_bytes = cursor_next_value(&mut cursor, INT_LEN as u64)?;
        let len: u64 = try_from_bytes(len_bytes.as_slice())?;
        let body_bytes = cursor_next_value(&mut cursor, len)?;

        String::from_utf8(body_bytes)
            .map_err(Into::into)
            .map(CStringLong::new)
    }
}

#[derive(Debug, Clone)]
pub struct CStringList {
    pub list: Vec<CString>,
}

impl CStringList {
    pub fn into_plain(self) -> Vec<String> {
        self.list
            .iter()
            .map(|string| string.clone().into_plain())
            .collect()
    }
}

impl IntoBytes for CStringList {
    fn into_cbytes(&self) -> Vec<u8> {
        let mut bytes = vec![];

        let l = to_short(self.list.len() as i16);
        bytes.extend_from_slice(l.as_slice());

        bytes = self.list.iter().fold(bytes, |mut _bytes, cstring| {
            _bytes.extend_from_slice(cstring.into_cbytes().as_slice());
            _bytes
        });

        bytes
    }
}

impl FromCursor for CStringList {
    fn from_cursor(mut cursor: &mut Cursor<&[u8]>) -> CDRSResult<CStringList> {
        // TODO: try to use slice instead
        let mut len_bytes = [0; SHORT_LEN];
        try!(cursor.read(&mut len_bytes));
        let len = try_from_bytes(len_bytes.to_vec().as_slice())? as usize;
        let mut list = Vec::with_capacity(len);
        for _ in 0..len {
            list.push(CString::from_cursor(&mut cursor)?);
        }

        Ok(CStringList { list: list })
    }
}

//

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
/// The structure that represents Cassandra byte type.
pub struct CBytes {
    bytes: Option<Vec<u8>>,
}

impl CBytes {
    pub fn new(bytes: Vec<u8>) -> CBytes {
        CBytes { bytes: Some(bytes) }
    }

    /// Creates Cassandra bytes that represent empty or null value
    pub fn new_empty() -> CBytes {
        CBytes { bytes: None }
    }

    /// Converts `CBytes` into a plain array of bytes
    pub fn into_plain(self) -> Option<Vec<u8>> {
        self.bytes
    }

    // TODO: try to replace usage of `as_plain` by `as_slice`
    pub fn as_plain(&self) -> Option<Vec<u8>> {
        self.bytes.clone()
    }
    pub fn as_slice(&self) -> Option<&[u8]> {
        match self.bytes {
            Some(ref v) => Some(v.as_slice()),
            None => None,
        }
        // self.bytes.map(|v| v.as_slice())
    }
    pub fn is_empty(&self) -> bool {
        self.bytes.is_some()
    }
}

impl FromCursor for CBytes {
    /// from_cursor gets Cursor who's position is set such that it should be a start of a [bytes].
    /// It reads required number of bytes and returns a CBytes
    fn from_cursor(mut cursor: &mut Cursor<&[u8]>) -> CDRSResult<CBytes> {
        let len = CInt::from_cursor(&mut cursor)?;
        // null or not set value
        if len < 0 {
            return Ok(CBytes { bytes: None });
        }

        cursor_next_value(&mut cursor, len as u64).map(CBytes::new)
    }
}

// Use extended Rust Vec<u8> as Cassandra [bytes]
impl IntoBytes for CBytes {
    fn into_cbytes(&self) -> Vec<u8> {
        match self.bytes {
            Some(ref b) => {
                let mut v: Vec<u8> = vec![];
                let l = b.len() as i32;
                v.extend_from_slice(to_int(l).as_slice());
                v.extend_from_slice(b.as_slice());
                v
            }
            None => vec![],
        }
    }
}

/// Cassandra short bytes
#[derive(Debug, Clone)]
pub struct CBytesShort {
    bytes: Option<Vec<u8>>,
}

impl CBytesShort {
    pub fn new(bytes: Vec<u8>) -> CBytesShort {
        CBytesShort { bytes: Some(bytes) }
    }
    /// Converts `CBytesShort` into plain vector of bytes;
    pub fn into_plain(self) -> Option<Vec<u8>> {
        self.bytes
    }
}

impl FromCursor for CBytesShort {
    /// from_cursor gets Cursor who's position is set such that it should be a start of a [bytes].
    /// It reads required number of bytes and returns a CBytes
    fn from_cursor(mut cursor: &mut Cursor<&[u8]>) -> CDRSResult<CBytesShort> {
        let len = CIntShort::from_cursor(&mut cursor)?;

        if len < 0 {
            return Ok(CBytesShort { bytes: None });
        }

        cursor_next_value(&mut cursor, len as u64)
            .map(CBytesShort::new)
            .map_err(Into::into)
    }
}

// Use extended Rust Vec<u8> as Cassandra [bytes]
impl IntoBytes for CBytesShort {
    fn into_cbytes(&self) -> Vec<u8> {
        match self.bytes {
            Some(ref b) => {
                let mut v: Vec<u8> = vec![];
                let l = b.len() as i16;
                v.extend_from_slice(to_short(l).as_slice());
                v.extend_from_slice(b.as_slice());
                v
            }
            None => vec![],
        }
    }
}


/// Cassandra int type.
pub type CInt = i32;

impl FromCursor for CInt {
    fn from_cursor(mut cursor: &mut Cursor<&[u8]>) -> CDRSResult<CInt> {
        let bytes = try!(cursor_next_value(&mut cursor, INT_LEN as u64));
        try_i32_from_bytes(bytes.as_slice()).map_err(Into::into)
    }
}

/// Cassandra int short type.
pub type CIntShort = i16;

impl FromCursor for CIntShort {
    fn from_cursor(mut cursor: &mut Cursor<&[u8]>) -> CDRSResult<CIntShort> {
        let bytes = try!(cursor_next_value(&mut cursor, SHORT_LEN as u64));
        try_i16_from_bytes(bytes.as_slice()).map_err(Into::into)
    }
}

// Use extended Rust Vec<u8> as Cassandra [bytes]
impl FromBytes for Vec<u8> {
    fn from_bytes(bytes: &[u8]) -> CDRSResult<Vec<u8>> {
        let mut cursor = Cursor::new(bytes);
        let len_bytes = cursor_next_value(&mut cursor, SHORT_LEN as u64)?;
        let len: u64 = try_from_bytes(len_bytes.as_slice())?;

        cursor_next_value(&mut cursor, len).map_err(Into::into)
    }
}

/// The structure wich represets Cassandra [inet]
/// (https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L222).
#[derive(Debug)]
pub struct CInet {
    pub addr: SocketAddr,
}

impl FromCursor for CInet {
    fn from_cursor(mut cursor: &mut Cursor<&[u8]>) -> CDRSResult<CInet> {
        let n = CIntShort::from_cursor(&mut cursor)?;
        let ip = decode_inet(cursor_next_value(&mut cursor, n as u64)?.as_slice())?;
        let port = CInt::from_cursor(&mut cursor)?;
        let socket_addr = SocketAddr::new(ip, port as u16);

        Ok(CInet { addr: socket_addr })
    }
}

pub fn cursor_next_value(cursor: &mut Cursor<&[u8]>, len: u64) -> CDRSResult<Vec<u8>> {
    let l = len as usize;
    let current_position = cursor.position();
    let mut buff: Vec<u8> = Vec::with_capacity(l);
    unsafe {
        buff.set_len(l);
    }
    try!(cursor.read_exact(&mut buff));
    cursor.set_position(current_position + len);
    Ok(buff)
}


#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use super::*;
    use {FromCursor, IntoBytes};
    use std::mem::transmute;

    // CString
    #[test]
    fn test_cstring_new() {
        let foo = "foo".to_string();
        let _ = CString::new(foo);
    }

    #[test]
    fn test_cstring_as_str() {
        let foo = "foo".to_string();
        let cstring = CString::new(foo);

        assert_eq!(cstring.as_str(), "foo");
    }

    #[test]
    fn test_cstring_into_plain() {
        let foo = "foo".to_string();
        let cstring = CString::new(foo);

        assert_eq!(cstring.into_plain(), "foo".to_string());
    }

    #[test]
    fn test_cstring_into_cbytes() {
        let foo = "foo".to_string();
        let cstring = CString::new(foo);

        assert_eq!(cstring.into_cbytes(), &[0, 3, 102, 111, 111]);
    }

    #[test]
    fn test_cstring_from_cursor() {
        let a = &[0, 3, 102, 111, 111, 0];
        let mut cursor: Cursor<&[u8]> = Cursor::new(a);
        let cstring = CString::from_cursor(&mut cursor).unwrap();
        assert_eq!(cstring.as_str(), "foo");
    }

    // CStringLong
    #[test]
    fn test_cstringlong_new() {
        let foo = "foo".to_string();
        let _ = CStringLong::new(foo);
    }

    #[test]
    fn test_cstringlong_as_str() {
        let foo = "foo".to_string();
        let cstring = CStringLong::new(foo);

        assert_eq!(cstring.as_str(), "foo");
    }

    #[test]
    fn test_cstringlong_into_plain() {
        let foo = "foo".to_string();
        let cstring = CStringLong::new(foo);

        assert_eq!(cstring.into_plain(), "foo".to_string());
    }

    #[test]
    fn test_cstringlong_into_cbytes() {
        let foo = "foo".to_string();
        let cstring = CStringLong::new(foo);

        assert_eq!(cstring.into_cbytes(), &[0, 0, 0, 3, 102, 111, 111]);
    }

    #[test]
    fn test_cstringlong_from_cursor() {
        let a = &[0, 0, 0, 3, 102, 111, 111, 0];
        let mut cursor: Cursor<&[u8]> = Cursor::new(a);
        let cstring = CStringLong::from_cursor(&mut cursor).unwrap();
        assert_eq!(cstring.as_str(), "foo");
    }

    // CStringList
    #[test]
    fn test_cstringlist() {
        let a = &[0, 2, 0, 3, 102, 111, 111, 0, 3, 102, 111, 111];
        let mut cursor: Cursor<&[u8]> = Cursor::new(a);
        let list = CStringList::from_cursor(&mut cursor).unwrap();
        let plain = list.into_plain();
        assert_eq!(plain.len(), 2);
        for s in plain.iter() {
            assert_eq!(s.as_str(), "foo");
        }
    }

    // CBytes
    #[test]
    fn test_cbytes_new() {
        let bytes_vec = vec![1, 2, 3];
        let _ = CBytes::new(bytes_vec);
    }

    #[test]
    fn test_cbytes_into_plain() {
        let cbytes = CBytes::new(vec![1, 2, 3]);
        assert_eq!(cbytes.into_plain().unwrap(), &[1, 2, 3]);
    }

    #[test]
    fn test_cbytes_from_cursor() {
        let a = &[0, 0, 0, 3, 1, 2, 3];
        let mut cursor: Cursor<&[u8]> = Cursor::new(a);
        let cbytes = CBytes::from_cursor(&mut cursor).unwrap();
        assert_eq!(cbytes.into_plain().unwrap(), vec![1, 2, 3]);
    }

    #[test]
    fn test_cbytes_into_cbytes() {
        let bytes_vec = vec![1, 2, 3];
        let cbytes = CBytes::new(bytes_vec);
        assert_eq!(cbytes.into_cbytes(), vec![0, 0, 0, 3, 1, 2, 3]);
    }

    // CBytesShort
    #[test]
    fn test_cbytesshort_new() {
        let bytes_vec = vec![1, 2, 3];
        let _ = CBytesShort::new(bytes_vec);
    }

    #[test]
    fn test_cbytesshort_into_plain() {
        let cbytes = CBytesShort::new(vec![1, 2, 3]);
        assert_eq!(cbytes.into_plain().unwrap(), vec![1, 2, 3]);
    }

    #[test]
    fn test_cbytesshort_from_cursor() {
        let a = &[0, 3, 1, 2, 3];
        let mut cursor: Cursor<&[u8]> = Cursor::new(a);
        let cbytes = CBytesShort::from_cursor(&mut cursor).unwrap();
        assert_eq!(cbytes.into_plain().unwrap(), vec![1, 2, 3]);
    }

    #[test]
    fn test_cbytesshort_into_cbytes() {
        let bytes_vec: Vec<u8> = vec![1, 2, 3];
        let cbytes = CBytesShort::new(bytes_vec);
        assert_eq!(cbytes.into_cbytes(), vec![0, 3, 1, 2, 3]);
    }

    // CInt
    #[test]
    fn test_cint_from_cursor() {
        let a = &[0, 0, 0, 5];
        let mut cursor: Cursor<&[u8]> = Cursor::new(a);
        let i = CInt::from_cursor(&mut cursor).unwrap();
        assert_eq!(i, 5);
    }

    // CIntShort
    #[test]
    fn test_cintshort_from_cursor() {
        let a = &[0, 5];
        let mut cursor: Cursor<&[u8]> = Cursor::new(a);
        let i = CIntShort::from_cursor(&mut cursor).unwrap();
        assert_eq!(i, 5);
    }

    // cursor_next_value
    #[test]
    fn test_cursor_next_value() {
        let a = &[0, 1, 2, 3, 4];
        let mut cursor: Cursor<&[u8]> = Cursor::new(a);
        let l: u64 = 3;
        let val = cursor_next_value(&mut cursor, l).unwrap();
        assert_eq!(val, vec![0, 1, 2]);
    }


    #[test]
    fn test_try_u16_from_bytes() {
        let bytes: [u8; 2] = unsafe { transmute(12u16.to_be()) }; // or .to_le()
        let val = try_u16_from_bytes(&bytes);
        assert_eq!(val.unwrap(), 12u16);
    }

    #[test]
    fn test_from_i_bytes() {
        let bytes: [u8; 8] = unsafe { transmute(12i64.to_be()) }; // or .to_le()
        let val = from_i_bytes(&bytes);
        assert_eq!(val, 12i64);
    }

}
