use std::io::Cursor;

use crate::error;
use crate::query;
use crate::types::rows::Row;
use crate::types::udt::UDT;

/// `IntoBytes` should be used to convert a structure into array of bytes.
pub trait IntoBytes {
    /// It should convert a struct into an array of bytes.
    fn into_cbytes(&self) -> Vec<u8>;
}

/// `FromBytes` should be used to parse an array of bytes into a structure.
pub trait FromBytes {
    /// It gets and array of bytes and should return an implementor struct.
    fn from_bytes(bytes: &[u8]) -> error::Result<Self>
    where
        Self: Sized;
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
    fn from_byte(byte: u8) -> Self;
}

/// `FromCursor` should be used to get parsed structure from an `io:Cursor`
/// wich bound to an array of bytes.
pub trait FromCursor {
    /// It should return an implementor from an `io::Cursor` over an array of bytes.
    fn from_cursor(cursor: &mut Cursor<&[u8]>) -> error::Result<Self>
    where
        Self: Sized;
}

/// The trait that allows transformation of `Self` to CDRS query values.
pub trait IntoQueryValues {
    fn into_query_values(self) -> query::QueryValues;
}

pub trait TryFromRow: Sized {
    fn try_from_row(row: Row) -> error::Result<Self>;
}

pub trait TryFromUDT: Sized {
    fn try_from_udt(udt: UDT) -> error::Result<Self>;
}
