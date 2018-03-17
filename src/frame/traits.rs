use std::io::Cursor;

use error;
use types;
use types::rows;
use query;

/// `IntoBytes` should be used to convert a structure into array of bytes.
pub trait IntoBytes {
  /// It should convert a struct into an array of bytes.
  fn into_cbytes(&self) -> Vec<u8>;
}

/// `FromBytes` should be used to parse an array of bytes into a structure.
pub trait FromBytes {
  /// It gets and array of bytes and should return an implementor struct.
  fn from_bytes(&[u8]) -> error::Result<Self>
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
  fn from_byte(u8) -> Self;
}

/// `FromCursor` should be used to get parsed structure from an `io:Cursor`
/// wich bound to an array of bytes.
pub trait FromCursor {
  /// It should return an implementor from an `io::Cursor` over an array of bytes.
  fn from_cursor(&mut Cursor<&[u8]>) -> error::Result<Self>
  where
    Self: Sized;
}

/// The trait that allows transformation of `Self` to `types::value::Value`.
pub trait IntoCDRSValue {
  /// It converts `Self` to `types::value::Value`.
  fn into_cdrs_value(self) -> types::value::Value;
}

impl<T: Into<types::value::Bytes>> IntoCDRSValue for T {
  fn into_cdrs_value(self) -> types::value::Value {
    let bytes: types::value::Bytes = self.into();
    bytes.into()
  }
}

/// The trait that allows transformation of `Self` to CDRS query values.
pub trait IntoQueryValues {
  fn into_query_values(self) -> query::QueryValues;
}

// The trait that tries to transform a CDRS `Row` into a structure of given type.
pub trait TryFromGetByName: Sized {
  fn try_from_get_by_name(with_get_by_name: types::IntoRustByName<Self>) -> error::Result<Self>;
}
