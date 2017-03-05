use std::ops::Mul;
use std::io;
use std::net;
use std::string::FromUtf8Error;
use uuid;
use super::*;
use FromCursor;


// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L813

// Decodes Cassandra `ascii` data (bytes) into Rust's `Result<String, FromUtf8Error>`.
pub fn decode_custom(bytes: &[u8]) -> Result<String, FromUtf8Error> {
    Ok(String::from_utf8_lossy(bytes).into_owned())
}

// Decodes Cassandra `ascii` data (bytes) into Rust's `Result<String, FromUtf8Error>`.
pub fn decode_ascii(bytes: &[u8]) -> Result<String, FromUtf8Error> {
    Ok(String::from_utf8_lossy(bytes).into_owned())
}

// Decodes Cassandra `varchar` data (bytes) into Rust's `Result<String, FromUtf8Error>`.
pub fn decode_varchar(bytes: &[u8]) -> Result<String, FromUtf8Error> {
    Ok(String::from_utf8_lossy(bytes).into_owned())
}

// Decodes Cassandra `bigint` data (bytes) into Rust's `Result<i32, io::Error>`
pub fn decode_bigint(bytes: &[u8]) -> Result<i64, io::Error> {
    try_from_bytes(bytes).map(|i| i as i64)
}

// Decodes Cassandra `blob` data (bytes) into Rust's `Result<Vec<u8>, io::Error>`
pub fn decode_blob(bytes: Vec<u8>) -> Result<Vec<u8>, io::Error> {
    // in fact we just pass it through.
    Ok(bytes)
}

// Decodes Cassandra `boolean` data (bytes) into Rust's `Result<i32, io::Error>`
pub fn decode_boolean(bytes: &[u8]) -> Result<bool, io::Error> {
    let false_byte: u8 = 0;
    if bytes.is_empty() {
        Err(io::Error::new(io::ErrorKind::UnexpectedEof, "no bytes were found"))
    } else {
        Ok(bytes[0] != false_byte)
    }
}

// Decodes Cassandra `int` data (bytes) into Rust's `Result<i32, io::Error>`
pub fn decode_int(bytes: &[u8]) -> Result<i32, io::Error> {
    try_from_bytes(bytes).map(|i| i as i32)
}

// Decodes Cassandra `date` data (bytes) into Rust's `Result<i32, io::Error>` in following way
//    0: -5877641-06-23
// 2^31: 1970-1-1
// 2^32: 5881580-07-11
pub fn decode_date(bytes: &[u8]) -> Result<i32, io::Error> {
    try_from_bytes(bytes).map(|i| i as i32)
}

// TODO: make sure this method meets the specification.
// Decodes Cassandra `decimal` data (bytes) into Rust's `Result<f32, io::Error>`
pub fn decode_decimal(bytes: &[u8]) -> Result<f32, io::Error> {
    let ref separator = b'E';
    let lr: Vec<Vec<u8>> = bytes.split(|ch| ch == separator).map(|p| p.to_vec()).collect();
    let unscaled = try_i_from_bytes(lr[0].as_slice());
    if unscaled.is_err() {
        return Err(unscaled.unwrap_err());
    }
    let scaled = try_i_from_bytes(lr[1].as_slice());
    if scaled.is_err() {
        return Err(scaled.unwrap_err());
    }

    let unscaled_unwrapped: f32 = unscaled.unwrap() as f32;
    let scaled_unwrapped: i32 = scaled.unwrap() as i32;
    let dec: f32 = 10.0;
    Ok(unscaled_unwrapped.mul(dec.powi(scaled_unwrapped)))
}

// Decodes Cassandra `double` data (bytes) into Rust's `Result<f32, io::Error>`
pub fn decode_double(bytes: &[u8]) -> Result<f64, io::Error> {
    try_f64_from_bytes(bytes)
}

// Decodes Cassandra `float` data (bytes) into Rust's `Result<f32, io::Error>`
pub fn decode_float(bytes: &[u8]) -> Result<f32, io::Error> {
    try_f32_from_bytes(bytes)
}

// Decodes Cassandra `inet` data (bytes) into Rust's `Result<net::IpAddr, io::Error>`
pub fn decode_inet(bytes: &[u8]) -> Result<net::IpAddr, io::Error> {
    match bytes.len() {
        // v4
        4 => Ok(net::IpAddr::V4(net::Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3]))),
        // v6
        16 => {
            let a = from_u16_bytes(&bytes[0..2]);
            let b = from_u16_bytes(&bytes[2..4]);
            let c = from_u16_bytes(&bytes[4..6]);
            let d = from_u16_bytes(&bytes[6..8]);
            let e = from_u16_bytes(&bytes[8..10]);
            let f = from_u16_bytes(&bytes[10..12]);
            let g = from_u16_bytes(&bytes[12..14]);
            let h = from_u16_bytes(&bytes[14..16]);
            Ok(net::IpAddr::V6(net::Ipv6Addr::new(a, b, c, d, e, f, g, h)))
        }
        _ => unreachable!(),
    }
}

// Decodes Cassandra `timestamp` data (bytes) into Rust's `Result<i64, io::Error>`
// `i32` represets a millisecond-precision
//  offset from the unix epoch (00:00:00, January 1st, 1970).  Negative values
//  represent a negative offset from the epoch.
pub fn decode_timestamp(bytes: &[u8]) -> Result<i64, io::Error> {
    try_from_bytes(bytes).map(|i| i as i64)
}

// Decodes Cassandra `list` data (bytes) into Rust's `Result<Vec<CBytes>, io::Error>`
pub fn decode_list(bytes: &[u8]) -> Result<Vec<CBytes>, io::Error> {
    let mut cursor: io::Cursor<&[u8]> = io::Cursor::new(bytes);
    let l = CInt::from_cursor(&mut cursor);
    let list = (0..l).map(|_| CBytes::from_cursor(&mut cursor)).collect();
    Ok(list)
}

// Decodes Cassandra `set` data (bytes) into Rust's `Result<Vec<CBytes>, io::Error>`
pub fn decode_set(bytes: &[u8]) -> Result<Vec<CBytes>, io::Error> {
    let mut cursor: io::Cursor<&[u8]> = io::Cursor::new(bytes);
    let l = CInt::from_cursor(&mut cursor);
    let list = (0..l).map(|_| CBytes::from_cursor(&mut cursor)).collect();
    Ok(list)
}

// Decodes Cassandra `map` data (bytes) into Rust's `Result<Vec<(CBytes, CBytes)>, io::Error>`
pub fn decode_map(bytes: &[u8]) -> Result<Vec<(CBytes, CBytes)>, io::Error> {
    let mut cursor: io::Cursor<&[u8]> = io::Cursor::new(bytes);
    let l = CInt::from_cursor(&mut cursor);
    let list = (0..l)
        .map(|_| (CBytes::from_cursor(&mut cursor), CBytes::from_cursor(&mut cursor)))
        .collect();
    Ok(list)
}

// Decodes Cassandra `smallint` data (bytes) into Rust's `Result<i16, io::Error>`
pub fn decode_smallint(bytes: &[u8]) -> Result<i16, io::Error> {
    try_from_bytes(bytes).map(|i| i as i16)
}

// Decodes Cassandra `tinyint` data (bytes) into Rust's `Result<i8, io::Error>`
pub fn decode_tinyint(bytes: &[u8]) -> Result<i8, io::Error> {
    Ok(bytes[0] as i8)
}

// Decodes Cassandra `text` data (bytes) into Rust's `Result<String, FromUtf8Error>`.
pub fn decode_text(bytes: &[u8]) -> Result<String, FromUtf8Error> {
    Ok(String::from_utf8_lossy(bytes).into_owned())
}

// Decodes Cassandra `time` data (bytes) into Rust's `Result<String, FromUtf8Error>`.
pub fn decode_time(bytes: &[u8]) -> Result<i64, io::Error> {
    try_i_from_bytes(bytes)
}

// Decodes Cassandra `timeuuid` data (bytes) into Rust's `Result<uuid::Uuid, uuid::ParseError>`
pub fn decode_timeuuid(bytes: &[u8]) -> Result<uuid::Uuid, uuid::ParseError> {
    uuid::Uuid::from_bytes(bytes)
}

// Decodes Cassandra `varint` data (bytes) into Rust's `Result<i64, io::Error>`
pub fn decode_varint(bytes: &[u8]) -> Result<i64, io::Error> {
    try_i_from_bytes(bytes)
}

// Decodes Cassandra `Udt` data (bytes) into Rust's `Result<Vec<CBytes>, io::Error>`
// each `CBytes` is encoded type of field of user defined type
pub fn decode_udt(bytes: &[u8], l: usize) -> Result<Vec<CBytes>, io::Error> {
    let mut cursor: io::Cursor<&[u8]> = io::Cursor::new(bytes);
    let list = (0..l).map(|_| CBytes::from_cursor(&mut cursor)).collect();
    Ok(list)
}

/// Decodes any Cassandra data type into the corresponding Rust type,
/// given the column type as `ColTypeOption` and the value as `CBytes`
/// plus the matching Rust type.
macro_rules! as_rust {
    ($data_type_option:ident, $data_value:ident, Vec<u8>) => (
        match $data_type_option.id {
            ColType::Blob => {
                decode_blob($data_value.as_plain())
                    .map_err(Into::into)
            }
            _ => Err(Error::General(format!("Invalid conversion. \
                    Cannot convert {:?} into Vec<u8> (valid types: Blob).",
                    $data_type_option.id)))
        }
    );
    ($data_type_option:ident, $data_value:ident, String) => (
        match $data_type_option.id {
            ColType::Custom => {
                decode_custom($data_value.as_slice())
                    .map_err(Into::into)
            }
            ColType::Ascii => {
                decode_ascii($data_value.as_slice())
                    .map_err(Into::into)
            }
            ColType::Varchar => {
                decode_varchar($data_value.as_slice())
                    .map_err(Into::into)
            }
            // TODO: clarify when to use decode_text.
            // it's not mentioned in
            // https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L582
            // ColType::XXX => decode_text($data_value)?
            _ => Err(Error::General(format!("Invalid conversion. \
                    Cannot convert {:?} into String (valid types: Custom, Ascii, Varchar).",
                    $data_type_option.id)))
        }
    );
    ($data_type_option:ident, $data_value:ident, bool) => (
        match $data_type_option.id {
            ColType::Boolean => {
                decode_boolean($data_value.as_slice())
                    .map_err(Into::into)
            }
            _ => Err(Error::General(format!("Invalid conversion. \
                    Cannot convert {:?} into bool (valid types: Boolean).",
                    $data_type_option.id)))
        }
    );
    ($data_type_option:ident, $data_value:ident, i64) => (
        match $data_type_option.id {
            ColType::Bigint => {
                decode_bigint($data_value.as_slice())
                    .map_err(Into::into)
            }
            ColType::Timestamp => {
                decode_timestamp($data_value.as_slice())
                    .map_err(Into::into)
            }
            ColType::Time => {
                decode_time($data_value.as_slice())
                    .map_err(Into::into)
            }
            ColType::Varint => {
                decode_varint($data_value.as_slice())
                    .map_err(Into::into)
            }
            _ => Err(Error::General(format!("Invalid conversion. \
                    Cannot convert {:?} into i64 (valid types: Bigint, Timestamp, Time, Variant).",
                    $data_type_option.id)))
        }
    );
    ($data_type_option:ident, $data_value:ident, i32) => (
        match $data_type_option.id {
            ColType::Int => {
                decode_int($data_value.as_slice())
                    .map_err(Into::into)
            }
            ColType::Date => {
                decode_date($data_value.as_slice())
                    .map_err(Into::into)
            }
            _ => Err(Error::General(format!("Invalid conversion. \
                    Cannot convert {:?} into i32 (valid types: Int, Date).",
                    $data_type_option.id)))
        }
    );
    ($data_type_option:ident, $data_value:ident, i16) => (
        match $data_type_option.id {
            ColType::Smallint => {
                decode_smallint($data_value.as_slice())
                    .map_err(Into::into)
            }
            _ => Err(Error::General(format!("Invalid conversion. \
                    Cannot convert {:?} into i16 (valid types: Smallint).",
                    $data_type_option.id)))
        }
    );
    ($data_type_option:ident, $data_value:ident, i8) => (
        match $data_type_option.id {
            ColType::Tinyint => {
                decode_tinyint($data_value.as_slice())
                    .map_err(Into::into)
            }
            _ => Err(Error::General(format!("Invalid conversion. \
                    Cannot convert {:?} into i8 (valid types: Tinyint).",
                    $data_type_option.id)))
        }
    );
    ($data_type_option:ident, $data_value:ident, f64) => (
        match $data_type_option.id {
            ColType::Double => {
                decode_double($data_value.as_slice())
                    .map_err(Into::into)
            }
            _ => Err(Error::General(format!("Invalid conversion. \
                    Cannot convert {:?} into f64 (valid types: Double).",
                    $data_type_option.id)))
        }
    );
    ($data_type_option:ident, $data_value:ident, f32) => (
        match $data_type_option.id {
            ColType::Decimal => {
                decode_decimal($data_value.as_slice())
                    .map_err(Into::into)
            }
            ColType::Float => {
                decode_float($data_value.as_slice())
                    .map_err(Into::into)
            }
            _ => Err(Error::General(format!("Invalid conversion. \
                    Cannot convert {:?} into f32 (valid types: Decimal, Float).",
                    $data_type_option.id)))
        }
    );
    ($data_type_option:ident, $data_value:ident, IpAddr) => (
        match $data_type_option.id {
            ColType::Inet => {
                decode_inet($data_value.as_slice())
                    .map_err(Into::into)
            }
            _ => Err(Error::General(format!("Invalid conversion. \
                    Cannot convert {:?} into IpAddr (valid types: Inet).",
                    $data_type_option.id)))
        }
    );
    ($data_type_option:ident, $data_value:ident, Uuid) => (
        match $data_type_option.id {
            ColType::Uuid |
            ColType::Timeuuid => {
                decode_timeuuid($data_value.as_slice())
                    .map_err(Into::into)
            }
            _ => Err(Error::General(format!("Invalid conversion. \
                    Cannot convert {:?} into Uuid (valid types: Uuid, Timeuuid).",
                    $data_type_option.id)))
        }
    );
    ($data_type_option:ident, $data_value:ident, List) => (
        match $data_type_option.id {
            ColType::List |
            ColType::Set => {
                decode_list($data_value.as_slice())
                    .map(|data| List::new(data, $data_type_option.clone()))
                    .map_err(Into::into)
            }
            _ => Err(Error::General(format!("Invalid conversion. \
                    Cannot convert {:?} into List (valid types: List, Set).",
                    $data_type_option.id)))
        }
    );
    ($data_type_option:ident, $data_value:ident, Map) => (
        match $data_type_option.id {
            ColType::Map => {
                decode_map($data_value.as_slice())
                    .map(|data| Map::new(data, $data_type_option.clone()))
                    .map_err(Into::into)
            }
            _ => Err(Error::General(format!("Invalid conversion. \
                    Cannot convert {:?} into Map (valid types: Map).",
                    $data_type_option.id)))
        }
    );
    ($data_type_option:ident, $data_value:ident, UDT) => (
        match *$data_type_option {
            ColTypeOption {
                id: ColType::Udt,
                value: Some(ColTypeOptionValue::UdtType(ref list_type_option))
            } => {
                decode_udt($data_value.as_slice(), list_type_option.descriptions.len())
                    .map(|data| UDT::new(data, list_type_option))
                    .map_err(Into::into)
            }
            _ => Err(Error::General(format!("Invalid conversion. \
                    Cannot convert {:?} into UDT (valid types: UDT).",
                    $data_type_option.id)))
        }
    );
}

macro_rules! into_rust_by_name {
    (Row, $($into_type:tt)*) => (
        impl IntoRustByName<$($into_type)*> for Row {
            fn get_by_name(&self, name: &str) -> Option<Result<$($into_type)*>> {
                self.get_col_spec_by_name(name)
                    .map(|(col_spec, cbytes)| {
                        if cbytes.is_empty() {
                            return Err(column_is_empty_err());
                        }

                        let ref col_type = col_spec.col_type;
                        as_rust!(col_type, cbytes, $($into_type)*)
                    })
            }
        }
    );

    (UDT, $($into_type:tt)*) => (
        impl IntoRustByName<$($into_type)*> for UDT {
            fn get_by_name(&self, name: &str) -> Option<Result<$($into_type)*>> {
                self.data.get(name).map(|v| {
                    let &(ref col_type, ref bytes) = v;

                    if bytes.as_plain().is_empty() {
                        return Err(column_is_empty_err());
                    }

                    let converted = as_rust!(col_type, bytes, $($into_type)*);
                    converted.map_err(|err| err.into())
                })
            }
        }
    );
}
