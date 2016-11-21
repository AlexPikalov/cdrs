use std::ops::Mul;
use std::io;
use std::net;
use std::string::FromUtf8Error;
use uuid;
use super::types::*;
use super::FromCursor;

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L813

// Decodes Cassandra `varchar` data (bytes) into Rust's `Result<String, FromUtf8Error>`.
pub fn decode_varchar(bytes: Vec<u8>) -> Result<String, FromUtf8Error> {
    return String::from_utf8(bytes);
}

// Decodes Cassandra `bigint` data (bytes) into Rust's `Result<i32, io::Error>`
pub fn decode_bigint(bytes: Vec<u8>) -> Result<i64, io::Error> {
    return try_from_bytes(bytes).map(|i| i as i64);
}

// Decodes Cassandra `blob` data (bytes) into Rust's `Result<Vec<u8>, io::Error>`
pub fn decode_blob(bytes: Vec<u8>) -> Result<Vec<u8>, io::Error> {
    // in fact we just pass it through.
    return Ok(bytes);
}

// Decodes Cassandra `boolean` data (bytes) into Rust's `Result<i32, io::Error>`
pub fn decode_boolean(bytes: Vec<u8>) -> Result<bool, io::Error> {
    let false_byte: u8 = 0;
    return bytes.first()
        .ok_or(io::Error::new(io::ErrorKind::UnexpectedEof, "no bytes were found"))
        .map(|b| b != &false_byte);
}

// Decodes Cassandra `int` data (bytes) into Rust's `Result<i32, io::Error>`
pub fn decode_int(bytes: Vec<u8>) -> Result<i32, io::Error> {
    return try_from_bytes(bytes).map(|i| i as i32);
}

// Decodes Cassandra `date` data (bytes) into Rust's `Result<i32, io::Error>` in following way
//    0: -5877641-06-23
// 2^31: 1970-1-1
// 2^32: 5881580-07-11
pub fn decode_date(bytes: Vec<u8>) -> Result<i32, io::Error> {
    return try_from_bytes(bytes).map(|i| i as i32);
}

// TODO: make sure this method meets the specification.
// Decodes Cassandra `decimal` data (bytes) into Rust's `Result<f32, io::Error>`
pub fn decode_decimal(bytes: Vec<u8>) -> Result<f32, io::Error> {
    let ref separator = b'E';
    let lr: Vec<Vec<u8>> = bytes.split(|ch| ch == separator).map(|p| p.to_vec()).collect();
    let unscaled = try_i_from_bytes(lr[0].clone());
    if unscaled.is_err() {
        return Err(unscaled.unwrap_err());
    }
    let scaled = try_i_from_bytes(lr[1].clone());
    if scaled.is_err() {
        return Err(scaled.unwrap_err());
    }

    let unscaled_unwrapped: f32 = unscaled.unwrap() as f32;
    let scaled_unwrapped: i32 = scaled.unwrap() as i32;
    let dec: f32 = 10.0;
    return Ok(unscaled_unwrapped.mul(dec.powi(scaled_unwrapped)));
}

// Decodes Cassandra `double` data (bytes) into Rust's `Result<f32, io::Error>`
pub fn decode_double(bytes: Vec<u8>) -> Result<f64, io::Error> {
    return try_f64_from_bytes(bytes);
}

// Decodes Cassandra `float` data (bytes) into Rust's `Result<f32, io::Error>`
pub fn decode_float(bytes: Vec<u8>) -> Result<f32, io::Error> {
    return try_f32_from_bytes(bytes);
}

// Decodes Cassandra `inet` data (bytes) into Rust's `Result<net::IpAddr, io::Error>`
pub fn decode_inet(bytes: Vec<u8>) -> Result<net::IpAddr, io::Error> {
    return match bytes.len() {
        // v4
        4 => {
            Ok(net::IpAddr::V4(net::Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3])))
        },
        // v6
        16 => {
            let a = from_u16_bytes(bytes[0..2].to_vec());
            let b = from_u16_bytes(bytes[2..4].to_vec());
            let c = from_u16_bytes(bytes[4..6].to_vec());
            let d = from_u16_bytes(bytes[6..8].to_vec());
            let e = from_u16_bytes(bytes[8..10].to_vec());
            let f = from_u16_bytes(bytes[10..12].to_vec());
            let g = from_u16_bytes(bytes[12..14].to_vec());
            let h = from_u16_bytes(bytes[14..16].to_vec());
            Ok(net::IpAddr::V6(net::Ipv6Addr::new(a, b, c, d, e, f, g, h)))
        },
        _ => unreachable!()
    };
}

// Decodes Cassandra `timestamp` data (bytes) into Rust's `Result<i32, io::Error>`
// `i32` represets a millisecond-precision
//  offset from the unix epoch (00:00:00, January 1st, 1970).  Negative values
//  represent a negative offset from the epoch.
pub fn decode_timestamp(bytes: Vec<u8>) -> Result<i64, io::Error> {
    return try_from_bytes(bytes).map(|i| i as i64);
}

// Decodes Cassandra `list` data (bytes) into Rust's `Result<Vec<CBytes>, io::Error>`
pub fn decode_list(bytes: Vec<u8>) -> Result<Vec<CBytes>, io::Error> {
    let mut cursor: io::Cursor<Vec<u8>> = io::Cursor::new(bytes);
    let l = CInt::from_cursor(&mut cursor);
    let list = (0..l).map(|_| CBytes::from_cursor(&mut cursor)).collect();
    return Ok(list);
}

// Decodes Cassandra `set` data (bytes) into Rust's `Result<Vec<CBytes>, io::Error>`
pub fn decode_set(bytes: Vec<u8>) -> Result<Vec<CBytes>, io::Error> {
    let mut cursor: io::Cursor<Vec<u8>> = io::Cursor::new(bytes);
    let l = CInt::from_cursor(&mut cursor);
    let list = (0..l).map(|_| CBytes::from_cursor(&mut cursor)).collect();
    return Ok(list);
}

// Decodes Cassandra `map` data (bytes) into Rust's `Result<Vec<(CBytes, CBytes)>, io::Error>`
pub fn decode_map(bytes: Vec<u8>) -> Result<Vec<(CBytes, CBytes)>, io::Error> {
    let mut cursor: io::Cursor<Vec<u8>> = io::Cursor::new(bytes);
    let l = CInt::from_cursor(&mut cursor);
    let list = (0..l).map(|_| {
        return (CBytes::from_cursor(&mut cursor), CBytes::from_cursor(&mut cursor));
    }).collect();
    return Ok(list);
}

// Decodes Cassandra `smallint` data (bytes) into Rust's `Result<i16, io::Error>`
pub fn decode_smallint(bytes: Vec<u8>) -> Result<i16, io::Error> {
    return try_from_bytes(bytes).map(|i| i as i16);
}

// Decodes Cassandra `text` data (bytes) into Rust's `Result<String, FromUtf8Error>`.
pub fn decode_text(bytes: Vec<u8>) -> Result<String, FromUtf8Error> {
    return String::from_utf8(bytes);
}

// Decodes Cassandra `time` data (bytes) into Rust's `Result<String, FromUtf8Error>`.
pub fn decode_time(bytes: Vec<u8>) -> Result<i64, io::Error> {
    return try_i_from_bytes(bytes);
}

// Decodes Cassandra `timeuuid` data (bytes) into Rust's `Result<uuid::Uuid, uuid::ParseError>`
pub fn decode_timeuuid(bytes: Vec<u8>) -> Result<uuid::Uuid, uuid::ParseError> {
    return uuid::Uuid::from_bytes(bytes.as_slice());
}

// Decodes Cassandra `varint` data (bytes) into Rust's `Result<i64, io::Error>`
pub fn decode_varint(bytes: Vec<u8>) -> Result<i64, io::Error> {
    return try_i_from_bytes(bytes);
}

// Decodes Cassandra `Udt` data (bytes) into Rust's `Result<Vec<CBytes>, io::Error>`
// each `CBytes` is encoded type of field of user defined type
pub fn decode_udt(bytes: Vec<u8>) -> Result<Vec<CBytes>, io::Error> {
    let mut cursor: io::Cursor<Vec<u8>> = io::Cursor::new(bytes);
    let l = CInt::from_cursor(&mut cursor);
    let list = (0..l).map(|_| CBytes::from_cursor(&mut cursor)).collect();
    return Ok(list);
}

#[macro_export]
macro_rules! cdrs_decode_value {
    ($val: expr, ColType::Ascii) => {
        {
            decode_varchar($val.clone())
        }
    };
    ($val: expr, ColType::Bigint) => {
        {
            decode_bigint($val.clone())
        }
    };
    ($val: expr, ColType::Blob) => {
        {
            decode_blob($val.clone())
        }
    };
    ($val: expr, ColType::Boolean) => {
        {
            decode_boolean($val.clone())
        }
    };
    ($val: expr, ColType::Boolean) => {
        {
            decode_boolean($val.clone())
        }
    };
    ($val: expr, ColType::Decimal) => {
        {
            decode_decimal($val.clone())
        }
    };
    ($val: expr, ColType::Double) => {
        {
            decode_double($val.clone())
        }
    };
    ($val: expr, ColType::Float) => {
        {
            decode_float($val.clone())
        }
    };
    ($val: expr, ColType::Float) => {
        {
            decode_int($val.clone())
        }
    };
    ($val: expr, ColType::Timestamp) => {
        {
            decode_timestamp($val.clone())
        }
    };
    ($val: expr, ColType::Uuid) => {
        {
            decode_timeuuid($val.clone())
        }
    };
    ($val: expr, ColType::Varchar) => {
        {
            decode_varchar($val.clone())
        }
    };
    ($val: expr, ColType::Varint) => {
        {
            decode_varint($val.clone())
        }
    };
    ($val: expr, ColType::Timeuuid) => {
        {
            decode_timeuuid($row[i].clone())
        }
    };
    ($row: expr, ColType::Inet) => {
        {
            decode_inet($val.clone())
        }
    };
    ($val: expr, ColType::Date) => {
        {
            decode_date($val.clone())
        }
    };
    ($val: expr, ColType::Time) => {
        {
            decode_time($val.clone())
        }
    };
    ($val: expr, ColType::Smallint) => {
        {
            decode_smallint($val.clone())
        }
    };
    ($val: expr, ColType::List) => {
        {
            decode_list($val.clone())
        }
    };
    ($val: expr, ColType::Map) => {
        {
            decode_map($val.clone())
        }
    };
    ($val: expr, ColType::Set) => {
        {
            decode_set($val.clone())
        }
    };
    ($val: expr, ColType::Udt) => {
        {
            decode_udt($val.clone())
        }
    };
    ($val: expr, ColType::Tuple) => {
        {
            unimplemented!()
        }
    };
}
