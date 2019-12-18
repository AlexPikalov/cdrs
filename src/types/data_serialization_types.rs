use std::io;
use std::net;
use std::string::FromUtf8Error;

use super::blob::Blob;
use super::decimal::Decimal;
use super::*;
use crate::error;
use crate::frame::FromCursor;
use uuid;

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
pub fn decode_blob(bytes: &Vec<u8>) -> Result<Blob, io::Error> {
    // in fact we just pass it through.
    Ok(bytes.clone().into())
}

// Decodes Cassandra `boolean` data (bytes) into Rust's `Result<i32, io::Error>`
pub fn decode_boolean(bytes: &[u8]) -> Result<bool, io::Error> {
    let false_byte: u8 = 0;
    if bytes.is_empty() {
        Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "no bytes were found",
        ))
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

// Decodes Cassandra `decimal` data (bytes) into Rust's `Result<f32, io::Error>`
pub fn decode_decimal(bytes: &[u8]) -> Result<Decimal, io::Error> {
    let lr = bytes.split_at(INT_LEN);

    let scale = try_i_from_bytes(lr.0)? as u32;
    let unscaled = try_i_from_bytes(lr.1)?;

    Ok(Decimal::new(unscaled, scale))
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
        4 => Ok(net::IpAddr::V4(net::Ipv4Addr::new(
            bytes[0], bytes[1], bytes[2], bytes[3],
        ))),
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
        _ => {
            // let message = format!("Unparseable  Ip address {:?}", bytes);
            Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Unparseable  Ip address {:?}", bytes),
            ))
        }
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
    let l = CInt::from_cursor(&mut cursor)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let mut list = Vec::with_capacity(l as usize);
    for _ in 0..l {
        let b = CBytes::from_cursor(&mut cursor)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        list.push(b);
    }
    Ok(list)
}

// Decodes Cassandra `set` data (bytes) into Rust's `Result<Vec<CBytes>, io::Error>`
pub fn decode_set(bytes: &[u8]) -> Result<Vec<CBytes>, io::Error> {
    decode_list(bytes)
}

// Decodes Cassandra `map` data (bytes) into Rust's `Result<Vec<(CBytes, CBytes)>, io::Error>`
pub fn decode_map(bytes: &[u8]) -> Result<Vec<(CBytes, CBytes)>, io::Error> {
    let mut cursor: io::Cursor<&[u8]> = io::Cursor::new(bytes);
    let l = CInt::from_cursor(&mut cursor)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    let mut map = Vec::with_capacity(l as usize);
    for _ in 0..l {
        let n = CBytes::from_cursor(&mut cursor)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        let v = CBytes::from_cursor(&mut cursor)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        map.push((n, v));
    }
    Ok(map)
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

// Decodes Cassandra `timeuuid` data (bytes) into Rust's `Result<uuid::Uuid, uuid::Error>`
pub fn decode_timeuuid(bytes: &[u8]) -> Result<uuid::Uuid, uuid::Error> {
    uuid::Uuid::from_slice(bytes)
}

// Decodes Cassandra `varint` data (bytes) into Rust's `Result<i64, io::Error>`
pub fn decode_varint(bytes: &[u8]) -> Result<i64, io::Error> {
    try_i_from_bytes(bytes)
}

// Decodes Cassandra `Udt` data (bytes) into Rust's `Result<Vec<CBytes>, io::Error>`
// each `CBytes` is encoded type of field of user defined type
pub fn decode_udt(bytes: &[u8], l: usize) -> Result<Vec<CBytes>, io::Error> {
    let mut cursor: io::Cursor<&[u8]> = io::Cursor::new(bytes);
    let mut udt = Vec::with_capacity(l);
    for _ in 0..l {
        let v = CBytes::from_cursor(&mut cursor)
            .or_else(|err| match err {
                error::Error::Io(io_err) => {
                    if io_err.kind() == io::ErrorKind::UnexpectedEof {
                        Ok(CBytes::new_empty())
                    } else {
                        Err(io_err.into())
                    }
                }
                _ => Err(err),
            })
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        udt.push(v);
    }
    Ok(udt)
}

// Decodes Cassandra `Tuple` data (bytes) into Rust's `Result<Vec<CBytes>, io::Error>`
// each `CBytes` is encoded type of field of user defined type
pub fn decode_tuple(bytes: &[u8], l: usize) -> Result<Vec<CBytes>, io::Error> {
    let mut cursor: io::Cursor<&[u8]> = io::Cursor::new(bytes);
    let mut udt = Vec::with_capacity(l);
    for _ in 0..l {
        let v = CBytes::from_cursor(&mut cursor)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        udt.push(v);
    }
    Ok(udt)
}

#[cfg(test)]
mod tests {
    use super::super::super::error::*;
    use super::super::super::frame::frame_result::*;
    use super::*;
    use std::net::IpAddr;

    #[test]
    fn decode_custom_test() {
        assert_eq!(decode_custom(b"abcd").unwrap(), "abcd".to_string());
    }

    #[test]
    fn decode_ascii_test() {
        assert_eq!(decode_ascii(b"abcd").unwrap(), "abcd".to_string());
    }

    #[test]
    fn decode_varchar_test() {
        assert_eq!(decode_varchar(b"abcd").unwrap(), "abcd".to_string());
    }

    #[test]
    fn decode_bigint_test() {
        assert_eq!(decode_bigint(&[0, 0, 0, 0, 0, 0, 0, 3]).unwrap(), 3);
    }

    #[test]
    fn decode_blob_test() {
        assert_eq!(
            decode_blob(&vec![0, 0, 0, 3]).unwrap().into_vec(),
            vec![0, 0, 0, 3]
        );
    }

    #[test]
    fn decode_boolean_test() {
        assert_eq!(decode_boolean(&[0]).unwrap(), false);
        assert_eq!(decode_boolean(&[1]).unwrap(), true);
        assert!(decode_boolean(&[]).is_err());
    }

    #[test]
    fn decode_int_test() {
        assert_eq!(decode_int(&[0, 0, 0, 3]).unwrap(), 3);
    }

    #[test]
    fn decode_date_test() {
        assert_eq!(decode_date(&[0, 0, 0, 3]).unwrap(), 3);
    }

    #[test]
    fn decode_double_test() {
        let bytes = to_float_big(0.3);
        assert_eq!(decode_double(bytes.as_slice()).unwrap(), 0.3);
    }

    #[test]
    fn decode_float_test() {
        let bytes = to_float(0.3);
        assert_eq!(decode_float(bytes.as_slice()).unwrap(), 0.3);
    }

    #[test]
    fn decode_inet_test() {
        let bytes_v4 = &[0, 0, 0, 0];
        match decode_inet(bytes_v4) {
            Ok(IpAddr::V4(ref ip)) => assert_eq!(ip.octets(), [0, 0, 0, 0]),
            _ => panic!("wrong ip v4 address"),
        }

        let bytes_v6 = &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        match decode_inet(bytes_v6) {
            Ok(IpAddr::V6(ref ip)) => assert_eq!(ip.segments(), [0, 0, 0, 0, 0, 0, 0, 0]),
            _ => panic!("wrong ip v6 address"),
        };
    }

    #[test]
    fn decode_timestamp_test() {
        assert_eq!(decode_timestamp(&[0, 0, 0, 0, 0, 0, 0, 3]).unwrap(), 3);
    }

    #[test]
    fn decode_list_test() {
        let results = decode_list(&[0, 0, 0, 1, 0, 0, 0, 2, 1, 2]).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].as_plain().unwrap(), vec![1, 2]);
    }

    #[test]
    fn decode_set_test() {
        let results = decode_set(&[0, 0, 0, 1, 0, 0, 0, 2, 1, 2]).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].as_plain().unwrap(), vec![1, 2]);
    }

    #[test]
    fn decode_map_test() {
        let results = decode_map(&[0, 0, 0, 1, 0, 0, 0, 2, 1, 2, 0, 0, 0, 2, 2, 1]).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0.as_plain().unwrap(), vec![1, 2]);
        assert_eq!(results[0].1.as_plain().unwrap(), vec![2, 1]);
    }

    #[test]
    fn decode_smallint_test() {
        assert_eq!(decode_smallint(&[0, 10]).unwrap(), 10);
    }

    #[test]
    fn decode_tinyint_test() {
        assert_eq!(decode_tinyint(&[10]).unwrap(), 10);
    }

    #[test]
    fn decode_decimal_test() {
        assert_eq!(
            decode_decimal(&[0, 0, 0, 0, 10u8]).unwrap(),
            Decimal::new(10, 0)
        );

        assert_eq!(
            decode_decimal(&[0, 0, 0, 0, 0x00, 0x81]).unwrap(),
            Decimal::new(129, 0)
        );

        assert_eq!(
            decode_decimal(&[0, 0, 0, 0, 0xFF, 0x7F]).unwrap(),
            Decimal::new(-129, 0)
        );

        assert_eq!(
            decode_decimal(&[0, 0, 0, 1, 0x00, 0x81]).unwrap(),
            Decimal::new(129, 1)
        );

        assert_eq!(
            decode_decimal(&[0, 0, 0, 1, 0xFF, 0x7F]).unwrap(),
            Decimal::new(-129, 1)
        );
    }

    #[test]
    fn decode_text_test() {
        assert_eq!(decode_text(b"abcba").unwrap(), "abcba");
    }

    #[test]
    fn decode_time_test() {
        assert_eq!(decode_time(&[0, 0, 0, 0, 0, 0, 0, 10]).unwrap(), 10);
    }

    #[test]
    fn decode_timeuuid_test() {
        assert_eq!(
            decode_timeuuid(&[4, 54, 67, 12, 43, 2, 98, 76, 32, 50, 87, 5, 1, 33, 43, 87])
                .unwrap()
                .as_bytes(),
            &[4, 54, 67, 12, 43, 2, 98, 76, 32, 50, 87, 5, 1, 33, 43, 87]
        );
    }

    #[test]
    fn decode_varint_test() {
        assert_eq!(decode_varint(&[0x00]).unwrap(), 0);
        assert_eq!(decode_varint(&[0x01]).unwrap(), 1);
        assert_eq!(decode_varint(&[0x7F]).unwrap(), 127);
        assert_eq!(decode_varint(&[0x00, 0x80]).unwrap(), 128);
        assert_eq!(decode_varint(&[0x00, 0x81]).unwrap(), 129);
        assert_eq!(decode_varint(&[0xFF]).unwrap(), -1);
        assert_eq!(decode_varint(&[0x80]).unwrap(), -128);
        assert_eq!(decode_varint(&[0xFF, 0x7F]).unwrap(), -129);
    }

    #[test]
    fn decode_udt_test() {
        let udt = decode_udt(&[0, 0, 0, 2, 1, 2], 1).unwrap();
        assert_eq!(udt.len(), 1);
        assert_eq!(udt[0].as_plain().unwrap(), vec![1, 2]);
    }

    #[test]
    fn as_rust_blob_test() {
        let d_type = DataType { id: ColType::Blob };
        let data = CBytes::new(vec![1, 2, 3]);
        assert_eq!(
            as_rust_type!(d_type, data, Blob)
                .unwrap()
                .unwrap()
                .into_vec(),
            vec![1, 2, 3]
        );
        let wrong_type = DataType { id: ColType::Map };
        assert!(as_rust_type!(wrong_type, data, Blob).is_err());
    }

    #[test]
    fn as_rust_string_test() {
        let type_custom = DataType {
            id: ColType::Custom,
        };
        let type_ascii = DataType { id: ColType::Ascii };
        let type_varchar = DataType {
            id: ColType::Varchar,
        };
        let data = CBytes::new(b"abc".to_vec());
        assert_eq!(
            as_rust_type!(type_custom, data, String).unwrap().unwrap(),
            "abc"
        );
        assert_eq!(
            as_rust_type!(type_ascii, data, String).unwrap().unwrap(),
            "abc"
        );
        assert_eq!(
            as_rust_type!(type_varchar, data, String).unwrap().unwrap(),
            "abc"
        );
        let wrong_type = DataType { id: ColType::Map };
        assert!(as_rust_type!(wrong_type, data, String).is_err());
    }

    #[test]
    fn as_rust_bool_test() {
        let type_boolean = DataType {
            id: ColType::Boolean,
        };
        let data_true = CBytes::new(vec![1]);
        let data_false = CBytes::new(vec![0]);
        assert_eq!(
            as_rust_type!(type_boolean, data_true, bool)
                .unwrap()
                .unwrap(),
            true
        );
        assert_eq!(
            as_rust_type!(type_boolean, data_false, bool)
                .unwrap()
                .unwrap(),
            false
        );
        let wrong_type = DataType { id: ColType::Map };
        assert!(as_rust_type!(wrong_type, data_false, bool).is_err());
    }

    #[test]
    fn as_rust_i64_test() {
        let type_bigint = DataType {
            id: ColType::Bigint,
        };
        let type_timestamp = DataType {
            id: ColType::Timestamp,
        };
        let type_time = DataType { id: ColType::Time };
        let type_varint = DataType {
            id: ColType::Varint,
        };
        let data = CBytes::new(vec![0, 0, 0, 0, 0, 0, 0, 100]);
        assert_eq!(as_rust_type!(type_bigint, data, i64).unwrap().unwrap(), 100);
        assert_eq!(
            as_rust_type!(type_timestamp, data, i64).unwrap().unwrap(),
            100
        );
        assert_eq!(as_rust_type!(type_time, data, i64).unwrap().unwrap(), 100);
        assert_eq!(as_rust_type!(type_varint, data, i64).unwrap().unwrap(), 100);
        let wrong_type = DataType { id: ColType::Map };
        assert!(as_rust_type!(wrong_type, data, i64).is_err());
    }

    #[test]
    fn as_rust_i32_test() {
        let type_int = DataType { id: ColType::Int };
        let type_date = DataType { id: ColType::Date };
        let data = CBytes::new(vec![0, 0, 0, 100]);
        assert_eq!(as_rust_type!(type_int, data, i32).unwrap().unwrap(), 100);
        assert_eq!(as_rust_type!(type_date, data, i32).unwrap().unwrap(), 100);
        let wrong_type = DataType { id: ColType::Map };
        assert!(as_rust_type!(wrong_type, data, i32).is_err());
    }

    #[test]
    fn as_rust_i16_test() {
        let type_smallint = DataType {
            id: ColType::Smallint,
        };
        let data = CBytes::new(vec![0, 100]);
        assert_eq!(
            as_rust_type!(type_smallint, data, i16).unwrap().unwrap(),
            100
        );
        let wrong_type = DataType { id: ColType::Map };
        assert!(as_rust_type!(wrong_type, data, i16).is_err());
    }

    #[test]
    fn as_rust_i8_test() {
        let type_tinyint = DataType {
            id: ColType::Tinyint,
        };
        let data = CBytes::new(vec![100]);
        assert_eq!(as_rust_type!(type_tinyint, data, i8).unwrap().unwrap(), 100);
        let wrong_type = DataType { id: ColType::Map };
        assert!(as_rust_type!(wrong_type, data, i8).is_err());
    }

    #[test]
    fn as_rust_f64_test() {
        let type_double = DataType {
            id: ColType::Double,
        };
        let data = CBytes::new(to_float_big(0.1 as f64));
        assert_eq!(as_rust_type!(type_double, data, f64).unwrap().unwrap(), 0.1);
        let wrong_type = DataType { id: ColType::Map };
        assert!(as_rust_type!(wrong_type, data, f64).is_err());
    }

    #[test]
    fn as_rust_f32_test() {
        // let type_decimal = DataType { id: ColType::Decimal };
        let type_float = DataType { id: ColType::Float };
        let data = CBytes::new(to_float(0.1 as f32));
        // assert_eq!(as_rust_type!(type_decimal, data, f32).unwrap(), 100.0);
        assert_eq!(as_rust_type!(type_float, data, f32).unwrap().unwrap(), 0.1);
        let wrong_type = DataType { id: ColType::Map };
        assert!(as_rust_type!(wrong_type, data, f32).is_err());
    }

    #[test]
    fn as_rust_inet_test() {
        let type_inet = DataType { id: ColType::Inet };
        let data = CBytes::new(vec![0, 0, 0, 0]);

        match as_rust_type!(type_inet, data, IpAddr) {
            Ok(Some(IpAddr::V4(ref ip))) => assert_eq!(ip.octets(), [0, 0, 0, 0]),
            _ => panic!("wrong ip v4 address"),
        }
        let wrong_type = DataType { id: ColType::Map };
        assert!(as_rust_type!(wrong_type, data, f32).is_err());
    }

    struct DataType {
        id: ColType,
    }
}
