use std::io;
use std::string::FromUtf8Error;
use super::types::*;
// use super::frame_result::ColType;

// https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L813

// Decodes Cassandra `varchar` data (bytes) into Rust's `Result<String, FromUtf8Error>`.
fn decode_varchar(bytes: Vec<u8>) -> Result<String, FromUtf8Error> {
    return String::from_utf8(bytes);
}

// Decodes Cassandra `bigint` data (bytes) into Rust's `Result<i32, io::Error>`
fn decode_bigint(bytes: Vec<u8>) -> Result<i64, io::Error> {
    return try_from_bytes(bytes).map(|i| i as i64);
}

// Decodes Cassandra `blob` data (bytes) into Rust's `Result<Vec<u8>, io::Error>`
fn decode_blob(bytes: Vec<u8>) -> Result<Vec<u8>, io::Error> {
    // in fact we just pass it through.
    return Ok(bytes);
}

// Decodes Cassandra `boolean` data (bytes) into Rust's `Result<i32, io::Error>`
fn decode_boolean(bytes: Vec<u8>) -> Result<bool, io::Error> {
    let false_byte: u8 = 0;
    return bytes.first()
        .ok_or(io::Error::new(io::ErrorKind::UnexpectedEof, "no bytes were found"))
        .map(|b| b != &false_byte);
}

// Decodes Cassandra `int` data (bytes) into Rust's `Result<i32, io::Error>`
fn decode_int(bytes: Vec<u8>) -> Result<i32, io::Error> {
    return try_from_bytes(bytes).map(|i| i as i32);
}

// Decodes Cassandra `date` data (bytes) into Rust's `Result<i32, io::Error>` in following way
//    0: -5877641-06-23
// 2^31: 1970-1-1
// 2^32: 5881580-07-11
fn decode_date(bytes: Vec<u8>) -> Result<i32, io::Error> {
    return try_from_bytes(bytes).map(|i| i as i32);
}

// Decodes Cassandra `timestamp` data (bytes) into Rust's `Result<i32, io::Error>`
// `i32` represets a millisecond-precision
//  offset from the unix epoch (00:00:00, January 1st, 1970).  Negative values
//  represent a negative offset from the epoch.
fn decode_timestamp(bytes: Vec<u8>) -> Result<i64, io::Error> {
    return try_from_bytes(bytes).map(|i| i as i64);
}
