#[macro_use]
extern crate cdrs;
extern crate regex;
extern crate time;
extern crate uuid;

mod common;

use common::*;

use uuid::Uuid;
use cdrs::types::map::Map;
use cdrs::types::value::{Bytes, Value};
use cdrs::types::{AsRust, ByName, IntoRustByName};
use cdrs::types::blob::Blob;
use cdrs::query::QueryExecutor;

use std::str::FromStr;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::collections::HashMap;

#[test]
#[cfg(not(feature = "appveyor"))]
fn string() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_string \
               (my_ascii ascii PRIMARY KEY, my_text text, my_varchar varchar)";
    let mut session = setup(cql).expect("setup");

    let my_ascii = "my_ascii";
    let my_text = "my_text";
    let my_varchar = "my_varchar";
    let values = query_values!(my_ascii, my_text, my_varchar);

    let query = "INSERT INTO cdrs_test.test_string \
                 (my_ascii, my_text, my_varchar) VALUES (?, ?, ?)";
    session
        .query_with_values(query, values)
        .expect("insert stings error");

    let cql = "SELECT * FROM cdrs_test.test_string";
    let rows = session
        .query(cql)
        .expect("select strings query error")
        .get_body()
        .expect("get body error")
        .into_rows()
        .expect("converting into rows error");

    assert_eq!(rows.len(), 1);
    for row in rows {
        let my_ascii_row: String = row.get_r_by_name("my_ascii").expect("my_ascii");
        let my_text_row: String = row.get_r_by_name("my_text").expect("my_text");
        let my_varchar_row: String = row.get_r_by_name("my_varchar").expect("my_varchar");
        assert_eq!(my_ascii_row, my_ascii);
        assert_eq!(my_text_row, my_text);
        assert_eq!(my_varchar_row, my_varchar);
    }
}

#[test]
#[cfg(not(feature = "appveyor"))]
fn counter() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_counter \
               (my_bigint bigint PRIMARY KEY, my_counter counter)";
    let mut session = setup(cql).expect("setup");

    let my_bigint: i64 = 10_000_000_000_000_000;
    let my_counter: i64 = 100_000_000;
    let values = query_values!(my_counter, my_bigint);

    let query = "UPDATE cdrs_test.test_counter SET my_counter = my_counter + ? \
                 WHERE my_bigint = ?";
    session.query_with_values(query, values).expect("insert");

    let cql = "SELECT * FROM cdrs_test.test_counter";
    let rows = session
        .query(cql)
        .expect("select counter query error")
        .get_body()
        .expect("get counter body error")
        .into_rows()
        .expect("converting coutner body into rows error");

    assert_eq!(rows.len(), 1);
    for row in rows {
        let my_bigint_row: i64 = row.get_r_by_name("my_bigint").expect("my_bigint");
        let my_counter_row: i64 = row.get_r_by_name("my_counter").expect("my_counter");
        assert_eq!(my_bigint_row, my_bigint);
        assert_eq!(my_counter_row, my_counter);
    }
}

// TODO varint
#[test]
#[cfg(not(feature = "appveyor"))]
fn integer() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_integer \
               (my_bigint bigint PRIMARY KEY, my_int int, my_boolean boolean)";
    let mut session = setup(cql).expect("setup");

    let my_bigint: i64 = 10_000_000_000_000_000;
    let my_int: i32 = 100_000_000;
    let my_boolean: bool = true;
    let values = query_values!(my_bigint, my_int, my_boolean);

    let query = "INSERT INTO cdrs_test.test_integer \
                 (my_bigint, my_int, my_boolean) VALUES (?, ?, ?)";
    session
        .query_with_values(query, values)
        .expect("insert integers error");

    let cql = "SELECT * FROM cdrs_test.test_integer";
    let rows = session
        .query(cql)
        .expect("select integers query error")
        .get_body()
        .expect("get body with integers error")
        .into_rows()
        .expect("converting body with integers into rows error");

    assert_eq!(rows.len(), 1);
    for row in rows {
        let my_bigint_row: i64 = row.get_r_by_name("my_bigint").expect("my_bigint");
        let my_int_row: i32 = row.get_r_by_name("my_int").expect("my_int");
        let my_boolean_row: bool = row.get_r_by_name("my_boolean").expect("my_boolean");
        assert_eq!(my_bigint_row, my_bigint);
        assert_eq!(my_int_row, my_int);
        assert_eq!(my_boolean_row, my_boolean);
    }
}

// TODO counter, varint
#[test]
#[cfg(feature = "v4")]
#[cfg(not(feature = "appveyor"))]
fn integer_v4() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_integer_v4 \
               (my_bigint bigint PRIMARY KEY, my_int int, my_smallint smallint, \
               my_tinyint tinyint, my_boolean boolean)";
    let mut session = setup(cql).expect("setup");

    let my_bigint: i64 = 10_000_000_000_000_000;
    let my_int: i32 = 100_000_000;
    let my_smallint: i16 = 10_000;
    let my_tinyint: i8 = 100;
    let my_boolean: bool = true;
    let values = query_values!(my_bigint, my_int, my_smallint, my_tinyint, my_boolean);

    let query = "INSERT INTO cdrs_test.test_integer_v4 \
                 (my_bigint, my_int, my_smallint, my_tinyint, my_boolean) VALUES (?, ?, ?, ?, ?)";
    session
        .query_with_values(query, values)
        .expect("insert integers error");

    let cql = "SELECT * FROM cdrs_test.test_integer_v4";
    let rows = session
        .query(cql)
        .expect("query integers error")
        .get_body()
        .expect("get body with integers error")
        .into_rows()
        .expect("converting body with integers into rows error");

    assert_eq!(rows.len(), 1);
    for row in rows {
        let my_bigint_row: i64 = row.get_r_by_name("my_bigint").expect("my_bigint");
        let my_int_row: i32 = row.get_r_by_name("my_int").expect("my_int");
        let my_smallint_row: i16 = row.get_r_by_name("my_smallint").expect("my_smallint");
        let my_tinyint_row: i8 = row.get_r_by_name("my_tinyint").expect("my_tinyint");
        let my_boolean_row: bool = row.get_r_by_name("my_boolean").expect("my_boolean");
        assert_eq!(my_bigint_row, my_bigint);
        assert_eq!(my_int_row, my_int);
        assert_eq!(my_smallint_row, my_smallint);
        assert_eq!(my_tinyint_row, my_tinyint);
        assert_eq!(my_boolean_row, my_boolean);
    }
}

// TODO decimal
#[test]
#[cfg(not(feature = "appveyor"))]
fn float() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_float \
               (my_float float PRIMARY KEY, my_double double)";
    let mut session = setup(cql).expect("setup");

    let my_float: f32 = 123.456;
    let my_double: f64 = 987.654;
    let values = query_values!(my_float, my_double);

    let query = "INSERT INTO cdrs_test.test_float (my_float, my_double) VALUES (?, ?)";
    session
        .query_with_values(query, values)
        .expect("insert floats error");

    let cql = "SELECT * FROM cdrs_test.test_float";
    let rows = session
        .query(cql)
        .expect("query floats error")
        .get_body()
        .expect("get body with floats error")
        .into_rows()
        .expect("converting body with floats into rows error");

    assert_eq!(rows.len(), 1);
    for row in rows {
        let my_float_row: f32 = row.get_r_by_name("my_float").expect("my_float");
        let my_double_row: f64 = row.get_r_by_name("my_double").expect("my_double");
        assert_eq!(my_float_row, my_float);
        assert_eq!(my_double_row, my_double);
    }
}

#[test]
#[cfg(not(feature = "appveyor"))]
fn blob() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_blob \
               (my_blob blob PRIMARY KEY, my_mapblob map<text, blob>)";
    let mut session = setup(cql).expect("setup");

    let my_blob: Blob = vec![0, 1, 2, 4, 8, 16, 32, 64, 128, 255].into();
    let my_map: HashMap<String, Blob> = [
        ("a".to_owned(), b"aaaaa".to_vec().into()),
        ("b".to_owned(), b"bbbbb".to_vec().into()),
        ("c".to_owned(), b"ccccc".to_vec().into()),
        ("d".to_owned(), b"ddddd".to_vec().into()),
    ].into_iter()
        .map(|x| x.clone())
        .collect();

    let val_map: HashMap<String, Bytes> = my_map
        .clone()
        .into_iter()
        .map(|(k, v)| (k, Bytes::new(v.into_vec())))
        .collect();

    let values = query_values!(my_blob.clone(), val_map);

    let query = "INSERT INTO cdrs_test.test_blob (my_blob, my_mapblob) VALUES (?,?)";
    session
        .query_with_values(query, values)
        .expect("insert blob error");

    let cql = "SELECT * FROM cdrs_test.test_blob";
    let rows = session
        .query(cql)
        .expect("query blobs error")
        .get_body()
        .expect("get body with blobs error")
        .into_rows()
        .expect("converting body with blobs into rows error");

    assert_eq!(rows.len(), 1);
    for row in rows {
        let my_blob_row: Blob = row.get_r_by_name("my_blob").expect("my_blob");
        assert_eq!(my_blob_row, my_blob);
        let my_map_row: HashMap<String, Blob> = row.r_by_name::<Map>("my_mapblob")
            .expect("my_mapblob by name")
            .as_r_rust()
            .expect("my_mapblob as r rust");
        assert_eq!(my_map_row, my_map);
    }
}

// TODO timeuuid
#[test]
#[cfg(not(feature = "appveyor"))]
fn uuid() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_uuid \
               (my_uuid uuid PRIMARY KEY)";
    let mut session = setup(cql).expect("setup");

    let my_uuid = Uuid::from_str("bb16106a-10bc-4a07-baa3-126ffe208c43").unwrap();
    let values = query_values!(my_uuid);

    let query = "INSERT INTO cdrs_test.test_uuid (my_uuid) VALUES (?)";
    session
        .query_with_values(query, values)
        .expect("insert UUID error");

    let cql = "SELECT * FROM cdrs_test.test_uuid";
    let rows = session
        .query(cql)
        .expect("query UUID error")
        .get_body()
        .expect("get body with UUID error")
        .into_rows()
        .expect("convertion body with UUID into rows error");

    assert_eq!(rows.len(), 1);
    for row in rows {
        let my_uuid_row: Uuid = row.get_r_by_name("my_uuid").expect("my_uuid");
        assert_eq!(my_uuid_row, my_uuid);
    }
}

// TODO date, time, duration
#[test]
#[cfg(not(feature = "appveyor"))]
fn time() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_time \
               (my_timestamp timestamp PRIMARY KEY)";
    let mut session = setup(cql).expect("setup");

    let my_timestamp = time::get_time();
    let values = query_values!(my_timestamp);

    let query = "INSERT INTO cdrs_test.test_time (my_timestamp) VALUES (?)";
    session
        .query_with_values(query, values)
        .expect("insert timestamp error");

    let cql = "SELECT * FROM cdrs_test.test_time";
    let rows = session
        .query(cql)
        .expect("query with time error")
        .get_body()
        .expect("get body with time error")
        .into_rows()
        .expect("converting body with time into rows error");

    assert_eq!(rows.len(), 1);
    for row in rows {
        let my_timestamp_row: time::Timespec =
            row.get_r_by_name("my_timestamp").expect("my_timestamp");
        assert_eq!(my_timestamp_row.sec, my_timestamp.sec);
        assert_eq!(
            my_timestamp_row.nsec / 1_000_000,
            my_timestamp.nsec / 1_000_000
        ); // C* `timestamp` has millisecond precision
    }
}

#[test]
#[cfg(not(feature = "appveyor"))]
fn inet() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_inet \
               (my_inet_v4 inet PRIMARY KEY, my_inet_v6 inet)";
    let mut session = setup(cql).expect("setup");

    let my_inet_v4 = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    let my_inet_v6 = IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1));
    let values = query_values!(my_inet_v4.clone(), my_inet_v6.clone());

    let query = "INSERT INTO cdrs_test.test_inet (my_inet_v4, my_inet_v6) VALUES (?, ?)";
    session
        .query_with_values(query, values)
        .expect("insert inet error");

    let query = "SELECT * FROM cdrs_test.test_inet";
    let rows = session
        .query(query)
        .expect("query inet error")
        .get_body()
        .expect("get body with inet error")
        .into_rows()
        .expect("converting body with inet into rows error");

    assert_eq!(rows.len(), 1);
    for row in rows {
        let my_inet_v4_row: IpAddr = row.get_r_by_name("my_inet_v4").expect("my_inet_v4");
        let my_inet_v6_row: IpAddr = row.get_r_by_name("my_inet_v6").expect("my_inet_v6");
        assert_eq!(my_inet_v4_row, my_inet_v4);
        assert_eq!(my_inet_v6_row, my_inet_v6);
    }
}
