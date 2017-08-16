#![feature(test)]
extern crate test;
extern crate rand;

extern crate cdrs;
extern crate uuid;
extern crate time;
extern crate regex;

use test::Bencher;
use rand::Rng;

mod common;

use common::*;

use uuid::Uuid;
use cdrs::query::QueryBuilder;
use cdrs::types::IntoRustByName;
use cdrs::types::value::{Value, Bytes};

use std::str::FromStr;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

// Measure how much time it takes to parse a row with
#[bench]
fn string_body_parse(b: &mut Bencher) {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_string \
               (my_ascii ascii PRIMARY KEY, my_text text, my_varchar varchar)";
    let mut session = setup(cql).expect("setup");

    let my_ascii = "my_ascii".to_string();
    let my_text = "my_text".to_string();
    let my_varchar = "my_varchar".to_string();
    let values: Vec<Value> = vec![my_ascii.clone().into(),
                                  my_text.clone().into(),
                                  my_varchar.clone().into()];

    let cql = "INSERT INTO cdrs_test.test_string \
               (my_ascii, my_text, my_varchar) VALUES (?, ?, ?)";
    let query = QueryBuilder::new(cql).values(values).finalize();
    session.query(query, false, false).expect("insert");

    let cql = "SELECT * FROM cdrs_test.test_string";
    let query = QueryBuilder::new(cql).finalize();
    let res = session.query(query, false, false).expect("query");

    b.iter(|| {
               let _ = res.get_body()
                   .expect("get body")
                   .into_rows()
                   .expect("into rows");
           })
}

#[bench]
fn string_convert(b: &mut Bencher) {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_string \
               (my_ascii ascii PRIMARY KEY, my_text text, my_varchar varchar)";
    let mut session = setup(cql).expect("setup");

    let my_ascii = "my_ascii".to_string();
    let my_text = "my_text".to_string();
    let my_varchar = "my_varchar".to_string();
    let values: Vec<Value> = vec![my_ascii.clone().into(),
                                  my_text.clone().into(),
                                  my_varchar.clone().into()];

    let cql = "INSERT INTO cdrs_test.test_string \
               (my_ascii, my_text, my_varchar) VALUES (?, ?, ?)";
    let query = QueryBuilder::new(cql).values(values).finalize();
    session.query(query, false, false).expect("insert");

    let cql = "SELECT * FROM cdrs_test.test_string";
    let query = QueryBuilder::new(cql).finalize();
    let res = session.query(query, false, false).expect("query");
    let ref row = res.get_body()
        .expect("get body")
        .into_rows()
        .expect("into rows")
        [0];

    b.iter(|| {
               let _: String = row.get_r_by_name("my_ascii").expect("my_ascii");
               let _: String = row.get_r_by_name("my_text").expect("my_text");
               let _: String = row.get_r_by_name("my_varchar").expect("my_varchar");
           })
}

#[bench]
fn counter_body_parse(b: &mut Bencher) {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_counter \
               (my_bigint bigint PRIMARY KEY, my_counter counter)";
    let mut session = setup(cql).expect("setup");

    let my_bigint: i64 = 10_000_000_000_000_000;
    let my_counter: i64 = 100_000_000;
    let values: Vec<Value> = vec![my_counter.into(), my_bigint.into()];

    let cql = "UPDATE cdrs_test.test_counter SET my_counter = my_counter + ? \
               WHERE my_bigint = ?";
    let query = QueryBuilder::new(cql).values(values).finalize();
    session.query(query, false, false).expect("insert");

    let cql = "SELECT * FROM cdrs_test.test_counter";
    let query = QueryBuilder::new(cql).finalize();
    let res = session.query(query, false, false).expect("query");

    b.iter(|| {
               res.get_body()
                   .expect("get body")
                   .into_rows()
                   .expect("into rows");
           })
}

#[bench]
fn counter_convert(b: &mut Bencher) {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_counter \
               (my_bigint bigint PRIMARY KEY, my_counter counter)";
    let mut session = setup(cql).expect("setup");

    let my_bigint: i64 = 10_000_000_000_000_000;
    let my_counter: i64 = 100_000_000;
    let values: Vec<Value> = vec![my_counter.into(), my_bigint.into()];

    let cql = "UPDATE cdrs_test.test_counter SET my_counter = my_counter + ? \
               WHERE my_bigint = ?";
    let query = QueryBuilder::new(cql).values(values).finalize();
    session.query(query, false, false).expect("insert");

    let cql = "SELECT * FROM cdrs_test.test_counter";
    let query = QueryBuilder::new(cql).finalize();
    let row = &session
        .query(query, false, false)
        .expect("query")
        .get_body()
        .expect("get body")
        .into_rows()
        .expect("into rows")
        [0];

    b.iter(|| {
               let _: i64 = row.get_r_by_name("my_bigint").expect("my_bigint");
               let _: i64 = row.get_r_by_name("my_counter").expect("my_counter");
           })
}

#[bench]
fn integer_body_parse(b: &mut Bencher) {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_integer \
               (my_bigint bigint PRIMARY KEY, my_int int, my_boolean boolean)";
    let mut session = setup(cql).expect("setup");

    let my_bigint: i64 = 10_000_000_000_000_000;
    let my_int: i32 = 100_000_000;
    let my_boolean: bool = true;
    let values: Vec<Value> = vec![my_bigint.into(), my_int.into(), my_boolean.into()];

    let cql = "INSERT INTO cdrs_test.test_integer \
               (my_bigint, my_int, my_boolean) VALUES (?, ?, ?)";
    let query = QueryBuilder::new(cql).values(values).finalize();
    session.query(query, false, false).expect("insert");

    let cql = "SELECT * FROM cdrs_test.test_integer";
    let query = QueryBuilder::new(cql).finalize();
    let res = session.query(query, false, false).expect("query");

    b.iter(|| {
               res.get_body()
                   .expect("get body")
                   .into_rows()
                   .expect("into rows");
           })
}

#[bench]
fn integer_convert(b: &mut Bencher) {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_integer \
               (my_bigint bigint PRIMARY KEY, my_int int, my_boolean boolean)";
    let mut session = setup(cql).expect("setup");

    let my_bigint: i64 = 10_000_000_000_000_000;
    let my_int: i32 = 100_000_000;
    let my_boolean: bool = true;
    let values: Vec<Value> = vec![my_bigint.into(), my_int.into(), my_boolean.into()];

    let cql = "INSERT INTO cdrs_test.test_integer \
               (my_bigint, my_int, my_boolean) VALUES (?, ?, ?)";
    let query = QueryBuilder::new(cql).values(values).finalize();
    session.query(query, false, false).expect("insert");

    let cql = "SELECT * FROM cdrs_test.test_integer";
    let query = QueryBuilder::new(cql).finalize();
    let row = &session
        .query(query, false, false)
        .expect("query")
        .get_body()
        .expect("get body")
        .into_rows()
        .expect("into rows")
        [0];

    b.iter(|| {
               let _: i64 = row.get_r_by_name("my_bigint").expect("my_bigint");
               let _: i32 = row.get_r_by_name("my_int").expect("my_int");
               let _: bool = row.get_r_by_name("my_boolean").expect("my_boolean");
           })
}

#[bench]
fn float_body_parse(b: &mut Bencher) {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_float \
               (my_float float PRIMARY KEY, my_double double)";
    let mut session = setup(cql).expect("setup");

    let my_float: f32 = 123.456;
    let my_double: f64 = 987.654;
    let values: Vec<Value> = vec![my_float.into(), my_double.into()];

    let cql = "INSERT INTO cdrs_test.test_float (my_float, my_double) VALUES (?, ?)";
    let query = QueryBuilder::new(cql).values(values).finalize();
    session.query(query, false, false).expect("insert");

    let cql = "SELECT * FROM cdrs_test.test_float";
    let query = QueryBuilder::new(cql).finalize();
    let res = session.query(query, false, false).expect("query");

    b.iter(|| {
               res.get_body()
                   .expect("get body")
                   .into_rows()
                   .expect("into rows");
           })
}

#[bench]
fn float_convert(b: &mut Bencher) {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_float \
               (my_float float PRIMARY KEY, my_double double)";
    let mut session = setup(cql).expect("setup");

    let my_float: f32 = 123.456;
    let my_double: f64 = 987.654;
    let values: Vec<Value> = vec![my_float.into(), my_double.into()];

    let cql = "INSERT INTO cdrs_test.test_float (my_float, my_double) VALUES (?, ?)";
    let query = QueryBuilder::new(cql).values(values).finalize();
    session.query(query, false, false).expect("insert");

    let cql = "SELECT * FROM cdrs_test.test_float";
    let query = QueryBuilder::new(cql).finalize();
    let row = &session
        .query(query, false, false)
        .expect("query")
        .get_body()
        .expect("get body")
        .into_rows()
        .expect("into rows")
        [0];

    b.iter(|| {
               let _: f32 = row.get_r_by_name("my_float").expect("my_float");
               let _: f64 = row.get_r_by_name("my_double").expect("my_double");
           })
}

#[bench]
fn blob_body_parse(b: &mut Bencher) {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_blob \
               (my_blob blob PRIMARY KEY)";
    let mut session = setup(cql).expect("setup");

    let my_blob: Vec<u8> = vec![0, 1, 2, 4, 8, 16, 32, 64, 128, 255];
    let values: Vec<Value> = vec![Bytes::new(my_blob.clone()).into()];

    let cql = "INSERT INTO cdrs_test.test_blob (my_blob) VALUES (?)";
    let query = QueryBuilder::new(cql).values(values).finalize();
    session.query(query, false, false).expect("insert");

    let cql = "SELECT * FROM cdrs_test.test_blob";
    let query = QueryBuilder::new(cql).finalize();
    let res = session.query(query, false, false).expect("query");

    b.iter(|| {
               res.get_body()
                   .expect("get body")
                   .into_rows()
                   .expect("into rows");
           })
}

#[bench]
fn blob_convert(b: &mut Bencher) {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_blob \
               (my_blob blob PRIMARY KEY)";
    let mut session = setup(cql).expect("setup");

    let my_blob: Vec<u8> = vec![0, 1, 2, 4, 8, 16, 32, 64, 128, 255];
    let values: Vec<Value> = vec![Bytes::new(my_blob.clone()).into()];

    let cql = "INSERT INTO cdrs_test.test_blob (my_blob) VALUES (?)";
    let query = QueryBuilder::new(cql).values(values).finalize();
    session.query(query, false, false).expect("insert");

    let cql = "SELECT * FROM cdrs_test.test_blob";
    let query = QueryBuilder::new(cql).finalize();
    let row = &session
        .query(query, false, false)
        .expect("query")
        .get_body()
        .expect("get body")
        .into_rows()
        .expect("into rows")
        [0];

    b.iter(|| { let _: Vec<u8> = row.get_r_by_name("my_blob").expect("my_blob"); })
}

#[bench]
fn uuid_body_parse(b: &mut Bencher) {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_uuid \
               (my_uuid uuid PRIMARY KEY)";
    let mut session = setup(cql).expect("setup");

    let my_uuid = Uuid::from_str("bb16106a-10bc-4a07-baa3-126ffe208c43").unwrap();
    let values: Vec<Value> = vec![my_uuid.into()];

    let cql = "INSERT INTO cdrs_test.test_uuid (my_uuid) VALUES (?)";
    let query = QueryBuilder::new(cql).values(values).finalize();
    session.query(query, false, false).expect("insert");

    let cql = "SELECT * FROM cdrs_test.test_uuid";
    let query = QueryBuilder::new(cql).finalize();
    let res = session.query(query, false, false).expect("query");

    b.iter(|| {
               res.get_body()
                   .expect("get body")
                   .into_rows()
                   .expect("into rows");
           })
}

#[bench]
fn uuid_convert(b: &mut Bencher) {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_uuid \
               (my_uuid uuid PRIMARY KEY)";
    let mut session = setup(cql).expect("setup");

    let my_uuid = Uuid::from_str("bb16106a-10bc-4a07-baa3-126ffe208c43").unwrap();
    let values: Vec<Value> = vec![my_uuid.into()];

    let cql = "INSERT INTO cdrs_test.test_uuid (my_uuid) VALUES (?)";
    let query = QueryBuilder::new(cql).values(values).finalize();
    session.query(query, false, false).expect("insert");

    let cql = "SELECT * FROM cdrs_test.test_uuid";
    let query = QueryBuilder::new(cql).finalize();
    let row = &session
        .query(query, false, false)
        .expect("query")
        .get_body()
        .expect("get body")
        .into_rows()
        .expect("into rows")
        [0];

    b.iter(|| { let _: Uuid = row.get_r_by_name("my_uuid").expect("my_uuid"); })
}

#[bench]
fn time_body_parse(b: &mut Bencher) {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_time \
               (my_timestamp timestamp PRIMARY KEY)";
    let mut session = setup(cql).expect("setup");

    let my_timestamp = time::get_time();
    let values: Vec<Value> = vec![my_timestamp.into()];

    let cql = "INSERT INTO cdrs_test.test_time (my_timestamp) VALUES (?)";
    let query = QueryBuilder::new(cql).values(values).finalize();
    session.query(query, false, false).expect("insert");

    let cql = "SELECT * FROM cdrs_test.test_time";
    let query = QueryBuilder::new(cql).finalize();
    let res = session.query(query, false, false).expect("query");

    b.iter(|| {
               res.get_body()
                   .expect("get body")
                   .into_rows()
                   .expect("into rows");
           })
}

#[bench]
fn time_convert(b: &mut Bencher) {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_time \
               (my_timestamp timestamp PRIMARY KEY)";
    let mut session = setup(cql).expect("setup");

    let my_timestamp = time::get_time();
    let values: Vec<Value> = vec![my_timestamp.into()];

    let cql = "INSERT INTO cdrs_test.test_time (my_timestamp) VALUES (?)";
    let query = QueryBuilder::new(cql).values(values).finalize();
    session.query(query, false, false).expect("insert");

    let cql = "SELECT * FROM cdrs_test.test_time";
    let query = QueryBuilder::new(cql).finalize();
    let row = &session
        .query(query, false, false)
        .expect("query")
        .get_body()
        .expect("get body")
        .into_rows()
        .expect("into rows")
        [0];

    b.iter(|| { let _: time::Timespec = row.get_r_by_name("my_timestamp").expect("my_timestamp"); })
}

#[bench]
fn inet_body_parse(b: &mut Bencher) {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_inet \
               (my_inet_v4 inet PRIMARY KEY, my_inet_v6 inet)";
    let mut session = setup(cql).expect("setup");

    let my_inet_v4 = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    let my_inet_v6 = IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1));
    let values: Vec<Value> = vec![my_inet_v4.clone().into(), my_inet_v6.clone().into()];

    let cql = "INSERT INTO cdrs_test.test_inet (my_inet_v4, my_inet_v6) VALUES (?, ?)";
    let query = QueryBuilder::new(cql).values(values).finalize();
    session.query(query, false, false).expect("insert");

    let cql = "SELECT * FROM cdrs_test.test_inet";
    let query = QueryBuilder::new(cql).finalize();
    let res = session.query(query, false, false).expect("query");

    b.iter(|| {
               res.get_body()
                   .expect("get body")
                   .into_rows()
                   .expect("into rows");
           })
}

#[bench]
fn inet_convert(b: &mut Bencher) {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_inet \
               (my_inet_v4 inet PRIMARY KEY, my_inet_v6 inet)";
    let mut session = setup(cql).expect("setup");

    let my_inet_v4 = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
    let my_inet_v6 = IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1));
    let values: Vec<Value> = vec![my_inet_v4.clone().into(), my_inet_v6.clone().into()];

    let cql = "INSERT INTO cdrs_test.test_inet (my_inet_v4, my_inet_v6) VALUES (?, ?)";
    let query = QueryBuilder::new(cql).values(values).finalize();
    session.query(query, false, false).expect("insert");

    let cql = "SELECT * FROM cdrs_test.test_inet";
    let query = QueryBuilder::new(cql).finalize();
    let row = &session
        .query(query, false, false)
        .expect("query")
        .get_body()
        .expect("get body")
        .into_rows()
        .expect("into rows")
        [0];

    b.iter(|| {
               let _: IpAddr = row.get_r_by_name("my_inet_v4").expect("my_inet_v4");
               let _: IpAddr = row.get_r_by_name("my_inet_v6").expect("my_inet_v6");
           })
}
