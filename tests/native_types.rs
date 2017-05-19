extern crate cdrs;
extern crate uuid;
extern crate time;
extern crate regex;

use uuid::Uuid;
use regex::Regex;
use cdrs::client::CDRS;
use cdrs::query::QueryBuilder;
use cdrs::authenticators::NoneAuthenticator;
use cdrs::compression::Compression;
use cdrs::transport::TransportTcp;
use cdrs::types::IntoRustByName;
use cdrs::types::value::{Value, Bytes};
use cdrs::error::Result;

use std::convert::Into;
use std::str::FromStr;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

const ADDR: &'static str = "127.0.0.1:9042";

pub type CSession = cdrs::client::Session<NoneAuthenticator, TransportTcp>;

fn setup(create_table_cql: &'static str) -> Result<CSession> {
    let authenticator = NoneAuthenticator;
    let tcp_transport = TransportTcp::new(ADDR)?;
    let client = CDRS::new(tcp_transport, authenticator);
    let mut session = client.start(Compression::None)?;

    let cql = "CREATE KEYSPACE IF NOT EXISTS cdrs_test WITH \
               replication = {'class': 'SimpleStrategy', 'replication_factor': 1} \
               AND durable_writes = false";
    let query = QueryBuilder::new(cql).finalize();
    session.query(query, true, true)?;

    let re_table_name = Regex::new(r"CREATE TABLE IF NOT EXISTS (\w+\.\w+)").unwrap();
    let table_name = re_table_name
        .captures(&create_table_cql)
        .expect("table name not found")
        .get(1)
        .unwrap()
        .as_str();

    // let cql = format!("DROP TABLE IF EXISTS {}", table_name);
    // let query = QueryBuilder::new(cql).finalize();
    // session.query(query, true, true)?;

    let query = QueryBuilder::new(create_table_cql).finalize();
    session.query(query, true, true)?;

    let cql = format!("TRUNCATE TABLE {}", table_name);
    let query = QueryBuilder::new(cql).finalize();
    session.query(query, true, true)?;

    Ok(session)
}

#[test]
fn string() {
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
    let rows = session
        .query(query, false, false)
        .expect("query")
        .get_body()
        .expect("get body")
        .into_rows()
        .expect("into rows");
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

// TODO counter, varint
#[test]
fn integer() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_integer \
               (my_bigint bigint PRIMARY KEY, my_int int, my_smallint smallint, \
               my_tinyint tinyint, my_boolean boolean)";
    let mut session = setup(cql).expect("setup");

    let my_bigint: i64 = 10_000_000_000_000_000;
    let my_int: i32 = 100_000_000;
    let my_smallint: i16 = 10_000;
    let my_tinyint: i8 = 100;
    let my_boolean: bool = true;
    let values: Vec<Value> = vec![my_bigint.into(),
                                  my_int.into(),
                                  my_smallint.into(),
                                  my_tinyint.into(),
                                  my_boolean.into()];

    let cql = "INSERT INTO cdrs_test.test_integer \
               (my_bigint, my_int, my_smallint, my_tinyint, my_boolean) VALUES (?, ?, ?, ?, ?)";
    let query = QueryBuilder::new(cql).values(values).finalize();
    session.query(query, false, false).expect("insert");

    let cql = "SELECT * FROM cdrs_test.test_integer";
    let query = QueryBuilder::new(cql).finalize();
    let rows = session
        .query(query, false, false)
        .expect("query")
        .get_body()
        .expect("get body")
        .into_rows()
        .expect("into rows");
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
fn float() {
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
    let rows = session
        .query(query, false, false)
        .expect("query")
        .get_body()
        .expect("get body")
        .into_rows()
        .expect("into rows");
    assert_eq!(rows.len(), 1);
    for row in rows {
        let my_float_row: f32 = row.get_r_by_name("my_float").expect("my_float");
        let my_double_row: f64 = row.get_r_by_name("my_double").expect("my_double");
        assert_eq!(my_float_row, my_float);
        assert_eq!(my_double_row, my_double);
    }
}

#[test]
fn blob() {
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
    let rows = session
        .query(query, false, false)
        .expect("query")
        .get_body()
        .expect("get body")
        .into_rows()
        .expect("into rows");
    assert_eq!(rows.len(), 1);
    for row in rows {
        let my_blob_row: Vec<u8> = row.get_r_by_name("my_blob").expect("my_blob");
        assert_eq!(my_blob_row, my_blob);
    }
}

// TODO timeuuid
#[test]
fn uuid() {
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
    let rows = session
        .query(query, false, false)
        .expect("query")
        .get_body()
        .expect("get body")
        .into_rows()
        .expect("into rows");
    assert_eq!(rows.len(), 1);
    for row in rows {
        let my_uuid_row: Uuid = row.get_r_by_name("my_uuid").expect("my_uuid");
        assert_eq!(my_uuid_row, my_uuid);
    }
}

// TODO date, time, duration
#[test]
fn time() {
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
    let rows = session
        .query(query, false, false)
        .expect("query")
        .get_body()
        .expect("get body")
        .into_rows()
        .expect("into rows");
    assert_eq!(rows.len(), 1);
    for row in rows {
        let my_timestamp_row: time::Timespec =
            row.get_r_by_name("my_timestamp").expect("my_timestamp");
        assert_eq!(my_timestamp_row.sec, my_timestamp.sec);
        assert_eq!(my_timestamp_row.nsec / 1_000_000,
                   my_timestamp.nsec / 1_000_000); // C* `timestamp` has millisecond precision
    }
}

#[test]
fn inet() {
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
    let rows = session
        .query(query, false, false)
        .expect("query")
        .get_body()
        .expect("get body")
        .into_rows()
        .expect("into rows");
    assert_eq!(rows.len(), 1);
    for row in rows {
        let my_inet_v4_row: IpAddr = row.get_r_by_name("my_inet_v4").expect("my_inet_v4");
        let my_inet_v6_row: IpAddr = row.get_r_by_name("my_inet_v6").expect("my_inet_v6");
        assert_eq!(my_inet_v4_row, my_inet_v4);
        assert_eq!(my_inet_v6_row, my_inet_v6);
    }
}
