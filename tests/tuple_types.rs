extern crate cdrs;
extern crate uuid;
extern crate time;
extern crate regex;

mod common;

use common::*;

use uuid::Uuid;
use time::Timespec;
use cdrs::query::QueryBuilder;
use cdrs::types::{IntoRustByName, IntoRustByIndex};
use cdrs::types::value::{Value, Bytes};
use cdrs::types::tuple::Tuple;
use cdrs::error::Result;
use cdrs::IntoBytes;

use std::str::FromStr;

#[test]
#[cfg(not(feature = "appveyor"))]
fn simple_tuple() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.simple_tuple \
               (my_tuple tuple<text, int> PRIMARY KEY)";
    let mut session = setup(cql).expect("setup");

    #[derive(Debug, Clone, PartialEq)]
    struct MyTuple {
        pub my_text: String,
        pub my_int: i32,
    }

    impl MyTuple {
        pub fn try_from(tuple: Tuple) -> Result<MyTuple> {
            let my_text: String = tuple.get_r_by_index(0)?;
            let my_int: i32 = tuple.get_r_by_index(1)?;
            Ok(MyTuple {
                   my_text: my_text,
                   my_int: my_int,
               })
        }
    }

    impl Into<Bytes> for MyTuple {
        fn into(self) -> Bytes {
            let mut bytes = Vec::new();
            let val_bytes: Bytes = self.my_text.into();
            bytes.extend_from_slice(Value::new_normal(val_bytes).into_cbytes().as_slice());
            let val_bytes: Bytes = self.my_int.into();
            bytes.extend_from_slice(Value::new_normal(val_bytes).into_cbytes().as_slice());
            Bytes::new(bytes)
        }
    }

    let my_tuple = MyTuple {
        my_text: "my_text".to_string(),
        my_int: 0,
    };
    let values: Vec<Value> = vec![my_tuple.clone().into()];

    let cql = "INSERT INTO cdrs_test.simple_tuple \
               (my_tuple) VALUES (?)";
    let query = QueryBuilder::new(cql).values(values).finalize();
    session.query(query, false, false).expect("insert");

    let cql = "SELECT * FROM cdrs_test.simple_tuple";
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
        let my_tuple_row: Tuple = row.get_r_by_name("my_tuple").expect("my_tuple");
        let my_tuple_row = MyTuple::try_from(my_tuple_row).expect("my_tuple as rust");
        assert_eq!(my_tuple_row, my_tuple);
    }
}

#[test]
#[cfg(not(feature = "appveyor"))]
fn nested_tuples() {
    let cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_nested_tuples \
               (my_key int PRIMARY KEY, \
               my_outer_tuple tuple<uuid, blob, tuple<text, int, timestamp>>)";
    let mut session = setup(cql).expect("setup");

    #[derive(Debug, Clone, PartialEq)]
    struct MyInnerTuple {
        pub my_text: String,
        pub my_int: i32,
        pub my_timestamp: Timespec,
    }

    impl MyInnerTuple {
        pub fn try_from(tuple: Tuple) -> Result<MyInnerTuple> {
            let my_text: String = tuple.get_r_by_index(0)?;
            let my_int: i32 = tuple.get_r_by_index(1)?;
            let my_timestamp: Timespec = tuple.get_r_by_index(2)?;
            Ok(MyInnerTuple {
                   my_text: my_text,
                   my_int: my_int,
                   my_timestamp: my_timestamp,
               })
        }
    }

    impl Into<Bytes> for MyInnerTuple {
        fn into(self) -> Bytes {
            let mut bytes = Vec::new();
            let val_bytes: Bytes = self.my_text.into();
            bytes.extend_from_slice(Value::new_normal(val_bytes).into_cbytes().as_slice());
            let val_bytes: Bytes = self.my_int.into();
            bytes.extend_from_slice(Value::new_normal(val_bytes).into_cbytes().as_slice());
            let val_bytes: Bytes = self.my_timestamp.into();
            bytes.extend_from_slice(Value::new_normal(val_bytes).into_cbytes().as_slice());
            Bytes::new(bytes)
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    struct MyOuterTuple {
        pub my_uuid: Uuid,
        pub my_blob: Vec<u8>,
        pub my_inner_tuple: MyInnerTuple,
    }

    impl MyOuterTuple {
        pub fn try_from(tuple: Tuple) -> Result<MyOuterTuple> {
            let my_uuid: Uuid = tuple.get_r_by_index(0)?;
            let my_blob: Vec<u8> = tuple.get_r_by_index(1)?;
            let my_inner_tuple: Tuple = tuple.get_r_by_index(2)?;
            let my_inner_tuple = MyInnerTuple::try_from(my_inner_tuple).expect("from tuple");
            Ok(MyOuterTuple {
                   my_uuid: my_uuid,
                   my_blob: my_blob,
                   my_inner_tuple: my_inner_tuple,
               })
        }
    }

    impl Into<Bytes> for MyOuterTuple {
        fn into(self) -> Bytes {
            let mut bytes = Vec::new();
            let val_bytes: Bytes = self.my_uuid.into();
            bytes.extend_from_slice(Value::new_normal(val_bytes).into_cbytes().as_slice());
            let val_bytes: Bytes = Bytes::new(self.my_blob);
            bytes.extend_from_slice(Value::new_normal(val_bytes).into_cbytes().as_slice());
            let val_bytes: Bytes = self.my_inner_tuple.into();
            bytes.extend_from_slice(Value::new_normal(val_bytes).into_cbytes().as_slice());
            Bytes::new(bytes)
        }
    }

    let my_uuid = Uuid::from_str("bb16106a-10bc-4a07-baa3-126ffe208c43").unwrap();
    let my_blob: Vec<u8> = vec![0, 1, 2, 4, 8, 16, 32, 64, 128, 255];
    let my_inner_tuple = MyInnerTuple {
        my_text: "my_text".to_string(),
        my_int: 1_000,
        my_timestamp: Timespec {
            sec: 1,
            nsec: 999_000_000,
        },
    };
    let my_outer_tuple = MyOuterTuple {
        my_uuid: my_uuid,
        my_blob: my_blob,
        my_inner_tuple: my_inner_tuple,
    };
    let values: Vec<Value> = vec![0i32.into(), my_outer_tuple.clone().into()];

    let cql = "INSERT INTO cdrs_test.test_nested_tuples \
               (my_key, my_outer_tuple) VALUES (?, ?)";
    let query = QueryBuilder::new(cql).values(values).finalize();
    session.query(query, false, false).expect("insert");

    let cql = "SELECT * FROM cdrs_test.test_nested_tuples";
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
        let my_outer_tuple_row: Tuple =
            row.get_r_by_name("my_outer_tuple").expect("my_outer_tuple");
        let my_outer_tuple_row = MyOuterTuple::try_from(my_outer_tuple_row).expect("from tuple");
        assert_eq!(my_outer_tuple_row, my_outer_tuple);
    }
}
