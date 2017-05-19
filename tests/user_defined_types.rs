extern crate cdrs;
extern crate uuid;
extern crate time;
extern crate regex;

mod common;

use common::*;

use cdrs::query::QueryBuilder;
use cdrs::types::IntoRustByName;
use cdrs::types::value::{Value, Bytes};
use cdrs::types::udt::UDT;
use cdrs::error::Result;
use cdrs::IntoBytes;

#[test]
fn simple_udt() {
    let create_type_cql = "CREATE TYPE IF NOT EXISTS cdrs_test.simple_udt (my_text text)";
    let create_table_cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_simple_udt \
                            (my_key int PRIMARY KEY, my_udt simple_udt)";
    let mut session = setup_multiple(&[create_type_cql, create_table_cql]).expect("setup");

    #[derive(Debug, Clone, PartialEq)]
    struct MyUdt {
        pub my_text: String,
    }

    impl MyUdt {
        pub fn try_from(udt: UDT) -> Result<MyUdt> {
            let my_text: String = udt.get_r_by_name("my_text")?;
            Ok(MyUdt {
                my_text: my_text,
            })
        }
    }

    impl Into<Bytes> for MyUdt {
        fn into(self) -> Bytes {
            let mut bytes = Vec::new();
            let val_bytes: Bytes = self.my_text.into();
            bytes.extend_from_slice(Value::new_normal(val_bytes).into_cbytes().as_slice());
            Bytes::new(bytes)
        }
    }

    let my_udt = MyUdt { my_text: "my_text".to_string() };
    let values: Vec<Value> = vec![0i32.into(), my_udt.clone().into()];

    let cql = "INSERT INTO cdrs_test.test_simple_udt \
               (my_key, my_udt) VALUES (?, ?)";
    let query = QueryBuilder::new(cql).values(values).finalize();
    session.query(query, false, false).expect("insert");

    let cql = "SELECT * FROM cdrs_test.test_simple_udt";
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
        let my_udt_row: UDT = row.get_r_by_name("my_udt").expect("my_udt");
        let my_udt_row = MyUdt::try_from(my_udt_row).expect("from udt");
        assert_eq!(my_udt_row, my_udt);
    }
}

#[test]
fn nested_udt() {
    let create_type1_cql = "CREATE TYPE IF NOT EXISTS cdrs_test.nested_inner_udt (my_text text)";
    let create_type2_cql = "CREATE TYPE IF NOT EXISTS cdrs_test.nested_outer_udt (my_inner_udt frozen<nested_inner_udt>)";
    let create_table_cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_nested_udt \
                            (my_key int PRIMARY KEY, my_outer_udt nested_outer_udt)";
    let mut session = setup_multiple(&[create_type1_cql, create_type2_cql, create_table_cql]).expect("setup");

    #[derive(Debug, Clone, PartialEq)]
    struct MyInnerUdt {
        pub my_text: String,
    }

    impl MyInnerUdt {
        pub fn try_from(udt: UDT) -> Result<MyInnerUdt> {
            let my_text: String = udt.get_r_by_name("my_text")?;
            Ok(MyInnerUdt {
                my_text: my_text,
            })
        }
    }

    impl Into<Bytes> for MyInnerUdt {
        fn into(self) -> Bytes {
            let mut bytes = Vec::new();
            let val_bytes: Bytes = self.my_text.into();
            bytes.extend_from_slice(Value::new_normal(val_bytes).into_cbytes().as_slice());
            Bytes::new(bytes)
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    struct MyOuterUdt {
        pub my_inner_udt: MyInnerUdt,
    }

    impl MyOuterUdt {
        pub fn try_from(udt: UDT) -> Result<MyOuterUdt> {
            let my_inner_udt: UDT = udt.get_r_by_name("my_inner_udt")?;
            let my_inner_udt = MyInnerUdt::try_from(my_inner_udt).expect("from udt");
            Ok(MyOuterUdt {
                my_inner_udt: my_inner_udt,
            })
        }
    }

    impl Into<Bytes> for MyOuterUdt {
        fn into(self) -> Bytes {
            let mut bytes = Vec::new();
            let val_bytes: Bytes = self.my_inner_udt.into();
            bytes.extend_from_slice(Value::new_normal(val_bytes).into_cbytes().as_slice());
            Bytes::new(bytes)
        }
    }

    let my_inner_udt = MyInnerUdt { my_text: "my_text".to_string() };
    let my_outer_udt = MyOuterUdt { my_inner_udt: my_inner_udt };
    let values: Vec<Value> = vec![0i32.into(), my_outer_udt.clone().into()];

    let cql = "INSERT INTO cdrs_test.test_nested_udt \
               (my_key, my_outer_udt) VALUES (?, ?)";
    let query = QueryBuilder::new(cql).values(values).finalize();
    session.query(query, false, false).expect("insert");

    let cql = "SELECT * FROM cdrs_test.test_nested_udt";
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
        let my_outer_udt_row: UDT = row.get_r_by_name("my_outer_udt").expect("my_outer_udt");
        let my_outer_udt_row = MyOuterUdt::try_from(my_outer_udt_row).expect("from udt");
        assert_eq!(my_outer_udt_row, my_outer_udt);
    }
}
