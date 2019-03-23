#[macro_use]
extern crate cdrs;
#[macro_use]
extern crate cdrs_helpers_derive;
#[macro_use]
extern crate maplit;
extern crate regex;
extern crate time;
extern crate uuid;

mod common;

use common::*;

use cdrs::error::Result as CDRSResult;
use cdrs::frame::IntoBytes;
use cdrs::frame::{TryFromRow, TryFromUDT};
use cdrs::query::QueryExecutor;
use cdrs::query::QueryValues;
use cdrs::types::blob::Blob;
use cdrs::types::from_cdrs::FromCDRSByName;
use cdrs::types::map::Map;
use cdrs::types::rows::Row;
use cdrs::types::udt::UDT;
use cdrs::types::value::{Bytes, Value};
use cdrs::types::{AsRust, AsRustType, IntoRustByName};
use std::str::FromStr;
use time::Timespec;
use uuid::Uuid;

use std::collections::HashMap;

#[test]
#[ignore]
fn simple_udt() {
  let create_type_cql = "CREATE TYPE IF NOT EXISTS cdrs_test.derive_udt (my_text text)";
  let create_table_cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_derived_udt \
                          (my_key int PRIMARY KEY, my_udt derive_udt, my_uuid uuid, my_blob blob)";
  let session = setup_multiple(&[create_type_cql, create_table_cql]).expect("setup");

  #[derive(Clone, Debug, IntoCDRSValue, TryFromRow, PartialEq)]
  struct RowStruct {
    my_key: i32,
    my_udt: MyUdt,
    my_uuid: Uuid,
    my_blob: Blob,
  }

  impl RowStruct {
    fn into_query_values(self) -> QueryValues {
      query_values!("my_key" => self.my_key, "my_udt" => self.my_udt, "my_uuid" => self.my_uuid, "my_blob" => self.my_blob)
    }
  }

  #[derive(Debug, Clone, PartialEq, IntoCDRSValue, TryFromUDT)]
  struct MyUdt {
    pub my_text: String,
  }

  let row_struct = RowStruct {
    my_key: 1i32,
    my_udt: MyUdt {
      my_text: "my_text".to_string(),
    },
    my_uuid: Uuid::from_str("bb16106a-10bc-4a07-baa3-126ffe208c43").unwrap(),
    my_blob: Blob::new(vec![]),
  };

  let cql = "INSERT INTO cdrs_test.test_derived_udt \
             (my_key, my_udt) VALUES (?, ?)";
  session
    .query_with_values(cql, row_struct.clone().into_query_values())
    .expect("insert");

  let cql = "SELECT * FROM cdrs_test.test_derived_udt";
  let rows = session
    .query(cql)
    .expect("query")
    .get_body()
    .expect("get body")
    .into_rows()
    .expect("into rows");

  assert_eq!(rows.len(), 1);
  for row in rows {
    let my_udt_row: RowStruct = RowStruct::try_from_row(row).expect("into RowStruct");
    assert_eq!(my_udt_row, row_struct);
  }
}

#[test]
#[ignore]
fn nested_udt() {
  let create_type1_cql = "CREATE TYPE IF NOT EXISTS cdrs_test.nested_inner_udt (my_text text)";
  let create_type2_cql = "CREATE TYPE IF NOT EXISTS cdrs_test.nested_outer_udt \
                          (my_inner_udt frozen<nested_inner_udt>)";
  let create_table_cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_nested_udt \
                          (my_key int PRIMARY KEY, my_outer_udt nested_outer_udt)";
  let session =
    setup_multiple(&[create_type1_cql, create_type2_cql, create_table_cql]).expect("setup");

  #[derive(Clone, Debug, IntoCDRSValue, TryFromRow, PartialEq)]
  struct RowStruct {
    my_key: i32,
    my_outer_udt: MyOuterUdt,
  }

  impl RowStruct {
    fn into_query_values(self) -> QueryValues {
      query_values!("my_key" => self.my_key, "my_outer_udt" => self.my_outer_udt)
    }
  }

  #[derive(Clone, Debug, IntoCDRSValue, TryFromUDT, PartialEq)]
  struct MyInnerUdt {
    pub my_text: String,
  }

  #[derive(Clone, Debug, IntoCDRSValue, TryFromUDT, PartialEq)]
  struct MyOuterUdt {
    pub my_inner_udt: MyInnerUdt,
  }

  let row_struct = RowStruct {
    my_key: 0,
    my_outer_udt: MyOuterUdt {
      my_inner_udt: MyInnerUdt {
        my_text: "my_text".to_string(),
      },
    },
  };

  let cql = "INSERT INTO cdrs_test.test_nested_udt \
             (my_key, my_outer_udt) VALUES (?, ?)";
  session
    .query_with_values(cql, row_struct.clone().into_query_values())
    .expect("insert");

  let cql = "SELECT * FROM cdrs_test.test_nested_udt";
  let rows = session
    .query(cql)
    .expect("query")
    .get_body()
    .expect("get body")
    .into_rows()
    .expect("into rows");

  assert_eq!(rows.len(), 1);
  for row in rows {
    let my_row_struct: RowStruct = RowStruct::try_from_row(row).expect("into RowStruct");
    assert_eq!(my_row_struct, row_struct);
  }
}

#[test]
#[ignore]
fn alter_udt_add() {
  let drop_table_cql = "DROP TABLE IF EXISTS cdrs_test.test_alter_udt_add";
  let drop_type_cql = "DROP TYPE IF EXISTS cdrs_test.alter_udt_add_udt";
  let create_type_cql = "CREATE TYPE cdrs_test.alter_udt_add_udt (my_text text)";
  let create_table_cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_alter_udt_add \
                          (my_key int PRIMARY KEY, my_map frozen<map<text, alter_udt_add_udt>>)";
  let session = setup_multiple(&[
    drop_table_cql,
    drop_type_cql,
    create_type_cql,
    create_table_cql,
  ])
  .expect("setup");

  #[derive(Clone, Debug, IntoCDRSValue, TryFromRow, PartialEq)]
  struct RowStruct {
    my_key: i32,
    my_map: HashMap<String, MyUdtA>,
  }

  impl RowStruct {
    fn into_query_values(self) -> QueryValues {
      query_values!("my_key" => self.my_key, "my_map" => self.my_map)
    }
  }

  #[derive(Clone, Debug, IntoCDRSValue, TryFromUDT, PartialEq)]
  struct MyUdtA {
    pub my_text: String,
  }

  #[derive(Clone, Debug, IntoCDRSValue, TryFromRow, PartialEq)]
  struct RowStructB {
    my_key: i32,
    my_map: HashMap<String, MyUdtB>,
  }

  #[derive(Clone, Debug, IntoCDRSValue, TryFromUDT, PartialEq)]
  struct MyUdtB {
    pub my_text: String,
    pub my_timestamp: Option<Timespec>,
  }

  let row_struct = RowStruct {
    my_key: 0,
    my_map: hashmap! { "1".to_string() => MyUdtA {my_text: "my_text".to_string()} },
  };

  let cql = "INSERT INTO cdrs_test.test_alter_udt_add \
             (my_key, my_map) VALUES (?, ?)";
  session
    .query_with_values(cql, row_struct.clone().into_query_values())
    .expect("insert");

  let cql = "ALTER TYPE cdrs_test.alter_udt_add_udt ADD my_timestamp timestamp";
  session.query(cql).expect("alter type");

  let expected_nested_udt = MyUdtB {
    my_text: "my_text".to_string(),
    my_timestamp: None,
  };

  let cql = "SELECT * FROM cdrs_test.test_alter_udt_add";
  let rows = session
    .query(cql)
    .expect("query")
    .get_body()
    .expect("get body")
    .into_rows()
    .expect("into rows");

  assert_eq!(rows.len(), 1);
  for row in rows {
    let altered_row: RowStructB = RowStructB::try_from_row(row).expect("into RowStructB");
    assert_eq!(
      altered_row.my_map,
      hashmap! { "1".to_string() => expected_nested_udt.clone() }
    );
  }
}
