#[macro_use]
extern crate cdrs;
#[macro_use]
extern crate maplit;
extern crate regex;
extern crate time;
extern crate uuid;

mod common;

use common::*;

use cdrs::error::Result;
use cdrs::frame::IntoBytes;
use cdrs::query::QueryExecutor;
use cdrs::types::map::Map;
use cdrs::types::udt::UDT;
use cdrs::types::value::{Bytes, Value};
use cdrs::types::{AsRust, IntoRustByName, IntoBytes};
use time::Timespec;

use std::collections::HashMap;

#[test]
#[ignore]
fn simple_udt() {
  let create_type_cql = "CREATE TYPE IF NOT EXISTS cdrs_test.simple_udt (my_text text)";
  let create_table_cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_simple_udt \
                          (my_key int PRIMARY KEY, my_udt simple_udt)";
  let session = setup_multiple(&[create_type_cql, create_table_cql]).expect("setup");

  #[derive(Debug, Clone, PartialEq)]
  struct MyUdt {
    pub my_text: String,
  }

  impl MyUdt {
    pub fn try_from(udt: UDT) -> Result<MyUdt> {
      let my_text: String = udt.get_r_by_name("my_text")?;
      Ok(MyUdt { my_text: my_text })
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

  let my_udt = MyUdt {
    my_text: "my_text".to_string(),
  };
  let values = query_values!(0i32, my_udt.clone());

  let cql = "INSERT INTO cdrs_test.test_simple_udt \
             (my_key, my_udt) VALUES (?, ?)";
  session.query_with_values(cql, values).expect("insert");

  let cql = "SELECT * FROM cdrs_test.test_simple_udt";
  let rows = session
    .query(cql)
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
#[ignore]
fn nested_udt() {
  let create_type1_cql = "CREATE TYPE IF NOT EXISTS cdrs_test.nested_inner_udt (my_text text)";
  let create_type2_cql = "CREATE TYPE IF NOT EXISTS cdrs_test.nested_outer_udt \
                          (my_inner_udt frozen<nested_inner_udt>)";
  let create_table_cql = "CREATE TABLE IF NOT EXISTS cdrs_test.test_nested_udt \
                          (my_key int PRIMARY KEY, my_outer_udt nested_outer_udt)";
  let session =
    setup_multiple(&[create_type1_cql, create_type2_cql, create_table_cql]).expect("setup");

  #[derive(Debug, Clone, PartialEq)]
  struct MyInnerUdt {
    pub my_text: String,
  }

  impl MyInnerUdt {
    pub fn try_from(udt: UDT) -> Result<MyInnerUdt> {
      let my_text: String = udt.get_r_by_name("my_text")?;
      Ok(MyInnerUdt { my_text: my_text })
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

  let my_inner_udt = MyInnerUdt {
    my_text: "my_text".to_string(),
  };
  let my_outer_udt = MyOuterUdt {
    my_inner_udt: my_inner_udt,
  };
  let values = query_values!(0i32, my_outer_udt.clone());

  let cql = "INSERT INTO cdrs_test.test_nested_udt \
             (my_key, my_outer_udt) VALUES (?, ?)";
  session.query_with_values(cql, values).expect("insert");

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
    let my_outer_udt_row: UDT = row.get_r_by_name("my_outer_udt").expect("my_outer_udt");
    let my_outer_udt_row = MyOuterUdt::try_from(my_outer_udt_row).expect("from udt");
    assert_eq!(my_outer_udt_row, my_outer_udt);
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
  ]).expect("setup");

  #[derive(Debug, Clone, PartialEq)]
  struct MyUdtA {
    pub my_text: String,
  }

  impl Into<Bytes> for MyUdtA {
    fn into(self) -> Bytes {
      let mut bytes = Vec::new();
      let val_bytes: Bytes = self.my_text.into();
      bytes.extend_from_slice(Value::new_normal(val_bytes).into_cbytes().as_slice());
      Bytes::new(bytes)
    }
  }

  #[derive(Debug, Clone, PartialEq)]
  struct MyUdtB {
    pub my_text: String,
    pub my_timestamp: Option<Timespec>,
  }

  impl MyUdtB {
    pub fn try_from(udt: UDT) -> Result<MyUdtB> {
      let my_text: String = udt.get_r_by_name("my_text")?;
      let my_timestamp: Option<Timespec> = udt.get_by_name("my_timestamp")?;
      Ok(MyUdtB {
        my_text: my_text,
        my_timestamp: my_timestamp,
      })
    }
  }

  let my_udt_a = MyUdtA {
    my_text: "my_text".to_string(),
  };
  let my_map_a = hashmap! { "1" => my_udt_a.clone() };
  let values = query_values!(0i32, my_map_a.clone());

  let cql = "INSERT INTO cdrs_test.test_alter_udt_add \
             (my_key, my_map) VALUES (?, ?)";
  session.query_with_values(cql, values).expect("insert");

  let cql = "ALTER TYPE cdrs_test.alter_udt_add_udt ADD my_timestamp timestamp";
  session.query(cql).expect("alter type");

  let my_udt_b = MyUdtB {
    my_text: my_udt_a.my_text,
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
    let my_map_row: Map = row.get_r_by_name("my_map").expect("my_map");
    let my_map_row: HashMap<String, UDT> = my_map_row.as_r_rust().expect("my_map as rust");

    for (key, my_udt_row) in my_map_row {
      let my_udt_row = MyUdtB::try_from(my_udt_row).expect("from udt");
      assert_eq!(key, "1");
      assert_eq!(my_udt_row, my_udt_b);
    }
  }
}
