#[macro_use]
extern crate cdrs;
#[macro_use]
extern crate cdrs_helpers_derive;
#[macro_use]
extern crate maplit;

use std::collections::HashMap;

use cdrs::authenticators::NoneAuthenticator;
use cdrs::cluster::{Cluster, Session};
use cdrs::query::*;
use cdrs::load_balancing::{RoundRobin};
use cdrs::transport::TransportTcp;

use cdrs::types::rows::Row;
use cdrs::types::udt::UDT;
use cdrs::types::map::Map;
use cdrs::types::list::List;
use cdrs::frame::{TryFromRow, TryFromUDT};
use cdrs::types::from_cdrs::FromCDRSByName;
use cdrs::types::AsRustType;
use cdrs::types::value::{Bytes, Value};
use cdrs::frame::IntoBytes;

const _ADDR: &'static str = "127.0.0.1:9042";

type CurrentSession = Session<RoundRobin<TransportTcp>, NoneAuthenticator>;

fn main() {
  let cluster = Cluster::new(vec!["127.0.0.1:9042"], NoneAuthenticator {});
  let mut no_compression = cluster
    .connect(RoundRobin::new())
    .expect("No compression connection error");

  create_keyspace(&mut no_compression);
  create_udt(&mut no_compression);
  create_table(&mut no_compression);
  insert_struct(&mut no_compression);
  select_struct(&mut no_compression);
  update_struct(&mut no_compression);
  delete_struct(&mut no_compression);
}

#[derive(Clone, Debug, IntoCDRSValue, TryFromRow, PartialEq)]
struct RowStruct {
  key: i32,
  user: User,
  map: HashMap<String, User>,
  list: Vec<User>,
}

impl RowStruct {
  fn into_query_values(self) -> QueryValues {
    query_values!("key" => self.key, "user" => self.user, "map" => self.map, "list" => self.list)
  }
}

#[derive(Debug, Clone, PartialEq, IntoCDRSValue, TryFromUDT)]
struct User {
  username: String,
}

fn create_keyspace(session: &mut CurrentSession) {
  let create_ks: &'static str = "CREATE KEYSPACE IF NOT EXISTS test_ks WITH REPLICATION = { \
                                 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
  session.query(create_ks).expect("Keyspace creation error");
}

fn create_udt(session: &mut CurrentSession) {
  let create_type_cql = "CREATE TYPE IF NOT EXISTS test_ks.user (username text)";
  session
    .query(create_type_cql)
    .expect("Keyspace creation error");
}

fn create_table(session: &mut CurrentSession) {
  let create_table_cql = "CREATE TABLE IF NOT EXISTS test_ks.my_test_table (key int PRIMARY KEY, \
                          user test_ks.user, map map<text, frozen<test_ks.user>>, list list<frozen<test_ks.user>>);";
  session
    .query(create_table_cql)
    .expect("Table creation error");
}

fn insert_struct(session: &mut CurrentSession) {
  let row = RowStruct {
    key: 3i32,
    user: User {
      username: "John".to_string(),
    },
    map: hashmap! { "John".to_string() => User { username: "John".to_string() } },
    list: vec![
      User {
        username: "John".to_string(),
      },
    ],
  };

  let insert_struct_cql = "INSERT INTO test_ks.my_test_table \
                           (key, user, map, list) VALUES (?, ?, ?, ?)";
  session
    .query_with_values(insert_struct_cql, row.into_query_values())
    .expect("insert");
}

fn select_struct(session: &mut CurrentSession) {
  let select_struct_cql = "SELECT * FROM test_ks.my_test_table";
  let rows = session
    .query(select_struct_cql)
    .expect("query")
    .get_body()
    .expect("get body")
    .into_rows()
    .expect("into rows");

  for row in rows {
    let my_row: RowStruct = RowStruct::try_from_row(row).expect("into RowStruct");
    println!("struct got: {:?}", my_row);
  }
}

fn update_struct(session: &mut CurrentSession) {
  let update_struct_cql = "UPDATE test_ks.my_test_table SET user = ? WHERE key = ?";
  let upd_user = User {
    username: "Marry".to_string(),
  };
  let user_key = 1i32;
  session
    .query_with_values(update_struct_cql, query_values!(upd_user, user_key))
    .expect("update");
}

fn delete_struct(session: &mut CurrentSession) {
  let delete_struct_cql = "DELETE FROM test_ks.my_test_table WHERE key = ?";
  let user_key = 1i32;
  session
    .query_with_values(delete_struct_cql, query_values!(user_key))
    .expect("delete");
}
