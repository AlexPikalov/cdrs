#[macro_use]
extern crate cdrs;
#[macro_use]
extern crate cdrs_helpers_derive;

use cdrs::authenticators::NoneAuthenticator;
use cdrs::cluster::{Cluster, Session};
use cdrs::query::*;
use cdrs::load_balancing::RoundRobin;
use cdrs::transport::TransportTcp;

use cdrs::types::prelude::*;
use cdrs::types::from_cdrs::FromCDRSByName;
use cdrs::frame::IntoBytes;

type CurrentSession = Session<RoundRobin<TransportTcp>, NoneAuthenticator>;

#[derive(Clone, Debug, IntoCDRSValue, TryFromRow, PartialEq)]
struct RowStruct {
  key: i32,
}

impl RowStruct {
  fn into_query_values(self) -> QueryValues {
    query_values!("key" => self.key)
  }
}

fn main() {
  let cluster = Cluster::new(vec!["127.0.0.1:9042"], NoneAuthenticator {});
  let mut no_compression = cluster
    .connect(RoundRobin::new())
    .expect("No compression connection error");

  create_keyspace(&mut no_compression);
  create_table(&mut no_compression);
  fill_table(&mut no_compression);
  paged_selection_query(&mut no_compression);
}

fn create_keyspace(session: &mut CurrentSession) {
  let create_ks: &'static str = "CREATE KEYSPACE IF NOT EXISTS test_ks WITH REPLICATION = { \
                                 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
  session.query(create_ks).expect("Keyspace creation error");
}

fn create_table(session: &mut CurrentSession) {
  let create_table_cql = "CREATE TABLE IF NOT EXISTS test_ks.my_test_table (key int PRIMARY KEY, \
                          user test_ks.user, map map<text, frozen<test_ks.user>>, list list<frozen<test_ks.user>>);";
  session
    .query(create_table_cql)
    .expect("Table creation error");
}

fn fill_table(session: &mut CurrentSession) {
  let insert_struct_cql = "INSERT INTO test_ks.my_test_table (key) VALUES (?)";

  for k in 100..110 {
    let row = RowStruct {key: k as i32};

    session
      .query_with_values(insert_struct_cql, row.into_query_values())
      .expect("insert");
  }
}

fn paged_selection_query(session: &mut CurrentSession) {
  let q = "SELECT * FROM test_ks.my_test_table;";
  let mut pager = session.paged(2);
  let mut query_pager = pager.query(q);

  loop {
    query_pager.next().expect("pager next");
    println!(" * Pager has more {:?}", query_pager.has_more());

    if !query_pager.has_more() {
      break;
    }
  }
}
