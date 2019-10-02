#[macro_use]
extern crate cdrs;
#[macro_use]
extern crate cdrs_helpers_derive;

use cdrs::authenticators::NoneAuthenticator;
use cdrs::cluster::session::{new as new_session, Session};
use cdrs::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, PagerState, TcpConnectionPool};
use cdrs::load_balancing::RoundRobin;
use cdrs::query::*;

use cdrs::frame::IntoBytes;
use cdrs::types::from_cdrs::FromCDRSByName;
use cdrs::types::prelude::*;

type CurrentSession = Session<RoundRobin<TcpConnectionPool<NoneAuthenticator>>>;

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
  let node = NodeTcpConfigBuilder::new("127.0.0.1:9042", NoneAuthenticator {}).build();
  let cluster_config = ClusterTcpConfig(vec![node]);
  let lb = RoundRobin::new();
  let no_compression = new_session(&cluster_config, lb).expect("session should be created");

  create_keyspace(&no_compression);
  create_table(&no_compression);
  fill_table(&no_compression);
  println!("Internal pager state\n");
  paged_selection_query(&no_compression);
  println!("\n\nExternal pager state for stateless executions\n");
  paged_selection_query_with_state(&no_compression, PagerState::new());
}

fn create_keyspace(session: &CurrentSession) {
  let create_ks: &'static str = "CREATE KEYSPACE IF NOT EXISTS test_ks WITH REPLICATION = { \
                                 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
  session.query(create_ks).expect("Keyspace creation error");
}

fn create_table(session: &CurrentSession) {
  let create_table_cql =
    "CREATE TABLE IF NOT EXISTS test_ks.my_test_table (key int PRIMARY KEY, \
     user test_ks.user, map map<text, frozen<test_ks.user>>, list list<frozen<test_ks.user>>);";
  session.query(create_table_cql)
         .expect("Table creation error");
}

fn fill_table(session: &CurrentSession) {
  let insert_struct_cql = "INSERT INTO test_ks.my_test_table (key) VALUES (?)";

  for k in 100..110 {
    let row = RowStruct { key: k as i32 };

    session.query_with_values(insert_struct_cql, row.into_query_values())
           .expect("insert");
  }
}

fn paged_selection_query(session: &CurrentSession) {
  let q = "SELECT * FROM test_ks.my_test_table;";
  let mut pager = session.paged(2);
  let mut query_pager = pager.query(q);

  loop {
    let rows = query_pager.next().expect("pager next");
    for row in rows {
      let my_row = RowStruct::try_from_row(row).expect("decode row");
      println!("row - {:?}", my_row);
    }

    if !query_pager.has_more() {
      break;
    }
  }
}

fn paged_selection_query_with_state(session: &CurrentSession, state: PagerState) {
  let mut st = state;

  loop {
    let q = "SELECT * FROM test_ks.my_test_table;";
    let mut pager = session.paged(2);
    let mut query_pager = pager.query_with_pager_state(q, st);

    let rows = query_pager.next().expect("pager next");
    for row in rows {
      let my_row = RowStruct::try_from_row(row).expect("decode row");
      println!("row - {:?}", my_row);
    }

    if !query_pager.has_more() {
      break;
    }

    st = query_pager.pager_state();
  }
}
