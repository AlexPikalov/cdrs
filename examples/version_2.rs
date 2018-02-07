#[macro_use]
extern crate cdrs;

use cdrs::authenticators::{Authenticator, NoneAuthenticator};
use cdrs::cluster::{Cluster, Session};
use cdrs::query::{ExecExecutor, PrepareExecutor, PreparedQuery, QueryExecutor, QueryValues};
use cdrs::load_balancing::{Random, RoundRobin};
use cdrs::transport::TransportTcp;

const _ADDR: &'static str = "127.0.0.1:9042";

const CREATE_KEY_SPACE: &'static str =
  "CREATE KEYSPACE IF NOT EXISTS new_test_ks WITH REPLICATION = { \
   'class' : 'SimpleStrategy', 'replication_factor' : 1 };";

type CurrentSession = Session<RoundRobin<TransportTcp>, NoneAuthenticator>;

// NO AUTHENTICATION
fn main() {
  let cluster = Cluster::new(vec!["127.0.0.1:9042"], NoneAuthenticator {});
  let mut no_compression = cluster.connect(RoundRobin::new())
                                  .expect("No compression connection error");
  let mut lz4_compression = cluster.connect_lz4(RoundRobin::new())
                                   .expect("LZ4 compression connection error");
  let mut snappy_compression = cluster.connect_snappy(RoundRobin::new())
                                      .expect("Snappy compression connection error");

  create_keyspace(&mut no_compression);
  create_keyspace(&mut lz4_compression);
  create_keyspace(&mut snappy_compression);

  create_table(&mut no_compression, "no_compression");
  create_table(&mut lz4_compression, "lz4_compression");
  create_table(&mut snappy_compression, "snappy_compression");

  insert_values(&mut no_compression, "no_compression");

  select_values(&mut no_compression, "no_compression");
  paged_selection_query(&mut no_compression, "no_compression");

  let prepared_query = prepare_query(&mut no_compression, "no_compression");
  paged_selection_exec(&mut no_compression, "no_compression", &prepared_query);

  let prepared_insert_query = prepare_insert_query(&mut no_compression, "no_compression");
  insert_named_values(&mut no_compression,
                      "no_compression",
                      &prepared_insert_query);
}

fn create_keyspace(session: &mut CurrentSession) {
  session.query(CREATE_KEY_SPACE)
         .expect("Keyspace creation error");
  println!("* Keyspace created");
}

fn create_table(session: &mut CurrentSession, table_name: &str) {
  let q = format!("CREATE TABLE IF NOT EXISTS new_test_ks.{} (my_bigint \
                   bigint PRIMARY KEY, my_int int);",
                  table_name);
  session.query(q).expect("Table creation error");
  println!("* Table {} created", table_name);
}

fn insert_values(session: &mut CurrentSession, table_name: &str) {
  let bigint: i64 = 200;
  let int: i32 = 101;
  let values = query_values!(bigint, int);
  let q = format!("INSERT INTO new_test_ks.{} (my_bigint, my_int) VALUES (?, ?)",
                  table_name);
  session.query_with_values(q, values)
         .expect("Insert values error");
  println!("* Values inserted into {}", table_name);
  let values_with_names = query_values!{"my_bigint" => bigint, "my_int" => int};
  println!("----- {:?}", values_with_names);
}

fn select_values(session: &mut CurrentSession, table_name: &str) {
  let q = format!("SELECT * FROM new_test_ks.{};", table_name);
  session.query(q).expect("Insert values error");
  println!("* Values selected from {}", table_name);
}

fn paged_selection_query(session: &mut CurrentSession, table_name: &str) {
  let q = format!("SELECT * FROM new_test_ks.{};", table_name);
  let mut pager = session.paged(1);
  let mut query_pager = pager.query(q);

  println!("* Paged quering from {}", table_name);

  query_pager.next().expect("ok 1");
  println!("* * Row has more {:?}", query_pager.has_more());

  query_pager.next().expect("ok 2");
  println!("* * Row has more {:?}", query_pager.has_more());

  query_pager.next().expect("ok 3");
  println!("* * Row has more {:?}", query_pager.has_more());
}

fn prepare_query(session: &mut CurrentSession, table_name: &str) -> PreparedQuery {
  let q = format!("SELECT * FROM new_test_ks.{};", table_name);
  session.prepare(q).expect("Prepare query error")
}

fn paged_selection_exec(session: &mut CurrentSession,
                        table_name: &str,
                        prepared_query: &PreparedQuery) {
  let mut pager = session.paged(1);
  let mut exec_pager = pager.exec(prepared_query);

  println!("* Paged exection from {}", table_name);

  exec_pager.next().expect("ok 1");
  println!("* * Row has more {:?}", exec_pager.has_more());

  exec_pager.next().expect("ok 2");
  println!("* * Row has more {:?}", exec_pager.has_more());

  exec_pager.next().expect("ok 3");
  println!("* * Row has more {:?}", exec_pager.has_more());
}

fn prepare_insert_query(session: &mut CurrentSession, table_name: &str) -> PreparedQuery {
  let q = format!("INSERT INTO new_test_ks.{} (my_bigint, my_int) VALUES (?, ?)",
                  table_name);
  session.prepare(q).expect("Prepare insert error")
}

fn insert_named_values(session: &mut CurrentSession,
                       table_name: &str,
                       prepared_query: &PreparedQuery) {
  let values_with_names = query_values!{"my_bigint" => (300 as i64), "my_int" => (301 as i32)};
  session.exec_with_values(prepared_query, values_with_names)
         .expect("exec_with_values error");
  println!("* Named values inserted via exec_with_values");
}
