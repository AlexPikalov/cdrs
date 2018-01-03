#[macro_use]
extern crate cdrs;

use cdrs::authenticators::NoneAuthenticator;
use cdrs::cluster::{Cluster, Session};
use cdrs::query::{QueryExecutor, QueryValues};
use cdrs::load_balancing::{Random, RoundRobin};

const _ADDR: &'static str = "127.0.0.1:9042";

const CREATE_KEY_SPACE: &'static str =
  "CREATE KEYSPACE IF NOT EXISTS new_test_ks WITH REPLICATION = { \
   'class' : 'SimpleStrategy', 'replication_factor' : 1 };";

type CurrentSession = Session<RoundRobin, NoneAuthenticator>;

// NO AUTHENTICATION
fn main() {
  let cluster = Cluster::new(vec![_ADDR]);
  let mut no_compression = cluster.connect(RoundRobin::new(), NoneAuthenticator {})
                                  .expect("No compression connection error");
  let mut lz4_compression = cluster.connect_lz4(RoundRobin::new(), NoneAuthenticator {})
                                   .expect("LZ4 compression connection error");
  let mut snappy_compression = cluster.connect_snappy(RoundRobin::new(), NoneAuthenticator {})
                                      .expect("Snappy compression connection error");

  create_keyspace(&mut no_compression);
  create_keyspace(&mut lz4_compression);
  create_keyspace(&mut snappy_compression);

  create_table(&mut no_compression, "no_compression");
  create_table(&mut lz4_compression, "lz4_compression");
  create_table(&mut snappy_compression, "snappy_compression");

  insert_values(&mut no_compression, "no_compression");
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
  session.query(q).expect("Keyspace creation error");
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
  let values_with_names = query_values!("my_bigint" => bigint, "my_int" => int);
}
