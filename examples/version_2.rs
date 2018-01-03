extern crate cdrs;

use cdrs::authenticators::NoneAuthenticator;
use cdrs::cluster::{Cluster, Session};
use cdrs::query::QueryExecutor;
use cdrs::load_balancing::{Random, RoundRobin};

const _ADDR: &'static str = "127.0.0.1:9042";

const CREATE_KEY_SPACE: &'static str =
  "CREATE KEYSPACE IF NOT EXISTS new_test_ks WITH REPLICATION = { \
   'class' : 'SimpleStrategy', 'replication_factor' : 1 };";

// NO AUTHENTICATION
fn main() {
  let cluster = Cluster::new(vec![_ADDR]);
  let mut no_compression = cluster.connect(RoundRobin::new(), NoneAuthenticator {})
                                  .expect("No compression connection error");
  let mut lz4_compression = cluster.connect(RoundRobin::new(), NoneAuthenticator {})
                                   .expect("LZ4 compression connection error");
  let mut snappy_compression = cluster.connect(RoundRobin::new(), NoneAuthenticator {})
                                      .expect("Snappy compression connection error");

  create_keyspace(&mut no_compression);
  create_keyspace(&mut lz4_compression);
  create_keyspace(&mut snappy_compression);
}

fn create_keyspace(session: &mut Session) {
  session.query(CREATE_KEY_SPACE)
         .expect("Keyspace creation error");
  println!("* Keyspace created");
}
