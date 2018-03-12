extern crate cdrs;

use std::collections::HashMap;

use cdrs::authenticators::NoneAuthenticator;
use cdrs::cluster::Cluster;
use cdrs::load_balancing::RoundRobin;
use cdrs::query::QueryExecutor;
use cdrs::types::{AsRust, ByName, IntoRustByName};
use cdrs::types::map::Map;

const ADDR: &'static str = "127.0.0.1:9042";

#[test]
#[ignore]
fn create_keyspace() {
  let cluster = Cluster::new(vec!["127.0.0.1:9042"], NoneAuthenticator {});
  let mut session = cluster.connect(RoundRobin::new())
                                  .expect("No compression connection error");

  let drop_query = "DROP KEYSPACE IF EXISTS create_ks_test";
  let keyspace_droped = session.query(drop_query).is_ok();
  assert!(keyspace_droped, "Should drop new keyspace without errors");

  let create_query = "CREATE KEYSPACE IF NOT EXISTS create_ks_test WITH \
                       replication = {'class': 'SimpleStrategy', 'replication_factor': 1} \
                       AND durable_writes = false";
  let keyspace_created = session.query(create_query).is_ok();
  assert!(keyspace_created,
          "Should create new keyspace without errors");

  let select_query =
    "SELECT * FROM system_schema.keyspaces WHERE keyspace_name = 'create_ks_test'";
  let keyspace_selected = session.query(select_query)
                                 .expect("select keyspace query")
                                 .get_body()
                                 .expect("get select keyspace query body")
                                 .into_rows()
                                 .expect("convert keyspaces results into rows");

  assert_eq!(keyspace_selected.len(), 1);
  let keyspace = &keyspace_selected[0];

  let keyspace_name: String = keyspace.get_r_by_name("keyspace_name")
                                      .expect("keyspace name into rust error");
  assert_eq!(keyspace_name,
             "create_ks_test".to_string(),
             "wrong keyspace name");

  let durable_writes: bool = keyspace.get_r_by_name("durable_writes")
                                     .expect("durable writes into rust error");
  assert_eq!(durable_writes, false, "wrong durable writes");

  let mut expected_strategy_options: HashMap<String, String> = HashMap::new();
  expected_strategy_options.insert("replication_factor".to_string(), "1".to_string());
  expected_strategy_options.insert("class".to_string(),
                                   "org.apache.cassandra.locator.SimpleStrategy".to_string());
  let strategy_options: HashMap<String, String> =
    keyspace.r_by_name::<Map>("replication")
            .expect("strategy optioins into rust error")
            .as_r_rust()
            .expect("uuid_key_map");
  assert_eq!(expected_strategy_options,
             strategy_options,
             "wrong strategy options");
}

#[test]
#[ignore]
fn alter_keyspace() {
  let cluster = Cluster::new(vec!["127.0.0.1:9042"], NoneAuthenticator {});
  let mut session = cluster.connect(RoundRobin::new())
                                  .expect("No compression connection error");

  let drop_query = "DROP KEYSPACE IF EXISTS alter_ks_test";
  let keyspace_droped = session.query(drop_query).is_ok();
  assert!(keyspace_droped, "Should drop new keyspace without errors");

  let create_query = "CREATE KEYSPACE IF NOT EXISTS alter_ks_test WITH \
                       replication = {'class': 'SimpleStrategy', 'replication_factor': 1} \
                       AND durable_writes = false";
  let keyspace_created = session.query(create_query).is_ok();
  assert!(keyspace_created,
          "Should create new keyspace without errors");

  let alter_query = "ALTER KEYSPACE alter_ks_test WITH \
                      replication = {'class': 'SimpleStrategy', 'replication_factor': 3} \
                      AND durable_writes = false";
  assert!(session.query(alter_query).is_ok(),
          "alter should be without errors");

  let select_query = "SELECT * FROM system_schema.keyspaces WHERE keyspace_name = 'alter_ks_test'";
  let keyspace_selected = session.query(select_query)
                                 .expect("select keyspace query")
                                 .get_body()
                                 .expect("get select keyspace query body")
                                 .into_rows()
                                 .expect("convert keyspaces results into rows");

  assert_eq!(keyspace_selected.len(), 1);
  let keyspace = &keyspace_selected[0];

  let strategy_options: HashMap<String, String> =
    keyspace.r_by_name::<Map>("replication")
            .expect("strategy optioins into rust error")
            .as_r_rust()
            .expect("uuid_key_map");

  assert_eq!(strategy_options.get("replication_factor")
                             .expect("replication_factor unwrap"),
             &"3".to_string());
}

#[test]
#[ignore]
fn use_keyspace() {
  let cluster = Cluster::new(vec!["127.0.0.1:9042"], NoneAuthenticator {});
  let mut session = cluster.connect(RoundRobin::new())
                                  .expect("No compression connection error");

  let create_query = "CREATE KEYSPACE IF NOT EXISTS use_ks_test WITH \
                       replication = {'class': 'SimpleStrategy', 'replication_factor': 1} \
                       AND durable_writes = false";
  let keyspace_created = session.query(create_query).is_ok();
  assert!(keyspace_created,
          "Should create new keyspace without errors");

  let use_query = "USE use_ks_test";
  let keyspace_used = session.query(use_query)
                             .expect("should use selected")
                             .get_body()
                             .expect("should get body")
                             .into_set_keyspace()
                             .expect("set keyspace")
                             .body;
  assert_eq!(keyspace_used.as_str(), "use_ks_test", "wrong kespace used");
}

#[test]
#[cfg(not(feature = "appveyor"))]
fn drop_keyspace() {
  let cluster = Cluster::new(vec!["127.0.0.1:9042"], NoneAuthenticator {});
  let mut session = cluster.connect(RoundRobin::new())
                                  .expect("No compression connection error");

  let create_query = "CREATE KEYSPACE IF NOT EXISTS drop_ks_test WITH \
                       replication = {'class': 'SimpleStrategy', 'replication_factor': 1} \
                       AND durable_writes = false";
  let keyspace_created = session.query(create_query).is_ok();
  assert!(keyspace_created,
          "Should create new keyspace without errors");

  let drop_query = "DROP KEYSPACE drop_ks_test";
  let keyspace_droped = session.query(drop_query).is_ok();
  assert!(keyspace_droped, "Should drop new keyspace without errors");
}
