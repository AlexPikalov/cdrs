extern crate cdrs;

use std::collections::HashMap;

use cdrs::types::ByName;
use cdrs::client::CDRS;
use cdrs::query::QueryBuilder;
use cdrs::types::{AsRust, IntoRustByName};
use cdrs::authenticators::NoneAuthenticator;
use cdrs::compression::Compression;
use cdrs::transport::TransportTcp;
use cdrs::types::map::Map;

const ADDR: &'static str = "127.0.0.1:9042";

#[test]
#[cfg(not(feature = "appveyor"))]
fn create_keyspace() {
  let authenticator = NoneAuthenticator;
  let tcp_transport = TransportTcp::new(ADDR).expect("create transport");
  let client = CDRS::new(tcp_transport, authenticator);
  let mut session = client.start(Compression::None).expect("start session");

  let drop_ks_cql = "DROP KEYSPACE IF EXISTS create_ks_test";
  let drop_query = QueryBuilder::new(drop_ks_cql).finalize();
  let keyspace_droped = session.query(drop_query, false, false).is_ok();
  assert!(keyspace_droped, "Should drop new keyspace without errors");

  let create_ks_cql = "CREATE KEYSPACE IF NOT EXISTS create_ks_test WITH \
                       replication = {'class': 'SimpleStrategy', 'replication_factor': 1} \
                       AND durable_writes = false";
  let create_query = QueryBuilder::new(create_ks_cql).finalize();
  let keyspace_created = session.query(create_query, false, false).is_ok();
  assert!(keyspace_created,
          "Should create new keyspace without errors");

  let select_ks_cql =
    "SELECT * FROM system_schema.keyspaces WHERE keyspace_name = 'create_ks_test'";
  let select_query = QueryBuilder::new(select_ks_cql).finalize();
  let keyspace_selected = session.query(select_query, false, false)
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
#[cfg(not(feature = "appveyor"))]
fn alter_keyspace() {
  let authenticator = NoneAuthenticator;
  let tcp_transport = TransportTcp::new(ADDR).expect("create transport");
  let client = CDRS::new(tcp_transport, authenticator);
  let mut session = client.start(Compression::None).expect("start session");

  let drop_ks_cql = "DROP KEYSPACE IF EXISTS alter_ks_test";
  let drop_query = QueryBuilder::new(drop_ks_cql).finalize();
  let keyspace_droped = session.query(drop_query, false, false).is_ok();
  assert!(keyspace_droped, "Should drop new keyspace without errors");

  let create_ks_cql = "CREATE KEYSPACE IF NOT EXISTS alter_ks_test WITH \
                       replication = {'class': 'SimpleStrategy', 'replication_factor': 1} \
                       AND durable_writes = false";
  let create_query = QueryBuilder::new(create_ks_cql).finalize();
  let keyspace_created = session.query(create_query, false, false).is_ok();
  assert!(keyspace_created,
          "Should create new keyspace without errors");

  let alter_ks_cql = "ALTER KEYSPACE alter_ks_test WITH \
                      replication = {'class': 'SimpleStrategy', 'replication_factor': 3} \
                      AND durable_writes = false";
  let alter_query = QueryBuilder::new(alter_ks_cql).finalize();
  assert!(session.query(alter_query, false, false).is_ok(),
          "alter should be without errors");

  let select_ks_cql = "SELECT * FROM system_schema.keyspaces WHERE keyspace_name = 'alter_ks_test'";
  let select_query = QueryBuilder::new(select_ks_cql).finalize();
  let keyspace_selected = session.query(select_query, false, false)
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
#[cfg(not(feature = "appveyor"))]
fn use_keyspace() {
  let authenticator = NoneAuthenticator;
  let tcp_transport = TransportTcp::new(ADDR).expect("create transport");
  let client = CDRS::new(tcp_transport, authenticator);
  let mut session = client.start(Compression::None).expect("start session");

  let create_ks_cql = "CREATE KEYSPACE IF NOT EXISTS use_ks_test WITH \
                       replication = {'class': 'SimpleStrategy', 'replication_factor': 1} \
                       AND durable_writes = false";
  let create_query = QueryBuilder::new(create_ks_cql).finalize();
  let keyspace_created = session.query(create_query, false, false).is_ok();
  assert!(keyspace_created,
          "Should create new keyspace without errors");

  let use_ks_cql = "USE use_ks_test";

  let use_query = QueryBuilder::new(use_ks_cql).finalize();
  let keyspace_used = session.query(use_query, false, false)
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
  let authenticator = NoneAuthenticator;
  let tcp_transport = TransportTcp::new(ADDR).expect("create transport");
  let client = CDRS::new(tcp_transport, authenticator);
  let mut session = client.start(Compression::None).expect("start session");

  let create_ks_cql = "CREATE KEYSPACE IF NOT EXISTS drop_ks_test WITH \
                       replication = {'class': 'SimpleStrategy', 'replication_factor': 1} \
                       AND durable_writes = false";
  let create_query = QueryBuilder::new(create_ks_cql).finalize();
  let keyspace_created = session.query(create_query, false, false).is_ok();
  assert!(keyspace_created,
          "Should create new keyspace without errors");

  let drop_ks_cql = "DROP KEYSPACE drop_ks_test";
  let drop_query = QueryBuilder::new(drop_ks_cql).finalize();
  let keyspace_droped = session.query(drop_query, false, false).is_ok();
  assert!(keyspace_droped, "Should drop new keyspace without errors");
}
