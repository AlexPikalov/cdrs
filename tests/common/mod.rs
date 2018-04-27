use regex::Regex;
use cdrs::authenticators::NoneAuthenticator;
use cdrs::compression::Compression;
use cdrs::error::Result;
use cdrs::load_balancing::RoundRobin;
use cdrs::cluster::{Cluster, Session};
use cdrs::query::QueryExecutor;
use cdrs::transport::TransportTcp;

const ADDR: &'static str = "127.0.0.1:9042";

pub type CSession = Session<RoundRobin<TransportTcp>, NoneAuthenticator>;

pub fn setup(create_table_cql: &'static str) -> Result<CSession> {
  setup_multiple(&[create_table_cql])
}

pub fn setup_multiple(create_cqls: &[&'static str]) -> Result<CSession> {
  let authenticator = NoneAuthenticator {};
  let nodes = vec!["127.0.0.1:9042"];
  let cluster = Cluster::new(nodes, authenticator);
  let mut session = cluster.connect(RoundRobin::new())
                           .expect("No compression connection error");
  let re_table_name = Regex::new(r"CREATE TABLE IF NOT EXISTS (\w+\.\w+)").unwrap();

  let create_keyspace_query = "CREATE KEYSPACE IF NOT EXISTS cdrs_test WITH \
                               replication = {'class': 'SimpleStrategy', 'replication_factor': 1} \
                               AND durable_writes = false";
  session.query(create_keyspace_query)?;

  for create_cql in create_cqls.iter() {
    let table_name = re_table_name.captures(create_cql)
                                  .map(|cap| cap.get(1).unwrap().as_str());

    // Re-using tables is a lot faster than creating/dropping them for every test.
    // But if table definitions change while editing tests
    // the old tables need to be dropped. For example by uncommenting the following lines.
    // if let Some(table_name) = table_name {
    //     let cql = format!("DROP TABLE IF EXISTS {}", table_name);
    //     let query = QueryBuilder::new(cql).finalize();
    //     session.query(query, true, true)?;
    // }

    session.query(create_cql.to_owned())?;

    if let Some(table_name) = table_name {
      let cql = format!("TRUNCATE TABLE {}", table_name);
      session.query(cql)?;
    }
  }

  Ok(session)
}
