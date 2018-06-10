extern crate cdrs;

use cdrs::client::CDRS;
use cdrs::query::BatchQueryBuilder;
use cdrs::authenticators::PasswordAuthenticator;
use cdrs::compression::Compression;
use cdrs::transport::TransportTcp;
use cdrs::query::QueryBuilder;

// default credentials
const _USER: &'static str = "cassandra";
const _PASS: &'static str = "cassandra";
const _ADDR: &'static str = "127.0.0.1:9042";

fn main() {
    let authenticator = PasswordAuthenticator::new(_USER, _PASS);
    let tcp_transport = TransportTcp::new(_ADDR).unwrap();
    let client = CDRS::new(tcp_transport, authenticator);
    let session = client.start(Compression::None).unwrap();

    let create_ks_query = "CREATE KEYSPACE IF NOT EXISTS ks
        WITH REPLICATION = { 
        'class' : 'SimpleStrategy', 
        'replication_factor' : 1 
        };";
    let create_table_query = "CREATE TABLE IF NOT EXISTS ks.integers (integer int PRIMARY KEY);";
    let prepare_query = "INSERT INTO ks.integers (integer) VALUES (1);".to_string();

    let with_tracing = false;
    let with_warnings = false;

    let create_table_query = "CREATE TABLE IF NOT EXISTS ks.integers (integer int PRIMARY KEY);";
    session.query(QueryBuilder::new(create_ks_query).finalize(),
                  with_tracing,
                  with_warnings)
           .expect("create keyspace");

    session.query(QueryBuilder::new(create_table_query).finalize(),
                  with_tracing,
                  with_warnings)
           .expect("create table");

    let prepared = session.prepare(prepare_query, with_tracing, with_warnings)
                          .unwrap()
                          .get_body()
                          .unwrap()
                          .into_prepared()
                          .unwrap();

    let run = "INSERT INTO ks.integers (integer) VALUES (?);".to_string();

    let batch_query = BatchQueryBuilder::new().add_query_prepared(prepared.id.clone(), vec![])
                                              .add_query(run, vec![(None, 2i32.into())])
                                              .finalize()
                                              .unwrap();

    let batched = session.batch(batch_query, false, false).unwrap();

    println!("batch result {:?}", batched.get_body());
}
