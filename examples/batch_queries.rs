
extern crate cdrs;

use cdrs::client::CDRS;
use cdrs::query::BatchQueryBuilder;
use cdrs::authenticators::PasswordAuthenticator;
use cdrs::compression::Compression;
use cdrs::transport::TransportPlain;
// default credentials
const _USER: &'static str = "cassandra";
const _PASS: &'static str = "cassandra";
const _ADDR: &'static str = "127.0.0.1:9042";

/// First create a keyspace 'keyspace'.
/// Then create a new table of integers:
/// CREATE TABLE keyspace.integers (integer int PRIMARY KEY);


fn main() {
    let authenticator = PasswordAuthenticator::new(_USER, _PASS);
    let tcp_transport = TransportPlain::new(_ADDR).unwrap();
    let client = CDRS::new(tcp_transport, authenticator);
    let mut session = client.start(Compression::None).unwrap();

    let prepare_query = "INSERT INTO keyspace.integers (integer) VALUES (1);".to_string();
    let with_tracing = false;
    let with_warnings = false;

    let prepared = session.prepare(prepare_query, with_tracing, with_warnings)
        .unwrap()
        .get_body()
        .into_prepared()
        .unwrap();

    let run = "INSERT INTO keyspace.integers (integer) VALUES (2);".to_string();

    let batch_query = BatchQueryBuilder::new()
        .add_query_prepared(prepared.id.clone(), vec![])
        .add_query(run, vec![])
        .finalize()
        .unwrap();

    let batched = session.batch(batch_query, false, false).unwrap();

    println!("batch result {:?}", batched.get_body());
}
