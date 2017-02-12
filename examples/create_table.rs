
extern crate cdrs;

use cdrs::client::CDRS;
use cdrs::query::QueryBuilder;
use cdrs::authenticators::PasswordAuthenticator;
use cdrs::compression::Compression;
use cdrs::consistency::Consistency;
use cdrs::transport::TransportTcp;
// default credentials
const _USER: &'static str = "cassandra";
const _PASS: &'static str = "cassandra";
const _ADDR: &'static str = "127.0.0.1:9042";


fn main() {
    let authenticator = PasswordAuthenticator::new(_USER, _PASS);
    let tcp_transport = TransportTcp::new(_ADDR).unwrap();
    let client = CDRS::new(tcp_transport, authenticator);
    let mut session = client.start(Compression::None).unwrap();

    // NOTE: keyspace "keyspace" should already exist
    let create_table_cql = "CREATE TABLE keyspace.users (
        user_name varchar PRIMARY KEY,
        password varchar,
        gender varchar,
        session_token varchar,
        state varchar,
        birth_year bigint
    );";
    let create_table_query = QueryBuilder::new(create_table_cql)
        .consistency(Consistency::One)
        .finalize();
    let with_tracing = false;
    let with_warnings = false;

    match session.query(create_table_query, with_tracing, with_warnings) {
        Ok(ref res) => println!("table created: {:?}", res.get_body()),
        Err(ref err) => println!("Error occured: {:?}", err),
    }
}
