extern crate cdrs;

use cdrs::client::{CDRS, QueryBuilder};
use cdrs::authenticators::PasswordAuthenticator;
use cdrs::compression::Compression;
use cdrs::consistency::Consistency;

// default credentials
const USER: &'static str = "cassandra";
const PASS: &'static str = "cassandra";
const ADDR: &'static str = "127.0.0.1:9042";

fn main() {
    let authenticator = PasswordAuthenticator::new(USER, PASS);
    let client = CDRS::new(ADDR, authenticator).unwrap();
    let session = client.start(Compression::None).unwrap();

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

    match session.query(create_table_query) {
        Ok(ref res) => println!("table created: {:?}", res.get_body()),
        Err(ref err) => println!("Error occured: {:?}", err)
    }
}
