extern crate cdrs;

use cdrs::client::{CDRS, Query};
use cdrs::authenticators::PasswordAuthenticator;
use cdrs::compression::Compression;

// default credentials
const user: &'static str = "cassandra";
const pass: &'static str = "cassandra";
const addr: &'static str = "127.0.0.1:9042";

fn main() {
    let authenticator = PasswordAuthenticator::new(user, pass);
    let client = CDRS::new(addr, authenticator).unwrap();
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
    let create_table_query = Query::new(create_table_cql);

    match session.query_with_builder(create_table_query) {
        Ok(ref res) => println!("table created: {:?}", res.get_body()),
        Err(ref err) => println!("Error occured: {:?}", err)
    }
}
