
extern crate cdrs;

use cdrs::client::CDRS;
use cdrs::query::QueryBuilder;
use cdrs::authenticators::NoneAuthenticator;
use cdrs::compression::Compression;
use cdrs::consistency::Consistency;
use cdrs::transport::TransportPlain;
// default credentials
const _ADDR: &'static str = "127.0.0.1:9042";


#[test]
fn it_works() {
    const _ADDR: &'static str = "127.0.0.1:9042";
    let authenticator = NoneAuthenticator;
    let tcp_transport = TransportPlain::new(_ADDR);
    assert_eq!(tcp_transport.is_ok(), true);

    let client = CDRS::new(tcp_transport.unwrap(), authenticator);
    let mut session = client.start(Compression::None).unwrap();
    let drop_ks = "DROP KEYSPACE my_ks;";
    let create_ks_cql = "CREATE KEYSPACE my_ks WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } ;";
    let create_table_cql = "CREATE TABLE my_ks.users (
        user_name varchar PRIMARY KEY,
        password varchar,
        gender varchar,
        session_token varchar,
        state varchar,
        birth_year bigint
    );";

    let with_tracing = false;
    let with_warnings = false;


    let drop_ks_query = QueryBuilder::new(drop_ks)
        .consistency(Consistency::One)
        .finalize();

    let drop_ks_query_result =  session.query(drop_ks_query, with_tracing, with_warnings) ;

    assert_eq!(drop_ks_query_result.is_ok(), true);

    match drop_ks_query_result {
        Ok(ref res) => println!("keyspace dropped: {:?}", res.get_body()),
        Err(ref err) => println!("Error occured: {:?}", err)
    }

    let create_ks_query = QueryBuilder::new(create_ks_cql)
        .consistency(Consistency::One)
        .finalize();

    let create_ks_query_result =  session.query(create_ks_query, with_tracing, with_warnings);

    assert_eq!(create_ks_query_result.is_ok(), true);


    match create_ks_query_result {
        Ok(ref res) => println!("keyspace created: {:?}", res.get_body()),
        Err(ref err) => println!("Error occured: {:?}", err)
    }


    let create_table_query = QueryBuilder::new(create_table_cql)
        .consistency(Consistency::One)
        .finalize();
    match session.query(create_table_query, with_tracing, with_warnings) {
        Ok(ref res) => println!("table created: {:?}", res.get_body()),
        Err(ref err) => println!("Error occured: {:?}", err)
    }
}
