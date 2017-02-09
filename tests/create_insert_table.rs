
extern crate cdrs;
#[macro_use]
extern crate log;
extern crate env_logger;

use cdrs::client::CDRS;
use cdrs::query::QueryBuilder;
use cdrs::authenticators::NoneAuthenticator;
use cdrs::compression::Compression;
use cdrs::consistency::Consistency;
use cdrs::transport::TransportPlain;
use cdrs::query::QueryParamsBuilder;
use cdrs::types::value::Value;

use cdrs::types::CString;
use cdrs::IntoBytes;
// default credentials
const _ADDR: &'static str = "127.0.0.1:9042";



pub struct TestContext {
    pub client: cdrs::client::CDRS<NoneAuthenticator, TransportPlain>,
}

impl TestContext {
    fn new() -> TestContext {
        const _ADDR: &'static str = "127.0.0.1:9042";
        let authenticator = NoneAuthenticator;
        let tcp_transport = TransportPlain::new(_ADDR);
        let client = CDRS::new(tcp_transport.unwrap(), authenticator);
        TestContext { client: client }
    }
}

#[test]
fn create_keyspace() {
    let ctx = TestContext::new();
    let mut session = ctx.client.start(Compression::None).unwrap();
    let create_ks_cql = "CREATE KEYSPACE user_keyspace WITH REPLICATION = { 'class' : \
                         'SimpleStrategy', 'replication_factor' : 1 } ;";

    let create_ks_query = QueryBuilder::new(create_ks_cql)
        .consistency(Consistency::One)
        .finalize();

    let create_ks_query_result = session.query(create_ks_query, false, false);

    assert_eq!(create_ks_query_result.is_ok(), true);


}

#[test]
fn create_table() {
    let ctx = TestContext::new();
    let mut session = ctx.client.start(Compression::None).unwrap();
    let create_table_cql = "CREATE TABLE user_keyspace.users (
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

    let create_table_query_result = session.query(create_table_query, false, false);

    assert_eq!(create_table_query_result.is_ok(), true);

    match create_table_query_result {
        Ok(ref res) => println!("table created: {:?}", res.get_body()),
        Err(ref err) => println!("Error occured: {:?}", err),
    }


}


#[test]
fn insert_data_users() {
    use cdrs::types::CString;
    let ctx = TestContext::new();
    let mut session = ctx.client.start(Compression::None).unwrap();
    let insert_table_cql = " insert into user_keyspace.users
            (user_name ,password,gender,session_token, state,birth_year)
    values  (?         ,  ?     ,     ?,            ?,     ?,  ?)";


    let prepared = session.prepare(insert_table_cql.to_string(), true, true)
        .unwrap()
        .get_body()
        .into_prepared()
        .unwrap();

    println!("prepared:\n{:?}", prepared);

    //let v =  v: Vec<Value>


    let v: Vec<Value> = vec![
    convert_to_value("harry"),
    convert_to_value("pwd"),
    convert_to_value("male"),
    convert_to_value("09000"),
    convert_to_value("FL"),
    convert_to_value("123")];
    let execution_params = QueryParamsBuilder::new(Consistency::One).values(v).finalize();
    //let execution_params = QueryParamsBuilder::new(Consistency::One).finalize();

    let query_id = prepared.id;
    let executed = session.execute(query_id, execution_params, true, true)
        .unwrap()
        .get_body()
        .into_set_keyspace()
        .unwrap();

    info!("executed:\n{:?}", executed);

}

#[test]
fn drop_keyspace() {
    let ctx = TestContext::new();
    let mut session = ctx.client.start(Compression::None).unwrap();
    let drop_ks = "DROP KEYSPACE user_keyspace;";
    let with_tracing = false;
    let with_warnings = false;
    let drop_ks_query = QueryBuilder::new(drop_ks).consistency(Consistency::One).finalize();
    let drop_ks_query_result = session.query(drop_ks_query, with_tracing, with_warnings);

    assert_eq!(drop_ks_query_result.is_ok(), true);

    match drop_ks_query_result {
        Ok(ref res) => println!("keyspace dropped: {:?}", res.get_body()),
        Err(ref err) => println!("Error occured: {:?}", err),
    }
}


fn convert_to_value (s:&str) -> Value {
    Value::new_normal(CString::new(s.to_string()).into_cbytes())

}