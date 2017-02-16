extern crate cdrs;
#[macro_use]
extern crate log;
extern crate env_logger;

use cdrs::client::CDRS;
use cdrs::query::QueryBuilder;
use cdrs::authenticators::NoneAuthenticator;
use cdrs::compression::Compression;
use cdrs::consistency::Consistency;
use cdrs::transport::TransportTcp;
use std::panic;
use cdrs::types::IntoRustByName;
use cdrs::prepared_statement::PrepareAndExecute;


// default credentials
const _ADDR: &'static str = "127.0.0.1:9042";
const _CUSTOMER_NAME: &'static str = "david candy";
const _PWD: &'static str = "password";
const _GENDER: &'static str = "male";
const _STATE: &'static str = "FLL";



pub struct TestContext {
    pub client: cdrs::client::CDRS<NoneAuthenticator, TransportTcp>,
}

#[derive(Debug, Default)]
struct User {
    pub user_name: String,
    pub password: String,
    pub gender: String,
    pub session_token: String,
    pub state: String,
}


#[test]
fn write_and_read_from_cassandra() {
    run_test(|| read_write())
}

fn read_write() {
    println!("read_write");
    insert_data_users();
    read_from_user_table();
}

///
/// right now we don't have setup and teardown that is the reason for this monstrosity
/// there might be better ways to write testing code; have to revisit later
/// the flow goes likes this
/// 1. setup() ==> create new keyspace -> create new tables
/// 2. read_write ==>
///     a) insert_data_users()
///     b) read_from_user_table()
/// 3. teardown() ==> drop the keyspace
fn run_test<T>(test: T) -> ()
    where T: FnOnce() -> () + panic::UnwindSafe
{
    setup();

    let result = panic::catch_unwind(|| test());

    teardown();

    assert!(result.is_ok())
}

fn setup() {
    create_keyspace();
    create_table();
}

fn teardown() {
    drop_keyspace();
}


fn insert_data_users() {
    println!("insert_data_users");
    let ctx = TestContext::new();
    let mut session = ctx.client.start(Compression::None).unwrap();
    let insert_table_cql = " insert into user_keyspace.users
            (user_name ,password,gender,session_token, state)
    values  (?         ,  ?     ,     ?,            ?,     ?)";



    let mut prepared = session.prepare_statement(insert_table_cql.to_string(), true, true).unwrap();

    prepared.set_string(1, _CUSTOMER_NAME).unwrap();
    prepared.set_string(2, _PWD).unwrap();
    prepared.set_string(3, _GENDER).unwrap();
    prepared.set_string(4, "09000").unwrap();
    prepared.set_string(5, _STATE).unwrap();

    println!("prepared:\n{:?}", prepared);

    let executed = session.execute_statement(prepared);


    info!("executed:\n{:?}", executed);
}

fn read_from_user_table() {
    println!("read_from_user_table");
    let ctx = TestContext::new();
    let mut session = ctx.client.start(Compression::None).unwrap();
    let select_query = QueryBuilder::new("SELECT user_name ,password,gender,session_token, state
     FROM user_keyspace.users")
        .finalize();


    let query_op = session.query(select_query, true, true);

    match query_op {
        Ok(res) => {
            let res_body = res.get_body();
            if let Some(rows) = res_body.into_rows() {
                let users: Vec<User> = rows.iter()
                    .map(|row| {
                        let mut user = User { ..Default::default() };
                        if let Some(Ok(user_name)) = row.get_by_name("user_name") {
                            user.user_name = user_name;
                        }

                        if let Some(Ok(password)) = row.get_by_name("password") {
                            user.password = password;
                        }

                        if let Some(Ok(gender)) = row.get_by_name("gender") {
                            user.gender = gender;
                        }

                        if let Some(Ok(session_token)) = row.get_by_name("session_token") {
                            user.session_token = session_token;
                        }

                        if let Some(Ok(state)) = row.get_by_name("state") {
                            user.state = state;
                        }

                        user
                    })
                    .collect();
                println!("Users {:?}", users);


                assert_eq!(users[0].user_name, _CUSTOMER_NAME);
                assert_eq!(users[0].password, _PWD);
                assert_eq!(users[0].gender, _GENDER);
                assert_eq!(users[0].state, _STATE);
            }
        }
        Err(err) => println!("{:?}", err),
    }
}


impl TestContext {
    fn new() -> TestContext {
        const _ADDR: &'static str = "127.0.0.1:9042";
        let authenticator = NoneAuthenticator;
        let tcp_transport = TransportTcp::new(_ADDR);
        let client = CDRS::new(tcp_transport.unwrap(), authenticator);
        TestContext { client: client }
    }
}


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
