// NOTE: having crud_operations do we still need it?
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
use cdrs::query::QueryParamsBuilder;
use cdrs::types::value::Value;
use std::panic;
use cdrs::types::IntoRustByName;
use cdrs::types::map::Map;


// default credentials
const _ADDR: &'static str = "127.0.0.1:9042";


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
    pub some_map: Option<Map>,
}

#[test]
#[cfg(not(feature = "appveyor"))]
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
    let insert_table_cql = "INSERT INTO user_keyspace.users
            (user_name, password, gender, session_token, state)
    VALUES (?, ?, ?, ?, ?)";

    let prepared = session
        .prepare(insert_table_cql.to_string(), true, true)
        .unwrap()
        .get_body()
        .unwrap()
        .into_prepared()
        .unwrap();

    println!("prepared:\n{:?}", prepared);

    let v: Vec<Value> = vec!["harry".into(),
                             "pwd".into(),
                             "male".into(),
                             "09000".into(),
                             "FL".into()];
    let execution_params = QueryParamsBuilder::new(Consistency::One)
        .values(v)
        .finalize();

    let ref query_id = prepared.id;
    let executed = session.execute(query_id, execution_params, true, true);

    info!("executed:\n{:?}", executed);
}

fn read_from_user_table() {
    println!("read_from_user_table");
    let ctx = TestContext::new();
    let mut session = ctx.client.start(Compression::None).unwrap();
    let select_query = QueryBuilder::new("\
        SELECT user_name, password, gender, session_token, state, some_map \
        FROM user_keyspace.users")
            .finalize();

    let query_op = session.query(select_query, true, true);

    match query_op {
        Ok(res) => {
            let res_body = res.get_body().expect("shold have body");
            if let Some(rows) = res_body.into_rows() {
                let users: Vec<User> = rows.iter()
                    .map(|row| {
                        let mut user = User { ..Default::default() };
                        if let Ok(Some(user_name)) = row.get_by_name("user_name") {
                            user.user_name = user_name;
                        }

                        if let Ok(Some(password)) = row.get_by_name("password") {
                            user.password = password;
                        }

                        if let Ok(Some(gender)) = row.get_by_name("gender") {
                            user.gender = gender;
                        }

                        if let Ok(Some(session_token)) = row.get_by_name("session_token") {
                            user.session_token = session_token;
                        }

                        if let Ok(Some(state)) = row.get_by_name("state") {
                            user.state = state;
                        }

                        if let Ok(Some(m)) = row.get_by_name("some_map") {
                            user.some_map = Some(m);
                        }

                        user
                    })
                    .collect();
                println!("Users {:?}", users);


                assert_eq!(users[0].user_name, "harry");
                assert_eq!(users[0].password, "pwd");
                assert_eq!(users[0].gender, "male");
                assert_eq!(users[0].state, "FL");
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
    let create_ks_cql = "CREATE KEYSPACE IF NOT EXISTS user_keyspace WITH REPLICATION = { 'class' \
                         : 'SimpleStrategy', 'replication_factor' : 1 } ;";

    let create_ks_query = QueryBuilder::new(create_ks_cql)
        .consistency(Consistency::One)
        .finalize();

    let create_ks_query_result = session.query(create_ks_query, false, false);

    assert_eq!(create_ks_query_result.is_ok(), true);
}


fn create_table() {
    let ctx = TestContext::new();
    let mut session = ctx.client.start(Compression::None).unwrap();
    let create_table_cql = "CREATE TABLE IF NOT EXISTS user_keyspace.users (
        user_name varchar PRIMARY KEY,
        password varchar,
        gender varchar,
        session_token varchar,
        state varchar,
        birth_year bigint,
        some_map map<text, text>
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
    let drop_ks = "DROP KEYSPACE IF EXISTS user_keyspace;";
    let with_tracing = false;
    let with_warnings = false;
    let drop_ks_query = QueryBuilder::new(drop_ks)
        .consistency(Consistency::One)
        .finalize();
    let drop_ks_query_result = session.query(drop_ks_query, with_tracing, with_warnings);

    assert!(drop_ks_query_result.is_ok(),
            "drop keyspace - query builder failed");

    match drop_ks_query_result {
        Ok(ref res) => println!("keyspace dropped: {:?}", res.get_body()),
        Err(ref err) => println!("Error occured: {:?}", err),
    }
}
