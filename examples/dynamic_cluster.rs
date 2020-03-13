#[macro_use]
extern crate cdrs;
#[macro_use]
extern crate cdrs_helpers_derive;
#[macro_use]
extern crate maplit;

use std::collections::HashMap;
use std::io;
use std::process::{Command, Output};

use cdrs::authenticators::NoneAuthenticator;
use cdrs::cluster::session::{new_dynamic as new_session, Session};
use cdrs::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool};
use cdrs::load_balancing::RoundRobin;
use cdrs::query::*;

use cdrs::frame::IntoBytes;
use cdrs::types::from_cdrs::FromCDRSByName;
use cdrs::types::prelude::*;

type CurrentSession = Session<RoundRobin<TcpConnectionPool<NoneAuthenticator>>>;

fn start_node_a<A>(_: A) -> io::Result<Output> {
    Command::new("docker")
        .args(&[
            "run",
            "-d",
            "-p",
            "9042:9042",
            "--name",
            "cass1",
            "cassandra:3.9",
        ])
        .output()
}

fn start_node_b<B>(_: B) -> io::Result<Output> {
    Command::new("docker")
        .args(&[
            "run",
            "-d",
            "-p",
            "9043:9042",
            "--name",
            "cass2",
            "-e",
            "CASSANDRA_SEEDS=\"$(docker inspect --format='{{ .NetworkSettings.IPAddress }}' cass1)\"",
            "cassandra:3.9",
        ])
        .output()
}

fn remove_container_a<A>(_: A) -> io::Result<Output> {
    Command::new("docker")
        .args(&["stop", "cass1"])
        .output()
        .and_then(|_| Command::new("docker").args(&["rm", "cass1"]).output())
}

fn remove_container_b<B>(_: B) -> io::Result<Output> {
    Command::new("docker")
        .args(&["stop", "cass2"])
        .output()
        .and_then(|_| Command::new("docker").args(&["rm", "cass2"]).output())
}

fn start_cluster() {
    println!("> > Starting node a...");
    remove_container_a(())
        .and_then(start_node_a)
        .expect("starting first node");

    ::std::thread::sleep_ms(15_000);

    println!("> > Starting node b...");
    remove_container_b(())
        .and_then(start_node_b)
        .expect("starting second node");

    ::std::thread::sleep_ms(15_000);
}

fn main() {
    let auth = NoneAuthenticator {};
    let node_a = NodeTcpConfigBuilder::new("127.0.0.1:9042", auth.clone()).build();
    let node_b = NodeTcpConfigBuilder::new("127.0.0.1:9043", auth.clone()).build();
    let event_src = NodeTcpConfigBuilder::new("127.0.0.1:9042", auth.clone()).build();
    let cluster_config = ClusterTcpConfig(vec![node_a, node_b]);

    // println!("> Starting cluster...");
    // start_cluster();

    let no_compression: CurrentSession = new_session(&cluster_config, RoundRobin::new(), event_src)
        .expect("session should be created");

    create_keyspace(&no_compression);
    create_udt(&no_compression);
    create_table(&no_compression);

    println!("> Stopping node b...");
    remove_container_b(());
    println!("> waiting 30 secs...");
    ::std::thread::sleep_ms(30_000);
    println!("> stopped");

    insert_struct(&no_compression);
    select_struct(&no_compression);
    update_struct(&no_compression);
    delete_struct(&no_compression);
}

#[derive(Clone, Debug, IntoCDRSValue, TryFromRow, PartialEq)]
struct RowStruct {
    key: i32,
    user: User,
    map: HashMap<String, User>,
    list: Vec<User>,
}

impl RowStruct {
    fn into_query_values(self) -> QueryValues {
        query_values!("key" => self.key, "user" => self.user, "map" => self.map, "list" => self.list)
    }
}

#[derive(Debug, Clone, PartialEq, IntoCDRSValue, TryFromUDT)]
struct User {
    username: String,
}

fn create_keyspace(session: &CurrentSession) {
    let create_ks: &'static str = "CREATE KEYSPACE IF NOT EXISTS test_ks WITH REPLICATION = { \
                                   'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
    session.query(create_ks).expect("Keyspace creation error");
}

fn create_udt(session: &CurrentSession) {
    let create_type_cql = "CREATE TYPE IF NOT EXISTS test_ks.user (username text)";
    session
        .query(create_type_cql)
        .expect("Keyspace creation error");
}

fn create_table(session: &CurrentSession) {
    let create_table_cql =
    "CREATE TABLE IF NOT EXISTS test_ks.my_test_table (key int PRIMARY KEY, \
     user frozen<test_ks.user>, map map<text, frozen<test_ks.user>>, list list<frozen<test_ks.user>>);";
    session
        .query(create_table_cql)
        .expect("Table creation error");
}

fn insert_struct(session: &CurrentSession) {
    let row = RowStruct {
        key: 3i32,
        user: User {
            username: "John".to_string(),
        },
        map: hashmap! { "John".to_string() => User { username: "John".to_string() } },
        list: vec![User {
            username: "John".to_string(),
        }],
    };

    let insert_struct_cql = "INSERT INTO test_ks.my_test_table \
                             (key, user, map, list) VALUES (?, ?, ?, ?)";
    session
        .query_with_values(insert_struct_cql, row.into_query_values())
        .expect("insert");
}

fn select_struct(session: &CurrentSession) {
    let select_struct_cql = "SELECT * FROM test_ks.my_test_table";
    let rows = session
        .query(select_struct_cql)
        .expect("query")
        .get_body()
        .expect("get body")
        .into_rows()
        .expect("into rows");

    for row in rows {
        let my_row: RowStruct = RowStruct::try_from_row(row).expect("into RowStruct");
        println!("struct got: {:?}", my_row);
    }
}

fn update_struct(session: &CurrentSession) {
    let update_struct_cql = "UPDATE test_ks.my_test_table SET user = ? WHERE key = ?";
    let upd_user = User {
        username: "Marry".to_string(),
    };
    let user_key = 1i32;
    session
        .query_with_values(update_struct_cql, query_values!(upd_user, user_key))
        .expect("update");
}

fn delete_struct(session: &CurrentSession) {
    let delete_struct_cql = "DELETE FROM test_ks.my_test_table WHERE key = ?";
    let user_key = 1i32;
    session
        .query_with_values(delete_struct_cql, query_values!(user_key))
        .expect("delete");
}
