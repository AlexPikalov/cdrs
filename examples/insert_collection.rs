#[macro_use]
extern crate cdrs;
#[macro_use]
extern crate cdrs_helpers_derive;
#[macro_use]
extern crate maplit;

use std::collections::HashMap;

use cdrs::authenticators::StaticPasswordAuthenticator;
use cdrs::cluster::session::{new as new_session, Session};
use cdrs::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool};
use cdrs::load_balancing::RoundRobin;
use cdrs::query::*;

use cdrs::frame::IntoBytes;
use cdrs::types::from_cdrs::FromCDRSByName;
use cdrs::types::prelude::*;

type CurrentSession = Session<RoundRobin<TcpConnectionPool<StaticPasswordAuthenticator>>>;

#[tokio::main]
async fn main() {
    let user = "user";
    let password = "password";
    let auth = StaticPasswordAuthenticator::new(&user, &password);
    let node = NodeTcpConfigBuilder::new("127.0.0.1:9042", auth).build();
    let cluster_config = ClusterTcpConfig(vec![node]);
    let no_compression: CurrentSession =
        new_session(&cluster_config, RoundRobin::new()).await.expect("session should be created");

    create_keyspace(&no_compression).await;
    create_udt(&no_compression).await;
    create_table(&no_compression).await;

    insert_struct(&no_compression).await;
    append_list(&no_compression).await;
    prepend_list(&no_compression).await;
    append_set(&no_compression).await;
    append_map(&no_compression).await;

    select_struct(&no_compression).await;
    delete_struct(&no_compression).await;
}

#[derive(Clone, Debug, IntoCDRSValue, TryFromRow, PartialEq)]
struct RowStruct {
    key: i32,
    map: HashMap<String, User>,
    list: Vec<User>,
    cset: Vec<User>,
}

impl RowStruct {
    fn into_query_values(self) -> QueryValues {
        query_values!("key" => self.key, "map" => self.map, "list" => self.list, "cset" => self.cset)
    }
}

#[derive(Debug, Clone, PartialEq, IntoCDRSValue, TryFromUDT)]
struct User {
    username: String,
}

async fn create_keyspace(session: &CurrentSession) {
    let create_ks: &'static str = "CREATE KEYSPACE IF NOT EXISTS test_ks WITH REPLICATION = { \
                                   'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
    session.query(create_ks).await.expect("Keyspace creation error");
}

async fn create_udt(session: &CurrentSession) {
    let create_type_cql = "CREATE TYPE IF NOT EXISTS test_ks.user (username text)";
    session
        .query(create_type_cql)
        .await
        .expect("Keyspace creation error");
}

async fn create_table(session: &CurrentSession) {
    let create_table_cql =
        "CREATE TABLE IF NOT EXISTS test_ks.collection_table (key int PRIMARY KEY, \
         user frozen<test_ks.user>, map map<text, frozen<test_ks.user>>, \
         list list<frozen<test_ks.user>>, cset set<frozen<test_ks.user>>);";
    session
        .query(create_table_cql)
        .await
        .expect("Table creation error");
}

async fn append_list(session: &CurrentSession) {
    let key = 3i32;
    let extra_values = vec![
        User {
            username: "William".to_string(),
        },
        User {
            username: "Averel".to_string(),
        },
    ];
    let append_list_cql = "UPDATE test_ks.collection_table SET list = list + ? \
                           WHERE key = ?";
    session
        .query_with_values(append_list_cql, query_values!(extra_values, key))
        .await
        .expect("append list");
}

async fn prepend_list(session: &CurrentSession) {
    let key = 3i32;
    let extra_values = vec![
        User {
            username: "Joe".to_string(),
        },
        User {
            username: "Jack".to_string(),
        },
    ];
    let prepend_list_cql = "UPDATE test_ks.collection_table SET list = ? + list \
                            WHERE key = ?";
    session
        .query_with_values(prepend_list_cql, query_values!(extra_values, key))
        .await
        .expect("prepend list");
}

async fn append_set(session: &CurrentSession) {
    let key = 3i32;
    let extra_values = vec![
        User {
            username: "William".to_string(),
        },
        User {
            username: "Averel".to_string(),
        },
    ];
    let append_set_cql = "UPDATE test_ks.collection_table SET cset = cset + ? \
                          WHERE key = ?";
    session
        .query_with_values(append_set_cql, query_values!(extra_values, key))
        .await
        .expect("append set");
}

async fn append_map(session: &CurrentSession) {
    let key = 3i32;
    let extra_values = hashmap![
        "Joe".to_string() => User { username: "Joe".to_string() },
        "Jack".to_string() => User { username: "Jack".to_string() },
    ];
    let append_map_cql = "UPDATE test_ks.collection_table SET map = map + ? \
                          WHERE key = ?";
    session
        .query_with_values(append_map_cql, query_values!(extra_values, key))
        .await
        .expect("append map");
}

async fn insert_struct(session: &CurrentSession) {
    let row = RowStruct {
        key: 3i32,
        map: hashmap! { "John".to_string() => User { username: "John".to_string() } },
        list: vec![User {
            username: "John".to_string(),
        }],
        cset: vec![User {
            username: "John".to_string(),
        }],
    };

    let insert_struct_cql = "INSERT INTO test_ks.collection_table \
                             (key, map, list, cset) VALUES (?, ?, ?, ?)";
    session
        .query_with_values(insert_struct_cql, row.into_query_values())
        .await
        .expect("insert");
}

async fn select_struct(session: &CurrentSession) {
    let select_struct_cql = "SELECT * FROM test_ks.collection_table";
    let rows = session
        .query(select_struct_cql)
        .await
        .expect("query")
        .get_body()
        .expect("get body")
        .into_rows()
        .expect("into rows");

    for row in rows {
        let my_row: RowStruct = RowStruct::try_from_row(row).expect("into RowStruct");
        println!("struct got: {:#?}", my_row);
    }
}

async fn delete_struct(session: &CurrentSession) {
    let delete_struct_cql = "DELETE FROM test_ks.collection_table WHERE key = ?";
    let user_key = 3i32;
    session
        .query_with_values(delete_struct_cql, query_values!(user_key))
        .await
        .expect("delete");
}
