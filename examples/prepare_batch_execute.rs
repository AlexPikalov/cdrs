#[macro_use]
extern crate cdrs;
#[macro_use]
extern crate cdrs_helpers_derive;

use cdrs::authenticators::NoneAuthenticator;
use cdrs::cluster::session::{new as new_session, Session};
use cdrs::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool};
use cdrs::load_balancing::RoundRobin;
use cdrs::query::*;

use cdrs::frame::IntoBytes;
use cdrs::types::from_cdrs::FromCDRSByName;
use cdrs::types::prelude::*;

type CurrentSession = Session<RoundRobin<TcpConnectionPool<NoneAuthenticator>>>;

#[derive(Clone, Debug, IntoCDRSValue, TryFromRow, PartialEq)]
struct RowStruct {
    key: i32,
}

impl RowStruct {
    fn into_query_values(self) -> QueryValues {
        // **IMPORTANT NOTE:** query values should be WITHOUT NAMES
        // https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L413
        query_values!(self.key)
    }
}

#[tokio::main]
async fn main() {
    let node = NodeTcpConfigBuilder::new("127.0.0.1:9042", NoneAuthenticator {}).build();
    let cluster_config = ClusterTcpConfig(vec![node]);
    let lb = RoundRobin::new();
    let no_compression = new_session(&cluster_config, lb).await.expect("session should be created");

    create_keyspace(&no_compression).await;
    create_table(&no_compression).await;

    let insert_struct_cql = "INSERT INTO test_ks.my_test_table (key) VALUES (?)";
    let prepared_query = no_compression
        .prepare(insert_struct_cql)
        .await
        .expect("Prepare query error");

    for k in 100..110 {
        let row = RowStruct { key: k as i32 };

        insert_row(&no_compression, row, &prepared_query).await;
    }

    batch_few_queries(&no_compression, &insert_struct_cql).await;
}

async fn create_keyspace(session: &CurrentSession) {
    let create_ks: &'static str = "CREATE KEYSPACE IF NOT EXISTS test_ks WITH REPLICATION = { \
                                   'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
    session.query(create_ks).await.expect("Keyspace creation error");
}

async fn create_table(session: &CurrentSession) {
    let create_table_cql =
        "CREATE TABLE IF NOT EXISTS test_ks.my_test_table (key int PRIMARY KEY);";
    session
        .query(create_table_cql)
        .await
        .expect("Table creation error");
}

async fn insert_row(session: &CurrentSession, row: RowStruct, prepared_query: &PreparedQuery) {
    session
        .exec_with_values(prepared_query, row.into_query_values())
        .await
        .expect("exec_with_values error");
}

async fn batch_few_queries(session: &CurrentSession, query: &str) {
    let prepared_query = session.prepare(query).await.expect("Prepare query error");
    let row_1 = RowStruct { key: 1001 as i32 };
    let row_2 = RowStruct { key: 2001 as i32 };

    let batch = BatchQueryBuilder::new()
        .add_query_prepared(prepared_query, row_1.into_query_values())
        .add_query(query, row_2.into_query_values())
        .finalize()
        .expect("batch builder");

    session.batch_with_params(batch).await.expect("batch query error");
}
