#[macro_use]
extern crate cdrs;
#[macro_use]
extern crate cdrs_helpers_derive;

use cdrs::authenticators::NoneAuthenticator;
use cdrs::cluster::session::{new as new_session, Session};
use cdrs::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, PagerState, TcpConnectionPool};
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
        query_values!("key" => self.key)
    }
}

#[derive(Clone, Debug, IntoCDRSValue, TryFromRow, PartialEq)]
struct AnotherTestTable {
    a: i32,
    b: i32,
    c: i32,
    d: i32,
    e: i32,
}

impl AnotherTestTable {
    fn into_query_values(self) -> QueryValues {
        query_values!("a" => self.a, "b" => self.b, "c" => self.c, "d" => self.d, "e" => self.e)
    }
}

fn main() {
    let node = NodeTcpConfigBuilder::new("127.0.0.1:9042", NoneAuthenticator {}).build();
    let cluster_config = ClusterTcpConfig(vec![node]);
    let lb = RoundRobin::new();
    let no_compression = new_session(&cluster_config, lb).expect("session should be created");

    create_keyspace(&no_compression);
    create_table(&no_compression);
    fill_table(&no_compression);
    println!("Internal pager state\n");
    paged_selection_query(&no_compression);
    println!("\n\nExternal pager state for stateless executions\n");
    paged_selection_query_with_state(&no_compression, PagerState::new());
    println!("\n\nPager with query values (list)\n");
    // TODO: Why does this method throws an error?
    //paged_with_values_list(&no_compression);
    println!("\n\nPager with query value (no list)\n");
    paged_with_value(&no_compression);
    println!("\n\nFinished paged query tests\n");
}

fn create_keyspace(session: &CurrentSession) {
    let create_ks: &'static str = "CREATE KEYSPACE IF NOT EXISTS test_ks WITH REPLICATION = { \
                                   'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
    session.query(create_ks).expect("Keyspace creation error");
}

fn create_table(session: &CurrentSession) {
    let create_table_cql =
        "CREATE TABLE IF NOT EXISTS test_ks.my_test_table (key int PRIMARY KEY, \
         user test_ks.user, map map<text, frozen<test_ks.user>>, list list<frozen<test_ks.user>>);";
    session
        .query(create_table_cql)
        .expect("Table creation error");
}

fn fill_table(session: &CurrentSession) {
    let insert_struct_cql = "INSERT INTO test_ks.my_test_table (key) VALUES (?)";

    for k in 100..110 {
        let row = RowStruct { key: k as i32 };

        session
            .query_with_values(insert_struct_cql, row.into_query_values())
            .expect("insert");
    }
}

fn paged_with_value(session: &CurrentSession) {
    let create_table_cql =
        "CREATE TABLE IF NOT EXISTS test_ks.another_test_table (a int, b int, c int, d int, e int, primary key((a, b), c, d));";
    session
        .query(create_table_cql)
        .expect("Table creation error");

    for v in 1..=10 {
        session
            .query_with_values("INSERT INTO test_ks.another_test_table (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)",
                               AnotherTestTable {
                                   a: 1,
                                   b: 1,
                                   c: 2,
                                   d: v,
                                   e: v,
                               }.into_query_values(),
            ).unwrap();
    }


    let q = "SELECT * FROM test_ks.another_test_table where a = ? and b = 1 and c = ?";
    let mut pager = session.paged(3);
    let mut query_pager = pager.query(q, Some(query_values!(1, 2)));

    // Oddly enough, this returns false the first time...
    assert!(!query_pager.has_more());

    let mut assert_amount = |a| {
        let rows = query_pager.next().expect("pager next");

        assert_eq!(a, rows.len());
    };

    assert_amount(3);
    assert_amount(3);
    assert_amount(3);
    assert_amount(1);

    assert!(!query_pager.has_more());
}

// TODO: Why does this throw 'Expected 4 or 0 byte int (52)'
fn paged_with_values_list(session: &CurrentSession) {
    let q = "SELECT * FROM test_ks.my_test_table where key in (?)";
    let mut pager = session.paged(2);
    let mut query_pager = pager.query(q, Some(query_values!(vec![100, 101, 102, 103, 104, 105])));

    let mut assert_amount = |a| {
        let rows = query_pager.next().expect("pager next");

        assert_eq!(a, rows.len());
    };

    assert_amount(2);
    assert_amount(2);
    assert_amount(1);
    assert_amount(0);

    assert!(!query_pager.has_more())
}

fn paged_selection_query(session: &CurrentSession) {
    let q = "SELECT * FROM test_ks.my_test_table;";
    let mut pager = session.paged(2);
    let mut query_pager = pager.query(q, None);

    loop {
        let rows = query_pager.next().expect("pager next");
        for row in rows {
            let my_row = RowStruct::try_from_row(row).expect("decode row");
            println!("row - {:?}", my_row);
        }

        if !query_pager.has_more() {
            break;
        }
    }
}

fn paged_selection_query_with_state(session: &CurrentSession, state: PagerState) {
    let mut st = state;

    loop {
        let q = "SELECT * FROM test_ks.my_test_table;";
        let mut pager = session.paged(2);
        let mut query_pager = pager.query_with_pager_state(q, None, st);

        let rows = query_pager.next().expect("pager next");
        for row in rows {
            let my_row = RowStruct::try_from_row(row).expect("decode row");
            println!("row - {:?}", my_row);
        }

        if !query_pager.has_more() {
            break;
        }

        st = query_pager.pager_state();
    }
}
