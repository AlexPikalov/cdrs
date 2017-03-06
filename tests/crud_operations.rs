// TODO: add update and delete operations
extern crate cdrs;
extern crate uuid;

use uuid::Uuid;
use std::convert::Into;
use cdrs::IntoBytes;
use cdrs::client::{CDRS, Session};
use cdrs::consistency::Consistency;
use cdrs::query::{QueryBuilder, QueryParamsBuilder};
use cdrs::authenticators::NoneAuthenticator;
use cdrs::compression::Compression;
use cdrs::transport::TransportTcp;
use cdrs::types::{IntoRustByName, CBytesShort, AsRust};
use cdrs::types::value::{Value, Bytes};
use cdrs::types::list::List;
use cdrs::types::map::Map;
use cdrs::types::udt::UDT;
use std::collections::HashMap;

const _ADDR: &'static str = "127.0.0.1:9042";
const CREATE_KEY_SPACE: &'static str = "CREATE KEYSPACE IF NOT EXISTS my_ks WITH REPLICATION = { \
                                        'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
const CREATE_UDT: &'static str = "CREATE TYPE IF NOT EXISTS my_ks.my_type (number int);";
const CREATE_TABLE_INT: &'static str = "CREATE TABLE IF NOT EXISTS my_ks.test_num (my_bigint \
                                        bigint PRIMARY KEY, my_int int, my_smallint smallint, \
                                        my_tinyint tinyint);";
const CREATE_TABLE_STR: &'static str = "CREATE TABLE IF NOT EXISTS my_ks.test_str (my_ascii \
                                        ascii PRIMARY KEY, my_text text, my_varchar varchar);";
const INSERT_STR: &'static str = "INSERT INTO my_ks.test_str (my_ascii, my_text, my_varchar) \
                                  VALUES (?, ?, ?);";
const SELECT_STR: &'static str = "SELECT * FROM my_ks.test_str;";
const CREATE_TABLE_LIST: &'static str = "CREATE TABLE IF NOT EXISTS my_ks.lists (my_string_list \
                                         frozen<list<text>> PRIMARY KEY, my_number_list \
                                         list<int>, my_complex_list list<frozen<list<smallint>>>);";
const INSERT_LIST: &'static str = "INSERT INTO my_ks.lists (my_string_list, \
                                   my_number_list, my_complex_list) VALUES (?, ?, ?);";
const SELECT_LIST: &'static str = "SELECT * FROM my_ks.lists;";
const CREATE_TABLE_MAP: &'static str =
    "CREATE TABLE IF NOT EXISTS my_ks.maps (my_string_map frozen<map<text, text>> PRIMARY KEY, \
     my_number_map map<text, int>, my_complex_map map<text, frozen<map<text, int>>>, \
     my_int_key_map map<int, text>, my_uuid_key_map map<uuid, text>);";
const INSERT_MAP: &'static str = "INSERT INTO my_ks.maps (my_string_map, my_number_map, \
                                  my_complex_map, my_int_key_map, my_uuid_key_map) VALUES (?, ?, \
                                  ?, ?, ?);";
const SELECT_MAP: &'static str = "SELECT * FROM my_ks.maps;";
const CREATE_TABLE_UDT: &'static str = "CREATE TABLE IF NOT EXISTS my_ks.udts (my_key int \
                                        PRIMARY KEY, my_udt my_type);";
const INSERT_UDT: &'static str = "INSERT INTO my_ks.udts (my_key, my_udt) VALUES (?, ?);";
const SELECT_UDT: &'static str = "SELECT * FROM my_ks.udts;";
const INSERT_INT: &'static str = "INSERT INTO my_ks.test_num (my_bigint, my_int, my_smallint, \
                                  my_tinyint) VALUES (?, ?, ?, ?)";
const SELECT_INT: &'static str = "SELECT * FROM my_ks.test_num";
const CREATE_TABLE_BOOL: &'static str = "CREATE TABLE IF NOT EXISTS my_ks.bool (my_key int \
                                        PRIMARY KEY, my_boolean boolean);";
const INSERT_BOOL: &'static str = "INSERT INTO my_ks.bool (my_key, my_boolean) \
                                    VALUES (?, ?);";
const SELECT_BOOL: &'static str = "SELECT * FROM my_ks.bool;";
const CREATE_TABLE_UUID: &'static str = "CREATE TABLE IF NOT EXISTS my_ks.uuid (my_key int \
                                        PRIMARY KEY, my_uuid uuid);";
const INSERT_UUID: &'static str = "INSERT INTO my_ks.uuid (my_key, my_uuid) \
                                    VALUES (?, ?);";
const SELECT_UUID: &'static str = "SELECT * FROM my_ks.uuid;";
const CREATE_TABLE_FLOAT: &'static str = "CREATE TABLE IF NOT EXISTS my_ks.float (my_float float \
                                        PRIMARY KEY, my_double double);";
const INSERT_FLOAT: &'static str = "INSERT INTO my_ks.float (my_float, my_double) \
                                    VALUES (?, ?);";
const SELECT_FLOAT: &'static str = "SELECT * FROM my_ks.float;";
const CREATE_TABLE_BLOB: &'static str = "CREATE TABLE IF NOT EXISTS my_ks.blob (my_key int \
                                        PRIMARY KEY, my_blob blob);";
const INSERT_BLOB: &'static str = "INSERT INTO my_ks.blob (my_key, my_blob) \
                                    VALUES (?, ?);";
const SELECT_BLOB: &'static str = "SELECT * FROM my_ks.blob;";

// we want to keep an order:
// 1. create keyspace
// 2. create custom type
// 3. create table x
// 4. insert values into x
// 5. select values from x and map to Rust
// ...
#[test]
fn main_crud_operations() {
    let authenticator = NoneAuthenticator;
    let tcp_transport = TransportTcp::new(_ADDR).unwrap();
    let client = CDRS::new(tcp_transport, authenticator);
    let mut session = client.start(Compression::None).unwrap();

    if create_keyspace(&mut session) {
        println!("0. keyspace created");
    }

    if create_type(&mut session) {
        println!("1. user type created");
    }

    if create_table(&mut session) {
        println!("2. table created");
    }

    let ref prepared_id = prepare_query(&mut session, INSERT_INT);

    if insert_ints(&mut session, &prepared_id) {
        println!("3. integers inserted");
    }

    if select_all_ints(&mut session) {
        println!("4. integers selected");
    }

    if create_table_str(&mut session) {
        println!("5. str table created");
    }

    if insert_table_str(&mut session) {
        println!("6. str table created");
    }

    if insert_table_str(&mut session) {
        println!("7. str table inserted");
    }

    if insert_table_string(&mut session) {
        println!("8. string table inserted");
    }

    if select_table_str(&mut session) {
        println!("9. strings selected");
    }

    if create_table_list(&mut session) {
        println!("10. list table created");
    }

    if insert_table_list(&mut session) {
        println!("11. list inserted");
    }

    if select_table_list(&mut session) {
        println!("12. list selected");
    }

    if create_table_map(&mut session) {
        println!("13. map table created");
    }

    if insert_table_map(&mut session) {
        println!("14. map table inserted");
    }

    if select_table_map(&mut session) {
        println!("15. map table created");
    }

    if create_table_udt(&mut session) {
        println!("16. udt table select");
    }

    if insert_table_udt(&mut session) {
        println!("17. udt table inserted");
    }

    if select_table_udt(&mut session) {
        println!("18. udt table selected");
    }

    if create_table_bool(&mut session) {
        println!("19. bool table created");
    }

    if insert_table_bool(&mut session) {
        println!("20. bool table inserted");
    }

    if select_table_bool(&mut session) {
        println!("21. bool table selected");
    }

    if create_table_uuid(&mut session) {
        println!("22. uuid table created");
    }

    if insert_table_uuid(&mut session) {
        println!("23. uuid table inserted");
    }

    if select_table_uuid(&mut session) {
        println!("24. uuid table selected");
    }

    if create_table_float(&mut session) {
        println!("25. float table created");
    }

    if insert_table_float(&mut session) {
        println!("26. float table inserted");
    }

    if select_table_float(&mut session) {
        println!("27. float table selected");
    }

    if create_table_blob(&mut session) {
        println!("28. blob table created");
    }

    if insert_table_blob(&mut session) {
        println!("29. blob table inserted");
    }

    if select_table_blob(&mut session) {
        println!("30. blob table selected");
    }
}

fn create_keyspace(session: &mut Session<NoneAuthenticator, TransportTcp>) -> bool {
    let q = QueryBuilder::new(CREATE_KEY_SPACE).finalize();
    match session.query(q, false, false) {
        Err(ref err) => panic!("create_keyspace {:?}", err),
        Ok(_) => true,
    }
}

fn create_type(session: &mut Session<NoneAuthenticator, TransportTcp>) -> bool {
    let q = QueryBuilder::new(CREATE_UDT).finalize();
    match session.query(q, false, false) {
        Err(ref err) => panic!("create_type {:?}", err),
        Ok(_) => true,
    }
}

fn create_table(session: &mut Session<NoneAuthenticator, TransportTcp>) -> bool {
    let q = QueryBuilder::new(CREATE_TABLE_INT).finalize();
    match session.query(q, false, false) {
        Err(ref err) => panic!("create_table {:?}", err),
        Ok(_) => true,
    }
}

fn prepare_query(session: &mut Session<NoneAuthenticator, TransportTcp>,
                 query: &'static str)
                 -> CBytesShort {
    session.prepare(query.to_string(), false, false).unwrap().get_body().into_prepared().unwrap().id
}

fn insert_ints(session: &mut Session<NoneAuthenticator, TransportTcp>,
               prepared_id: &CBytesShort)
               -> bool {
    let ints = Ints {
        bigint: 123,
        int: 234,
        smallint: 256,
        tinyint: 56,
    };
    let values_i: Vec<Value> =
        vec![ints.bigint.into(), ints.int.into(), ints.smallint.into(), ints.tinyint.into()];

    let execute_params = QueryParamsBuilder::new(Consistency::One).values(values_i).finalize();
    let executed = session.execute(prepared_id, execute_params, false, false);
    match executed {
        Err(ref err) => panic!("executed int {:?}", err),
        Ok(_) => true,
    }
}

fn select_all_ints(session: &mut Session<NoneAuthenticator, TransportTcp>) -> bool {
    let select_query = QueryBuilder::new(SELECT_INT).finalize();
    let all = session.query(select_query, false, false).unwrap().get_body().into_rows().unwrap();

    for row in all {
        let _ = Ints {
            bigint: row.get_by_name("my_bigint").expect("my_bigint").unwrap(),
            int: row.get_by_name("my_int").expect("my_int").unwrap(),
            smallint: row.get_by_name("my_smallint").expect("my_smallint").unwrap(),
            tinyint: row.get_by_name("my_tinyint").expect("my_tinyint").unwrap(),
        };
    }

    true
}

fn create_table_str(session: &mut Session<NoneAuthenticator, TransportTcp>) -> bool {
    let q = QueryBuilder::new(CREATE_TABLE_STR).finalize();
    match session.query(q, false, false) {
        Err(ref err) => panic!("create_table str {:?}", err),
        Ok(_) => true,
    }
}

fn insert_table_str(session: &mut Session<NoneAuthenticator, TransportTcp>) -> bool {
    let strs = Strs {
        my_ascii: "my_ascii",
        my_text: "my_text",
        my_varchar: "my_varchar",
    };
    let values_s: Vec<Value> =
        vec![strs.my_ascii.into(), strs.my_text.into(), strs.my_varchar.into()];

    let query = QueryBuilder::new(INSERT_STR).values(values_s).finalize();
    let inserted = session.query(query, false, false);
    match inserted {
        Err(ref err) => panic!("inserted str {:?}", err),
        Ok(_) => true,
    }
}

fn insert_table_string(session: &mut Session<NoneAuthenticator, TransportTcp>) -> bool {
    let strings = Strings {
        my_ascii: "my_ascii".to_string(),
        my_text: "my_text".to_string(),
        my_varchar: "my_varchar".to_string(),
    };
    let values_s: Vec<Value> =
        vec![strings.my_ascii.into(), strings.my_text.into(), strings.my_varchar.into()];

    let query = QueryBuilder::new(INSERT_STR).values(values_s).finalize();
    let inserted = session.query(query, false, false);
    match inserted {
        Err(ref err) => panic!("inserted strings {:?}", err),
        Ok(_) => true,
    }
}

fn select_table_str(session: &mut Session<NoneAuthenticator, TransportTcp>) -> bool {
    let select_query = QueryBuilder::new(SELECT_STR).finalize();
    let all = session.query(select_query, false, false).unwrap().get_body().into_rows().unwrap();

    for row in all {
        let _ = Strings {
            my_ascii: row.get_by_name("my_ascii").expect("my_ascii").unwrap(),
            my_text: row.get_by_name("my_text").expect("my_text").unwrap(),
            my_varchar: row.get_by_name("my_varchar").expect("my_ascii").unwrap(),
        };
    }

    true
}

fn create_table_list(session: &mut Session<NoneAuthenticator, TransportTcp>) -> bool {
    let q = QueryBuilder::new(CREATE_TABLE_LIST).finalize();
    match session.query(q, false, false) {
        Err(ref err) => panic!("create_table list {:?}", err),
        Ok(_) => true,
    }
}

fn insert_table_list(session: &mut Session<NoneAuthenticator, TransportTcp>) -> bool {
    let lists = Lists {
        string_list: vec!["hello".to_string(), "world".to_string()],
        number_list: vec![1, 2, 3],
        complex_list: vec![vec![1, 3, 4], vec![4, 5, 6]],
    };


    let values: Vec<Value> =
        vec![lists.string_list.into(), lists.number_list.into(), lists.complex_list.into()];

    let query = QueryBuilder::new(INSERT_LIST).values(values).finalize();
    let inserted = session.query(query, false, false);
    match inserted {
        Err(ref err) => panic!("inserted lists {:?}", err),
        Ok(_) => true,
    }
}

fn select_table_list(session: &mut Session<NoneAuthenticator, TransportTcp>) -> bool {
    let select_query = QueryBuilder::new(SELECT_LIST).finalize();
    let all = session.query(select_query, false, false)
        .unwrap()
        .get_body()
        .into_rows()
        .unwrap();

    for row in all {
        let cl = CassandraLists {
            string_list: row.get_by_name("my_string_list").expect("string_list").unwrap(),
            number_list: row.get_by_name("my_number_list").expect("number_list").unwrap(),
            complex_list: row.get_by_name("my_complex_list").expect("complex_list").unwrap(),
        };
        let complex_list_c: Vec<List> = cl.complex_list.as_rust().expect("my_complex_list");
        let _ = Lists {
            string_list: cl.string_list.as_rust().expect("string_list"),
            number_list: cl.number_list.as_rust().expect("number_list"),
            complex_list: complex_list_c.iter()
                .map(|it| it.as_rust().expect("number_list_c"))
                .collect(),
        };
    }

    true
}

fn insert_table_map(session: &mut Session<NoneAuthenticator, TransportTcp>) -> bool {
    let mut string_map: HashMap<String, String> = HashMap::new();
    string_map.insert("a".to_string(), "A".to_string());
    let mut number_map: HashMap<String, i32> = HashMap::new();
    number_map.insert("one".to_string(), 1);
    let mut complex_map: HashMap<String, HashMap<String, i32>> = HashMap::new();
    complex_map.insert("nested".to_string(), number_map.clone());
    let mut int_key_map: HashMap<i32, String> = HashMap::new();
    int_key_map.insert(1, "one".to_string());
    let uuid: Uuid = Uuid::parse_str("6f586cab-cd6e-4b05-89a8-c7f27215adc8").unwrap();
    let mut uuid_key_map: HashMap<Uuid, String> = HashMap::new();
    uuid_key_map.insert(uuid, "random uuid".to_string());
    let maps = Maps {
        string_map: string_map,
        number_map: number_map,
        complex_map: complex_map,
        int_key_map: int_key_map,
        uuid_key_map: uuid_key_map,
    };


    let values: Vec<Value> = vec![maps.string_map.into(),
                                  maps.number_map.into(),
                                  maps.complex_map.into(),
                                  maps.int_key_map.into(),
                                  maps.uuid_key_map.into()];

    let query = QueryBuilder::new(INSERT_MAP).values(values).finalize();
    let inserted = session.query(query, false, false);
    match inserted {
        Err(ref err) => panic!("inserted maps {:?}", err),
        Ok(_) => true,
    }
}

fn select_table_map(session: &mut Session<NoneAuthenticator, TransportTcp>) -> bool {

    let select_query = QueryBuilder::new(SELECT_MAP).finalize();
    let all = session.query(select_query, false, false)
        .unwrap()
        .get_body()
        .into_rows()
        .unwrap();

    for row in all {
        let cm = CassandraMaps {
            string_map: row.get_by_name("my_string_map").expect("string_map").unwrap(),
            number_map: row.get_by_name("my_number_map").expect("number_map").unwrap(),
            complex_map: row.get_by_name("my_complex_map").expect("complex_map").unwrap(),
            int_key_map: row.get_by_name("my_int_key_map").expect("int_key_map").unwrap(),
            uuid_key_map: row.get_by_name("my_uuid_key_map").expect("uuid_key_map").unwrap(),
        };
        let complex_map_c: HashMap<String, Map> = cm.complex_map.as_rust().expect("my_complex_map");
        let _ = Maps {
            string_map: cm.string_map.as_rust().expect("string_map"),
            number_map: cm.number_map.as_rust().expect("number_map"),
            complex_map: complex_map_c.iter()
                .fold(HashMap::new(), |mut hm, (k, v)| {
                    hm.insert(k.clone(), v.as_rust().expect("complex_map_c"));
                    hm
                }),
            int_key_map: cm.int_key_map.as_rust().expect("int_key_map"),
            uuid_key_map: cm.uuid_key_map.as_rust().expect("uuid_key_map"),
        };
    }

    true
}

fn create_table_map(session: &mut Session<NoneAuthenticator, TransportTcp>) -> bool {
    let q = QueryBuilder::new(CREATE_TABLE_MAP).finalize();
    match session.query(q, false, false) {
        Err(ref err) => panic!("create_table map {:?}", err),
        Ok(_) => true,
    }
}

fn create_table_udt(session: &mut Session<NoneAuthenticator, TransportTcp>) -> bool {
    let q = QueryBuilder::new(CREATE_TABLE_UDT).finalize();
    match session.query(q, false, false) {
        Err(ref err) => panic!("create_table udt {:?}", err),
        Ok(_) => true,
    }
}

fn insert_table_udt(session: &mut Session<NoneAuthenticator, TransportTcp>) -> bool {
    let udt = Udt { number: 12 };
    let values: Vec<Value> = vec![(1 as i32).into(), udt.into()];

    let query = QueryBuilder::new(INSERT_UDT).values(values).finalize();
    let inserted = session.query(query, false, false);
    match inserted {
        Err(ref err) => panic!("inserted udt {:?}", err),
        Ok(_) => true,
    }
}

fn select_table_udt(session: &mut Session<NoneAuthenticator, TransportTcp>) -> bool {

    let select_query = QueryBuilder::new(SELECT_UDT).finalize();
    let all = session.query(select_query, false, false)
        .unwrap()
        .get_body()
        .into_rows()
        .unwrap();

    for row in all {
        let udt_c: UDT = row.get_by_name("my_udt").expect("my_udt").unwrap();
        let _ = Udt { number: udt_c.get_by_name("number").expect("number").unwrap() };
    }

    true
}

fn create_table_bool(session: &mut Session<NoneAuthenticator, TransportTcp>) -> bool {
    let q = QueryBuilder::new(CREATE_TABLE_BOOL).finalize();
    match session.query(q, false, false) {
        Err(ref err) => panic!("create_table uuid {:?}", err),
        Ok(_) => true,
    }
}

fn insert_table_bool(session: &mut Session<NoneAuthenticator, TransportTcp>) -> bool {
    let values: Vec<Value> = vec![(1 as i32).into(), false.into()];

    let query = QueryBuilder::new(INSERT_BOOL).values(values).finalize();
    let inserted = session.query(query, false, false);
    match inserted {
        Err(ref err) => panic!("inserted bool {:?}", err),
        Ok(_) => true,
    }
}

fn select_table_bool(session: &mut Session<NoneAuthenticator, TransportTcp>) -> bool {

    let select_query = QueryBuilder::new(SELECT_BOOL).finalize();
    let all = session.query(select_query, false, false)
        .unwrap()
        .get_body()
        .into_rows()
        .unwrap();

    for row in all {
        let _: bool = row.get_by_name("my_boolean").expect("my_boolean").unwrap();
    }

    true
}

fn create_table_uuid(session: &mut Session<NoneAuthenticator, TransportTcp>) -> bool {
    let q = QueryBuilder::new(CREATE_TABLE_UUID).finalize();
    match session.query(q, false, false) {
        Err(ref err) => panic!("create_table uuid {:?}", err),
        Ok(_) => true,
    }
}

fn insert_table_uuid(session: &mut Session<NoneAuthenticator, TransportTcp>) -> bool {
    let uuid: Uuid = Uuid::parse_str("0000002a-000c-0005-0c03-0938362b0809").unwrap();
    let values: Vec<Value> = vec![(1 as i32).into(), uuid.into()];

    let query = QueryBuilder::new(INSERT_UUID).values(values).finalize();
    let inserted = session.query(query, false, false);
    match inserted {
        Err(ref err) => panic!("inserted UUID {:?}", err),
        Ok(_) => true,
    }
}

fn select_table_uuid(session: &mut Session<NoneAuthenticator, TransportTcp>) -> bool {

    let select_query = QueryBuilder::new(SELECT_UUID).finalize();
    let all = session.query(select_query, false, false)
        .unwrap()
        .get_body()
        .into_rows()
        .unwrap();

    for row in all {
        let _: Uuid = row.get_by_name("my_uuid").expect("my_uuid").unwrap();
    }

    true
}

fn create_table_float(session: &mut Session<NoneAuthenticator, TransportTcp>) -> bool {
    let q = QueryBuilder::new(CREATE_TABLE_FLOAT).finalize();
    match session.query(q, false, false) {
        Err(ref err) => panic!("create_table float {:?}", err),
        Ok(_) => true,
    }
}

fn insert_table_float(session: &mut Session<NoneAuthenticator, TransportTcp>) -> bool {
    let float: f32 = 4321.0;
    let double: f64 = 1234.0;
    let values: Vec<Value> = vec![float.into(), double.into()];

    let query = QueryBuilder::new(INSERT_FLOAT).values(values).finalize();
    let inserted = session.query(query, false, false);
    match inserted {
        Err(ref err) => panic!("inserted float {:?}", err),
        Ok(_) => true,
    }
}

fn select_table_float(session: &mut Session<NoneAuthenticator, TransportTcp>) -> bool {

    let select_query = QueryBuilder::new(SELECT_FLOAT).finalize();
    let all = session.query(select_query, false, false)
        .unwrap()
        .get_body()
        .into_rows()
        .unwrap();

    for row in all {
        let _: f32 = row.get_by_name("my_float").expect("my_float").unwrap();
        let _: f64 = row.get_by_name("my_double").expect("my_double").unwrap();
    }

    true
}

fn create_table_blob(session: &mut Session<NoneAuthenticator, TransportTcp>) -> bool {
    let q = QueryBuilder::new(CREATE_TABLE_BLOB).finalize();
    match session.query(q, false, false) {
        Err(ref err) => panic!("create_table blob {:?}", err),
        Ok(_) => true,
    }
}

fn insert_table_blob(session: &mut Session<NoneAuthenticator, TransportTcp>) -> bool {
    let blob: Vec<u8> = vec![0, 1, 2, 4, 8, 16, 32, 64, 128, 255];
    let values: Vec<Value> = vec![(1 as i32).into(), Bytes::new(blob).into()];

    let query = QueryBuilder::new(INSERT_BLOB).values(values).finalize();
    let inserted = session.query(query, false, false);
    match inserted {
        Err(ref err) => panic!("inserted blob {:?}", err),
        Ok(_) => true,
    }
}

fn select_table_blob(session: &mut Session<NoneAuthenticator, TransportTcp>) -> bool {
    let select_query = QueryBuilder::new(SELECT_BLOB).finalize();
    let all = session.query(select_query, false, false)
        .unwrap()
        .get_body()
        .into_rows()
        .unwrap();

    for row in all {
        let _: Vec<u8> = row.get_by_name("my_blob").expect("my_blob").unwrap();
    }

    true
}

struct Ints {
    pub bigint: i64,
    pub int: i32,
    pub smallint: i16,
    pub tinyint: i8,
}

struct Strs<'a> {
    pub my_ascii: &'a str,
    pub my_text: &'a str,
    pub my_varchar: &'a str,
}

struct Strings {
    pub my_ascii: String,
    pub my_text: String,
    pub my_varchar: String,
}

#[derive(Debug)]
struct Lists {
    pub string_list: Vec<String>,
    pub number_list: Vec<i32>,
    pub complex_list: Vec<Vec<i16>>,
}

#[derive(Debug)]
struct CassandraLists {
    pub string_list: List,
    pub number_list: List,
    pub complex_list: List,
}

#[derive(Debug)]
struct Maps {
    pub string_map: HashMap<String, String>,
    pub number_map: HashMap<String, i32>,
    pub complex_map: HashMap<String, HashMap<String, i32>>,
    pub int_key_map: HashMap<i32, String>,
    pub uuid_key_map: HashMap<Uuid, String>,
}

struct CassandraMaps {
    pub string_map: Map,
    pub number_map: Map,
    pub complex_map: Map,
    pub int_key_map: Map,
    pub uuid_key_map: Map,
}

#[derive(Debug)]
struct Udt {
    pub number: i32,
}

impl Into<Bytes> for Udt {
    fn into(self) -> Bytes {
        let mut bytes: Vec<u8> = vec![];
        let val_bytes: Bytes = self.number.into();
        bytes.extend_from_slice(Value::new_normal(val_bytes).into_cbytes().as_slice());
        Bytes::new(bytes)
    }
}
