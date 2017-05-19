use regex::Regex;
use cdrs::client::{CDRS, Session};
use cdrs::query::QueryBuilder;
use cdrs::authenticators::NoneAuthenticator;
use cdrs::compression::Compression;
use cdrs::transport::TransportTcp;
use cdrs::error::Result;

const ADDR: &'static str = "127.0.0.1:9042";

pub type CSession = Session<NoneAuthenticator, TransportTcp>;

pub fn setup(create_table_cql: &'static str) -> Result<CSession> {
    setup_multiple(&[create_table_cql])
}

pub fn setup_multiple(create_cqls: &[&'static str]) -> Result<CSession> {
    let authenticator = NoneAuthenticator;
    let tcp_transport = TransportTcp::new(ADDR)?;
    let client = CDRS::new(tcp_transport, authenticator);
    let mut session = client.start(Compression::None)?;
    let re_table_name = Regex::new(r"CREATE TABLE IF NOT EXISTS (\w+\.\w+)").unwrap();

    let cql = "CREATE KEYSPACE IF NOT EXISTS cdrs_test WITH \
               replication = {'class': 'SimpleStrategy', 'replication_factor': 1} \
               AND durable_writes = false";
    let query = QueryBuilder::new(cql).finalize();
    session.query(query, true, true)?;

    for create_cql in create_cqls.iter() {
        let table_name = re_table_name
            .captures(create_cql)
            .map(|cap| cap.get(1).unwrap().as_str());

        // if let Some(table_name) = table_name {
        //     let cql = format!("DROP TABLE IF EXISTS {}", table_name);
        //     let query = QueryBuilder::new(cql).finalize();
        //     session.query(query, true, true)?;
        // }

        let query = QueryBuilder::new(create_cql.to_owned()).finalize();
        session.query(query, true, true)?;

        if let Some(table_name) = table_name {
            let cql = format!("TRUNCATE TABLE {}", table_name);
            let query = QueryBuilder::new(cql).finalize();
            session.query(query, true, true)?;
        }
    }

    Ok(session)
}
