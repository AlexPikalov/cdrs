
extern crate cdrs;

use cdrs::client::CDRS;
use cdrs::query::QueryBuilder;
use cdrs::authenticators::NoneAuthenticator;
use cdrs::compression::Compression;
use cdrs::consistency::Consistency;
use cdrs::transport::TransportTcp;
// default credentials
const _ADDR: &'static str = "127.0.0.1:9042";



#[test] #[ignore]
#[cfg(not(feature = "appveyor"))]
fn connect_to_cassandra() {
    const _ADDR: &'static str = "127.0.0.1:9042";
    let authenticator = NoneAuthenticator;
    let tcp_transport = TransportTcp::new(_ADDR);
    assert_eq!(tcp_transport.is_ok(), true);

    let client = CDRS::new(tcp_transport.unwrap(), authenticator);
    let session = client.start(Compression::None).unwrap();

    let select_peers = "SELECT peer,data_center,rack,tokens,rpc_address,release_version FROM \
                        system.peers";
    let with_tracing = false;
    let with_warnings = false;


    let select_peers_query = QueryBuilder::new(select_peers)
        .consistency(Consistency::One)
        .finalize();

    let select_peers_query_result = session.query(select_peers_query, with_tracing, with_warnings);

    assert_eq!(select_peers_query_result.is_ok(), true);


    match select_peers_query_result {
        Ok(ref res) => println!("peers Selected: {:?}", res.get_body()),
        Err(ref err) => println!("Error occured: {:?}", err),
    }



}
