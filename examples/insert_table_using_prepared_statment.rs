extern crate cdrs;
use cdrs::client::CDRS;
use cdrs::authenticators::NoneAuthenticator;
use cdrs::compression::Compression;
use cdrs::transport::TransportTcp;
use cdrs::prepared_statement::PrepareAndExecute;

// default credentials
const _CUSTOMER_NAME: &'static str = "david candy0";

//#[test]
fn main() {
    println!("insert_data_users");
    const _ADDR: &'static str = "127.0.0.1:9042";

    let tcp_transport = TransportTcp::new(_ADDR);
    let client = CDRS::new(tcp_transport.unwrap(), NoneAuthenticator);
    let mut session = client.start(Compression::None).unwrap();
    let insert_table_cql = " INSERT INTO my_namespace.emp (emp_id, emp_name, emp_city,emp_phone,emp_sal)
    values   (?     ,  ?   ,     ?   ,  ?,     ?)";

    let mut prepared = session.prepare_statement(insert_table_cql.to_string(), true, true).unwrap();

    prepared.set_int(0, 9000).unwrap();
    prepared.set_string(1, _CUSTOMER_NAME).unwrap();
    prepared.set_string(2, "fll").unwrap();
    prepared.set_int(3, 908080800).unwrap();
    prepared.set_int(4, 100000).unwrap();


    println!("prepared:\n{:?}", prepared);

    let executed = session.execute_statement(prepared).unwrap();


    println!("executed:\n{:?}", executed);
}
