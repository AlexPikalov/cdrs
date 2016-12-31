
extern crate cdrs;
use cdrs::client::CDRS;
use cdrs::authenticators::PasswordAuthenticator;
use cdrs::consistency::Consistency;
use cdrs::types::CBytes;
use cdrs::compression::Compression;
use cdrs::types::value::Value;

fn main() {

    let authenticator = PasswordAuthenticator::new("user", "pass");
    let addr = "127.0.0.1:9042";

    // pass authenticator into CDRS' constructor
    let client = CDRS::new(addr, authenticator).unwrap();

    // start session without compression
    let select_query = String::from("SELECT * FROM my_namespace.emp;");
    // Query parameters:
    let consistency = Consistency::One;
    let values: Option<Vec<Value>> = None;
    let with_names: Option<bool> = None;
    let page_size: Option<i32> = None;
    let paging_state: Option<CBytes> = None;
    let serial_consistency: Option<Consistency> = None;
    let timestamp: Option<i64> = None;

    let use_query = String::from("USE my_namespace;");
    match client.start(Compression::None) {
        Ok(session) => {
            match session.prepare(use_query) {
                Ok(set_keyspace) => {
                    // use_keyspace is a result frame of type SetKeyspace
                    let query_op = session.query(select_query,
                                                consistency,
                                                values,
                                                with_names,
                                                page_size,
                                                paging_state,
                                                serial_consistency,
                                                timestamp);

                    match query_op {

                        Ok(res) => {
                            println!("Result frame: {:?},\nparsed body: {:?}",
                                     res,
                                     res.get_body())
                        }
                        Err(err) => println!("{:?}", err),
                    }
                }
                Err(err) => println!("{:?}", err),
            }

        }
        Err(err) => println!("{:?}", err),
    }


}
