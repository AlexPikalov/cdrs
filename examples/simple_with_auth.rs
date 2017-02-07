// in feature="ssl" imports are unused until examples are implemented
#![allow(unused_imports, unused_variables)]
extern crate cdrs;
use cdrs::client::CDRS;
use cdrs::query::QueryBuilder;
use cdrs::authenticators::PasswordAuthenticator;
use cdrs::compression::Compression;
#[cfg(not(feature = "ssl"))]
use cdrs::transport::Transport;
#[cfg(feature = "ssl")]
use cdrs::transport_ssl::Transport;

#[cfg(not(feature = "ssl"))]
fn main() {

    let authenticator = PasswordAuthenticator::new("user", "pass");
    let addr = "127.0.0.1:9042";
    let tcp_transport = Transport::new(addr).unwrap();

    // pass authenticator into CDRS' constructor
    let client = CDRS::new(tcp_transport, authenticator);

    // start session without compression
    let select_query = QueryBuilder::new("SELECT * FROM my_namespace.emp;").finalize();

    match client.start(Compression::None) {
        Ok(mut session) => {
            let with_tracing = false;
            let with_warnings = false;
            let query_op = session.query(select_query, with_tracing, with_warnings);

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

#[cfg(feature = "ssl")]
fn main() {
    unimplemented!();
}
