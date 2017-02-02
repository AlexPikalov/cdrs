extern crate cdrs;
extern crate r2d2;

use std::thread;
use std::sync::mpsc::channel;

use cdrs::client::CDRS;
use cdrs::connection_manager::ConnectionManager;
use cdrs::transport::Transport;
use cdrs::authenticators::PasswordAuthenticator;
use cdrs::compression::Compression;
use cdrs::query::QueryBuilder;
use cdrs::frame::events::SimpleServerEvent;

// default credentials
const USER: &'static str = "cassandra";
const PASS: &'static str = "cassandra";
const ADDR: &'static str = "127.0.0.1:9042";

fn main() {
    let transport = Transport::new(ADDR).unwrap();
    let authenticator = PasswordAuthenticator::new(USER, PASS);
    let client = CDRS::new(transport, authenticator);
    let mut session = client.start(Compression::None).unwrap();

    let res = session.listen_for(vec![SimpleServerEvent::SchemaChange]).unwrap();

    println!("result {:?}", res.get_body());
}
