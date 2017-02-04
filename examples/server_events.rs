extern crate cdrs;
extern crate r2d2;

use std::thread;

use cdrs::client::CDRS;
use cdrs::transport::Transport;
use cdrs::authenticators::PasswordAuthenticator;
use cdrs::compression::Compression;
use cdrs::frame::events::SimpleServerEvent;

// default credentials
const USER: &'static str = "cassandra";
const PASS: &'static str = "cassandra";
const ADDR: &'static str = "127.0.0.1:9042";

fn main() {
    let transport = Transport::new(ADDR).unwrap();
    let authenticator = PasswordAuthenticator::new(USER, PASS);
    let client = CDRS::new(transport, authenticator);
    let session = client.start(Compression::None).unwrap();

    let (mut listener, stream) = session.listen_for(vec![SimpleServerEvent::SchemaChange]).unwrap();

    thread::spawn(move|| {
        listener.start(&Compression::None).unwrap()
    });

    println!("Start listen for server events");

    for event in stream {
        println!("server event {:?}", event.get_body());
    }
}
