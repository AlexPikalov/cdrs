// in feature="ssl" imports are unused until examples are implemented
#![allow(unused_imports, unused_variables)]
extern crate cdrs;
extern crate r2d2;

use std::thread;

use cdrs::client::CDRS;
use cdrs::authenticators::PasswordAuthenticator;
use cdrs::compression::Compression;
use cdrs::frame::events::SimpleServerEvent;
#[cfg(not(feature = "ssl"))]
use cdrs::transport::Transport;
#[cfg(feature = "ssl")]
use cdrs::transport_ssl::Transport;

// default credentials
const _USER: &'static str = "cassandra";
const _PASS: &'static str = "cassandra";
const _ADDR: &'static str = "127.0.0.1:9042";

#[cfg(not(feature = "ssl"))]
fn main() {
    let transport = Transport::new(_ADDR).unwrap();
    let authenticator = PasswordAuthenticator::new(_USER, _PASS);
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

#[cfg(feature = "ssl")]
fn main() {
    unimplemented!()
}
