
extern crate cdrs;
extern crate r2d2;

use std::thread;

use cdrs::client::CDRS;
use cdrs::authenticators::PasswordAuthenticator;
use cdrs::compression::Compression;
use cdrs::frame::events::{SimpleServerEvent, ServerEvent, ChangeType, Target};
use cdrs::transport::TransportTcp;

// default credentials
const _USER: &'static str = "cassandra";
const _PASS: &'static str = "cassandra";
const _ADDR: &'static str = "127.0.0.1:9042";

fn main() {
    let transport = TransportTcp::new(_ADDR).unwrap();
    let authenticator = PasswordAuthenticator::new(_USER, _PASS);
    let client = CDRS::new(transport, authenticator);
    let session = client.start(Compression::None).unwrap();

    let (mut listener, stream) = session
        .listen_for(vec![SimpleServerEvent::SchemaChange])
        .unwrap();

    thread::spawn(move || listener.start(&Compression::None).unwrap());

    let new_tables = stream
        // inspects all events in a stream
        .inspect(|event| println!("inspect event {:?}", event))
        // filter by event's type: schema changes
        .filter(|event| event == &SimpleServerEvent::SchemaChange)
        // filter by event's specific information: new node was added
        .filter(|event| {
            match event {
                &ServerEvent::SchemaChange(ref event) => {
                    event.change_type == ChangeType::Created && event.target == Target::Table
                },
                _ => false
            }
        });

    println!("Start listen for server events");

    for change in new_tables {
        println!("server event {:?}", change);
    }
}
