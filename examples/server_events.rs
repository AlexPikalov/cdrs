
extern crate cdrs;
extern crate r2d2;

use std::thread;

use cdrs::client::CDRS;
use cdrs::authenticators::PasswordAuthenticator;
use cdrs::compression::Compression;
use cdrs::frame::events::{
    SimpleServerEvent,
    ServerEvent,
    TopologyChangeType
};
use cdrs::transport::TransportPlain;

// default credentials
const _USER: &'static str = "cassandra";
const _PASS: &'static str = "cassandra";
const _ADDR: &'static str = "127.0.0.1:9042";

fn main() {
    let transport = TransportPlain::new(_ADDR).unwrap();
    let authenticator = PasswordAuthenticator::new(_USER, _PASS);
    let client = CDRS::new(transport, authenticator);
    let session = client.start(Compression::None).unwrap();

    let (mut listener, stream) = session.listen_for(vec![SimpleServerEvent::SchemaChange]).unwrap();

    thread::spawn(move|| {
        listener.start(&Compression::None).unwrap()
    });

    let topology_changes = stream
        // inspects all events in a stream
        .inspect(|event| println!("inspect event {:?}", event))
        // filter by event's type: topology changes
        .filter(|event| event == &SimpleServerEvent::TopologyChange)
        // filter by event's specific information: new node was added
        .filter(|event| {
            match event {
                &ServerEvent::TopologyChange(ref event) => {
                    event.change_type == TopologyChangeType::NewNode
                },
                _ => false
            }
        });

    println!("Start listen for server events");

    for change in topology_changes {
        println!("server event {:?}", change);
    }
}
