extern crate cdrs;

use std::thread;
use std::iter::Iterator;

use cdrs::authenticators::{Authenticator, NoneAuthenticator};
use cdrs::cluster::{Cluster, Session};
use cdrs::load_balancing::RoundRobin;
use cdrs::transport::TransportTcp;
use cdrs::compression::Compression;
use cdrs::frame::events::{ChangeType, ServerEvent, SimpleServerEvent, Target};

const _ADDR: &'static str = "127.0.0.1:9042";

type CurrentSession = Session<RoundRobin<TransportTcp>, NoneAuthenticator>;

fn main() {
  let cluster = Cluster::new(vec!["127.0.0.1:9042"], NoneAuthenticator {});
  let mut no_compression = cluster
    .connect(RoundRobin::new())
    .expect("No compression connection error");

  let (mut listener, stream) = no_compression
    .listen(vec![SimpleServerEvent::SchemaChange])
    .expect("listen error");

  thread::spawn(move || listener.start(&Compression::None).unwrap());

  let new_tables = stream
        // inspects all events in a stream
        .inspect(|event| println!("inspect event {:?}", event))
        // filter by event's type: schema changes
        .filter(|event| event == &SimpleServerEvent::SchemaChange);
  // // filter by event's specific information: new node was added
  // .filter(|event| {
  //     match event {
  //         &ServerEvent::SchemaChange(ref event) => {
  //             event.change_type == ChangeType::Created && event.target == Target::Table
  //         },
  //         _ => false
  //     }
  // });

  println!("Start listen for server events");

  for change in new_tables {
    println!("server event {:?}", change);
  }
}
