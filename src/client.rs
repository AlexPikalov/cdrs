//! Cassandra client that is responsible for synchronization of requests and responses.
use std::net::{SocketAddr};
use std::io;

use tokio_core::net::{TcpStream};
use tokio_core::reactor::{Core, Handle};
use tokio_core::io::{write_all, read_to_end};
use futures::Future;

use super::IntoBytes;
use super::frame::Frame;
use super::parser::parse_frame;

const CASSANDRA_PORT: u16 = 9042;

pub struct Client {
    _handle: Handle,
    core: Core,
    tcp_stream: TcpStream
    // TODO: add compression
}

impl Client {
    pub fn new(ip: String) -> Client {
        let mut addr = ip.parse::<SocketAddr>().unwrap();
        addr.set_port(CASSANDRA_PORT);
        let core = Core::new().unwrap();
        let handle = core.handle();
        let tcp_stream = TcpStream::connect(&addr, &handle).wait().unwrap();

        return Client {
            _handle: handle,
            core: core,
            tcp_stream: tcp_stream
        };
    }

    pub fn start(&mut self) -> Result<Frame, io::Error> {
        let compression = None;
        let startup_frame = Frame::new_req_startup(compression).into_cbytes();
        let request = write_all(&self.tcp_stream, startup_frame.as_slice());;
        let response = request.and_then(|(socket, _)| {
            return read_to_end(socket, Vec::new());
        });
        return self.core.run(response).map(|(_, vec)| {
            return parse_frame(vec);
        });
    }
}
