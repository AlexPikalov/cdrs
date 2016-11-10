//! Cassandra client that is responsible for synchronization of requests and responses.
use std::net::{SocketAddr};
use std::io;

use tokio_core::net::{TcpStream};
use tokio_core::reactor::{Core, Handle};
use tokio_core::io::write_all;
use futures::Future;

use super::IntoBytes;
use super::frame::*;
use super::parser::parse_frame_from_future;

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

        let mut core = Core::new().unwrap();
        let handle = core.handle();
        let tcp_stream = core.run(TcpStream::connect(&addr, &handle)).unwrap();
        return Client {
            _handle: handle,
            core: core,
            tcp_stream: tcp_stream
        };
    }

    pub fn start(self) -> Result<Frame, io::Error> {
        let compression = None;
        let startup_frame = Frame::new_req_startup(compression).into_cbytes();
        let mut core = self.core;
        let request = write_all(self.tcp_stream, startup_frame.as_slice());;
        let response = request.and_then(|(socket, _)| {
            return parse_frame_from_future(socket);
        });
        return core.run(response);
    }
}
