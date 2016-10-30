//! Cassandra client that is responsible for synchronization of requests and responses.
use std::net::SocketAddr;
use tokio_core::net::TcpListener;
use std::panic::UnwindSafe;

pub struct Client {
    listener: TcpListener
    // TODO: add compression
}

impl Client {
    pub connect() -> Client {

    }
}
