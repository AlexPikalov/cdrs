use std::io;

use transport::{CDRSTransport, TransportTcp};

#[cfg(feature = "ssl")]
use transport::TransportTls;
#[cfg(feature = "ssl")]
use openssl::ssl::SslConnector;

pub trait TransportBuilder<T: CDRSTransport> {
  fn init<A: ToString>(&self, addr: A) -> io::Result<T>;
  fn cluster<A: ToString>(&self, addr: &Vec<A>) -> io::Result<Vec<T>>;
}

pub struct TransportTcpBuilder;

impl TransportBuilder<TransportTcp> for TransportTcpBuilder {
  fn init<A: ToString>(&self, addr: A) -> io::Result<TransportTcp> {
    TransportTcp::new(addr.to_string().as_str())
  }

  fn cluster<A: ToString>(&self, addr: &Vec<A>) -> io::Result<Vec<TransportTcp>> {
    unimplemented!()
  }
}

#[cfg(feature = "ssl")]
pub struct TransportTlsBuilder<'a> {
  connector: &'a SslConnector,
}

#[cfg(feature = "ssl")]
impl<'a> TransportTlsBuilder<'a> {
  pub fn new(connector_ref: &'a SslConnector) -> Self {
    TransportTlsBuilder { connector: connector_ref, }
  }
}

#[cfg(feature = "ssl")]
impl<'a> TransportBuilder<TransportTls> for TransportTlsBuilder<'a> {
  fn init<A: ToString>(&self, addr: A) -> io::Result<TransportTls> {
    TransportTls::new(addr.to_string().as_str(), self.connector)
  }

  fn cluster<A: ToString>(&self, addr: &Vec<A>) -> io::Result<Vec<TransportTls>> {
    unimplemented!()
  }
}
