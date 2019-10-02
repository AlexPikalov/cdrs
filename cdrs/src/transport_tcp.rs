use std::io;
use std::net;
use std::time;

use super::transport::CDRSTransport;
use super::transport_builder_trait::CDRSTransportBuilder;

/// Default Tcp transport.
pub struct TransportTcp {
  tcp: net::TcpStream,
  addr: String,
}

impl TransportTcp {
  /// Constructs a new `TransportTcp`.
  ///
  /// # Examples
  ///
  /// ```no_run
  /// use cdrs::transport::TransportTcp;
  /// let addr = "127.0.0.1:9042";
  /// let tcp_transport = TransportTcp::new(addr).unwrap();
  /// ```
  pub fn new(addr: &str) -> io::Result<TransportTcp> {
    net::TcpStream::connect(addr).map(|socket| TransportTcp {
      tcp: socket,
      addr: addr.to_string(),
    })
  }
}

impl io::Read for TransportTcp {
  fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
    self.tcp.read(buf)
  }
}

impl io::Write for TransportTcp {
  fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
    self.tcp.write(buf)
  }

  fn flush(&mut self) -> io::Result<()> {
    self.tcp.flush()
  }
}

impl CDRSTransport for TransportTcp {
  fn try_clone(&self) -> io::Result<TransportTcp> {
    net::TcpStream::connect(self.addr.as_str()).map(|socket| TransportTcp {
      tcp: socket,
      addr: self.addr.clone(),
    })
  }

  fn close(&mut self, close: net::Shutdown) -> io::Result<()> {
    self.tcp.shutdown(close)
  }

  fn set_timeout(&mut self, dur: Option<time::Duration>) -> io::Result<()> {
    self
      .tcp
      .set_read_timeout(dur)
      .and_then(|_| self.tcp.set_write_timeout(dur))
  }

  fn is_alive(&self) -> bool {
    self.tcp.peer_addr().is_ok()
  }
}

pub struct TcpTransportBuilder {
  addr: String,
}

impl TcpTransportBuilder {
  pub fn new(addr: String) -> Self {
    TcpTransportBuilder { addr }
  }
}

impl CDRSTransportBuilder<TransportTcp> for TcpTransportBuilder {
  fn create(&self) -> io::Result<TransportTcp> {
    TransportTcp::new(self.addr.as_str())
  }
}
