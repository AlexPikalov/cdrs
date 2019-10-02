#[cfg(feature = "ssl")]
use openssl::ssl::{SslConnector, SslStream};
use std::io;
use std::io::{Read, Write};
use std::net;
use std::time::Duration;

// TODO [v 2.x.x]: CDRSTransport: ... + BufReader + ButWriter + ...
///General CDRS transport trait. Both [`TranportTcp`][transportTcp]
///and [`TransportTls`][transportTls] has their own implementations of this trait. Generaly
///speaking it extends/includes `io::Read` and `io::Write` traits and should be thread safe.
///[transportTcp]:struct.TransportTcp.html
///[transportTls]:struct.TransportTls.html
pub trait CDRSTransport: Sized + Read + Write + Send + Sync {
  /// Creates a new independently owned handle to the underlying socket.
  ///
  /// The returned TcpStream is a reference to the same stream that this object references.
  /// Both handles will read and write the same stream of data, and options set on one stream
  /// will be propagated to the other stream.
  fn try_clone(&self) -> io::Result<Self>;

  /// Shuts down the read, write, or both halves of this connection.
  fn close(&mut self, close: net::Shutdown) -> io::Result<()>;

  /// Method which set given duration both as read and write timeout.
  /// If the value specified is None, then read() calls will block indefinitely.
  /// It is an error to pass the zero Duration to this method.
  fn set_timeout(&mut self, dur: Option<Duration>) -> io::Result<()>;

  /// Method that checks that transport is alive
  fn is_alive(&self) -> bool;
}
