use std::io;
use std::io::{Read, Write};
use std::net;
use std::net::TcpStream;
use std::time::Duration;
#[cfg(feature = "ssl")]
use openssl::ssl::{SslStream, SslConnector};

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
}

pub struct TransportTcp {
    tcp: TcpStream,
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
        TcpStream::connect(addr).map(|socket| TransportTcp { tcp: socket })
    }
}

impl Read for TransportTcp {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.tcp.read(buf)
    }
}

impl Write for TransportTcp {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.tcp.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.tcp.flush()
    }
}

impl CDRSTransport for TransportTcp {
    fn try_clone(&self) -> io::Result<TransportTcp> {
        let addr = try!(self.tcp.peer_addr());
        TcpStream::connect(addr).map(|socket| TransportTcp { tcp: socket })
    }

    fn close(&mut self, close: net::Shutdown) -> io::Result<()> {
        self.tcp.shutdown(close)
    }

    fn set_timeout(&mut self, dur: Option<Duration>) -> io::Result<()> {
        self.tcp.set_read_timeout(dur).and_then(|_| self.tcp.set_write_timeout(dur))
    }
}

/// **********************************
/** TLS**/
/// ***********************************
#[cfg(feature = "ssl")]
pub struct TransportTls {
    ssl: SslStream<TcpStream>,
    connector: SslConnector,
}
#[cfg(feature = "ssl")]
impl TransportTls {
    pub fn new(addr: &str, connector: &SslConnector) -> io::Result<TransportTls> {
        let a: Vec<&str> = addr.split(':').collect();
        let res = net::TcpStream::connect(addr).map(|socket| {
            connector.connect(a[0], socket)
                .map(|sslsocket| {
                    TransportTls {
                        ssl: sslsocket,
                        connector: connector.clone(),
                    }
                })
        });

        res.and_then(|res| {
            res.map(|n: TransportTls| n).map_err(|e| io::Error::new(io::ErrorKind::Other, e))

        })
    }
}
#[cfg(feature = "ssl")]
impl Read for TransportTls {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.ssl.read(buf)
    }
}
#[cfg(feature = "ssl")]
impl Write for TransportTls {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.ssl.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.ssl.flush()
    }
}

#[cfg(feature = "ssl")]
impl CDRSTransport for TransportTls {
    /// This method
    /// creates absolutely new connection - it gets an address
    /// of a peer from `TransportTls` and creates a new encrypted
    /// connection with a new TCP stream under hood.
    fn try_clone(&self) -> io::Result<TransportTls> {
        let addr = try!(self.ssl.get_ref().peer_addr());
        let ip_string = format!("{}", addr.ip());

        let res = net::TcpStream::connect(addr).map(|socket| {
            self.connector
                .connect(ip_string.as_str(), socket)
                .map(|sslsocket| {
                    TransportTls {
                        ssl: sslsocket,
                        connector: self.connector.clone(),
                    }
                })
        });

        res.and_then(|res| {
            res.map(|n: TransportTls| n).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
        })
    }

    fn close(&mut self, _close: net::Shutdown) -> io::Result<()> {
        self.ssl
            .shutdown()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
            .and_then(|_| Ok(()))
    }

    fn set_timeout(&mut self, dur: Option<Duration>) -> io::Result<()> {
        let stream = self.ssl.get_mut();
        stream.set_read_timeout(dur).and_then(|_| stream.set_write_timeout(dur))
    }
}
