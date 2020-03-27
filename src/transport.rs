//!This module contains a declaration of `CDRSTransport` trait which should be implemented
//!for particular transport in order to be able using it as a trasport of CDRS client.
//!
//!Curently CDRS provides to concrete transports which implement `CDRSTranpsport` trait. There
//! are:
//!
//! * [`TransportTcp`][tTcp] is default TCP transport which is usually used to establish
//!connection and exchange frames.
//!
//! * `TransportTls` is a transport which is used to establish SSL encrypted connection
//!with Apache Cassandra server. **Note:** this option is available if and only if CDRS is imported
//!with `ssl` feature.

#[cfg(feature = "ssl")]
use native_tls::TlsConnector;
use std::io;
use tokio::io::AsyncWriteExt;
use tokio::prelude::*;
use std::task::Context;
use tokio::macros::support::{Pin, Poll};
use std::io::Error;
use std::net;
use tokio::net::TcpStream;
use async_trait::async_trait;
#[cfg(feature = "ssl")]
use tokio_tls::TlsStream;

// TODO [v 2.x.x]: CDRSTransport: ... + BufReader + ButWriter + ...
///General CDRS transport trait. Both [`TranportTcp`][transportTcp]
///and [`TransportTls`][transportTls] has their own implementations of this trait. Generaly
///speaking it extends/includes `io::Read` and `io::Write` traits and should be thread safe.
///[transportTcp]:struct.TransportTcp.html
///[transportTls]:struct.TransportTls.html
#[async_trait]
pub trait CDRSTransport: Sized + AsyncRead + AsyncWriteExt + Send + Sync {
    /// Creates a new independently owned handle to the underlying socket.
    ///
    /// The returned TcpStream is a reference to the same stream that this object references.
    /// Both handles will read and write the same stream of data, and options set on one stream
    /// will be propagated to the other stream.
    async fn try_clone(&self) -> io::Result<Self>;

    /// Shuts down the read, write, or both halves of this connection.
    async fn close(&mut self, close: net::Shutdown) -> io::Result<()>;

    /// Method that checks that transport is alive
    fn is_alive(&self) -> bool;
}

/// Default Tcp transport.
pub struct TransportTcp {
    tcp: TcpStream,
    addr: String,
}

impl TransportTcp {
    /// Constructs a new `TransportTcp`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use cdrs::transport::TransportTcp;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let addr = "127.0.0.1:9042";
    ///     let tcp_transport = TransportTcp::new(addr).await.unwrap();
    /// }
    /// ```
    pub async fn new(addr: &str) -> io::Result<TransportTcp> {
        TcpStream::connect(addr).await.map(|socket| TransportTcp {
            tcp: socket,
            addr: addr.to_string(),
        })
    }
}

impl AsyncRead for TransportTcp {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.tcp).poll_read(cx, buf)
    }
}

impl AsyncWrite for TransportTcp {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Error>> {
        Pin::new(&mut self.tcp).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Pin::new(&mut self.tcp).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Pin::new(&mut self.tcp).poll_shutdown(cx)
    }
}

#[async_trait]
impl CDRSTransport for TransportTcp {
    async fn try_clone(&self) -> io::Result<TransportTcp> {
        TcpStream::connect(self.addr.as_str()).await.map(|socket| TransportTcp {
            tcp: socket,
            addr: self.addr.clone(),
        })
    }

    async fn close(&mut self, close: net::Shutdown) -> io::Result<()> {
        self.tcp.shutdown(close)
    }

    fn is_alive(&self) -> bool {
        self.tcp.peer_addr().is_ok()
    }
}

/// ***********************************
#[cfg(feature = "ssl")]
pub struct TransportTls {
    ssl: TlsStream<TcpStream>,
    addr: String,
}
#[cfg(feature = "ssl")]
impl TransportTls {
    pub async fn new(addr: &str) -> io::Result<TransportTls> {
        let a: Vec<&str> = addr.split(':').collect();
        let socket = TcpStream::connect(addr).await?;
        let builder = TlsConnector::builder();
        let connector = builder.build().map_err(|error| io::Error::new(io::ErrorKind::Other, error))?;
        let connector = tokio_tls::TlsConnector::from(connector);
        connector
            .connect(a[0], socket)
            .await
            .map_err(|error| io::Error::new(io::ErrorKind::Other, error))
            .map(|sslsocket| TransportTls {
                ssl: sslsocket,
                addr: addr.to_string(),
            })
            // .and_then(|res| {
            //     res.map(|n: TransportTls| n)
            //         .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
            // })
    }
}
#[cfg(feature = "ssl")]
impl AsyncRead for TransportTls {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.ssl).poll_read(cx, buf)
    }
}
#[cfg(feature = "ssl")]
impl AsyncWrite for TransportTls {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Error>> {
        Pin::new(&mut self.ssl).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Pin::new(&mut self.ssl).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Pin::new(&mut self.ssl).poll_shutdown(cx)
    }
}

#[cfg(feature = "ssl")]
#[async_trait]
impl CDRSTransport for TransportTls {
    /// This method
    /// creates absolutely new connection - it gets an address
    /// of a peer from `TransportTls` and creates a new encrypted
    /// connection with a new TCP stream under hood.
    async fn try_clone(&self) -> io::Result<TransportTls> {
        let ip = match self.addr.split(":").nth(0) {
            Some(_ip) => _ip,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Wrong addess string - IP is missed",
                ));
            }
        };

        let socket = TcpStream::connect(self.addr.as_str()).await?;
        let builder = TlsConnector::builder();
        let connector = builder.build().map_err(|error| io::Error::new(io::ErrorKind::Other, error))?;
        let connector = tokio_tls::TlsConnector::from(connector);
        connector
            .connect(ip, socket)
            .await
            .map_err(|error| io::Error::new(io::ErrorKind::Other, error))
            .map(|sslsocket| TransportTls {
                ssl: sslsocket,
                addr: self.addr.clone(),
            })
    }

    async fn close(&mut self, _close: net::Shutdown) -> io::Result<()> {
        self.ssl
            .shutdown()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
            .and_then(|_| Ok(()))
    }

    fn is_alive(&self) -> bool {
        self.ssl.get_ref().peer_addr().is_ok()
    }
}
