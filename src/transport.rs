use std::io;
use std::io::{Read, Write};
use std::net;
use std::net::TcpStream;
#[cfg(feature = "ssl")]
use openssl::ssl::{SslStream, SslConnector};

pub  trait  CDRSTransport: Sized+Read+Write+Send+Sync{

    fn try_clone(&self) -> io::Result<Self>;
    fn close(&mut self, close: net::Shutdown) -> io::Result<()>;
}


#[derive(Debug)]
pub struct Transport {
    tcp: TcpStream
}

impl Transport {
    pub fn new(addr: &str) -> io::Result<Transport> {
        TcpStream::connect(addr).map(|socket| Transport {tcp: socket})
    }

}

impl Read for Transport {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        return self.tcp.read(buf);
    }
}

impl Write for Transport {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        return self.tcp.write(buf);
    }

    fn flush(&mut self) -> io::Result<()> {
        return self.tcp.flush();
    }
}

impl CDRSTransport  for Transport {

    /// In opposite to `TcpStream`'s `try_clone` this method
    /// creates absolutely new connection - it gets an address
    /// of a peer from `Transport` and creates a new encrypted
    /// transport with new TCP stream under hood.
    fn try_clone(&self) -> io::Result<Transport> {
        let addr = try!( self.tcp.peer_addr());
        TcpStream::connect(addr).map( | socket | Transport {tcp: socket})
    }

    fn close(&mut self, close: net::Shutdown) -> io::Result<()> {
        return self.tcp.shutdown(close);
    }


}

/*************************************/
/** TLS**/
/**************************************/

#[cfg(feature = "ssl")]
pub struct TransportTls {
    ssl: SslStream<TcpStream>,
    connector: SslConnector
}
#[cfg(feature = "ssl")]
impl TransportTls {

    pub fn new(addr: &str, connector: & SslConnector) -> io::Result<TransportTls> {
        let a: Vec<&str> = addr.split(':').collect();
        let res =  net::TcpStream::connect(addr)
            .map(|socket|
                     connector.connect(a[0], socket)
                         .map(|sslsocket|
                                  TransportTls {
                                      ssl: sslsocket,
                                      connector: connector.clone()
                                  }
                         )
            );

        res.and_then(|res| {
            res.map(|n: TransportTls| n ).map_err(|e| io::Error::new(io::ErrorKind::Other,e))

        })
    }


}
#[cfg(feature = "ssl")]
impl Read for TransportTls {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        return self.ssl.read(buf);
    }
}
#[cfg(feature = "ssl")]
impl Write for TransportTls {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        return self.ssl.write(buf);
    }

    fn flush(&mut self) -> io::Result<()> {
        return self.ssl.flush();
    }
}

#[cfg(feature = "ssl")]
impl CDRSTransport for TransportTls{

    /// In opposite to `TcpStream`'s `try_clone` this method
    /// creates absolutely new connection - it gets an address
    /// of a peer from `TransportTls` and creates a new encrypted
    /// connection with a new TCP stream under hood.
    fn try_clone(&self) -> io::Result<TransportTls> {
        let addr = try!(self.ssl.get_ref().peer_addr());
        let ip_string = format!("{}", addr.ip());

        let res =  net::TcpStream::connect(addr)
            .map(|socket|
                     self.connector.connect(ip_string.as_str(), socket)
                         .map(|sslsocket| TransportTls {ssl: sslsocket, connector: self.connector.clone()}));

        res.and_then(|res| {
            res.map(|n: TransportTls| n ).map_err(|e| io::Error::new(io::ErrorKind::Other,e))
        })
    }

    fn close(&mut self, _close: net::Shutdown) -> io::Result<()> {
        return self.ssl.shutdown().map_err(|e| io::Error::new(io::ErrorKind::Other,e)).and_then(|_| Ok(()));
    }
}

