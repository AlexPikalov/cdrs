use std::io;
use std::io::{Read, Write};
use std::net;
use std::net::TcpStream;
use openssl::ssl::{SslStream, SslConnector,HandshakeError};


pub struct Transport {
    ssl: SslStream<TcpStream>,
    connector: SslConnector
}

impl Transport {
    pub fn new(addr: &str, connector: & SslConnector) -> io::Result<Transport> {
        let a: Vec<&str> = addr.split(':').collect();
        let res:Result< Result<Transport,HandshakeError<net::TcpStream>>,io::Error> =  net::TcpStream::connect(addr)
            .map(|socket|
                 connector.connect(a[0], socket)
                 .map(|sslsocket|
                    Transport {
                        ssl: sslsocket,
                        connector: connector.clone()
                    }
                 )
            );

        res.and_then(|res: Result<Transport,HandshakeError<net::TcpStream>>| {
        // transform `Ok(Result<Transport,HandshakeError<net::TcpStream>>)` into `Ok(Result<Transport, io::Error>)`
        res
        // transform n to Transport
        .map(|n: Transport| n )
        // transform `HandshakeError` into `'io::Error`
        .map_err(|e| io::Error::new(io::ErrorKind::Other,e))

        })
    }

    /// In opposite to `TcpStream`'s `try_clone` this method
    /// creates absolutely new connection - it gets an address
    /// of a peer from `Transport` and creates a new encrypted
    /// connection with a new TCP stream under hood.
    pub fn try_clone(&self) -> io::Result<Transport> {
        let addr = try!(self.ssl.get_ref().peer_addr());
        let ip_string = format!("{}", addr.ip());

        let res =  net::TcpStream::connect(addr)
            .map(|socket|
                  self.connector.connect(ip_string.as_str(), socket)
                      .map(|sslsocket| Transport {ssl: sslsocket, connector: self.connector.clone()}));

        res.and_then(|res| {
            res.map(|n: Transport| n ).map_err(|e| io::Error::new(io::ErrorKind::Other,e))
        })
    }

    pub fn close(&mut self, _close: net::Shutdown) -> io::Result<()> {
        return self.ssl.shutdown().map_err(|e| io::Error::new(io::ErrorKind::Other,e)).and_then(|_| Ok(()));
    }
}

impl Read for Transport {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        return self.ssl.read(buf);
    }
}

impl Write for Transport {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        return self.ssl.write(buf);
    }

    fn flush(&mut self) -> io::Result<()> {
        return self.ssl.flush();
    }
}
