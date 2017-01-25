use std::io;
use std::io::{Read, Write};
use std::net;
use std::net::TcpStream;

use openssl::ssl::{SslStream, SslConnector};

pub struct Transport {
    ssl: SslStream<TcpStream>,
    connector: SslConnector
}

impl Transport {
    pub fn new(addr: &str, connector: & SslConnector) -> io::Result<Transport> {
        let a: Vec<&str> = addr.split(':').collect();
        return net::TcpStream::connect(addr)
            .map(|socket| Transport {
                ssl: connector.connect(a[0], socket).unwrap(),
                connector: connector.clone()
            });
    }

    /// In opposite to `TcpStream`'s `try_clone` this method
    /// creates absolutely new connection - it gets an address
    /// of a peer from `Transport` and creates a new encrypted
    /// connection with a new TCP stream under hood.
    pub fn try_clone(&self) -> io::Result<Transport> {
        let addr = try!(self.ssl.get_ref().peer_addr());
        let ip_string = format!("{}", addr.ip());

        net::TcpStream::connect(addr)
            .map(|socket| Transport {
                ssl: self.connector.connect(ip_string.as_str(), socket).unwrap(),
                connector: self.connector.clone()
            })
    }

    pub fn close(&mut self, _close: net::Shutdown) -> io::Result<()> {
        self.ssl.shutdown().unwrap();
        return Ok(());
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
