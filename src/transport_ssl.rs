use std::io;
use std::io::{Read, Write};
use std::net;
use std::net::TcpStream;

use openssl::ssl::{SslStream, SslConnector};

pub struct Transport {
    ssl: SslStream<TcpStream>
}

impl Transport {
    pub fn new(addr: &str, connector: &SslConnector) -> io::Result<Transport> {
        let a: Vec<&str> = addr.split(':').collect();
        return net::TcpStream::connect(addr)
            .map(|socket| Transport {
                ssl: connector.connect(a[0], socket).unwrap()
            });
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
