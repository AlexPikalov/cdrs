use std::io;
use std::io::{Read, Write};
use std::net;
use std::net::TcpStream;

pub struct Transport {
    tcp: TcpStream
}

impl Transport {
    pub fn new(addr: &str) -> io::Result<Transport> {
        return net::TcpStream::connect(addr)
            .map(|socket| Transport {
                tcp: socket
            });
    }

    /// In opposite to `TcpStream`'s `try_clone` this method
    /// creates absolutely new connection - it gets an address
    /// of a peer from `Transport` and creates a new instance
    /// with new TCP stream under hood.
    pub fn try_clone(&self) -> io::Result<Transport> {
        let addr = try!(self.tcp.peer_addr());

        net::TcpStream::connect(addr)
            .map(|socket| Transport {
                tcp: socket
            })
    }

    pub fn close(&mut self, close: net::Shutdown) -> io::Result<()> {
        return self.tcp.shutdown(close);
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
