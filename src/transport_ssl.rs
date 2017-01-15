use std::io;
use std::io::{Read, Write};
use std::net;
use std::net::TcpStream;

use rustls::{ClientSession, Session};

pub struct Transport {
    tcp: TcpStream,
    ssl: ClientSession
}

impl Transport {
    pub fn new(addr: &str, ssl: ClientSession) -> io::Result<Transport> {
        return net::TcpStream::connect(addr)
            .map(|socket| Transport {
                tcp: socket,
                ssl: ssl
            });
    }
}

impl Read for Transport {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        try!(self.ssl.read_tls(&mut self.tcp));
        return self.ssl.read(buf);
    }
}

impl Write for Transport {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        try!(self.ssl.write(buf));
        return self.ssl.write_tls(&mut self.tcp);
    }

    fn flush(&mut self) -> io::Result<()> {
        return self.tcp.flush().and(self.ssl.flush());
    }
}
