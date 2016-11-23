use std::net;
use std::io;
use std::io::Write;

use super::consistency::Consistency;
use super::frame::Frame;
use super::IntoBytes;
use super::parser::parse_frame;

pub struct CDRS {
    tcp: net::TcpStream
}

impl CDRS {
    pub fn new(addr: String) -> io::Result<CDRS> {
        return net::TcpStream::connect(format!("{}:9042", addr).as_str())
            .map(|socket| CDRS {tcp: socket});
    }

    pub fn start(&self) -> io::Result<Frame> {
        let compression = None;
        let mut tcp = try!(self.tcp.try_clone());
        let startup_frame = Frame::new_req_startup(compression).into_cbytes();

        try!(tcp.write(startup_frame.as_slice()));
        return parse_frame(tcp);
    }

    pub fn query(&self, q: String) -> io::Result<Frame> {
        let mut tcp = try!(self.tcp.try_clone());
        let query_frame = Frame::new_req_query(q.clone(), Consistency::One, None, None, None, None, None, None).into_cbytes();

        try!(tcp.write(query_frame.as_slice()));
        return parse_frame(tcp);
    }
}
