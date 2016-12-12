use std::net;
use std::io;
use std::io::Write;

use consistency::Consistency;
use frame::{Frame, Opcode};
use IntoBytes;
use frame::parser::parse_frame;
use types::*;
use types::value::*;

use frame::frame_query::*;
use compression::Compression;
use authenticators::Authenticator;
use error;

#[derive(Clone, Debug)]
pub struct Credentials {
    pub username: String,
    pub password: String
}

pub struct CDRS<T: Authenticator> {
    tcp: net::TcpStream,
    compressor: Compression,
    authenticator: T
}

impl<T: Authenticator> CDRS<T> {
    pub fn new(addr: String, authenticator: T) -> error::Result<CDRS<T>> {
        return net::TcpStream::connect(format!("{}:9042", addr).as_str())
            .map(|socket| CDRS {
                tcp: socket,
                compressor: Compression::None,
                authenticator: authenticator
            })
            .map_err(|err| error::Error::Io(err));
    }

    pub fn start(&mut self, compressor: Compression) -> error::Result<Frame> {
        self.compressor = compressor;
        let mut tcp = try!(self.tcp.try_clone());
        let startup_frame = Frame::new_req_startup(compressor.into_string()).into_cbytes();

        try!(tcp.write(startup_frame.as_slice()));
        let start_response = try!(parse_frame(tcp, &compressor));

        if start_response.opcode == Opcode::Ready {
            return Ok(start_response);
        }

        if start_response.opcode == Opcode::Authenticate {
            let body = start_response.get_body();
            let authenticator = body.get_authenticator().unwrap();

            if authenticator.as_str() == self.authenticator.get_cassandra_name() {
                let mut tcp_auth = try!(self.tcp.try_clone());
                let auth_token_bytes = self.authenticator.get_auth_token().into_cbytes();
                try!(tcp_auth.write(Frame::new_req_auth_response(auth_token_bytes).into_cbytes().as_slice()));
                let auth_response = try!(parse_frame(tcp_auth, &compressor));

                return Ok(auth_response);
            } else {
                let io_err = io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Unsupported type of authenticator. {:?} got, but {} is supported.",
                        authenticator,
                        self.authenticator.get_cassandra_name()));
                return Err(error::Error::Io(io_err));
            }
        }

        unimplemented!();
    }

    pub fn options(&self) -> error::Result<Frame> {
        let mut tcp = try!(self.tcp.try_clone());
        let options_frame = Frame::new_req_options().into_cbytes();

        try!(tcp.write(options_frame.as_slice()));
        return parse_frame(tcp, &self.compressor);
    }

    pub fn prepare(&self, query: String) -> error::Result<Frame> {
        let mut tcp = try!(self.tcp.try_clone());
        let options_frame = Frame::new_req_prepare(query).into_cbytes();

        try!(tcp.write(options_frame.as_slice()));
        return parse_frame(tcp, &self.compressor);
    }

    pub fn execute(&self, id: CBytesShort, query_parameters: ParamsReqQuery) -> error::Result<Frame> {
        let mut tcp = try!(self.tcp.try_clone());
        let options_frame = Frame::new_req_execute(id, query_parameters).into_cbytes();

        try!(tcp.write(options_frame.as_slice()));
        return parse_frame(tcp, &self.compressor);
    }

    pub fn query(&self,
            query: String,
            consistency: Consistency,
            values: Option<Vec<Value>>,
            with_names: Option<bool>,
            page_size: Option<i32>,
            paging_state: Option<CBytes>,
            serial_consistency: Option<Consistency>,
            timestamp: Option<i64>) -> error::Result<Frame> {

        let mut tcp = try!(self.tcp.try_clone());
        let query_frame = Frame::new_req_query(query.clone(),
            consistency,
            values,
            with_names,
            page_size,
            paging_state,
            serial_consistency,
            timestamp).into_cbytes();

        try!(tcp.write(query_frame.as_slice()));
        return parse_frame(tcp, &self.compressor);
    }
}
