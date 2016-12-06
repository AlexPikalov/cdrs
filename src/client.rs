use std::net;
use std::io;
use std::io::Write;

use super::consistency::Consistency;
use super::frame::{Frame, Opcode};
use super::IntoBytes;
use super::frame::parser::parse_frame;
use types::*;
use types::value::*;
use frame::frame_query::*;
use compression::Compression;

#[derive(Clone, Debug)]
pub struct Credentials {
    pub username: String,
    pub password: String
}

pub struct CDRS {
    tcp: net::TcpStream,
    compressor: Compression,
    credentials: Option<Credentials>
}

impl CDRS {
    pub fn new(addr: String, credentials: Option<Credentials>) -> io::Result<CDRS> {
        return net::TcpStream::connect(format!("{}:9042", addr).as_str())
            .map(|socket| CDRS {
                tcp: socket,
                compressor: Compression::None,
                credentials: credentials
            });
    }

    pub fn start(&mut self, compressor: Compression) -> io::Result<Frame> {
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
            if authenticator.as_str() == "org.apache.cassandra.auth.PasswordAuthenticator" {
                let mut tcp_auth = try!(self.tcp.try_clone());

                let credentials = self.credentials.clone().ok_or(io::Error::new(
                    io::ErrorKind::NotFound,
                    "Credetials required, but not found"
                ))?;

                let mut token = vec![0];
                token.extend_from_slice(credentials.username.into_bytes().as_slice());
                token.push(0);
                token.extend_from_slice(credentials.password.into_bytes().as_slice());
                let auth_token_bytes = CBytes::new(token).into_cbytes();

                try!(tcp_auth.write(Frame::new_req_auth_response(auth_token_bytes).into_cbytes().as_slice()));
                let auth_response = try!(parse_frame(tcp_auth, &compressor));

                return Ok(auth_response);
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Unsupported type of authenticator. {:?} got, but PasswordAuthenticator is supported.", authenticator)));
            }
        }

        unimplemented!();
    }

    pub fn options(&self) -> io::Result<Frame> {
        let mut tcp = try!(self.tcp.try_clone());
        let options_frame = Frame::new_req_options().into_cbytes();

        try!(tcp.write(options_frame.as_slice()));
        return parse_frame(tcp, &self.compressor);
    }

    pub fn prepare(&self, query: String) -> io::Result<Frame> {
        let mut tcp = try!(self.tcp.try_clone());
        let options_frame = Frame::new_req_prepare(query).into_cbytes();

        try!(tcp.write(options_frame.as_slice()));
        return parse_frame(tcp, &self.compressor);
    }

    pub fn execute(&self, id: CBytesShort, query_parameters: ParamsReqQuery) -> io::Result<Frame> {
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
            timestamp: Option<i64>) -> io::Result<Frame> {

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
