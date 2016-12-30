//! The modules which contains CDRS Cassandra client.
use std::net;
use std::io;
use std::io::Write;
use std::collections::HashMap;

use consistency::Consistency;
use frame::{Frame, Opcode};
use frame::frame_response::ResponseBody;
use IntoBytes;
use frame::parser::parse_frame;
use types::*;
use types::value::*;

use frame::frame_query::*;
use compression::Compression;
use authenticators::Authenticator;
use error;

/// DB user's credentials.
#[derive(Clone, Debug)]
pub struct Credentials {
    /// DB user's username
    pub username: String,
    /// DB user's password
    pub password: String
}

/// CDRS driver structure that provides a basic functionality to work with DB including
/// establishing new connection, getting supported options, preparing and executing CQL
/// queries, using compression and others.
pub struct CDRS<T: Authenticator + Clone> {
    tcp: net::TcpStream,
    compressor: Compression,
    authenticator: T
}

/// Map of options supported by Cassandra server.
type CassandraOptions = HashMap<String, Vec<String>>;

impl<'a, T: Authenticator + Clone + 'a> CDRS<T> {
    /// The method creates new instance of CDRS driver. At this step an instance doesn't
    /// connected to DB Server. To create new instance two parameters are needed to be
    /// provided - `addr` is IP address of DB Server, `authenticator` is a selected authenticator
    /// that is supported by particular DB Server. There are few authenticators already
    /// provided by this trait.
    pub fn new(addr: &str, authenticator: T) -> error::Result<CDRS<T>> {
        return net::TcpStream::connect(addr)
            .map(|socket| CDRS {
                tcp: socket,
                compressor: Compression::None,
                authenticator: authenticator
            })
            .map_err(|err| error::Error::Io(err));
    }

    /// The method makes an Option request to DB Server. As a response the server returns
    /// a map of supported options.
    pub fn get_options(&self) -> error::Result<CassandraOptions> {
        let mut tcp = try!(self.tcp.try_clone());
        let options_frame = Frame::new_req_options().into_cbytes();

        try!(tcp.write(options_frame.as_slice()));

        return parse_frame(tcp, &self.compressor)
            .map(|frame| {
                let body = frame.get_body();
                return match body {
                    ResponseBody::Supported(ref supported_body) => {
                        return supported_body.data.clone();
                    },
                    _ => unreachable!()
                };
            });
    }

    /// The method establishes connection to the server which address was provided on previous
    /// step. To create connection it's required to provide a compression method from a list
    /// of supported ones. In 4-th version of Cassandra protocol lz4 (`Compression::Lz4`)
    /// and snappy (`Compression::Snappy`) are supported. There is also one special compression
    /// method provided by CRDR driver, it's `Compression::None` that tells drivers that it
    /// should work without compression. If compression is provided then incomming frames
    /// will be decompressed automatically.
    pub fn start(mut self, compressor: Compression) -> error::Result<Session<T>> {
        self.compressor = compressor;
        let mut tcp = try!(self.tcp.try_clone());
        let startup_frame = Frame::new_req_startup(compressor.into_string()).into_cbytes();

        try!(tcp.write(startup_frame.as_slice()));
        let start_response = try!(parse_frame(tcp, &compressor));

        if start_response.opcode == Opcode::Ready {
            return Ok(Session::start(self));
        }

        if start_response.opcode == Opcode::Authenticate {
            let body = start_response.get_body();
            let authenticator = body.get_authenticator().unwrap();

            if authenticator.as_str() == self.authenticator.get_cassandra_name() {
                let mut tcp_auth = try!(self.tcp.try_clone());
                let auth_token_bytes = self.authenticator.get_auth_token().into_cbytes();
                try!(tcp_auth.write(Frame::new_req_auth_response(auth_token_bytes).into_cbytes().as_slice()));
                try!(parse_frame(tcp_auth, &compressor));

                return Ok(Session::start(self));
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

    fn drop_connection(&self) -> error::Result<()> {
        return self.tcp.shutdown(net::Shutdown::Both)
            .map_err(|err| error::Error::Io(err));
    }
}

impl<T: Authenticator + Clone> Drop for CDRS<T> {
    fn drop(&mut self) {
        match self.drop_connection() {
            Ok(_) => (),
            Err(err) => {
                println!("Error occured during dropping CDRS {:?}", err);
            }
        }
    }
}

impl<T: Authenticator + Clone> Clone for CDRS<T> {
    /// Creates a clone of CDRS instance
    /// # Panics
    /// It panics if tcp.try_clone() returns an error.
    fn clone(&self) -> CDRS<T> {
        return CDRS {
            tcp: self.tcp.try_clone().unwrap(),
            compressor: self.compressor.clone(),
            authenticator: self.authenticator.clone()
        };
    }
}

/// The object that provides functionality for communication with Cassandra server.
pub struct Session<T: Authenticator + Clone> {
    started: bool,
    cdrs: CDRS<T>
}

impl<T: Authenticator + Clone> Session<T> {
    /// Creates new session basing on CDRS instance.
    pub fn start(cdrs: CDRS<T>) -> Session<T> {
        return Session {
            cdrs: cdrs,
            started: true
        };
    }

    /// Manually ends current session.
    /// Apart of that session will be ended automatically when the instance is dropped.
    pub fn end(&mut self) {
        if self.started {
            self.started = false;
            self.cdrs.drop_connection().expect("should not fail during ending session");
        }
    }

    /// The method makes a request to DB Server to prepare provided query.
    pub fn prepare(&self, query: String) -> error::Result<Frame> {
        let mut tcp = try!(self.cdrs.tcp.try_clone());
        let options_frame = Frame::new_req_prepare(query).into_cbytes();

        try!(tcp.write(options_frame.as_slice()));
        return parse_frame(tcp, &self.cdrs.compressor);
    }

    /// The method makes a request to DB Server to execute a query with provided id
    /// using provided query parameters. `id` is an ID of a query which Server
    /// returns back to a driver as a response to `prepare` request.
    pub fn execute(&self, id: CBytesShort, query_parameters: ParamsReqQuery) -> error::Result<Frame> {
        let mut tcp = try!(self.cdrs.tcp.try_clone());
        let options_frame = Frame::new_req_execute(id, query_parameters).into_cbytes();

        try!(tcp.write(options_frame.as_slice()));
        return parse_frame(tcp, &self.cdrs.compressor);
    }

    /// The method makes a request to DB Server to execute a query provided in `query` argument.
    /// The rest of parameters are the same to ones described in [Cassandra v4 protocol]
    /// (https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L304)
    pub fn query(&self,
            query: String,
            consistency: Consistency,
            values: Option<Vec<Value>>,
            with_names: Option<bool>,
            page_size: Option<i32>,
            paging_state: Option<CBytes>,
            serial_consistency: Option<Consistency>,
            timestamp: Option<i64>) -> error::Result<Frame> {

        let mut tcp = try!(self.cdrs.tcp.try_clone());
        let query_frame = Frame::new_req_query(query.clone(),
            consistency,
            values,
            with_names,
            page_size,
            paging_state,
            serial_consistency,
            timestamp).into_cbytes();

        try!(tcp.write(query_frame.as_slice()));
        return parse_frame(tcp, &self.cdrs.compressor);
    }
}

impl<T: Authenticator + Clone> Drop for Session<T> {
    fn drop(&mut self) {
        self.end();
    }
}
