//! The modules which contains CDRS Cassandra client.
use std::net;
use std::io;
use std::io::Write;
use std::collections::HashMap;
use std::default::Default;

use consistency::Consistency;
use frame::{Frame, Opcode, Flag};
use frame::frame_response::ResponseBody;
use IntoBytes;
use frame::parser::parse_frame;
use types::*;
use types::value::*;

use frame::frame_query::*;
use compression::Compression;
use authenticators::Authenticator;
use error;

/// instead of writing functions which resemble
/// ```
/// pub fn query<'a> (&'a mut self,query: String) -> &'a mut Self{
///     self.query = Some(query);
///            self
/// }
/// ```
/// and repeating it for all the attributes; it is extracted out as a macro so that code is more concise
/// see @https://doc.rust-lang.org/book/method-syntax.html
///
///
macro_rules! builder_opt_field {
    ($field:ident, $field_type:ty) => {
        pub fn $field<'a>(&'a mut self,
                          $field: $field_type) -> &'a mut Self {
            self.$field = Some($field);
            self
        }
    };
}

/// Structure that represents CQL query and parameters which will be applied during
/// its execution
#[derive(Debug, Default)]
pub struct Query {
    query: String,
    // query parameters
    consistency: Option<Consistency>,
    values: Option<Vec<Value>>,
    with_names: Option<bool>,
    page_size: Option<i32>,
    paging_state: Option<CBytes>,
    serial_consistency: Option<Consistency>,
    timestamp: Option<i64>
}

/// QueryBuilder is a helper sturcture that helps to construct `Query`. `Query` itself
/// consists of CQL query string and list of parameters.
/// Parameters are the same as ones described in [Cassandra v4 protocol]
/// (https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L304)
#[derive(Debug, Default)]
pub struct QueryBuilder {
    query: String,
    consistency: Option<Consistency>,
    values: Option<Vec<Value>>,
    with_names: Option<bool>,
    page_size: Option<i32>,
    paging_state: Option<CBytes>,
    serial_consistency: Option<Consistency>,
    timestamp: Option<i64>
}

impl QueryBuilder {
    /// Factory function that takes CQL `&str` as an argument and returns new `QueryBuilder`
    pub fn new(query: &str) -> QueryBuilder {
        return QueryBuilder {
            query: query.to_string(),
            ..Default::default()
        };
    }

    /// Sets new query consistency
    builder_opt_field!(consistency, Consistency);

    /// Sets new query values
    builder_opt_field!(values, Vec<Value>);

    /// Sets new query with_names
    builder_opt_field!(with_names, bool);

    /// Sets new query pagesize
    builder_opt_field!(page_size, i32);

    /// Sets new query pagin state
    builder_opt_field!(paging_state, CBytes);

    /// Sets new query serial_consistency
    builder_opt_field!(serial_consistency, Consistency);

    /// Sets new quey timestamp
    builder_opt_field!(timestamp, i64);

    /// Finalizes query building process and returns query itself
    pub fn finalize(&self) -> Query {
        return Query {
            query: self.query.clone(),
            consistency: self.consistency.clone(),
            values: self.values.clone(),
            with_names: self.with_names.clone(),
            page_size: self.page_size.clone(),
            paging_state: self.paging_state.clone(),
            serial_consistency: self.serial_consistency.clone(),
            timestamp: self.timestamp.clone()
        };
    }
}

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
pub struct CDRS<T: Authenticator> {
    tcp: net::TcpStream,
    compressor: Compression,
    authenticator: T
}

/// Map of options supported by Cassandra server.
pub type CassandraOptions = HashMap<String, Vec<String>>;

impl<'a, T: Authenticator + 'a> CDRS<T> {
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
            .map(|frame| match frame.get_body() {
                ResponseBody::Supported(ref supported_body) => {
                    return supported_body.data.clone();
                },
                _ => unreachable!()
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

impl<T: Authenticator> Clone for CDRS<T> {
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
pub struct Session<T: Authenticator> {
    started: bool,
    cdrs: CDRS<T>,
    compressor: Compression
}

impl<T: Authenticator> Session<T> {
    /// Creates new session basing on CDRS instance.
    pub fn start(cdrs: CDRS<T>) -> Session<T> {
        let compressor = cdrs.compressor.clone();
        return Session {
            cdrs: cdrs,
            started: true,
            compressor: compressor
        };
    }

    /// The method overrides a compression method of current session
    pub fn compressor(&mut self, compressor: Compression) -> &mut Self {
        self.compressor = compressor;
        return self;
    }

    /// Manually ends current session.
    /// Apart of that session will be ended automatically when the instance is dropped.
    pub fn end(&mut self) {
        if self.started {
            self.started = false;
            match self.cdrs.drop_connection() {
                Ok(_) => (),
                Err(err) => {
                    println!("Error occured during dropping CDRS {:?}", err);
                }
            }
        }
    }

    /// The method makes a request to DB Server to prepare provided query.
    pub fn prepare(&self, query: String, with_tracing: bool, with_warnings: bool) -> error::Result<Frame> {
        let mut tcp = try!(self.cdrs.tcp.try_clone());

        let mut flags = vec![];
        if with_tracing {
            flags.push(Flag::Tracing);
        }
        if with_warnings {
            flags.push(Flag::Warning);
        }

        let options_frame = Frame::new_req_prepare(query, flags).into_cbytes();

        try!(tcp.write(options_frame.as_slice()));
        return parse_frame(tcp, &self.compressor);
    }

    /// The method makes a request to DB Server to execute a query with provided id
    /// using provided query parameters. `id` is an ID of a query which Server
    /// returns back to a driver as a response to `prepare` request.
    pub fn execute(&self,
        id: CBytesShort,
        query_parameters: ParamsReqQuery,
        with_tracing: bool,
        with_warnings: bool) -> error::Result<Frame> {

        let mut tcp = try!(self.cdrs.tcp.try_clone());

        let mut flags = vec![];
        if with_tracing {
            flags.push(Flag::Tracing);
        }
        if with_warnings {
            flags.push(Flag::Warning);
        }

        let options_frame = Frame::new_req_execute(id, query_parameters, flags).into_cbytes();

        try!(tcp.write(options_frame.as_slice()));
        return parse_frame(tcp, &self.compressor);
    }

    /// The method makes a request to DB Server to execute a query provided in `query` argument.
    /// you can build the query with QueryBuilder
    /// ```
    /// let qb = QueryBuilder::new().query("select * from emp").consistency(Consistency::One).page_size(Some(4));
    /// session.query_with_builder(qb);
    /// ```
    pub fn query(&self, query: Query, with_tracing: bool, with_warnings: bool) -> error::Result<Frame> {
        let mut tcp = try!(self.cdrs.tcp.try_clone());
        let consistency = match query.consistency {
            Some(cs) => cs,
            None => Consistency::One,
        };

        let mut flags = vec![];
        if with_tracing {
            flags.push(Flag::Tracing);
        }
        if with_warnings {
            flags.push(Flag::Warning);
        }

        let query_frame = Frame::new_req_query(query.query,
            consistency,
            query.values,
            query.with_names,
            query.page_size,
            query.paging_state,
            query.serial_consistency,
            query.timestamp,
            flags).into_cbytes();

        try!(tcp.write(query_frame.as_slice()));
        return parse_frame(tcp, &self.compressor);
    }
}
