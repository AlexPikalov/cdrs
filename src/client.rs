//! The modules which contains CDRS Cassandra client.
use std::net;
use std::io;
use std::collections::HashMap;
use query::{Query, QueryParams, QueryBatch};
use frame::{Frame, Opcode, Flag};
use frame::frame_response::ResponseBody;
use IntoBytes;
use frame::parser::parse_frame;
use types::*;
use frame::events::SimpleServerEvent;

use compression::Compression;
use authenticators::Authenticator;
use error;
use transport::CDRSTransport;

use events::{Listener, EventStream, new_listener};

/// CDRS driver structure that provides a basic functionality to work with DB including
/// establishing new connection, getting supported options, preparing and executing CQL
/// queries, using compression and other.
pub struct CDRS<T: Authenticator, X: CDRSTransport> {
    compressor: Compression,
    authenticator: T,
    transport: X,
}

/// Map of options supported by Cassandra server.
pub type CassandraOptions = HashMap<String, Vec<String>>;

impl<'a, T: Authenticator + 'a, X: CDRSTransport + 'a> CDRS<T, X> {
    /// The method creates new instance of CDRS driver. At this step an instance doesn't
    /// connected to DB Server. To create new instance two parameters are needed to be
    /// provided - `addr` is IP address of DB Server, `authenticator` is a selected authenticator
    /// that is supported by particular DB Server. There are few authenticators already
    /// provided by this trait.
    pub fn new(transport: X, authenticator: T) -> CDRS<T, X> {
        return CDRS {
            compressor: Compression::None,
            authenticator: authenticator,
            transport: transport,
        };
    }

    /// The method makes an Option request to DB Server. As a response the server returns
    /// a map of supported options.
    pub fn get_options(&mut self) -> error::Result<CassandraOptions> {
        let options_frame = Frame::new_req_options().into_cbytes();

        try!(self.transport.write(options_frame.as_slice()));

        return parse_frame(&mut self.transport, &self.compressor)
            .map(|frame| match frame.get_body() {
                ResponseBody::Supported(ref supported_body) => supported_body.data.clone(),
                _ => unreachable!(),
            });
    }

    /// The method establishes connection to the server which address was provided on previous
    /// step. To create connection it's required to provide a compression method from a list
    /// of supported ones. In 4-th version of Cassandra protocol lz4 (`Compression::Lz4`)
    /// and snappy (`Compression::Snappy`) are supported. There is also one special compression
    /// method provided by CRDR driver, it's `Compression::None` that tells drivers that it
    /// should work without compression. If compression is provided then incomming frames
    /// will be decompressed automatically.
    pub fn start(mut self, compressor: Compression) -> error::Result<Session<T, X>> {
        self.compressor = compressor;
        let startup_frame = Frame::new_req_startup(compressor.as_str()).into_cbytes();

        try!(self.transport.write(startup_frame.as_slice()));
        let start_response = try!(parse_frame(&mut self.transport, &compressor));

        if start_response.opcode == Opcode::Ready {
            return Ok(Session::start(self));
        }

        if start_response.opcode == Opcode::Authenticate {
            let body = start_response.get_body();
            let authenticator = body.get_authenticator()
                .expect("Cassandra Server did communicate that it needed password
                authentication but the  auth schema was missing in the body response");

            // This creates a new scope; avoiding a clone
            // and we check whether
            // 1. any authenticators has been passed in by client and if not send error back
            // 2. authenticator is provided by the client and `auth_scheme` presented by
            //      the server and client are same if not send error back
            // 3. if it falls through it means the preliminary conditions are true

            let auth_check = self.authenticator
                .get_cassandra_name()
                .ok_or(error::Error::General("No authenticator was provided ".to_string()))
                .map(|auth| {
                    if authenticator != auth {
                        let io_err =
                            io::Error::new(io::ErrorKind::NotFound,
                                           format!("Unsupported type of authenticator. {:?} got,
                             but {} is supported.",
                                                   authenticator,
                                                   authenticator));
                        return Err(error::Error::Io(io_err));
                    }
                    Ok(())
                });

            if let Err(err) = auth_check {
                return Err(err);
            }

            let auth_token_bytes = self.authenticator.get_auth_token().into_cbytes();
            try!(self.transport
                .write(Frame::new_req_auth_response(auth_token_bytes).into_cbytes().as_slice()));
            try!(parse_frame(&mut self.transport, &compressor));

            return Ok(Session::start(self));


        }

        unimplemented!();
    }

    fn drop_connection(&mut self) -> error::Result<()> {
        return self.transport
            .close(net::Shutdown::Both)
            .map_err(|err| error::Error::Io(err));
    }
}

/// The object that provides functionality for communication with Cassandra server.
pub struct Session<T: Authenticator, X: CDRSTransport> {
    started: bool,
    cdrs: CDRS<T, X>,
    compressor: Compression,
}

impl<T: Authenticator, X: CDRSTransport> Session<T, X> {
    /// Creates new session basing on CDRS instance.
    pub fn start(cdrs: CDRS<T, X>) -> Session<T, X> {
        let compressor = cdrs.compressor.clone();
        return Session {
            cdrs: cdrs,
            started: true,
            compressor: compressor,
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
    pub fn prepare(&mut self,
                   query: String,
                   with_tracing: bool,
                   with_warnings: bool)
                   -> error::Result<Frame> {
        let mut flags = vec![];
        if with_tracing {
            flags.push(Flag::Tracing);
        }
        if with_warnings {
            flags.push(Flag::Warning);
        }

        let options_frame = Frame::new_req_prepare(query, flags).into_cbytes();

        try!(self.cdrs.transport.write(options_frame.as_slice()));

        parse_frame(&mut self.cdrs.transport, &self.compressor)
    }

    /// The method makes a request to DB Server to execute a query with provided id
    /// using provided query parameters. `id` is an ID of a query which Server
    /// returns back to a driver as a response to `prepare` request.
    pub fn execute(&mut self,
                   id: &CBytesShort,
                   query_parameters: QueryParams,
                   with_tracing: bool,
                   with_warnings: bool)
                   -> error::Result<Frame> {

        let mut flags = vec![];
        if with_tracing {
            flags.push(Flag::Tracing);
        }
        if with_warnings {
            flags.push(Flag::Warning);
        }
        let options_frame = Frame::new_req_execute(id, query_parameters, flags).into_cbytes();

        (self.cdrs.transport.write(options_frame.as_slice()))?;
        return parse_frame(&mut self.cdrs.transport, &self.compressor);
    }

    /// The method makes a request to DB Server to execute a query provided in `query` argument.
    /// you can build the query with QueryBuilder
    /// ```
    /// let qb = QueryBuilder::new().query("select * from emp")
    /// .consistency(Consistency::One).page_size(Some(4));
    /// session.query_with_builder(qb);
    /// ```
    pub fn query(&mut self,
                 query: Query,
                 with_tracing: bool,
                 with_warnings: bool)
                 -> error::Result<Frame> {
        let mut flags = vec![];

        if with_tracing {
            flags.push(Flag::Tracing);
        }

        if with_warnings {
            flags.push(Flag::Warning);
        }

        let query_frame = Frame::new_req_query(query.query,
                                               query.consistency,
                                               query.values,
                                               query.with_names,
                                               query.page_size,
                                               query.paging_state,
                                               query.serial_consistency,
                                               query.timestamp,
                                               flags)
            .into_cbytes();

        try!(self.cdrs.transport.write(query_frame.as_slice()));
        return parse_frame(&mut self.cdrs.transport, &self.compressor);
    }

    pub fn batch(&mut self,
                 batch_query: QueryBatch,
                 with_tracing: bool,
                 with_warnings: bool)
                 -> error::Result<Frame> {
        let mut flags = vec![];

        if with_tracing {
            flags.push(Flag::Tracing);
        }

        if with_warnings {
            flags.push(Flag::Warning);
        }

        let query_frame = Frame::new_req_batch(batch_query, flags).into_cbytes();

        try!(self.cdrs.transport.write(query_frame.as_slice()));
        return parse_frame(&mut self.cdrs.transport, &self.compressor);
    }

    /// It consumes CDRS
    pub fn listen_for<'a>(mut self,
                          events: Vec<SimpleServerEvent>)
                          -> error::Result<(Listener<X>, EventStream)> {
        let query_frame = Frame::new_req_register(events).into_cbytes();
        try!(self.cdrs.transport.write(query_frame.as_slice()));
        try!(parse_frame(&mut self.cdrs.transport, &self.compressor));
        Ok(new_listener(self.cdrs.transport))
    }
}
/**
Prepare and execute statement with prepared Statement
*/

pub trait Prepare_And_Executer {
    fn prepare_statement(&mut self,
                         query: String,
                         with_tracing: bool,
                         with_warnings: bool)
                         -> error::Result<Frame>;
    fn execute_statement(&mut self,
                         id: &CBytesShort,
                         query_parameters: QueryParams,
                         with_tracing: bool,
                         with_warnings: bool)
                         -> error::Result<Frame>;
}


impl <'a, T: Authenticator + 'a, X: CDRSTransport + 'a> Prepare_And_Executer for  Session<T, X> {
    fn prepare_statement(&mut self,
                         query: String,
                         with_tracing: bool,
                         with_warnings: bool)
                         -> error::Result<Frame> {
        let mut flags = vec![];
        if with_tracing {
            flags.push(Flag::Tracing);
        }
        if with_warnings {
            flags.push(Flag::Warning);
        }

        let options_frame = Frame::new_req_prepare(query, flags).into_cbytes();

        (self.cdrs.transport.write(options_frame.as_slice()))?;

        parse_frame(&mut self.cdrs.transport, &self.compressor)
    }

    fn execute_statement(&mut self,
                         id: &CBytesShort,
                         query_parameters: QueryParams,
                         with_tracing: bool,
                         with_warnings: bool)
                         -> error::Result<Frame> {
        let mut flags = vec![];
        if with_tracing {
            flags.push(Flag::Tracing);
        }
        if with_warnings {
            flags.push(Flag::Warning);
        }
        let options_frame = Frame::new_req_execute(id, query_parameters, flags).into_cbytes();

        (self.cdrs.transport.write(options_frame.as_slice()))?;
        return parse_frame(&mut self.cdrs.transport, &self.compressor);
    }
}

