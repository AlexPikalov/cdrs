use error;
use transport::TransportTcp;
use load_balancing::LoadBalancingStrategy;
use cluster::GetTransport;


use std::io::Write;

use compression::Compression;
use frame::{Flag, Frame, IntoBytes};
use frame::parser::parse_frame;
use query::{Query, QueryParams, QueryParamsBuilder, QueryValues};


pub struct Session<LB> {
  nodes: Vec<TransportTcp>,
  load_balancing: LB,
  compression: Compression,
}

impl<'a, LB: LoadBalancingStrategy<'a, TransportTcp> + Sized> Session<LB> {
  pub fn new(addrs: &Vec<&str>, load_balancing: LB) -> error::Result<Session<LB>> {
    let mut nodes: Vec<TransportTcp> = Vec::with_capacity(addrs.len());

    for addr in addrs {
      nodes.push(TransportTcp::new(&addr)?);
    }

    Ok(Session {
         nodes,
         load_balancing: load_balancing,
         compression: Compression::None,
       })
  }

  /// Executes a query with default parameters:
  /// * TDB
  pub fn query<Q: ToString>(&'a mut self, query: Q) -> error::Result<Frame> {
    self.query_tw(query, false, false)
  }

  /// Executes a query with ability to trace it and see warnings, and default parameters:
  /// * TBD
  pub fn query_tw<Q: ToString>(&'a mut self,
                               query: Q,
                               with_tracing: bool,
                               with_warnings: bool)
                               -> error::Result<Frame> {
    let query_params = QueryParamsBuilder::new().finalize();
    self.query_with_params_tw(query, query_params, with_tracing, with_warnings)
  }

  /// Executes a query with bounded values (either with or without names).
  pub fn query_with_values<Q: ToString>(&'a mut self,
                                        query: Q,
                                        values: QueryValues)
                                        -> error::Result<Frame> {
    self.query_with_values_tw(query, values, false, false)
  }

  /// Executes a query with bounded values (either with or without names)
  /// and ability to see warnings, trace a request and default parameters.
  pub fn query_with_values_tw<Q: ToString>(&'a mut self,
                                           query: Q,
                                           values: QueryValues,
                                           with_tracing: bool,
                                           with_warnings: bool)
                                           -> error::Result<Frame> {
    let query_params_builder = QueryParamsBuilder::new();
    let query_params = query_params_builder.values(values).finalize();
    self.query_with_params_tw(query, query_params, with_tracing, with_warnings)
  }

  /// Executes a query with query params.
  pub fn query_with_params<Q: ToString>(&'a mut self,
                                        query: Q,
                                        query_params: QueryParams)
                                        -> error::Result<Frame> {
    self.query_with_params_tw(query, query_params, false, false)
  }

  /// Executes a query with query params and ability to trace a request and see warnings.
  pub fn query_with_params_tw<Q: ToString>(&'a mut self,
                                           query: Q,
                                           query_params: QueryParams,
                                           with_tracing: bool,
                                           with_warnings: bool)
                                           -> error::Result<Frame> {
    let query = Query {
      query: query.to_string(),
      params: query_params,
    };

    let mut flags = vec![];

    if with_tracing {
      flags.push(Flag::Tracing);
    }

    if with_warnings {
      flags.push(Flag::Warning);
    }

    let query_frame = Frame::new_query(query, flags).into_cbytes();
    let ref compression = self.compression.clone();
    let transport = self.get_transport();
    try!(transport.write(query_frame.as_slice()));
    parse_frame(transport, compression)
  }
}

impl<'a, LB: LoadBalancingStrategy<'a, TransportTcp> + Sized> GetTransport<'a, TransportTcp>
  for Session<LB> {
  fn get_transport(&'a mut self) -> &'a mut TransportTcp {
    self.load_balancing.next(&mut self.nodes).expect("")
  }
}
