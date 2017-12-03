use std::io::{Read, Write};

use error;
use cluster::{GetCompressor, GetTransport};
use compression::Compression;
use frame::{Flag, Frame, IntoBytes};
use frame::parser::parse_frame;
use query::{Query, QueryParams, QueryValues};
use transport::CDRSTransport;

pub trait SessionQuery<'a, T: CDRSTransport + 'a>
  : GetTransport<'a, T> + GetCompressor<'a> {
  /// Executes a query with default parameters:
  /// * TDB
  fn query<Q: ToString>(&mut self, query: Q);

  /// Executes a query with ability to trace it and see warnings, and default parameters:
  /// * TBD
  fn query_tw<Q: ToString>(&mut self, query: Q, with_tracing: bool, with_warnings: bool);

  /// Executes a query with bounded values (either with or without names).
  fn query_with_values<Q: ToString>(&mut self, query: Q, values: QueryValues);

  /// Executes a query with bounded values (either with or without names)
  /// and ability to see warnings, trace a request and default parameters.
  fn query_with_values_tw<Q: ToString>(&mut self,
                                       query: Q,
                                       values: QueryValues,
                                       with_tracing: bool,
                                       with_warnings: bool);

  /// Executes a query with query params.
  fn query_with_params<Q: ToString>(&mut self, query: Q, query_params: QueryParams);

  /// Executes a query with query params and ability to trace a request and see warnings.
  fn query_with_params_tw<Q: ToString>(&'a mut self,
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
    let compressor = self.get_compressor();
    let transport = self.get_transport();
    try!(transport.write(query_frame.as_slice()));
    parse_frame(transport, &compressor)
  }
}
