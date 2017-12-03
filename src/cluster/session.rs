use error;
use transport::TransportTcp;
use load_balancing::LoadBalancingStrategy;
use cluster::GetTransport;


use std::io::Write;

use compression::Compression;
use frame::{Flag, Frame, IntoBytes};
use frame::parser::parse_frame;
use query::{ExecExecutor, PrepareExecutor, PreparedQuery, Query, QueryExecutor, QueryParams};


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
}

impl<'a, LB: LoadBalancingStrategy<'a, TransportTcp> + Sized> GetTransport<'a, TransportTcp>
  for Session<LB> {
  fn get_transport(&'a mut self) -> &'a mut TransportTcp {
    self.load_balancing.next(&mut self.nodes).expect("")
  }
}

impl<'a, LB: LoadBalancingStrategy<'a, TransportTcp> + Sized> QueryExecutor<'a> for Session<LB> {
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
    let ref compression = self.compression.clone();
    let transport = self.get_transport();
    try!(transport.write(query_frame.as_slice()));
    parse_frame(transport, compression)
  }
}

impl<'a, LB: LoadBalancingStrategy<'a, TransportTcp> + Sized> PrepareExecutor<'a> for Session<LB> {
  fn prepare_tw<Q: ToString>(&'a mut self,
                             query: Q,
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

    let options_frame = Frame::new_req_prepare(query.to_string(), flags).into_cbytes();
    let ref compression = self.compression.clone();
    let transport = self.get_transport();

    try!(transport.write(options_frame.as_slice()));
    parse_frame(transport, compression)
  }
}

impl<'a, LB: LoadBalancingStrategy<'a, TransportTcp> + Sized> ExecExecutor<'a> for Session<LB> {
  fn exec_with_params_tw(&'a mut self,
                         prepared: &PreparedQuery,
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

    let options_frame = Frame::new_req_execute(prepared, query_parameters, flags).into_cbytes();
    let ref compression = self.compression.clone();
    let transport = self.get_transport();

    (transport.write(options_frame.as_slice()))?;
    parse_frame(transport, compression)
  }
}
