use error;
use frame::Frame;
use query::{QueryParams, QueryParamsBuilder, QueryValues};

pub trait QueryExecutor<'a> {
  fn query_with_params_tw<Q: ToString>(&'a mut self,
                                       query: Q,
                                       query_params: QueryParams,
                                       with_tracing: bool,
                                       with_warnings: bool)
                                       -> error::Result<Frame>;

  /// Executes a query with default parameters:
  /// * TDB
  fn query<Q: ToString>(&'a mut self, query: Q) -> error::Result<Frame> {
    self.query_tw(query, false, false)
  }

  /// Executes a query with ability to trace it and see warnings, and default parameters:
  /// * TBD
  fn query_tw<Q: ToString>(&'a mut self,
                           query: Q,
                           with_tracing: bool,
                           with_warnings: bool)
                           -> error::Result<Frame> {
    let query_params = QueryParamsBuilder::new().finalize();
    self.query_with_params_tw(query, query_params, with_tracing, with_warnings)
  }

  /// Executes a query with bounded values (either with or without names).
  fn query_with_values<Q: ToString>(&'a mut self,
                                    query: Q,
                                    values: QueryValues)
                                    -> error::Result<Frame> {
    self.query_with_values_tw(query, values, false, false)
  }

  /// Executes a query with bounded values (either with or without names)
  /// and ability to see warnings, trace a request and default parameters.
  fn query_with_values_tw<Q: ToString>(&'a mut self,
                                       query: Q,
                                       values: QueryValues,
                                       with_tracing: bool,
                                       with_warnings: bool)
                                       -> error::Result<Frame> {
    let query_params_builder = QueryParamsBuilder::new();
    let query_params = query_params_builder.values(values).finalize();
    self.query_with_params_tw(query, query_params, with_tracing, with_warnings)
  }

  /// Executes a query with query params without warnings and tracing.
  fn query_with_params<Q: ToString>(&'a mut self,
                                    query: Q,
                                    query_params: QueryParams)
                                    -> error::Result<Frame> {
    self.query_with_params_tw(query, query_params, false, false)
  }
}
