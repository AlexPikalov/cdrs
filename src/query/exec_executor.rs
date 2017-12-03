use error;
use frame::Frame;
use query::{QueryParams, QueryParamsBuilder, QueryValues};
use types::CBytesShort;

pub type PreparedQuery = CBytesShort;

pub trait ExecExecutor<'a> {
  fn exec_with_params_tw(&'a mut self,
                         prepared: &PreparedQuery,
                         query_parameters: QueryParams,
                         with_tracing: bool,
                         with_warnings: bool)
                         -> error::Result<Frame>;

  fn exec_with_params(&'a mut self,
                      prepared: &PreparedQuery,
                      query_parameters: QueryParams)
                      -> error::Result<Frame> {
    self.exec_with_params_tw(prepared, query_parameters, false, false)
  }

  fn exec_with_values_tw(&'a mut self,
                         prepared: &PreparedQuery,
                         values: QueryValues,
                         with_tracing: bool,
                         with_warnings: bool)
                         -> error::Result<Frame> {
    let query_params_builder = QueryParamsBuilder::new();
    let query_params = query_params_builder.values(values).finalize();
    self.exec_with_params_tw(prepared, query_params, with_tracing, with_warnings)
  }

  fn exec_with_values(&'a mut self,
                      prepared: &PreparedQuery,
                      values: QueryValues)
                      -> error::Result<Frame> {
    self.exec_with_values_tw(prepared, values, false, false)
  }

  fn exec_tw(&'a mut self,
             prepared: &PreparedQuery,
             with_tracing: bool,
             with_warnings: bool)
             -> error::Result<Frame> {
    let query_params = QueryParamsBuilder::new().finalize();
    self.exec_with_params_tw(prepared, query_params, with_tracing, with_warnings)
  }

  fn exec(&'a mut self,
          prepared: &PreparedQuery)
          -> error::Result<Frame> {
    self.exec_tw(prepared, false, false)
  }
}
