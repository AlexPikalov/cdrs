use cluster::{GetCompressor, GetTransport};
use error;
use frame::parser::parse_frame;
use frame::{Flag, Frame, IntoBytes};
use query::{QueryParams, QueryParamsBuilder, QueryValues};
use transport::CDRSTransport;
use types::CBytesShort;

pub type PreparedQuery = CBytesShort;

pub trait ExecExecutor<T: CDRSTransport + 'static>:
  GetTransport<'static, T> + GetCompressor<'static>
{
  fn exec_with_params_tw(
    &self,
    prepared: &PreparedQuery,
    query_parameters: QueryParams,
    with_tracing: bool,
    with_warnings: bool,
  ) -> error::Result<Frame> {
    let mut flags = vec![];
    if with_tracing {
      flags.push(Flag::Tracing);
    }
    if with_warnings {
      flags.push(Flag::Warning);
    }

    let options_frame = Frame::new_req_execute(prepared, query_parameters, flags).into_cbytes();
    let ref compression = self.get_compressor();

    self
      .get_transport()
      .ok_or(error::Error::from("Unable to get transport"))
      .and_then(|transport_cell| {
        let write_res = transport_cell
          .borrow_mut()
          .write(options_frame.as_slice())
          .map_err(error::Error::from);
        write_res.map(|_| transport_cell)
      })
      .and_then(|transport_cell| parse_frame(transport_cell, compression))
  }

  fn exec_with_params(
    &self,
    prepared: &PreparedQuery,
    query_parameters: QueryParams,
  ) -> error::Result<Frame> {
    self.exec_with_params_tw(prepared, query_parameters, false, false)
  }

  fn exec_with_values_tw<V: Into<QueryValues>>(
    &self,
    prepared: &PreparedQuery,
    values: V,
    with_tracing: bool,
    with_warnings: bool,
  ) -> error::Result<Frame> {
    let query_params_builder = QueryParamsBuilder::new();
    let query_params = query_params_builder.values(values.into()).finalize();
    self.exec_with_params_tw(prepared, query_params, with_tracing, with_warnings)
  }

  fn exec_with_values<V: Into<QueryValues>>(
    &self,
    prepared: &PreparedQuery,
    values: V,
  ) -> error::Result<Frame> {
    self.exec_with_values_tw(prepared, values, false, false)
  }

  fn exec_tw(
    &self,
    prepared: &PreparedQuery,
    with_tracing: bool,
    with_warnings: bool,
  ) -> error::Result<Frame> {
    let query_params = QueryParamsBuilder::new().finalize();
    self.exec_with_params_tw(prepared, query_params, with_tracing, with_warnings)
  }

  fn exec(&mut self, prepared: &PreparedQuery) -> error::Result<Frame> {
    self.exec_tw(prepared, false, false)
  }
}
