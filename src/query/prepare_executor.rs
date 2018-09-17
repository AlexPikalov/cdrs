use r2d2;
use std::cell::RefCell;

use cluster::{GetCompressor, GetConnection};
use error;
use frame::parser::from_connection;
use frame::{Flag, Frame, IntoBytes};
use transport::CDRSTransport;
use types::CBytesShort;

pub type PreparedQuery = CBytesShort;

pub trait PrepareExecutor<
  T: CDRSTransport + 'static,
  M: r2d2::ManageConnection<Connection = RefCell<T>, Error = error::Error> + Sized,
>: GetConnection<T, M> + GetCompressor<'static>
{
  /// It prepares a query for execution, along with query itself
  /// the method takes `with_tracing` and `with_warnings` flags
  /// to get tracing information and warnings.
  fn prepare_tw<Q: ToString>(
    &self,
    query: Q,
    with_tracing: bool,
    with_warnings: bool,
  ) -> error::Result<PreparedQuery> {
    let mut flags = vec![];
    if with_tracing {
      flags.push(Flag::Tracing);
    }
    if with_warnings {
      flags.push(Flag::Warning);
    }

    let query_frame = Frame::new_req_prepare(query.to_string(), flags).into_cbytes();
    let ref compression = self.get_compressor();

    self
      .get_connection()
      .ok_or(error::Error::from("Unable to get transport"))
      .and_then(|transport_cell| {
        let write_res = transport_cell
          .borrow_mut()
          .write(query_frame.as_slice())
          .map_err(error::Error::from);
        write_res.map(|_| transport_cell)
      })
      .and_then(|transport_cell| from_connection(&transport_cell, compression))
      .and_then(|response| response.get_body())
      .and_then(|body| {
        Ok(
          body
            .into_prepared()
            .expect("CDRS BUG: cannot convert frame into prepared")
            .id,
        )
      })
  }

  /// It prepares query without additional tracing information and warnings.
  fn prepare<Q: ToString>(&self, query: Q) -> error::Result<PreparedQuery> {
    self.prepare_tw(query, false, false)
  }
}
