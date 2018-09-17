use r2d2;
use std::cell::RefCell;

use cluster::{GetCompressor, GetConnection};
use error;
use frame::parser::from_connection;
use frame::traits::IntoBytes;
use frame::{Flag, Frame};
use query::batch_query_builder::QueryBatch;
use transport::CDRSTransport;

pub trait BatchExecutor<
  T: CDRSTransport + 'static,
  M: r2d2::ManageConnection<Connection = RefCell<T>, Error = error::Error> + Sized,
>: GetConnection<T, M> + GetCompressor<'static>
{
  fn batch_with_params_tw(
    &self,
    batch: QueryBatch,
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

    let query_frame = Frame::new_req_batch(batch, flags).into_cbytes();
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
  }

  fn batch_with_params(&self, batch: QueryBatch) -> error::Result<Frame> {
    self.batch_with_params_tw(batch, false, false)
  }
}
