use r2d2;
use std::cell::RefCell;

use crate::cluster::{GetCompressor, GetConnection};
use crate::error;
use crate::frame::traits::IntoBytes;
use crate::frame::Frame;
use crate::query::batch_query_builder::QueryBatch;
use crate::transport::CDRSTransport;

use super::utils::{prepare_flags, send_frame};

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
  ) -> error::Result<Frame>
  where
    Self: Sized,
  {
    let flags = prepare_flags(with_tracing, with_warnings);

    let query_frame = Frame::new_req_batch(batch, flags).into_cbytes();

    send_frame(self, query_frame)
  }

  fn batch_with_params(&self, batch: QueryBatch) -> error::Result<Frame>
  where
    Self: Sized,
  {
    self.batch_with_params_tw(batch, false, false)
  }
}
