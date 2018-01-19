use error;
use transport::CDRSTransport;
use cluster::{GetCompressor, GetTransport};
use frame::{Flag, Frame};
use frame::parser::parse_frame;
use frame::traits::IntoBytes;
use query::batch_query_builder::QueryBatch;

pub trait BatchExecutor<'a, T: CDRSTransport + 'a>
  : GetTransport<'a, T> + GetCompressor<'a> {
  fn batch_with_params_tw(&mut self,
                          batch: QueryBatch,
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

    let query_frame = Frame::new_req_batch(batch, flags).into_cbytes();
    let ref compression = self.get_compressor();
    let transport = self.get_transport().ok_or("Unable to get transport")?;

    try!(transport.write(query_frame.as_slice()));
    parse_frame(transport, compression)
  }

  fn batch_with_params(&mut self, batch: QueryBatch) -> error::Result<Frame> {
    self.batch_with_params_tw(batch, false, false)
  }
}
