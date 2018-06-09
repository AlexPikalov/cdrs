use error;
use frame::{Flag, Frame, IntoBytes};
use types::CBytesShort;
use cluster::{GetCompressor, GetTransport};
use transport::CDRSTransport;
use frame::parser::parse_frame;

pub type PreparedQuery = CBytesShort;

pub trait PrepareExecutor<'a, T: CDRSTransport + 'a>
  : GetTransport<'a, T> + GetCompressor<'a> {
  /// It prepares a query for execution, along with query itself
  /// the method takes `with_tracing` and `with_warnings` flags
  /// to get tracing information and warnings.
  fn prepare_tw<Q: ToString>(&'a mut self,
                             query: Q,
                             with_tracing: bool,
                             with_warnings: bool)
                             -> error::Result<PreparedQuery> {
    let mut flags = vec![];
    if with_tracing {
      flags.push(Flag::Tracing);
    }
    if with_warnings {
      flags.push(Flag::Warning);
    }

    let options_frame = Frame::new_req_prepare(query.to_string(), flags).into_cbytes();
    let ref compression = self.get_compressor();
    let transport = self.get_transport().ok_or("Unable to get transport")?;

    try!(transport.write(options_frame.as_slice()));
    parse_frame(transport, compression).and_then(|response| response.get_body())
                                       .and_then(|body| Ok(body.into_prepared().expect("").id))
  }

  /// It prepares query without additional tracing information and warnings.
  fn prepare<Q: ToString>(&'a mut self, query: Q) -> error::Result<PreparedQuery> {
    self.prepare_tw(query, false, false)
  }
}
