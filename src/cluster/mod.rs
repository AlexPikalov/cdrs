mod cluster;
mod pager;
mod session;

pub use cluster::cluster::Cluster;
pub use cluster::session::Session;
pub use cluster::pager::{QueryPager, SessionPager};

use transport::CDRSTransport;
use compression::Compression;
use query::{ExecExecutor, PrepareExecutor, QueryExecutor};

pub trait GetTransport<'a, T: CDRSTransport + 'a> {
  fn get_transport(&mut self) -> Option<&mut T>;
}

pub trait GetCompressor<'a> {
  fn get_compressor(&self) -> Compression;
}

pub trait CDRSSession<'a, T: CDRSTransport + 'a>
  : GetCompressor<'a>
  + GetTransport<'a, T>
  + QueryExecutor<'a, T>
  + PrepareExecutor<'a, T>
  + ExecExecutor<'a, T> {
}
