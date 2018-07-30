use std::cell;

mod cluster;
mod pager;
mod session;

pub use cluster::cluster::Cluster;
pub use cluster::pager::{QueryPager, SessionPager};
pub use cluster::session::Session;

use compression::Compression;
use query::{BatchExecutor, ExecExecutor, PrepareExecutor, QueryExecutor};
use transport::CDRSTransport;

pub trait GetTransport<'a, T: CDRSTransport + 'a> {
  fn get_transport(&self) -> Option<&cell::RefCell<T>>;
}

pub trait GetCompressor<'a> {
  fn get_compressor(&self) -> Compression;
}

pub trait CDRSSession<'a, T: CDRSTransport + 'static>:
  GetCompressor<'static>
  + GetTransport<'static, T>
  + QueryExecutor<T>
  + PrepareExecutor<T>
  + ExecExecutor<T>
  + BatchExecutor<T>
{
}
