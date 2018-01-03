mod cluster;
mod pager;
mod session;

pub use cluster::cluster::Cluster;
pub use cluster::session::Session;
pub use cluster::pager::{ExecPager, QueryPager, SessionPager};

use transport::CDRSTransport;
use compression::Compression;

pub trait GetTransport<'a, T: CDRSTransport + 'a> {
  fn get_transport(&'a mut self) -> &'a mut T;
}

pub trait GetCompressor<'a> {
  fn get_compressor(&'a self) -> Compression;
}
