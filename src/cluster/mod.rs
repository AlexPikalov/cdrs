mod cluster;
// mod session_query;
mod session;

pub use cluster::cluster::Cluster;
pub use cluster::session::Session;

use transport::CDRSTransport;
use compression::Compression;

pub trait GetTransport<'a, T: CDRSTransport + 'a> {
  fn get_transport(&'a mut self) -> &'a mut T;
}

pub trait GetCompressor<'a> {
  fn get_compressor(&'a self) -> Compression;
}
