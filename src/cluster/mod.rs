use r2d2;
use std::cell;

mod config;
mod connection_pool;
mod pager;
pub mod session;

pub use cluster::config::{ClusterConfig, NodeConfig, NodeConfigBuilder};
pub use cluster::connection_pool::{
  new_tcp_pool, startup, TcpConnectionPool, TcpConnectionsManager,
};
pub use cluster::pager::{QueryPager, SessionPager};

use compression::Compression;
use error;
use query::{BatchExecutor, ExecExecutor, PrepareExecutor, QueryExecutor};
use transport::CDRSTransport;

pub trait GetConnection<
  T: CDRSTransport + Send + Sync + 'static,
  M: r2d2::ManageConnection<Connection = cell::RefCell<T>, Error = error::Error>,
>
{
  /// It selects a node from a cluster
  /// and return pooled connection pool.
  fn get_connection(&self) -> Option<r2d2::PooledConnection<M>>;
}

pub trait GetCompressor<'a> {
  fn get_compressor(&self) -> Compression;
}

pub trait CDRSSession<
  'a,
  T: CDRSTransport + 'static,
  M: r2d2::ManageConnection<Connection = cell::RefCell<T>, Error = error::Error>,
>:
  GetCompressor<'static>
  + GetConnection<T, M>
  + QueryExecutor<T, M>
  + PrepareExecutor<T, M>
  + ExecExecutor<T, M>
  + BatchExecutor<T, M>
{
}
