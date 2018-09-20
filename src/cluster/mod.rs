use r2d2;
use std::cell;

#[cfg(feature = "ssl")]
mod config_ssl;
mod config_tcp;
mod pager;
pub mod session;
#[cfg(feature = "ssl")]
mod ssl_connection_pool;
mod tcp_connection_pool;

#[cfg(feature = "ssl")]
pub use cluster::config_ssl::{ClusterSslConfig, NodeSslConfig, NodeSslConfigBuilder};
pub use cluster::config_tcp::{ClusterTcpConfig, NodeTcpConfig, NodeTcpConfigBuilder};
pub use cluster::pager::{QueryPager, SessionPager};
#[cfg(feature = "ssl")]
pub use cluster::ssl_connection_pool::{new_ssl_pool, SslConnectionPool, SslConnectionsManager};
pub use cluster::tcp_connection_pool::{
  new_tcp_pool, startup, TcpConnectionPool, TcpConnectionsManager,
};

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
