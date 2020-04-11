use r2d2;
use std::cell;

#[cfg(feature = "ssl")]
mod config_ssl;
#[cfg(feature = "rust-tls")]
mod config_rustls;
mod config_tcp;
mod generic_connection_pool;
mod pager;
pub mod session;
#[cfg(feature = "ssl")]
mod ssl_connection_pool;
#[cfg(feature = "rust-tls")]
mod rustls_connection_pool;
mod tcp_connection_pool;

#[cfg(feature = "ssl")]
pub use crate::cluster::config_ssl::{ClusterSslConfig, NodeSslConfig, NodeSslConfigBuilder};
#[cfg(feature = "rust-tls")]
pub use crate::cluster::config_rustls::{ClusterRustlsConfig, NodeRustlsConfig, NodeRustlsConfigBuilder};
pub use crate::cluster::config_tcp::{ClusterTcpConfig, NodeTcpConfig, NodeTcpConfigBuilder};
pub use crate::cluster::pager::{PagerState, QueryPager, SessionPager};
#[cfg(feature = "ssl")]
pub use crate::cluster::ssl_connection_pool::{
    new_ssl_pool, SslConnectionPool, SslConnectionsManager,
};
#[cfg(feature = "rust-tls")]
pub use crate::cluster::rustls_connection_pool::{
    new_rustls_pool, RustlsConnectionPool, RustlsConnectionsManager,
};
pub use crate::cluster::tcp_connection_pool::{
    new_tcp_pool, startup, TcpConnectionPool, TcpConnectionsManager,
};
pub(crate) use generic_connection_pool::ConnectionPool;

use crate::compression::Compression;
use crate::error;
use crate::query::{BatchExecutor, ExecExecutor, PrepareExecutor, QueryExecutor};
use crate::transport::CDRSTransport;

/// `GetConnection` trait provides a unified interface for Session to get a connection
/// from a load balancer
pub trait GetConnection<
    T: CDRSTransport + Send + Sync + 'static,
    M: r2d2::ManageConnection<Connection = cell::RefCell<T>, Error = error::Error>,
>
{
    /// Returns connection from a load balancer.
    fn get_connection(&self) -> Option<r2d2::PooledConnection<M>>;
}

/// `GetCompressor` trait provides a unified interface for Session to get a compressor
/// for further decompressing received data.
pub trait GetCompressor<'a> {
    /// Returns actual compressor.
    fn get_compressor(&self) -> Compression;
}

/// `CDRSSession` trait wrap ups whole query functionality. Use it only if whole query
/// machinery is needed and direct sub traits otherwise.
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
