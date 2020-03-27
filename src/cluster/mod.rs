use bb8;
use async_trait::async_trait;
use tokio::sync::Mutex;
use std::sync::Arc;

#[cfg(feature = "ssl")]
mod config_ssl;
mod config_tcp;
mod generic_connection_pool;
mod pager;
pub mod session;
#[cfg(feature = "ssl")]
mod ssl_connection_pool;
mod tcp_connection_pool;

#[cfg(feature = "ssl")]
pub use crate::cluster::config_ssl::{ClusterSslConfig, NodeSslConfig, NodeSslConfigBuilder};
pub use crate::cluster::config_tcp::{ClusterTcpConfig, NodeTcpConfig, NodeTcpConfigBuilder};
pub use crate::cluster::pager::{PagerState, QueryPager, SessionPager};
#[cfg(feature = "ssl")]
pub use crate::cluster::ssl_connection_pool::{
    new_ssl_pool, SslConnectionPool, SslConnectionsManager,
};
pub use crate::cluster::tcp_connection_pool::{
    new_tcp_pool, startup, TcpConnectionPool, TcpConnectionsManager,
};
pub(crate) use generic_connection_pool::ConnectionPool;

use crate::compression::Compression;
use crate::error;
use crate::query::{BatchExecutor, ExecExecutor, PrepareExecutor, QueryExecutor};
use crate::transport::CDRSTransport;
use crate::frame::{Frame, StreamId};

/// `GetConnection` trait provides a unified interface for Session to get a connection
/// from a load balancer
#[async_trait]
pub trait GetConnection<
    T: CDRSTransport + Send + Sync + 'static,
    M: bb8::ManageConnection<Connection = Mutex<T>, Error = error::Error>,
>
{
    /// Returns connection from a load balancer.
    async fn get_connection(&self) -> Option<Arc<ConnectionPool<M>>>;
}

/// `GetCompressor` trait provides a unified interface for Session to get a compressor
/// for further decompressing received data.
pub trait GetCompressor<'a> {
    /// Returns actual compressor.
    fn get_compressor(&self) -> Compression;
}

/// `ResponseCache` caches responses to match them by their stream id to requests.
#[async_trait]
pub trait ResponseCache {
    async fn match_or_cache_response(&self, stream_id: StreamId, frame: Frame) -> Option<Frame>;
}

/// `CDRSSession` trait wrap ups whole query functionality. Use it only if whole query
/// machinery is needed and direct sub traits otherwise.
pub trait CDRSSession<
    'a,
    T: CDRSTransport + Unpin + 'static,
    M: bb8::ManageConnection<Connection = Mutex<T>, Error = error::Error>,
>:
    GetCompressor<'static>
    + GetConnection<T, M>
    + QueryExecutor<T, M>
    + PrepareExecutor<T, M>
    + ExecExecutor<T, M>
    + BatchExecutor<T, M>
{
}
