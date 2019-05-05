use r2d2;
use std::cell::RefCell;
use std::io::Write;

#[cfg(feature = "ssl")]
use cluster::{new_ssl_pool, ClusterSslConfig, SslConnectionPool};
use cluster::{
  new_tcp_pool, startup, CDRSSession, ClusterTcpConfig, GetCompressor, GetConnection,
  TcpConnectionPool,
};
use error;
use load_balancing::LoadBalancingStrategy;
use transport::{CDRSTransport, TransportTcp};

use authenticators::Authenticator;
use cluster::SessionPager;
use compression::Compression;
use events::{new_listener, EventStream, Listener};
use frame::events::SimpleServerEvent;
use frame::parser::parse_frame;
use frame::{Frame, IntoBytes};
use query::{BatchExecutor, ExecExecutor, PrepareExecutor, QueryExecutor};

#[cfg(feature = "ssl")]
use openssl::ssl::SslConnector;
#[cfg(feature = "ssl")]
use transport::TransportTls;

/// CDRS session that holds one pool of authorized connecitons per node.
/// `compression` field contains data compressor that will be used
/// for decompressing data received from Cassandra server.
pub struct Session<LB> {
  load_balancing: LB,
  #[allow(dead_code)]
  pub compression: Compression,
}

impl<'a, LB> GetCompressor<'a> for Session<LB> {
  /// Returns compression that current session has.
  fn get_compressor(&self) -> Compression {
    self.compression.clone()
  }
}

impl<'a, LB: Sized> Session<LB> {
  /// Basing on current session returns new `SessionPager` that can be used
  /// for performing paged queries.
  pub fn paged<
    T: CDRSTransport + 'static,
    M: r2d2::ManageConnection<Connection = RefCell<T>, Error = error::Error>,
  >(
    &'a self,
    page_size: i32,
  ) -> SessionPager<'a, M, Session<LB>, T>
  where
    Session<LB>: CDRSSession<'static, T, M>,
  {
    return SessionPager::new(self, page_size);
  }
}

impl<
    T: CDRSTransport + Send + Sync + 'static,
    M: r2d2::ManageConnection<Connection = RefCell<T>, Error = error::Error> + Sized,
    LB: LoadBalancingStrategy<r2d2::Pool<M>>,
  > GetConnection<T, M> for Session<LB>
{
  fn get_connection(&self) -> Option<r2d2::PooledConnection<M>> {
    self.load_balancing.next().and_then(|pool| pool.get().ok())
  }
}

impl<
    'a,
    T: CDRSTransport + 'static,
    M: r2d2::ManageConnection<Connection = RefCell<T>, Error = error::Error> + Sized,
    LB: LoadBalancingStrategy<r2d2::Pool<M>> + Sized,
  > QueryExecutor<T, M> for Session<LB>
{
}

impl<
    'a,
    T: CDRSTransport + 'static,
    LB: LoadBalancingStrategy<r2d2::Pool<M>> + Sized,
    M: r2d2::ManageConnection<Connection = RefCell<T>, Error = error::Error> + Sized,
  > PrepareExecutor<T, M> for Session<LB>
{
}

impl<
    'a,
    T: CDRSTransport + 'static,
    LB: LoadBalancingStrategy<r2d2::Pool<M>> + Sized,
    M: r2d2::ManageConnection<Connection = RefCell<T>, Error = error::Error> + Sized,
  > ExecExecutor<T, M> for Session<LB>
{
}

impl<
    'a,
    T: CDRSTransport + 'static,
    LB: LoadBalancingStrategy<r2d2::Pool<M>> + Sized,
    M: r2d2::ManageConnection<Connection = RefCell<T>, Error = error::Error> + Sized,
  > BatchExecutor<T, M> for Session<LB>
{
}

impl<
    'a,
    T: CDRSTransport + 'static,
    M: r2d2::ManageConnection<Connection = RefCell<T>, Error = error::Error> + Sized,
    LB: LoadBalancingStrategy<r2d2::Pool<M>> + Sized,
  > CDRSSession<'a, T, M> for Session<LB>
{
}

/// Creates new session that will perform queries without any compression. `Compression` type
/// can be changed at any time.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
pub fn new<'a, A, LB>(
  node_configs: &ClusterTcpConfig<'a, A>,
  mut load_balancing: LB,
) -> error::Result<Session<LB>>
where
  A: Authenticator + 'static + Sized,
  LB: LoadBalancingStrategy<TcpConnectionPool<A>> + Sized,
{
  let mut nodes: Vec<TcpConnectionPool<A>> = Vec::with_capacity(node_configs.0.len());

  for node_config in &node_configs.0 {
    let node_connection_pool = new_tcp_pool(node_config.clone())?;
    nodes.push(node_connection_pool);
  }

  load_balancing.init(nodes);

  Ok(Session {
    load_balancing,
    compression: Compression::None,
  })
}

/// Creates new session that will perform queries with Snappy compression. `Compression` type
/// can be changed at any time.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
pub fn new_snappy<'a, A, LB>(
  node_configs: &ClusterTcpConfig<'a, A>,
  mut load_balancing: LB,
) -> error::Result<Session<LB>>
where
  A: Authenticator + 'static + Sized,
  LB: LoadBalancingStrategy<TcpConnectionPool<A>> + Sized,
{
  let mut nodes: Vec<TcpConnectionPool<A>> = Vec::with_capacity(node_configs.0.len());

  for node_config in &node_configs.0 {
    let node_connection_pool = new_tcp_pool(node_config.clone())?;
    nodes.push(node_connection_pool);
  }

  load_balancing.init(nodes);

  Ok(Session {
    load_balancing,
    compression: Compression::Snappy,
  })
}

/// Creates new session that will perform queries with LZ4 compression. `Compression` type
/// can be changed at any time.
/// As a parameter it takes:
/// * cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
pub fn new_lz4<'a, A, LB>(
  node_configs: &ClusterTcpConfig<'a, A>,
  mut load_balancing: LB,
) -> error::Result<Session<LB>>
where
  A: Authenticator + 'static + Sized,
  LB: LoadBalancingStrategy<TcpConnectionPool<A>> + Sized,
{
  let mut nodes: Vec<TcpConnectionPool<A>> = Vec::with_capacity(node_configs.0.len());

  for node_config in &node_configs.0 {
    let node_connection_pool = new_tcp_pool(node_config.clone())?;
    nodes.push(node_connection_pool);
  }

  load_balancing.init(nodes);

  Ok(Session {
    load_balancing,
    compression: Compression::Lz4,
  })
}

impl<'a, L> Session<L> {
  /// Returns new event listener.
  pub fn listen<A: Authenticator + 'static + Sized>(
    &self,
    node: &str,
    authenticator: A,
    events: Vec<SimpleServerEvent>,
  ) -> error::Result<(Listener<RefCell<TransportTcp>>, EventStream)> {
    let compression = self.get_compressor();
    let transport = TransportTcp::new(&node).map(RefCell::new)?;

    startup(&transport, &authenticator)?;

    let query_frame = Frame::new_req_register(events).into_cbytes();
    transport.borrow_mut().write(query_frame.as_slice())?;
    parse_frame(&transport, &compression)?;

    Ok(new_listener(transport))
  }
}

/// Creates new SSL-based session that will perform queries without any compression. `Compression` type
/// can be changed at any time.
/// As a parameter it takes:
/// * SSL cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
#[cfg(feature = "ssl")]
pub fn new_ssl<'a, A, LB>(
  node_configs: &ClusterSslConfig<'a, A>,
  mut load_balancing: LB,
) -> error::Result<Session<LB>>
where
  A: Authenticator + 'static + Sized,
  LB: LoadBalancingStrategy<SslConnectionPool<A>> + Sized,
{
  let mut nodes: Vec<SslConnectionPool<A>> = Vec::with_capacity(node_configs.0.len());

  for node_config in &node_configs.0 {
    let node_connection_pool = new_ssl_pool(node_config.clone())?;
    nodes.push(node_connection_pool);
  }

  load_balancing.init(nodes);

  Ok(Session {
    load_balancing,
    compression: Compression::None,
  })
}

/// Creates new SSL-based session that will perform queries with Snappy compression. `Compression` type
/// can be changed at any time.
/// As a parameter it takes:
/// * SSL cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
#[cfg(feature = "ssl")]
pub fn new_snappy_ssl<'a, A, LB>(
  node_configs: &ClusterSslConfig<'a, A>,
  mut load_balancing: LB,
) -> error::Result<Session<LB>>
where
  A: Authenticator + 'static + Sized,
  LB: LoadBalancingStrategy<SslConnectionPool<A>> + Sized,
{
  let mut nodes: Vec<SslConnectionPool<A>> = Vec::with_capacity(node_configs.0.len());

  for node_config in &node_configs.0 {
    let node_connection_pool = new_ssl_pool(node_config.clone())?;
    nodes.push(node_connection_pool);
  }

  load_balancing.init(nodes);

  Ok(Session {
    load_balancing,
    compression: Compression::Snappy,
  })
}

/// Creates new SSL-based session that will perform queries with LZ4 compression. `Compression` type
/// can be changed at any time.
/// As a parameter it takes:
/// * SSL cluster config
/// * load balancing strategy (cannot be changed during `Session` life time).
#[cfg(feature = "ssl")]
pub fn new_lz4_ssl<'a, A, LB>(
  node_configs: &ClusterSslConfig<'a, A>,
  mut load_balancing: LB,
) -> error::Result<Session<LB>>
where
  A: Authenticator + 'static + Sized,
  LB: LoadBalancingStrategy<SslConnectionPool<A>> + Sized,
{
  let mut nodes: Vec<SslConnectionPool<A>> = Vec::with_capacity(node_configs.0.len());

  for node_config in &node_configs.0 {
    let node_connection_pool = new_ssl_pool(node_config.clone())?;
    nodes.push(node_connection_pool);
  }

  load_balancing.init(nodes);

  Ok(Session {
    load_balancing,
    compression: Compression::Lz4,
  })
}

/// Returns new SSL-based event listener.
#[cfg(feature = "ssl")]
impl<'a, L> Session<L> {
  pub fn listen_ssl<A: Authenticator + 'static + Sized>(
    &self,
    node: (&str, &SslConnector),
    authenticator: A,
    events: Vec<SimpleServerEvent>,
  ) -> error::Result<(Listener<RefCell<TransportTls>>, EventStream)> {
    let (addr_ref, ssl_connector_ref) = node;
    let compression = self.get_compressor();
    let transport = TransportTls::new(addr_ref, ssl_connector_ref).map(RefCell::new)?;

    startup(&transport, &authenticator)?;

    let query_frame = Frame::new_req_register(events).into_cbytes();
    transport.borrow_mut().write(query_frame.as_slice())?;
    parse_frame(&transport, &compression)?;

    Ok(new_listener(transport))
  }
}
