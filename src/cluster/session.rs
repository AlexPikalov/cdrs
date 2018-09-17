use r2d2;
use std::cell::RefCell;
use std::io::Write;

use cluster::{
  new_tcp_pool, startup, CDRSSession, ClusterConfig, GetCompressor, GetConnection,
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

pub struct Session<LB> {
  load_balancing: LB,
  #[allow(dead_code)]
  pub compression: Compression,
}

impl<'a, LB> GetCompressor<'a> for Session<LB> {
  fn get_compressor(&self) -> Compression {
    self.compression.clone()
  }
}

impl<'a, LB: Sized> Session<LB> {
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
    self.load_balancing.next().and_then(|pool| pool.try_get())
  }
}

impl<
    'a,
    T: CDRSTransport + 'static,
    M: r2d2::ManageConnection<Connection = RefCell<T>, Error = error::Error> + Sized,
    LB: LoadBalancingStrategy<r2d2::Pool<M>> + Sized,
  > QueryExecutor<T, M> for Session<LB>
{}

impl<
    'a,
    T: CDRSTransport + 'static,
    LB: LoadBalancingStrategy<r2d2::Pool<M>> + Sized,
    M: r2d2::ManageConnection<Connection = RefCell<T>, Error = error::Error> + Sized,
  > PrepareExecutor<T, M> for Session<LB>
{}

impl<
    'a,
    T: CDRSTransport + 'static,
    LB: LoadBalancingStrategy<r2d2::Pool<M>> + Sized,
    M: r2d2::ManageConnection<Connection = RefCell<T>, Error = error::Error> + Sized,
  > ExecExecutor<T, M> for Session<LB>
{}

impl<
    'a,
    T: CDRSTransport + 'static,
    LB: LoadBalancingStrategy<r2d2::Pool<M>> + Sized,
    M: r2d2::ManageConnection<Connection = RefCell<T>, Error = error::Error> + Sized,
  > BatchExecutor<T, M> for Session<LB>
{}

impl<
    'a,
    T: CDRSTransport + 'static,
    M: r2d2::ManageConnection<Connection = RefCell<T>, Error = error::Error> + Sized,
    LB: LoadBalancingStrategy<r2d2::Pool<M>> + Sized,
  > CDRSSession<'a, T, M> for Session<LB>
{}

pub fn new<A, LB>(
  node_configs: &ClusterConfig<'static, A>,
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

pub fn new_snappy<A, LB>(
  node_configs: &ClusterConfig<'static, A>,
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

pub fn new_lz4<A, LB>(
  node_configs: &ClusterConfig<'static, A>,
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
  pub fn listen<A: Authenticator + 'static + Sized>(
    &self,
    node: &str,
    authenticator: A,
    events: Vec<SimpleServerEvent>,
  ) -> error::Result<(Listener<RefCell<TransportTcp>>, EventStream)> {
    let authenticator = authenticator;
    let compression = self.get_compressor();
    let transport = TransportTcp::new(&node).map(RefCell::new)?;

    startup(&transport, &authenticator)?;

    let query_frame = Frame::new_req_register(events).into_cbytes();
    transport.borrow_mut().write(query_frame.as_slice())?;
    parse_frame(&transport, &compression)?;

    Ok(new_listener(transport))
  }
}

// #[cfg(feature = "ssl")]
// impl<'a, LB: LoadBalancingStrategy<RefCell<TransportTls>> + Sized, A: Authenticator + 'a + Sized>
//   Session<LB, A>
// {
//   pub fn new_ssl(
//     addrs: &Vec<&str>,
//     mut load_balancing: LB,
//     authenticator: A,
//     ssl_connector: &SslConnector,
//   ) -> error::Result<Session<LB, A>> {
//     let mut nodes: Vec<RefCell<TransportTls>> = Vec::with_capacity(addrs.len());

//     for addr in addrs {
//       let transport = RefCell::new(TransportTls::new(&addr, ssl_connector)?);
//       Self::startup(&transport, &authenticator)?;
//       nodes.push(transport);
//     }

//     load_balancing.init(nodes);

//     Ok(Session {
//       load_balancing,
//       authenticator,
//       compression: Compression::None,
//     })
//   }

//   pub fn new_snappy_ssl(
//     addrs: &Vec<&str>,
//     mut load_balancing: LB,
//     authenticator: A,
//     ssl_connector: &SslConnector,
//   ) -> error::Result<Session<LB, A>> {
//     let mut nodes: Vec<RefCell<TransportTls>> = Vec::with_capacity(addrs.len());

//     for addr in addrs {
//       let transport = RefCell::new(TransportTls::new(&addr, ssl_connector)?);
//       Self::startup(&transport, &authenticator)?;
//       nodes.push(transport);
//     }

//     load_balancing.init(nodes);

//     Ok(Session {
//       load_balancing,
//       authenticator,
//       compression: Compression::Snappy,
//     })
//   }

//   pub fn new_lz4_ssl(
//     addrs: &Vec<&str>,
//     mut load_balancing: LB,
//     authenticator: A,
//     ssl_connector: &SslConnector,
//   ) -> error::Result<Session<LB, A>> {
//     let mut nodes: Vec<RefCell<TransportTls>> = Vec::with_capacity(addrs.len());

//     for addr in addrs {
//       let transport = RefCell::new(TransportTls::new(&addr, ssl_connector)?);
//       Self::startup(&transport, &authenticator)?;
//       nodes.push(transport);
//     }

//     load_balancing.init(nodes);

//     Ok(Session {
//       load_balancing,
//       authenticator,
//       compression: Compression::Lz4,
//     })
//   }

//   pub fn listen_ssl(
//     &self,
//     node: (&str, &SslConnector),
//     events: Vec<SimpleServerEvent>,
//   ) -> error::Result<(Listener<RefCell<TransportTls>>, EventStream)> {
//     let (addr, ssl_connector) = node;
//     let authenticator = self.authenticator.clone();
//     let compression = self.get_compressor();
//     let transport = self
//       .get_transport()
//       .ok_or("Cannot connect to a cluster - no nodes provided")?;
//     let transport_cell = RefCell::new(TransportTls::new(&addr, ssl_connector)?);
//     Self::startup(&transport, &authenticator)?;

//     let query_frame = Frame::new_req_register(events).into_cbytes();
//     transport_cell.borrow_mut().write(query_frame.as_slice())?;
//     parse_frame(&transport_cell, &compression)?;

//     Ok(new_listener(transport_cell))
//   }
// }
