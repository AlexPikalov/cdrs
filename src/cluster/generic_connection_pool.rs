use std::net::SocketAddr;
use std::sync::Arc;

use bb8;

/// Generic pool connection that is able to return an
/// `bb8::Pool` as well as an IP address of a node.
#[derive(Debug)]
pub struct ConnectionPool<M: bb8::ManageConnection> {
  pool: Arc<bb8::Pool<M>>,
  addr: SocketAddr,
}

impl<M: bb8::ManageConnection> ConnectionPool<M> {
  pub fn new(pool: bb8::Pool<M>, addr: SocketAddr) -> Self {
    ConnectionPool {
      pool: Arc::new(pool),
      addr,
    }
  }

  /// Returns reference to underlying `bb8::Pool`.
  pub fn get_pool(&self) -> Arc<bb8::Pool<M>> {
    self.pool.clone()
  }

  /// Return an IP address.
  pub fn get_addr(&self) -> SocketAddr {
    self.addr
  }
}
