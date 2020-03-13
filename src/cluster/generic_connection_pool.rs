use std::net::SocketAddr;
use std::sync::Arc;

use r2d2;

/// Generic pool connection that is able to return an
/// `r2r2::Pool` as well as an IP address of a node.
pub struct ConnectionPool<M: r2d2::ManageConnection> {
  pool: Arc<r2d2::Pool<M>>,
  addr: SocketAddr,
}

impl<M: r2d2::ManageConnection> ConnectionPool<M> {
  pub fn new(pool: r2d2::Pool<M>, addr: SocketAddr) -> Self {
    ConnectionPool {
      pool: Arc::new(pool),
      addr,
    }
  }

  /// Returns reference to underlying `r2d2::Pool`.
  pub fn get_pool(&self) -> Arc<r2d2::Pool<M>> {
    self.pool.clone()
  }

  /// Return an IP address.
  pub fn get_addr(&self) -> SocketAddr {
    self.addr
  }
}
