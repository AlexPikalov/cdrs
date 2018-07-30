use std::cell::RefCell;

use authenticators::Authenticator;
use cluster::session::Session;
use error;
use load_balancing::LoadBalancingStrategy;
use transport::TransportTcp;

#[cfg(feature = "ssl")]
use openssl::ssl::SslConnector;
#[cfg(feature = "ssl")]
use transport::TransportTls;

pub struct Cluster<A> {
  nodes_addrs: Vec<&'static str>,
  authenticator: A,
}

impl<'a, A: Authenticator + Sized> Cluster<A> {
  pub fn new(nodes_addrs: Vec<&'static str>, authenticator: A) -> Cluster<A> {
    Cluster {
      nodes_addrs,
      authenticator,
    }
  }

  pub fn connect<LB>(&self, lb: LB) -> error::Result<Session<LB, A>>
  where
    LB: LoadBalancingStrategy<RefCell<TransportTcp>> + Sized,
  {
    Session::new(&self.nodes_addrs, lb, self.authenticator.clone())
  }

  pub fn connect_snappy<LB>(&self, lb: LB) -> error::Result<Session<LB, A>>
  where
    LB: LoadBalancingStrategy<RefCell<TransportTcp>> + Sized,
    A: Authenticator + 'a + Sized,
  {
    Session::new_snappy(&self.nodes_addrs, lb, self.authenticator.clone())
  }

  pub fn connect_lz4<LB>(&self, lb: LB) -> error::Result<Session<LB, A>>
  where
    LB: LoadBalancingStrategy<RefCell<TransportTcp>> + Sized,
  {
    Session::new_lz4(&self.nodes_addrs, lb, self.authenticator.clone())
  }

  #[cfg(feature = "ssl")]
  pub fn connect_ssl<LB>(
    &self,
    lb: LB,
    authenticator: A,
    ssl_connector: &SslConnector,
  ) -> error::Result<Session<LB, A>>
  where
    LB: LoadBalancingStrategy<RefCell<TransportTls>> + Sized,
  {
    Session::new_ssl(&self.nodes_addrs, lb, authenticator, ssl_connector)
  }

  #[cfg(feature = "ssl")]
  pub fn connect_snappy_ssl<LB>(
    &self,
    lb: LB,
    authenticator: A,
    ssl_connector: &SslConnector,
  ) -> error::Result<Session<LB, A>>
  where
    LB: LoadBalancingStrategy<RefCell<TransportTls>> + Sized,
  {
    Session::new_snappy_ssl(&self.nodes_addrs, lb, authenticator, ssl_connector)
  }

  #[cfg(feature = "ssl")]
  pub fn connect_lz4_ssl<LB>(
    &self,
    lb: LB,
    authenticator: A,
    ssl_connector: &SslConnector,
  ) -> error::Result<Session<LB, A>>
  where
    LB: LoadBalancingStrategy<RefCell<TransportTls>> + Sized,
  {
    Session::new_lz4_ssl(&self.nodes_addrs, lb, authenticator, ssl_connector)
  }
}
