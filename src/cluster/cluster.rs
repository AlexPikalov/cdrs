use error;

use authenticators::Authenticator;
use transport::TransportTcp;
use cluster::session::Session;
use load_balancing::LoadBalancingStrategy;

pub struct Cluster {
  nodes_addrs: Vec<&'static str>,
}

impl<'a> Cluster {
  pub fn new(nodes_addrs: Vec<&'static str>) -> Cluster {
    Cluster { nodes_addrs }
  }

  pub fn connect<LB, A>(&self, lb: LB, authenticator: A) -> error::Result<Session<LB, A>>
    where LB: LoadBalancingStrategy<'a, TransportTcp> + Sized,
          A: Authenticator + 'a + Sized
  {
    Session::new(&self.nodes_addrs, lb, authenticator)
  }

  pub fn connect_snappy<LB, A>(&self, lb: LB, authenticator: A) -> error::Result<Session<LB, A>>
    where LB: LoadBalancingStrategy<'a, TransportTcp> + Sized,
          A: Authenticator + 'a + Sized
  {
    Session::new_snappy(&self.nodes_addrs, lb, authenticator)
  }

  pub fn connect_lz4<LB, A>(&self, lb: LB, authenticator: A) -> error::Result<Session<LB, A>>
    where LB: LoadBalancingStrategy<'a, TransportTcp> + Sized,
    A: Authenticator + 'a + Sized
  {
    Session::new_lz4(&self.nodes_addrs, lb, authenticator)
  }

  #[cfg(feature = "ssl")]
  pub fn connect_ssl() {
    unimplemented!()
  }
}
