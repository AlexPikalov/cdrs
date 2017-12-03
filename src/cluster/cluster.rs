use error;

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

  pub fn connect<LB>(&self, lb: LB) -> error::Result<Session<LB>>
    where LB: LoadBalancingStrategy<'a, TransportTcp> + Sized
  {
    Session::new(&self.nodes_addrs, lb)
  }

  #[cfg(feature = "ssl")]
  pub fn connect_ssl() {
    unimplemented!()
  }
}
