use super::LoadBalancingStrategy;

pub struct RoundRobin<N> {
  cluster: Vec<N>,
  prev_idx: usize,
}

impl<N> RoundRobin<N> {
  pub fn new() -> Self {
    RoundRobin { prev_idx: 0,
                 cluster: vec![], }
  }
}

impl<N> From<Vec<N>> for RoundRobin<N> {
  fn from(cluster: Vec<N>) -> RoundRobin<N> {
    RoundRobin { prev_idx: 0,
                 cluster, }
  }
}

impl<N> LoadBalancingStrategy<N> for RoundRobin<N> {
  fn init(&mut self, cluster: Vec<N>) {
    self.cluster = cluster;
  }

  /// Returns next node from a cluster
  fn next(&mut self) -> Option<&mut N> {
    self.prev_idx = (self.prev_idx + 1) % self.cluster.len();
    self.cluster.get_mut(self.prev_idx)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn round_robin() {
    let nodes = vec!["a", "b", "c"];
    let nodes_c = nodes.clone();
    let mut load_balancer = RoundRobin::from(nodes);
    for i in 0..10 {
      assert_eq!(&nodes_c[(i + 1) % 3], load_balancer.next().unwrap());
    }
  }
}
