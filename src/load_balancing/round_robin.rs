use super::LoadBalancingStrategy;

pub struct RoundRobin {
  prev_idx: usize,
}

impl RoundRobin {
  pub fn new() -> Self {
    RoundRobin { prev_idx: 0 }
  }
}

impl<'a, N> LoadBalancingStrategy<'a, N> for RoundRobin {
  /// Returns next node from a cluster
  fn next(&'a mut self, cluster: &'a mut Vec<N>) -> Option<&'a mut N> {
    self.prev_idx = (self.prev_idx + 1) % cluster.len();
    cluster.get_mut(self.prev_idx)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn round_robin() {
    let nodes = vec!["a", "b", "c"];
    let nodes_c = nodes.clone();
    let load_balancer = LoadBalancer::new(nodes, LoadBalancingStrategy::RoundRobin);
    for i in 0..10 {
      assert_eq!(&nodes_c[i % 3], load_balancer.next().unwrap());
    }
  }
}
