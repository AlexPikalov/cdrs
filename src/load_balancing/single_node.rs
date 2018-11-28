use super::LoadBalancingStrategy;

pub struct SingleNode<N> {
  cluster: Vec<N>
}

impl<N> SingleNode<N> {
  pub fn new() -> Self {
    SingleNode {
      cluster: vec![]
    }
  }
}

impl<N> From<Vec<N>> for SingleNode<N> {
  fn from(cluster: Vec<N>) -> SingleNode<N> {
    SingleNode {
      cluster: cluster,
    }
  }
}

impl<N> LoadBalancingStrategy<N> for SingleNode<N> {
  fn init(&mut self, cluster: Vec<N>) {
    self.cluster = cluster;
  }

  /// Returns first node from cluster
  fn next(&self) -> Option<&N> {
    self.cluster.get(0)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn single_node() {
    let nodes = vec!["a"];
    let nodes_c = nodes.clone();
    let load_balancer = SingleNode::from(nodes);
    assert_eq!(&nodes_c[0], load_balancer.next().unwrap());
  }
}
