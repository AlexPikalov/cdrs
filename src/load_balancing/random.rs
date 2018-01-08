use rand;

use super::LoadBalancingStrategy;

pub struct Random<N> {
  cluster: Vec<N>,
}

impl<N> Random<N> {
  pub fn new() -> Self {
    Random { cluster: vec![] }
  }

  /// Returns random number from a range
  fn rnd_idx(bounds: (usize, usize)) -> usize {
    let min = bounds.0;
    let max = bounds.1;
    let rnd = rand::random::<usize>();
    rnd % (max - min) + min
  }
}

impl<N> From<Vec<N>> for Random<N> {
  fn from(cluster: Vec<N>) -> Random<N> {
    Random { cluster }
  }
}

impl<N> LoadBalancingStrategy<N> for Random<N> {
  fn init(&mut self, cluster: Vec<N>) {
    self.cluster = cluster;
  }
  /// Returns next random node from a cluster
  fn next(&mut self) -> Option<&mut N> {
    let len = self.cluster.len();
    self.cluster.get_mut(Self::rnd_idx((0, len)))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn random() {
    let nodes = vec!["a", "b", "c", "d", "e", "f", "g"];
    let load_balancer = LoadBalancer::new(nodes, Random);
    for _ in 0..100 {
      let s = load_balancer.next();
      assert!(s.is_some());
    }
  }
}
