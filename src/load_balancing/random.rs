use rand;

use super::LoadBalancingStrategy;

pub struct Random<N> {
  pub cluster: Vec<N>,
}

impl<N> Random<N> {
  pub fn new(cluster: Vec<N>) -> Self {
    Random { cluster }
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
  fn next(&self) -> Option<&N> {
    let len = self.cluster.len();
    self.cluster.get(Self::rnd_idx((0, len)))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn random() {
    let nodes = vec!["a", "b", "c", "d", "e", "f", "g"];
    let load_balancer = Random::from(nodes);
    for _ in 0..100 {
      let s = load_balancer.next();
      assert!(s.is_some());
    }
  }
}
