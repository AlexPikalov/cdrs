use rand;

use super::LoadBalancingStrategy;

pub struct Random {}

impl Random {
  /// Returns random number from a range
  fn rnd_idx(bounds: (usize, usize)) -> usize {
    let min = bounds.0;
    let max = bounds.1;
    let rnd = rand::random::<usize>();
    rnd % (max - min) + min
  }
}

impl<'a, N> LoadBalancingStrategy<'a, N> for Random {
  /// Returns next random node from a cluster
  fn next(&'a mut self, cluster: &'a Vec<N>) -> Option<&N> {
    cluster.get(Self::rnd_idx((0, cluster.len())))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn random() {
    let nodes = vec!["a", "b", "c", "d", "e", "f", "g"];
    let load_balancer = LoadBalancer::new(nodes, LoadBalancingStrategy::Random);
    for _ in 0..100 {
      let s = load_balancer.next();
      assert!(s.is_some());
    }
  }
}
