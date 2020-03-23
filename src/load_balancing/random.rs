use std::sync::Arc;
use rand;

use super::LoadBalancingStrategy;

pub struct Random<N> {
    pub cluster: Vec<Arc<N>>,
}

impl<N> Random<N> {
    pub fn new(cluster: Vec<Arc<N>>) -> Self {
        Random { cluster }
    }

    /// Returns a random number from a range
    fn rnd_idx(bounds: (usize, usize)) -> usize {
        let min = bounds.0;
        let max = bounds.1;
        let rnd = rand::random::<usize>();
        rnd % (max - min) + min
    }
}

impl<N> From<Vec<Arc<N>>> for Random<N> {
    fn from(cluster: Vec<Arc<N>>) -> Random<N> {
        Random { cluster }
    }
}

impl<N> LoadBalancingStrategy<N> for Random<N> where N: Sync {
    fn init(&mut self, cluster: Vec<Arc<N>>) {
        self.cluster = cluster;
    }

    /// Returns next random node from a cluster
    fn next(&self) -> Option<Arc<N>> {
        let len = self.cluster.len();
        if len == 0 {
            return None;
        }
        self.cluster.get(Self::rnd_idx((0, len))).map(|node| node.clone())
    }

    fn remove_node<F>(&mut self, mut filter: F)
    where
        F: FnMut(&N) -> bool,
    {
        if let Some(i) = self.cluster.iter().position(|node| filter(node)) {
            self.cluster.remove(i);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_random() {
        let nodes = vec!["a", "b", "c", "d", "e", "f", "g"];
        let load_balancer = Random::from(nodes.iter().map(|value| Arc::new(*value)).collect::<Vec<Arc<&str>>>());
        for _ in 0..100 {
            let s = load_balancer.next();
            assert!(s.is_some());
        }
    }

    #[test]
    fn remove_from_random() {
        let nodes = vec!["a"];
        let mut load_balancer = Random::from(nodes.iter().map(|value| Arc::new(*value)).collect::<Vec<Arc<&str>>>());

        let s = load_balancer.next();
        assert!(s.is_some());

        load_balancer.remove_node(|n| n == &"a");
        let s = load_balancer.next();
        assert!(s.is_none());
    }
}
