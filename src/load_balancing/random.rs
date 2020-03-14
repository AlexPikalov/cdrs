use rand;

use super::LoadBalancingStrategy;
use std::borrow::Borrow;

pub struct Random<N> {
    pub cluster: Vec<N>,
}

impl<N> Random<N> {
    pub fn new(cluster: Vec<N>) -> Self {
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
        if len == 0 {
            return None;
        }
        self.cluster.get(Self::rnd_idx((0, len)))
    }

    fn get_all_nodes(&self) -> &Vec<N> {
        self.cluster.borrow()
    }

    fn remove_node<F>(&mut self, filter: F)
    where
        F: FnMut(&N) -> bool,
    {
        if let Some(i) = self.cluster.iter().position(filter) {
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
        let load_balancer = Random::from(nodes);
        for _ in 0..100 {
            let s = load_balancer.next();
            assert!(s.is_some());
        }
    }

    #[test]
    fn remove_from_random() {
        let nodes = vec!["a"];
        let mut load_balancer = Random::from(nodes);

        let s = load_balancer.next();
        assert!(s.is_some());

        load_balancer.remove_node(|n| n == &"a");
        let s = load_balancer.next();
        assert!(s.is_none());
    }
}
