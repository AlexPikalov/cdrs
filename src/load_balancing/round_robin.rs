use std::cell::RefCell;

use super::LoadBalancingStrategy;

pub struct RoundRobin<N> {
    cluster: Vec<N>,
    prev_idx: RefCell<usize>,
}

impl<N> RoundRobin<N> {
    pub fn new() -> Self {
        RoundRobin {
            prev_idx: RefCell::new(0),
            cluster: vec![],
        }
    }
}

impl<N> From<Vec<N>> for RoundRobin<N> {
    fn from(cluster: Vec<N>) -> RoundRobin<N> {
        RoundRobin {
            prev_idx: RefCell::new(0),
            cluster: cluster,
        }
    }
}

impl<N> LoadBalancingStrategy<N> for RoundRobin<N> {
    fn init(&mut self, cluster: Vec<N>) {
        self.cluster = cluster;
    }

    /// Returns next node from a cluster
    fn next(&self) -> Option<&N> {
        let prev_idx = *self.prev_idx.borrow();
        let next_idx = (prev_idx + 1) % self.cluster.len();
        self.prev_idx.replace(next_idx);
        self.cluster.get(next_idx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_robin() {
        let nodes = vec!["a", "b", "c"];
        let nodes_c = nodes.clone();
        let load_balancer = RoundRobin::from(nodes);
        for i in 0..10 {
            assert_eq!(&nodes_c[(i + 1) % 3], load_balancer.next().unwrap());
        }
    }
}
