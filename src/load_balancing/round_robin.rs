use std::sync::{Mutex, Arc};

use super::LoadBalancingStrategy;

#[derive(Debug)]
pub struct RoundRobin<N> {
    cluster: Vec<Arc<N>>,
    prev_idx: Mutex<usize>,
}

impl<N> RoundRobin<N> {
    pub fn new() -> Self {
        RoundRobin {
            prev_idx: Mutex::new(0),
            cluster: vec![],
        }
    }
}

impl<N> From<Vec<Arc<N>>> for RoundRobin<N> {
    fn from(cluster: Vec<Arc<N>>) -> RoundRobin<N> {
        RoundRobin {
            prev_idx: Mutex::new(0),
            cluster: cluster,
        }
    }
}

impl<N> LoadBalancingStrategy<N> for RoundRobin<N> where N: Sync + Send {
    fn init(&mut self, cluster: Vec<Arc<N>>) {
        self.cluster = cluster;
    }

    /// Returns next node from a cluster
    fn next(&self) -> Option<Arc<N>> {
        let mut prev_idx = self.prev_idx.lock().unwrap();
        let next_idx = (*prev_idx + 1) % self.cluster.len();
        *prev_idx = next_idx;

        self.cluster.get(next_idx).map(|node| node.clone())
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
    fn round_robin() {
        let nodes = vec!["a", "b", "c"];
        let nodes_c = nodes.clone();
        let load_balancer = RoundRobin::from(nodes.iter().map(|value| Arc::new(*value)).collect::<Vec<Arc<&str>>>());
        for i in 0..10 {
            assert_eq!(&nodes_c[(i + 1) % 3], load_balancer.next().unwrap().as_ref());
        }
    }

    #[test]
    fn remove_from_round_robin() {
        let nodes = vec!["a", "b"];
        let mut load_balancer = RoundRobin::from(nodes.iter().map(|value| Arc::new(*value)).collect::<Vec<Arc<&str>>>());
        assert_eq!(&"b", load_balancer.next().unwrap().as_ref());

        load_balancer.remove_node(|n| n == &"a");
        assert_eq!(&"b", load_balancer.next().unwrap().as_ref());
    }
}
