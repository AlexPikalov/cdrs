use std::sync::Arc;

use super::LoadBalancingStrategy;

pub struct SingleNode<N> {
    cluster: Vec<Arc<N>>,
}

impl<N> SingleNode<N> {
    pub fn new() -> Self {
        SingleNode { cluster: vec![] }
    }
}

impl<N> From<Vec<Arc<N>>> for SingleNode<N> {
    fn from(cluster: Vec<Arc<N>>) -> SingleNode<N> {
        SingleNode { cluster: cluster }
    }
}

impl<N> LoadBalancingStrategy<N> for SingleNode<N> where N: Sync + Send {
    fn init(&mut self, cluster: Vec<Arc<N>>) {
        self.cluster = cluster;
    }

    /// Returns first node from a cluster
    fn next(&self) -> Option<Arc<N>> {
        self.cluster.get(0).map(|node| node.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_node() {
        let nodes = vec!["a"];
        let nodes_c = nodes.clone();
        let load_balancer = SingleNode::from(nodes.iter().map(|value| Arc::new(*value)).collect::<Vec<Arc<&str>>>());
        assert_eq!(&nodes_c[0], load_balancer.next().unwrap().as_ref());
        // and one more time to check
        assert_eq!(&nodes_c[0], load_balancer.next().unwrap().as_ref());
    }
}
