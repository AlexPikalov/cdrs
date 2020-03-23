use std::sync::Arc;

mod random;
mod round_robin;
mod single_node;

pub use crate::load_balancing::random::Random;
pub use crate::load_balancing::round_robin::RoundRobin;
pub use crate::load_balancing::single_node::SingleNode;

pub trait LoadBalancingStrategy<N>: Sized {
    fn init(&mut self, cluster: Vec<Arc<N>>);
    fn next(&self) -> Option<Arc<N>>;
    fn remove_node<F>(&mut self, _filter: F)
    where
        F: FnMut(&N) -> bool,
    {
        // default implementation does nothing
    }
}
