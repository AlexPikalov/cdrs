mod random;
mod round_robin;
mod round_robin_sync;
mod single_node;

pub use crate::load_balancing::random::Random;
pub use crate::load_balancing::round_robin::RoundRobin;
pub use crate::load_balancing::round_robin_sync::RoundRobinSync;
pub use crate::load_balancing::single_node::SingleNode;

pub trait LoadBalancingStrategy<N>: Sized {
    fn init(&mut self, cluster: Vec<N>);
    fn next(&self) -> Option<&N>;
}
