mod random;
mod round_robin;

pub use load_balancing::random::Random;
pub use load_balancing::round_robin::RoundRobin;

pub trait LoadBalancingStrategy<'a, N>: Sized {
  fn next(&'a mut self, nodes: &'a mut Vec<N>) -> Option<&'a mut N>;
}
