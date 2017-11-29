mod random;
mod round_robin;

pub use load_balancing::random::Random;
pub use load_balancing::round_robin::RoundRobin;

pub trait LoadBalancingStrategy<'a, N> {
  fn next(&'a mut self, nodes: &'a Vec<N>) -> Option<&N>;
}
