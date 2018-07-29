mod random;
mod round_robin;

pub use load_balancing::random::Random;
pub use load_balancing::round_robin::RoundRobin;

pub trait LoadBalancingStrategy<N>: Sized {
  fn init(&mut self, cluster: Vec<N>);
  fn next(&self) -> Option<&N>;
}
