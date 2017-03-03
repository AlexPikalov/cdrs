//! This modules contains an implementation of [r2d2](https://github.com/sfackler/r2d2)
//! functionality of connection pools. To get more details about creating r2d2 pools
//! please refer to original documentation.
use std::iter::Iterator;
use query::QueryBuilder;
use client::{CDRS, Session};
use error::{Error as CError, Result as CResult};
use authenticators::Authenticator;
use compression::Compression;
use r2d2;
use transport::CDRSTransport;
use rand;
use std::sync::atomic::{AtomicIsize, Ordering};

/// Load balancing strategy
#[derive(PartialEq)]
pub enum LoadBalancingStrategy {
    RoundRobin,
    Random,
}

impl LoadBalancingStrategy {
    /// Returns next value for selected load balancing strategy
    pub fn next<'a, N>(&'a self, nodes: &'a Vec<N>, i: usize) -> Option<&N> {
        match self {
            &LoadBalancingStrategy::Random => {
                nodes.iter()
                    .nth(self.rnd_idx((0, Some(nodes.len()))))
            }
            &LoadBalancingStrategy::RoundRobin => {
                let mut cycle = nodes.iter().cycle().skip(i);
                cycle.next()
            }
        }
    }

    fn rnd_idx(&self, bounds: (usize, Option<usize>)) -> usize {
        let min = bounds.0;
        let max = bounds.1.unwrap_or(u8::max_value() as usize);
        let rnd = rand::random::<usize>();
        min + rnd * (max - min) / (u8::max_value() as usize)
    }
}

/// Load balancer
///
/// #Example
///
/// ```no_run
/// let load_balancer = LoadBalancer::new(transports, LoadBalancingStrategy::RoundRobin);
/// let node = load_balancer.next().unwrap();
/// ```
pub struct LoadBalancer<T> {
    strategy: LoadBalancingStrategy,
    nodes: Vec<T>,
    i: AtomicIsize,
}

impl<T> LoadBalancer<T> {
    /// Factory function which creates new `LoadBalancer` with provided strategy.
    pub fn new(nodes: Vec<T>, strategy: LoadBalancingStrategy) -> LoadBalancer<T> {
        LoadBalancer {
            nodes: nodes,
            strategy: strategy,
            i: AtomicIsize::new(0),
        }
    }

    /// Returns next node basing on provided strategy.
    pub fn next(&self) -> Option<&T> {
        let next = self.strategy.next(&self.nodes, self.i.load(Ordering::Relaxed) as usize);
        if self.strategy == LoadBalancingStrategy::RoundRobin {
            self.i.fetch_add(1, Ordering::Relaxed);
        }
        next
    }
}

/// [r2d2](https://github.com/sfackler/r2d2) `ManageConnection`.
pub struct ClusterConnectionManager<T, X> {
    load_balancer: LoadBalancer<X>,
    authenticator: T,
    compression: Compression,
}

impl<T, X> ClusterConnectionManager<T, X>
    where T: Authenticator + Send + Sync + 'static
{
    /// Creates a new instance of `ConnectionManager`.
    /// It requires transport, authenticator and compression as inputs.
    pub fn new(load_balancer: LoadBalancer<X>,
               authenticator: T,
               compression: Compression)
               -> ClusterConnectionManager<T, X> {
        ClusterConnectionManager {
            load_balancer: load_balancer,
            authenticator: authenticator,
            compression: compression,
        }
    }
}

impl<T: Authenticator + Send + Sync + 'static, X: CDRSTransport + Send + Sync + 'static>
r2d2::ManageConnection for ClusterConnectionManager<T, X> {
    type Connection = Session<T,X>;
    type Error = CError;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let transport_res: CResult<X> = self.load_balancer.next()
            .ok_or("Cannot get next node".to_string().into())
            .and_then(|x| x.try_clone().map_err(|e| e.into()));
        let transport = try!(transport_res);
        let compression = self.compression.clone();
        let cdrs = CDRS::new(transport, self.authenticator.clone());

        cdrs.start(compression)
    }

    fn is_valid(&self, connection: &mut Self::Connection) -> Result<(), Self::Error> {
        let query = QueryBuilder::new("SELECT * FROM system.peers;").finalize();

        connection.query(query, false, false).map(|_| (()))
    }

    fn has_broken(&self, _connection: &mut Self::Connection) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_robin() {
        let nodes = vec!["a", "b", "c"];
        let nodes_c = nodes.clone();
        let mut load_balancer = LoadBalancer::new(nodes, LoadBalancingStrategy::RoundRobin);
        for i in 0..10 {
            assert_eq!(&nodes_c[i % 3], load_balancer.next().unwrap());
        }
    }
}
