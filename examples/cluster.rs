
extern crate cdrs;
extern crate r2d2;

use std::thread;
use std::sync::mpsc::channel;

use cdrs::cluster::{LoadBalancingStrategy, LoadBalancer, ClusterConnectionManager};
use cdrs::authenticators::PasswordAuthenticator;
use cdrs::compression::Compression;
use cdrs::query::QueryBuilder;
use cdrs::transport::TransportTcp;

// default credentials
const _USER: &'static str = "cassandra";
const _PASS: &'static str = "cassandra";
const _ADDR1: &'static str = "127.0.0.1:9042";
const _ADDR2: &'static str = "127.0.0.1:9043";

fn main() {
    // TODO: setup cluster with different nodes and test it
    let cluster = vec![_ADDR1, _ADDR2]
        .iter()
        .map(|addr| TransportTcp::new(addr).unwrap())
        .collect();
    let authenticator = PasswordAuthenticator::new(_USER, _PASS);
    let load_balancer = LoadBalancer::new(cluster, LoadBalancingStrategy::RoundRobin);
    let manager = ClusterConnectionManager::new(load_balancer, authenticator, Compression::None);
    let pool = r2d2::Pool::builder().max_size(15).build(manager).unwrap();

    let (tx, rx) = channel();
    for i in 0..20 {
        let tx = tx.clone();
        let pool = pool.clone();

        thread::spawn(move || {
            let query = QueryBuilder::new("SELECT * FROM system.peers;").finalize();
            let mut conn = pool.get().unwrap();
            let res = if conn.query(query, false, false).is_ok() {
                format!("Thread #{} - ok", i)
            } else {
                format!("Thread #{} - err", i)
            };

            tx.send(res).unwrap();
        });
    }

    for _ in 0..20 {
        let res = rx.recv().unwrap();
        println!("{:?}", res);
    }
}
