//! **cdrs** is a native Cassandra DB client written in Rust.

extern crate byteorder;
extern crate snap;

extern crate log;
extern crate lz4_compress;
#[cfg(feature = "ssl")]
extern crate openssl;
extern crate r2d2;
extern crate rand;
extern crate time;
extern crate uuid;

extern crate cassandra_proto;

pub mod cluster;
pub mod load_balancing;
pub mod query;

pub mod authenticators;
pub mod compression;
pub mod error;
pub mod events;
pub mod frame;
pub mod transport;
pub mod types;

mod transport_builder_trait;
mod transport_tcp;

pub use cassandra_proto::macros::*;

pub type Error = error::Error;
pub type Result<T> = error::Result<T>;
