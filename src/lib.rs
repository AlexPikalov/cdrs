//! **cdrs** is a native Cassandra DB client written in Rust.

extern crate byteorder;
extern crate snap;
#[macro_use]
pub mod macros;

#[macro_use]
extern crate log;
extern crate lz4_compress;
#[cfg(feature = "ssl")]
extern crate openssl;
extern crate rand;
extern crate time;
extern crate uuid;

pub mod cluster;
pub mod frame;
pub mod load_balancing;
pub mod query;
pub mod types;

pub mod authenticators;
pub mod compression;
pub mod consistency;
pub mod error;
pub mod events;
pub mod transport;
