//! **cdrs** is a native Cassandra DB client written in Rust.

extern crate byteorder;
#[macro_use]
pub mod macros;

#[macro_use]
extern crate log;
extern crate rand;
extern crate time;
extern crate uuid;

pub mod frame;
pub mod query;
pub mod types;

pub mod compression;
pub mod consistency;
pub mod error;

pub type Error = error::Error;
pub type Result<T> = error::Result<T>;
