# CDRS [![crates.io version](https://img.shields.io/crates/v/cdrs.svg)](https://crates.io/crates/cdrs) [![Build Status](https://travis-ci.org/AlexPikalov/cdrs.svg?branch=master)](https://travis-ci.org/AlexPikalov/cdrs) [![Build status](https://ci.appveyor.com/api/projects/status/sirj4flws6o0dvb7/branch/master?svg=true)](https://ci.appveyor.com/project/harrydevnull/cdrs/branch/master)

CDRS is Apache **C**assandra **d**river written in pure **R**u**s**t.

## Features

- TCP/SSL connection;
- Load balancing;
- Connection pooling;
- LZ4, Snappy compression;
- Cassandra-to-Rust data deserialization;
- Pluggable authentication strategies;
- [ScyllaDB](https://www.scylladb.com/) support;
- Server events listening;
- Multiple CQL version support (3, 4), full spec implementation;
- Query tracing information;

## Getting started

Add CDRS to your `Cargo.toml` file as a dependency:

```toml
cdrs = { version = "2" }
```

or

```toml
cdrs = { version = "2", features = ["ssl"] }
```

to enable SSL feature.

Then add it as an external crate to your `main.rs`:

```rust
extern crate cdrs;

use cdrs::authenticators::NoneAuthenticator;
use cdrs::cluster::session::{new as new_session};
use cdrs::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder};
use cdrs::load_balancing::RoundRobin;
use cdrs::query::*;

fn main() {
  let node = NodeTcpConfigBuilder::new("127.0.0.1:9042", NoneAuthenticator {}).build();
  let cluster_config = ClusterTcpConfig(vec![node]);
  let no_compression =
    new_session(&cluster_config, RoundRobin::new()).expect("session should be created");

  let create_ks: &'static str = "CREATE KEYSPACE IF NOT EXISTS test_ks WITH REPLICATION = { \
                                 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
  no_compression.query(create_ks).expect("Keyspace create error");
}
```

This example configures a cluster consisting of a single node, and uses round robin load balancing and default `r2d2` values for connection pool.

## Documentation and examples

- User guide
- Examples
- API docs (release)

## License

This project is licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0))
- MIT license ([LICENSE-MIT](LICENSE-MIT) or [http://opensource.org/licenses/MIT](http://opensource.org/licenses/MIT))

at your option.
