new `Session` has its own load balancing strategy as well as data compression. (CDRS supports both [Snappy](<https://en.wikipedia.org/wiki/Snappy_(compression)>) and [LZ4](<https://en.wikipedia.org/wiki/LZ4_(compression_algorithm)>) compresssions)

```rust
let mut no_compression = cluster.connect(RoundRobin::new())
                                  .expect("No compression connection error");
  let mut lz4_compression = cluster.connect_lz4(RoundRobin::new())
                                   .expect("LZ4 compression connection error");
  let mut snappy_compression = cluster.connect_snappy(RoundRobin::new())
                                      .expect("Snappy compression connection error");
```

where the first argument of each connect methods is a load balancer. Each structure that implements `cdrs::load_balancing::LoadBalancingStrategy` could be used as a load balancer during establishing new `Session`. CDRS provides two strategies out of the box: `cdrs::load_balancing::{RoundRobin, Random}`. Having been set once at the start load balancing strategy cannot be changed during the session.

Unlike to load balancing compression method could be changed without session restart:

```rust
use compression::Compression;
let mut session = cluster.connect(RoundRobin::new())
                                  .expect("No compression connection error");
session.compression = Compression::LZ4;
```

### Starting new SSL-encrypted Session

SSL-encrypted connection is also awailable with CDRS however to get this working CDRS itself should be imported with `ssl` feature enabled:

```toml
[dependencies]
openssl = "0.9.6"

[dependencies.cdrs]
version = "*"
features = ["ssl"]
```

Another difference comparing to non-encrypted connection is necessity to create [`SSLConnector`](https://docs.rs/openssl/0.10.2/openssl/ssl/struct.SslConnector.html)

```rust
use std::path::Path;
use openssl::ssl::{SslConnectorBuilder, SslMethod};
use cdrs::client::CDRS;
use cdrs::authenticators::PasswordAuthenticator;
use cdrs::transport::TransportTls;

// here needs to be a path to your SSL certificate
let path = Path::new("./node0.cer.pem");
let mut ssl_connector_builder = SslConnectorBuilder::new(SslMethod::tls()).unwrap();
ssl_connector_builder.builder_mut().set_ca_file(path).unwrap();
let connector = ssl_connector_builder.build();
```

When these preparation are done we're good to start SSL-encrypted session.

```rust
let mut no_compression = cluster.connect_ssl(RoundRobin::new(), authenticator, connector)
                                  .expect("No compression connection error");
let mut lz4_compression = cluster.connect_lz4_ssl(RoundRobin::new(), authenticator, connector)
                                  .expect("LZ4 compression connection error");
let mut snappy_compression = cluster.connect_snappy_ssl(RoundRobin::new(), authenticator, connector)
                                  .expect("Snappy compression connection error");
```

More details regarding configuration Cassandra server for SSL-encrypted Client-Node communication could be found, for instance, on [DataStax website](https://docs.datastax.com/en/cassandra/3.0/cassandra/configuration/secureSSLClientToNode.html).
