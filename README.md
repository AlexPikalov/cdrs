# CDRS [![crates.io version](https://img.shields.io/crates/v/cdrs.svg)](https://crates.io/crates/cdrs) [![Build Status](https://travis-ci.org/AlexPikalov/cdrs.svg?branch=master)](https://travis-ci.org/AlexPikalov/cdrs) [![Build status](https://ci.appveyor.com/api/projects/status/sirj4flws6o0dvb7/branch/master?svg=true)](https://ci.appveyor.com/project/harrydevnull/cdrs/branch/master)

CDRS is Apache **C**assandra **d**river written in pure **R**u**s**t. The driver implements all the features described in Cassandra binary protocol specification (versions 3 and 4).

### Describing Cassandra Cluster and starting new Session

In order to start any communication with Cassandra cluster that requires authentication there should be provided a list of Cassandra nodes (IP addresses of machines where Cassandra is installed and included into a cluster). To get more details how to configure multinode cluster refer, for instance, to [DataStax documentation](https://docs.datastax.com/en/cassandra/3.0/cassandra/initialize/initTOC.html).

```rust
use cdrs::cluster::Cluster;
let cluster = Cluster::new(vec!["youraddress_1:9042", "youraddress_2:9042"], authenticator);
```

First agrument is a Rust `Vec` of node addresses, the second argument could be any structure that implements `cdrs::authenticators::Authenticator` trait. This allows to use custom authentication strategies, but in this case developers should implement authenticators by themselves. Out of the box CDRS provides two types of authenticators:

* `cdrs::authenticators::NoneAuthenticator` that should be used if authentication is disabled ([Cassandra authenticator](http://cassandra.apache.org/doc/latest/configuration/cassandra_config_file.html#authenticator) is set to `AllowAllAuthenticator`) on server.

* `cdrs::authenticators::PasswordAuthenticator` that should be used if authentication is enabled on the server and [authenticator](http://cassandra.apache.org/doc/latest/configuration/cassandra_config_file.html#authenticator) is `PasswordAuthenticator`:

```rust
use cdrs::authenticators::PasswordAuthenticator;
let authenticator = PasswordAuthenticator::new("user", "pass");
```

When cluster nodes are described new `Session` could be established. Each new `Session` has its own load balancing strategy as well as data compression. (CDRS supports both [Snappy](<https://en.wikipedia.org/wiki/Snappy_(compression)>) and [LZ4](<https://en.wikipedia.org/wiki/LZ4_(compression_algorithm)>) compresssions)

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
let mut no_compression = cluster.connect_ssl(RoundRobin::new())
                                  .expect("No compression connection error");
  let mut lz4_compression = cluster.connect_lz4_ssl(RoundRobin::new())
                                   .expect("LZ4 compression connection error");
  let mut snappy_compression = cluster.connect_snappy_ssl(RoundRobin::new())
                                      .expect("Snappy compression connection error");
```

More details regarding configuration Cassandra server for SSL-encrypted Client-Node communication could be found, for instance, on [DataStax website](https://docs.datastax.com/en/cassandra/3.0/cassandra/configuration/secureSSLClientToNode.html).

### Executing queries

CDRS `Session` implements `cdrs::query::QueryExecutor` trait that provides few options for immediate query execution:

```rust
// simple query
session.query("SELECT * from my.store").unwrap();

// simple query with tracing and warnings
let with_tracing = true;
let with_warnings = true;
session.query_tw("SELECT * FROM my.store", with_tracing, with_warnings).unwrap();

// query with query values
let values = query_values!(1 as i32, 1 as i64);
session.query_tw("INSERT INTO my.numbers (my_int, my_bigint) VALUES (?, ?)", values).unwrap();

// query with query values, tracing and warnings
let with_tracing = true;
let with_warnings = true;
let values = query_values!(1 as i32, 1 as i64);
session.query_tw("INSERT INTO my.numbers (my_int, my_bigint) VALUES (?, ?)", values, with_tracing, with_warnings).unwrap();

// query with query params
use cdrs::query::QueryParamsBuilder;
use cdrs::consistency::Consistency;

let mut params = QueryParamsBuilder::new();
params = params.consistency(Consistency::Any);
session.query_with_params("SELECT * FROM my.store", params.finalize()).unwrap();

// query with query params and tracing, warnings
use cdrs::query::QueryParamsBuilder;
use cdrs::consistency::Consistency;

let with_tracing = true;
let with_warnings = true;

let mut params = QueryParamsBuilder::new();
params = params.consistency(Consistency::Any);

session.query_with_params_tw("SELECT * FROM my.store", params.finalize(), with_tracing, with_warnings).unwrap();
```

### Preparing queries

During preparing a query a server parses the query, saves parsing result into cache and returns back to a client an ID that could be further used for executing prepared statement with different parameters (such as values, consistency etc.). When a server executes prepared query it doesn't need to parse it so parsing step will be skipped.

CDRS `Session` implements `cdrs::query::PrepareExecutor` trait that provides few option for preparing query:

```rust
let prepared_query = session.prepare("INSERT INTO my.store (my_int, my_bigint) VALUES (?, ?)").unwrap();

// or with tracing and warnings
let with_tracing = true;
let with_warnings = true;

let prepred_query = session.prepare_tw("INSERT INTO my.store (my_int, my_bigint) VALUES (?, ?)", with_tracing, with_warnings).unwrap();
```

### Executing prepared queries

When query is prepared on the server client gets prepared query id of type `cdrs::query::PreparedQuery`. Having such id it's possible to execute prepared query using session methods from `cdrs::query::ExecExecutor`:

```rust
// execute prepared query without specifying any extra parameters or values
session.exec(&preparedQuery).unwrap();

// execute prepared query with tracing and warning information
let with_tracing = true;
let with_warnings = true;

session.exec_tw(&preparedQuery, with_tracing, with_warnings).unwrap();

// execute prepared query with values
let values_with_names = query_values!{"my_bigint" => bigint, "my_int" => int};

session.exec_with_values(&preparedQuery, values_with_names).unwrap();

// execute prepared query with values with warnings and tracing information
let with_tracing = true;
let with_warnings = true;

let values_with_names = query_values!{"my_bigint" => 1 as i64, "my_int" => 2 as i32};

session.exec_with_values_tw(&preparedQuery, values_with_names, with_tracing, with_warnings).unwrap();

// execute prepared query with parameters
use cdrs::query::QueryParamsBuilder;
use cdrs::consistency::Consistency;

let mut params = QueryParamsBuilder::new();
params = params.consistency(Consistency::Any);
session.exec_with_parameters(&preparedQuery, params.finalize()).unwrap();

// execute prepared query with parameters, tracing and warning information
use cdrs::query::QueryParamsBuilder;
use cdrs::consistency::Consistency;

let with_tracing = true;
let with_warnings = true;
let mut params = QueryParamsBuilder::new();
params = params.consistency(Consistency::Any);
session.exec_with_parameters_tw(&preparedQuery, params.finalize(), with_tracing, with_warnings).unwrap();
```

### Batch queries

CDRS `Session` supports batching few queries in a single request to Apache Cassandra via implementing `cdrs::query::BatchExecutor` trait:

```rust
// batch two queries
use cdrs::query::{BatchQueryBuilder, QueryBatch};

let mut queries = BatchQueryBuilder::new();
queries = queries.add_query_prepared(&prepared_query);
queries = queries.add_query("INSERT INTO my.store (my_int) VALUES (?)", query_values!(1 as i32));
session.batch_with_params(queries.finalyze());

// batch queries with tracing and warning information
use cdrs::query::{BatchQueryBuilder, QueryBatch};

let with_tracing = true;
let with_warnings = true;
let mut queries = BatchQueryBuilder::new();
queries = queries.add_query_prepared(&prepared_query);
queries = queries.add_query("INSERT INTO my.store (my_int) VALUES (?)", query_values!(1 as i32));
session.batch_with_params_tw(queries.finalyze(), with_tracing, with_warnings);
```

### Query values types

Accordingly to specification along with queries there could be provided something that is called values. Apache Cassandra server will use values instead of `?` symbols from a query string.

There are two types of queries defined in the spec and supported by CDRS driver. Each of these two types could be easily constructed via provided `query_values!` macros.

* simple values - could be imagine as a list of values. The order of simple values matters because server will put them in the same number as columns were provided in query string.

```rust
let simple_values = query_values!(1 as i32, 2 as i32);
```

* named values are similar to hash maps, where keys represent column names which the a value has to be assigned to.

```rust
let values_with_names = query_values!{"my_bigint" => 1 as i64, "my_int" => 2 as i32};
```

Each type that implements `Into<cdrs::types::value::Value>` could be used as a value in `query_values!` macros. For primitive types please refer to following [wrapper CDRS types](https://github.com/AlexPikalov/cdrs/blob/master/type-mapping.md) that could be easily converted to `Value`. For custom types (in Cassandra terminology User Defined Types) `IntoCDRSValue` derive could be used:

```rust
#[derive(Debug, IntoCDRSValue)]
struct Udt {
    pub number: i32,
    pub number_16: i16,
    pub number_8: N,
}

// for nested structures it works as well
#[derive(Debug, IntoCDRSValue)]
struct N {
    pub n: i16,
}
```

Look into this [link](https://github.com/AlexPikalov/into-cdrs-value-derive/tree/master/example) to find a full example how to use CDRS + [_into-cdrs-value-derive_](https://github.com/AlexPikalov/into-cdrs-value-derive) crate.
