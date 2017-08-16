# CDRS [![Build Status](https://travis-ci.org/AlexPikalov/cdrs.svg?branch=master)](https://travis-ci.org/AlexPikalov/cdrs) [![Build status](https://ci.appveyor.com/api/projects/status/sirj4flws6o0dvb7/branch/master?svg=true)](https://ci.appveyor.com/project/harrydevnull/cdrs/branch/master)


[![crates.io version](https://img.shields.io/crates/v/cdrs.svg)](https://crates.io/crates/cdrs)
[![Join the chat at https://gitter.im/cdrs-rs/Lobby](https://badges.gitter.im/cdrs-rs/Lobby.svg)](https://gitter.im/cdrs-rs/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Coverage Status](https://coveralls.io/repos/github/harrydevnull/cdrs/badge.svg?branch=master)](https://coveralls.io/github/harrydevnull/cdrs?branch=master)
[![codecov](https://codecov.io/gh/harrydevnull/cdrs/branch/master/graph/badge.svg)](https://codecov.io/gh/harrydevnull/cdrs)


**CDRS** is a native driver for [Apache Cassandra](http://cassandra.apache.org) written in [Rust](https://www.rust-lang.org).
The motivation to write it in Rust is a lack of native one.
Existing ones are bindings to C clients.

[Documentation](https://docs.rs/cdrs)

**CDRS** completely implements 4-th version of [Cassandra protocol](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec).
Also it provides tools for [mapping results](#select-query-mapping-results)
into Rust structures.

## Content
* [Creating new connection and authorization](#creating-new-connection-and-authorization)
* [Creating new encrypted connection](#creating-new-encrypted-connection)
* [Connecting via r2d2 connection pool](#connecting-via-r2d2-connection-pool)
* [Retrieving supported options](#getting-supported-options)
* [Using compression](#using-compression)
* [Query execution](#query-execution)
  * [USE query](https://github.com/AlexPikalov/cdrs#use-query)
  * [CREATE query](#create-query)
  * [SELECT query](#select-query)
  * [SELECT query (mapping results)](#select-query-mapping-results)
  * [PREPARE and EXECUTE query](#prepare-and-execute-a-query)
* [Listen to server events](#listen-to-server-events)
* [Cassandra clusters and load balancing](#cassandra-clusters-and-load-balancing)
* [Performance](#performance)
* [Supported features](#supported-features)


### Creating new connection


```rust
use cdrs::client::CDRS;
use cdrs::authenticators::NoneAuthenticator;
use cdrs::transport::TransportTcp;
```

After that you can create a new instance of `CDRS` and establish new connection:

```rust
let authenticator = NoneAuthenticator;
let addr = "127.0.0.1:9042";
let tcp_transport = TransportTcp::new(addr).unwrap();

// pass authenticator and transport into CDRS' constructor
let client = CDRS::new(tcp_transport, authenticator);
use cdrs::compression;
// start session without compression
let mut session = try!(client.start(compression::None));
```



### Creating new connection with authentication

To use password authenticator, just include the one implemented in
`cdrs::authenticators`.

```rust
use cdrs::client::CDRS;
use cdrs::authenticators::PasswordAuthenticator;
use cdrs::transport::TransportTcp;
```

After that you can create a new instance of `CDRS` and establish new connection:

```rust
let authenticator = PasswordAuthenticator::new("user", "pass");
let addr = "127.0.0.1:9042";
let tcp_transport = TransportTcp::new(addr).unwrap();

// pass authenticator and transport into CDRS' constructor
let client = CDRS::new(tcp_transport, authenticator);
use cdrs::compression;
// start session without compression
let mut session = try!(client.start(compression::None));
```


### Creating new encrypted connection

To be able to create SSL-encrypted connection CDRS should be used with
`ssl` feature enabled. Apart of CDRS itself _openssl_ must also be imported.

```toml
[dependencies]
openssl = "0.9.6"

[dependencies.cdrs]
version = "*"
features = ["ssl"]
```

```rust
use cdrs::client::CDRS;
use cdrs::authenticators::PasswordAuthenticator;
use cdrs::transport::TransportTls;
use openssl::ssl::{SslConnectorBuilder, SslMethod};
use std::path::Path;
```

After that you can create a new instance of `CDRS` and establish new connection:

```rust
let authenticator = PasswordAuthenticator::new("user", "pass");
let addr = "127.0.0.1:9042";

// here needs to be a path of your SSL certificate
let path = Path::new("./node0.cer.pem");
let mut ssl_connector_builder = SslConnectorBuilder::new(SslMethod::tls()).unwrap();
ssl_connector_builder.builder_mut().set_ca_file(path).unwrap();
let connector = ssl_connector_builder.build();

let ssl_transport = TransportTls::new(addr, &connector).unwrap();

// pass authenticator and SSL transport into CDRS' constructor
let client = CDRS::new(ssl_transport, authenticator);
```

### Connecting via r2d2 connection pool

There is an option to create [r2d2](https://github.com/sfackler/r2d2) connection pool
of CDRS connections both plain and SSL-encrypted:

```rust
use cdrs::connection_manager::ConnectionManager;

let config = r2d2::Config::builder()
    .pool_size(15)
    .build();
let transport = TransportTcp::new(ADDR).unwrap();
let authenticator = PasswordAuthenticator::new(USER, PASS);
let manager = ConnectionManager::new(transport, authenticator, Compression::None);

let pool = r2d2::Pool::new(config, manager).unwrap();

for _ in 0..20 {
    let pool = pool.clone();
    thread::spawn(move || {
        let conn = pool.get().unwrap();
        // use the connection
        // it will be returned to the pool when it falls out of scope.
    });
}

```

There is a related example.

### Getting supported options

Before session established an application may want to know which options are
supported by a server (for instance to figure out which compression to use).
That's why `CDRS` instance has a method `get_options` which could be called
before session get started. Options are presented as `HashMap<String, Vec<String>>`.

```rust
let options = try!(client.get_options());
```

**This should be called before session started** to let you know which compression
to choose and because session object borrows `CDRS` instance.

### Using compression

Two types of compression are supported - [snappy](https://code.google.com/p/snappy/)
and [lz4](https://code.google.com/p/lz4/). To use compression just start connection
with desired type:

```rust
use cdrs::compression::Compression;
// session without compression
let mut session_res = client.start(Compression::None);
// session  lz4 compression
let mut session_res = client.start(Compression::Lz4);
// v with snappy compression
let mut session_res = client.start(Compression::Snappy);
```

### Query execution

Query execution is provided in scope of Session. So to start executing queries
you need to start Session first.

#### Use Query:

```rust

let create_query: Query = QueryBuilder::new("USE my_namespace;").finalize();
let with_tracing = false;
let with_warnings = false;

match session.query(create_query, with_tracing, with_warnings) {
    Ok(set_keyspace) => {
        // use_keyspace is a result frame of type SetKeyspace
    },
    Err(err) => log!(err)
}
```

#### Create Query:

Creating new table could be performed via `session.query`. In case of success
method return Schema Change frame that contains Change Type, Target and options
that contain namespace and a name of created table.

```rust
use std::default::Default;
use cdrs::query::{Query, QueryBuilder};
use cdrs::consistency::Consistency;

let mut create_query: Query = QueryBuilder::new("CREATE TABLE keyspace.emp (
    empID int,
    deptID int,
    first_name varchar,
    last_name varchar,
    PRIMARY KEY (empID, deptID)
    );")
    .consistency(Consistency::One)
    .finalize();
let with_tracing = false;
let with_warnings = false;

let table_created = session.query(create_query, with_tracing, with_warnings).is_ok();

```

#### Select Query:

As a response to select query CDRS returns a result frame of type Rows with
data items (columns) encoded in Cassandra's way.

```rust
use std::default::Default;
use cdrs::client::Query;
use cdrs::consistency::Consistency;

let select_query: Query = QueryBuilder::new("SELECT * FROM keyspace.table;").finalize();
let with_tracing = false;
let with_warnings = false;

match session.query(select_query, with_tracing, with_warnings) {
    Ok(res) => println!("Result frame: {:?},\nparsed body: {:?}", res, res.get_body());,
    Err(err) => log!(err)
}
```

#### Select Query (mapping results):

Once CDRS got response to `SELECT` query you can map rows encapsulated within
`Result` frame into Rust values or into `List`, `Map` or `UDT` helper structures
which provide a way to convert wrapped values into plain ones.

As an example let's consider a case when application gets a collection
of messages of following format:

```rust

struct Message {
    pub author: String,
    pub text: String,
    pub optional_field: Option<String>
}

```

To get a collection of messages `Vec<Message>` let's convert a result of query
into collection of rows `Vec<cdrs::types::row::Row>` and then convert each column
into appropriate Rust type:

```rust
use cdrs::error::{Result as CResult};

let res_body = parsed.get_body().unwrap();
let rows = res_body.into_rows().unwrap();
let messages: Vec<CResult<Message>> = rows
    .iter()
    .map(|row| Message {
        author: row.r_by_name("author").unwrap(),
        text: row.r_by_name("text").unwrap(),
        optional_field: row.get_by_name("optional_field")
    })
    .collect();

```

or by column position:

```rust
let messages: Vec<CResult<Message>> = rows
    .iter()
    .map(|row| Message {
        author: row.r_by_index(0).unwrap(),
        text: row.r_by_index(1).unwrap(),
        optional_field: row.get_by_index(2)
    })
    .collect();
```

There is no difference between Cassandra's List and Sets in terms of Rust.
They could be represented as `Vec<T>`. To convert a frame into a structure
that contains a collection of elements do as follows:

```rust

struct Author {
    pub name: String,
    pub messages: Vec<String>
}

//...
use cdrs::error::{Result as CResult};
let res_body = parsed.get_body().unwrap();
let rows = res_body.into_rows().unwrap();
let messages: Vec<CAuthor> = rows
    .iter()
    .map(|row| {
        let name: String = row.r_by_name("name").unwrap();
        let messages: Vec<String> = row
            // unwrap Option<CResult<T>>, where T implements AsRust
            .r_by_name("messages").unwrap()
            .as_r_rust().unwrap();
        return Author {
            author: name,
            text: messages
        };
    })
    .collect();

```

#### Prepare and execute a query:

Prepare-execute query is also supported:

```rust
  // NOTE: keyspace "keyspace" should already exist
  let create_table_cql = "USE keyspace;".to_string();
  let with_tracing = false;
  let with_warnings = false;

  // prepare a query
  let prepared = session.prepare(create_table_cql, with_tracing, with_warnings)
    .unwrap()
    .get_body()
    .into_prepared()
    .unwrap();

  // execute prepared query
  let execution_params = QueryParamsBuilder::new(Consistency::One).finalize();
  let query_id = prepared.id;
  let executed = session.execute(query_id, execution_params, false, false)
    .unwrap()
    .get_body()
    .unwrap()
    .into_set_keyspace()
    .unwrap();
```

It's also makes sense to use prepare query in pair with [batching](https://github.com/AlexPikalov/cdrs/blob/master/examples/batch_queries.rs) few queries.

### Listen to Server events

CDRS provides functionality which allows listening to server events. Events
inform user about following changes:

* **Topology change** - events related to change in the cluster topology.
Currently, events are sent when new nodes are added to the cluster, and
when nodes are removed.

* **Status change** - events related to change of node status. Currently,
up/down events are sent.

* **Schema_change** - events related to schema change.

Current implementation allows to move listener and stream handler into separate
threads so then (as we believe) developers could leverage whatever
async IO library they want.

To find an examples please refer to [examples](./examples/server_events.rs).

### Cassandra clusters and load balancing

CDRS supports Apache Cassandra clusters and load balancing. In order to connect
to desired nodes you have to provide related transports (either TCP or TLS)
and to configure r2d2 pool.

```rust
let cluster = vec![_ADDR1, _ADDR2]
    .iter()
    .map(|addr| TransportTcp::new(addr).unwrap())
    .collect();
let config = r2d2::Config::builder()
    .pool_size(15)
    .build();
```
After that you need to choose desired load balancing strategy and instantiate
cluster collection manager. At current moment
two static strategies were implemented: `Random` and `RoundRobin`.
```rust
let load_balancer = LoadBalancer::new(cluster, LoadBalancingStrategy::RoundRobin);
let manager = ClusterConnectionManager::new(load_balancer, authenticator, Compression::None);
```
After that you'll be able to communicate with cluster via r2d2 connection pool.

### Performance

Folder `./benches` contains benchmark tests. This is an attempt to measure the
driver performance and to compare it with already existing solutions. Also
periodically running benchmark tests might help to identify slow parts of CDRS
and prevent performance degradation caused by changes.

To get current result you need either to have nightly Rust installed on your
machine or install [rustup](https://www.rustup.rs/).

In case you have nightly Rust, just run `cargo bench`. If you have rustup --
`rustup run nightly cargo bench`.

To find last results refer to [benchmarks.md](./benchmarks.md)

### Supported features

- [x] lz4 decompression
- [x] snappy decompression
- [x] password authorization
- [x] tracing information
- [x] warning information
- [x] SSL encrypted connection
- [x] load balancing
- [x] connection pooling

### Frames

#### Request

- [x] STARTUP
- [x] AUTH_RESPONSE
- [x] OPTIONS
- [x] QUERY
- [x] PREPARE
- [x] EXECUTE
- [x] BATCH
- [x] REGISTER

#### Response

- [x] ERROR
- [x] READY
- [x] AUTHENTICATE
- [x] SUPPORTED
- [x] RESULT (Void)
- [x] RESULT (Rows)
- [x] RESULT (Set_keyspace)
- [x] RESULT (Prepared)
- [x] RESULT (Schema_change)
  - [x] Target KEYSPACE
  - [x] Target TABLE
  - [x] Target TYPE
  - [x] Target FUNCTION
  - [x] Target AGGREGATE
- [x] EVENT
- [x] AUTH_CHALLENGE
- [x] AUTH_SUCCESS


Issues
------

Feel free to submit issues and enhancement requests.

Contributing
------------

Please refer to each project's style guidelines and guidelines for submitting patches and additions. In general, we follow the "fork-and-pull" Git workflow.

 1. **Fork** the repo on GitHub
 2. **Clone** the project to your own machine
 3. **Commit** changes to your own branch
 4. **Run  ```cargo test --all-features && cargo fmt -- --write-mode=diff```
 5. **Push** your work back up to your fork
 6. Submit a **Pull request** so that we can review your changes

NOTE: Be sure to merge the latest from "upstream" before making a pull request!
while running the tests you might need a local cassandra server working.
The easiest way was to run cassandra on docker on local machine


Running Cassandra on Local
---------------------------

 1. If you have docker on the machine type the below command
     ```
     docker run --name cassandra-1 -d -p 9042:9042 -p 9160:9160 cassandra:2.2.1

     ```
     `docker ps `
       should show an output like below

     ```
        CONTAINER ID        IMAGE               COMMAND                  CREATED             STATUS              PORTS                                                                     NAMES
        a78c3a43bf1b        cassandra:2.2.1     "/docker-entrypoin..."   4 days ago          Up 4 days           7000-7001/tcp, 0.0.0.0:9042->9042/tcp, 7199/tcp, 0.0.0.0:9160->9160/tcp   cassandra-1
      ```

 2. If docker is new to your tool set; it is never too late to know this awesome tool https://docs.docker.com/docker-for-mac/

 Running Cassandra Cluster on local
 -----------------------------------

To start Apache Cassandra cluster on local just run `tests/build-cluster.sh`.
This script will create two nodes of Apache Cassandra 3.9 with following exposed
ports: 9042 and 9043.

### License

* [MIT License](https://github.com/AlexPikalov/cdrs/blob/master/LICENSE-MIT)

* [Apache-2.0](https://github.com/AlexPikalov/cdrs/blob/master/LICENSE-APACHE)
