# CDRS [![Build Status](https://travis-ci.org/AlexPikalov/cdrs.svg?branch=master)](https://travis-ci.org/AlexPikalov/cdrs)
[![crates.io version](https://img.shields.io/crates/v/cdrs.svg)](https://crates.io/crates/cdrs)

**CDRS** is a native Cassandra driver written in [Rust](https://www.rust-lang.org).
The motivation to write it in Rust is a lack of native one.
Existing ones are bindings to C clients.

[Documentation](https://docs.rs/cdrs)

**CDRS** completely implements 4-th version of [Cassandra protocol](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec).
Also it provides tools for [mapping results](#select-query-mapping-results)
into Rust structures.

### Supported features
- [x] lz4 decompression
- [x] snappy decompression
- [x] password authorization
- [x] tracing information
- [x] warning information
- [x] SSL encrypted connection
- [ ] load balancing
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

* - [x] Target KEYSPACE

* - [x] Target TABLE

* - [x] Target TYPE

* - [x] Target FUNCTION

* - [x] Target AGGREGATE
- [x] EVENT
- [x] AUTH_CHALLENGE
- [x] AUTH_SUCCESS

### Examples

#### Creating new connection and authorization

To use password authenticator, just include the one implemented in
`cdrs::authenticators`.

```rust
use cdrs::client::CDRS;
use cdrs::authenticators::PasswordAuthenticator;
use cdrs::transport::Transport;
```

After that you can create a new instance of `CDRS` and establish new connection:

```rust
let authenticator = PasswordAuthenticator::new("user", "pass");
let addr = "127.0.0.1:9042";
let tcp_transport = Transport::new(addr).unwrap();

// pass authenticator and transport into CDRS' constructor
let client = CDRS::new(tcp_transport, authenticator);
use cdrs::compression;
// start session without compression
let mut session = try!(client.start(compression::None));
```

If Server does not require authorization `authenticator` won't be used, but is still
required for the constructor (most probably it will be refactored in future).

#### Creating new encrypted connection

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
use cdrs::transport_ssl::Transport;
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

let ssl_transport = Transport::new(addr, &connector).unwrap();

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
let transport = Transport::new(ADDR).unwrap();
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

#### Using compression

Two types of compression are supported - [snappy](https://code.google.com/p/snappy/)
and [lz4](https://code.google.com/p/lz4/). To use compression just start connection
with desired type:

```rust
// session without compression
let mut session_res = client.start(compression::None);
// session  lz4 compression
let mut session_res = client.start(compression::Lz4);
// v with snappy compression
let mut session_res = client.start(compression::Snappy);
```

#### Query execution

Query execution is provided in scope of Session. So to start executing queries
you need to start Session first.

##### Use Query:

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

##### Create Query:

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

##### Select Query:

As a response to select query CDRS returns a result frame of type Rows with
data items (columns) encoded in Cassandra's way.

```rust
use std::default::Default;
use cdrs::client::Query;
use cdrs::consistency::Consistency;

let select_query: Query = QueryBuilder::new(use_query.clone()).finalize();
let with_tracing = false;
let with_warnings = false;

match session.query(select_query, with_tracing, with_warnings) {
    Ok(res) => println!("Result frame: {:?},\nparsed body: {:?}", res, res.get_body());,
    Err(err) => log!(err)
}
```

##### Select Query (mapping results):

Once CDRS got response to `SELECT` query you can map rows encapsulated within
`Result` frame into Rust values or into `List`, `Map` or `UDT` helper structures
which provide a way to convert wrapped values into plain ones.

As an example let's consider a case when application gets a collection
of messages of following format:

```rust

struct Message {
    pub author: String,
    pub text: String
}

```

To get a collection of messages `Vec<Message>` let's convert a result of query
into collection of rows `Vec<cdrs::types::row::Row>` and then convert each column
into appropriate Rust type:

```rust
use cdrs::error::{Result as CResult};

let res_body = parsed.get_body();
let rows = res_body.into_rows().unwrap();
let messages: Vec<CResult<Message>> = rows
    .iter()
    .map(|row| Message {
        author: row.get_by_name("author").unwrap(),
        text: row.get_by_name("text").unwrap()
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
let res_body = parsed.get_body();
let rows = res_body.into_rows().unwrap();
let messages: Vec<CAuthor> = rows
    .iter()
    .map(|row| {
        let name: String = row.get_by_name("name").unwrap();
        let messages: Vec<String> = row
            // unwrap Option<CResult<T>>, where T implements AsRust
            .get_by_name("messages").unwrap().unwrap()
            .as_rust().unwrap();
        return Author {
            author: name,
            text: messages
        };
    })
    .collect();

```

##### Prepare and execute a query:

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
    .into_set_keyspace()
    .unwrap();
```

#### Listen to Server events

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

### License

The MIT License (MIT)

Copyright (c) 2016 Alex Pikalov

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
