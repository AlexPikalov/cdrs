# CDRS [![Build Status](https://travis-ci.org/AlexPikalov/cdrs.svg?branch=master)](https://travis-ci.org/AlexPikalov/cdrs)

**CDRS** is a native Cassandra driver written in [Rust](https://www.rust-lang.org).
The motivation to write it in Rust is a lack of native one.
Existing ones are bindings to C clients.

[Documentation](https://docs.rs/cdrs)

CDRS is under active development at the moment, so API may not be stable.

As well **CDRS** provides tools for [mapping results](#select-query-mapping-results)
into Rust structures It supports 4-th version of [Cassandra protocol](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec).

### Supported features
- [x] lz4 decompression
- [x] snappy decompression
- [x] password authorization

### Frames

#### Request

- [x] STARTUP
- [x] AUTH_RESPONSE
- [x] OPTIONS
- [x] QUERY
- [x] PREPARE
- [x] EXECUTE
- [ ] BATCH
- [ ] REGISTER

#### Response

- [x] ERROR
- [x] READY
- [x] AUTHENTICATE
- [x] SUPPORTED
- [x] RESULT (Void)
- [x] RESULT (Rows)
- [x] RESULT (Set_keyspace)
- [x] RESULT (Prepared)
- [ ] RESULT (Schema_change)

* - [x] Target KEYSPACE

* - [x] Target TABLE

* - [x] Target TYPE

* - [ ] Target FUNCTION

* - [ ] Target AGGREGATE
- [ ] EVENT
- [x] AUTH_CHALLENGE
- [x] AUTH_SUCCESS

### Examples

#### Creating new connection and authorization

To use password authenticator, just include the one implemented in
`cdrs::authenticators`.

```rust
use cdrs::client::CDRS;
use cdrs::authenticators::PasswordAuthenticator;
```

After that you can create a new instace of `CDRS` and establish new connection:

```rust
let authenticator = PasswordAuthenticator::new("user", "pass");
let addr = "127.0.0.1:9042";

// pass authenticator into CDRS' constructor
let client = CDRS::new(addr, authenticator).unwrap();
use cdrs::compression;
// start session without compression
let mut session = try!(client.start(compression::None));
```

If Server does not require authorization `authenticator` won't be used, but is still
required for the constructor (most probably it will be refactored in future).

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

let use_query_string = String::from("USE my_namespace;");

match session.prepare(use_query_string) {
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
use cdrs::client::{Query, QueryBuilder};
use cdrs::consistency::Consistency;

let mut select_query: Query = QueryBuilder::new("CREATE TABLE keyspace.emp (
    empID int,
    deptID int,
    first_name varchar,
    last_name varchar,
    PRIMARY KEY (empID, deptID)
    );")
    .consistency(Consistency::One)
    .finalize();

let table_created = session.query(select_query).is_ok();

```

##### Select Query:

As a response to select query CDRS returns a result frame of type Rows with
data items (columns) encoded in Cassandra's way.

```rust
use std::default::Default;
use cdrs::client::Query;
use cdrs::consistency::Consistency;

let mut select_query: Query = QueryBuilder::new(use_query.clone()).finalize();

match session.query(select_query) {
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

### License

The MIT License (MIT)

Copyright (c) 2016 Alex Pikalov

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
