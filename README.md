# CDRS [![Build Status](https://travis-ci.org/AlexPikalov/cdrs.svg?branch=master)](https://travis-ci.org/AlexPikalov/cdrs)

**CDRS** is a native Cassandra driver written in [Rust](https://www.rust-lang.org).
The motivation to write it in Rust is a lack of native one.
Existing ones are bindings to C clients.

[Documentation](https://docs.rs/cdrs)

CDRS is under active development at the moment, so there is a lack of many
features and API may not be stable (but in case of any breaking changes
we will update a major version of the package in accordance to common practices
of versioning).

At the moment **CDRS** is not an ORM or a client in usual meaning
but rather a kind of quite low level driver which deals with different kind of frames.
It supports 4-th version of [Cassandra protocol](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec).

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

// pass authenticator into CDRS' constructor
let client = CDRS::new(addr, authenticator).unwrap();
use cdrs::compression;
// without compression
let response_frame = try!(client.start(compression::None));
```

If Server does not require authorization `authenticator` won't be used, but is still
required for the constructor (most probably it will be refactored in future).

#### Using compression

Two types of compression are supported - [snappy](https://code.google.com/p/snappy/)
and [lz4](https://code.google.com/p/lz4/). To use compression just start connection
with desired type:

```rust
// client without compression
client.start(compression::None);
// client  lz4 compression
client.start(compression::Lz4);
// client with snappy compression
client.start(compression::Snappy);
```

#### Query execution

##### Use Query:

```rust
let use_query = String::from("USE my_namespace;");

match client.query(use_query) {
    Ok(set_keyspace) => {
        // use_keyspace is a result frame of type SetKeyspace
    },
    Err(err) => log!(err)
}
```

##### Select Query:

As a response to select query CDRS returns a result frame of type Rows with
data items (columns) encoded in Cassandra's way. Currently there are decode
helpers only, but no helper methods which could easily map results into
Rust structures. To have them is a goal of first stable version of CDRS.

```rust
use cdrs::consistency::Consistency;
use cdrs::types::CBytes;

let select_query = String::from("SELECT * FROM ks.table;");
// Query parameters:
let consistency = Consistency::One;
let values: Option<Vec<Value>> = None;
let with_names: Option<bool> = None;
let page_size: Option<i32> = None;
let paging_state Option<CBytes> = None;
let serial_consistency: Option<Consistency> = None;
let timestamp: Option<i64> = None;

match client.query(select_query,
    consistency,
    values,
    with_names,
    page_size,
    paging_state,
    serial_consistency,
    timestamp) {

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
let res_body = parsed.get_body();
let rows = res_body.into_rows().unwrap();
let messages: Vec<Message> = rows
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

let res_body = parsed.get_body();
let rows = res_body.into_rows().unwrap();
let messages: Vec<Author> = rows
    .iter()
    .map(|row| {
        let name: String = row.get_by_name("name").unwrap();
        let messages: Vec<String> = row
            .get_by_name("messages").unwrap()
            .as_rust().unwrap();
        return Author {
            author: name,
            text: messages
        }
    })
    .collect();

```

### License

The MIT License (MIT)

Copyright (c) 2016 Alex Pikalov

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
