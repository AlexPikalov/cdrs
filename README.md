# CDRS [![Build Status](https://travis-ci.org/AlexPikalov/cdrs.svg?branch=master)](https://travis-ci.org/AlexPikalov/cdrs)

**CDRS** is a native Cassandra driver written in [Rust](https://www.rust-lang.org).
The motivation to write it in Rust is a lack of native one.
Existing ones are bindings to C clients.

[Documentation](https://alexpikalov.github.io/cdrs/cdrs/index.html)

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
let user = "user".to_string();
let pass = "pass".to_string();
let authenticator = PasswordAuthenticator::new(user, pass);

// pass authenticator into CDRS' constructor
let client = CDRS::new(addr, authenticator).unwrap();
use cdrs::compression;
// without compression
let response_frame = try!(client.start(compression::None));
```

If Server does not require authorization `authenticator` won't be used, but is still
required for the constructor (most probably it will be refactored in future).

### Using compression

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

Rest of examples TBD.
