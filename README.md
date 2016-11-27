**CDRS** is a native Cassandra client written in [Rust](https://www.rust-lang.org).
The motivation to write new Cassandra client in Rust is a lack of native one.
Existing ones are bindings to C clients.

[Documentation](https://alexpikalov.github.io/cdrs/cdrs/index.html)

CDRS is under active development at the moment, so there is a lack of many
features and API may not be stable (but in case of any breaking changes
we will update a major version of the package in accordance to common practices
of versioning).

At the moment **CDRS** is not an ORM but rather a kind of quite low level driver
which deals with different kind of frames. It supports 4-th version of [Cassandra
protocol](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec).

### Supported features
TBD

### Frames

#### Request

- [x] STARTUP
- [ ] AUTH_RESPONSE
- [x] OPTIONS
- [x] QUERY
- [x] PREPARE
- [ ] EXECUTE
- [ ] BATCH
- [ ] REGISTER

#### Response

- [x] ERROR
- [x] READY
- [ ] AUTHENTICATE
- [x] SUPPORTED
- [x] RESULT (Void)
- [x] RESULT (Rows)
- [x] RESULT (Set_keyspace)
- [x] RESULT (Prepared)
- [ ] RESULT (Schema_change)
- [ ] EVENT
- [ ] AUTH_CHALLENGE
- [ ] AUTH_SUCCESS

### Examples

```rs
use cdrs::client::CDRS;

let client = CDRS::new(addr).unwrap();
let use_query = String::from("SELECT * FROM loghub.syslogs;");

// start new session
match client.start() {
    Ok(parsed) => println!("OK: {:?} {:?}", parsed, parsed.get_body()),
    Err(err) => println!("Err: {:?}", err)
}

// this will receive an error because session is already started
match client.start() {
    Ok(parsed) => println!("OK: {:?} {:?}", parsed, parsed.get_body()),
    Err(err) => println!("Err: {:?}", err)
}

// this will execute a query.
match client.query(use_query) {
    Ok(parsed) => println!("OK 3: {:?} {:?}", parsed, parsed.get_body),
    Err(err) => println!("Err 3: {:?}", err)
}

```
