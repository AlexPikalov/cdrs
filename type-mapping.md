###Type relations between Rust (in CDRS approach) and Apache Cassandra

#### primitive types (`T`)

| Cassandra | Rust |
|-----------|--------------|
| tinyint | i8 |
| smallint | i16 |
| int | i32 |
| bigint | i64 |
| ascii | String |
| text | String |
| varchar | String |
| boolean | bool |
| time | i64 |
| timestamp | i64 |
| uuid | [Uuid](https://doc.rust-lang.org/uuid/uuid/struct.Uuid.html) |

#### complex types
| Cassandra | Rust + CDRS |
|-----------|-------------|
| list | [List](https://docs.rs/cdrs/1.0.0-beta.2/cdrs/types/list/struct.List.html) -> Vec<T> [example](https://github.com/AlexPikalov/cdrs/blob/master/examples/all.rs#L297) |
| set | [List](https://docs.rs/cdrs/1.0.0-beta.2/cdrs/types/list/struct.List.html) -> Vec<T> [example](https://github.com/AlexPikalov/cdrs/blob/master/examples/all.rs#L297)|
| map | [Map](https://docs.rs/cdrs/1.0.0-beta.2/cdrs/types/map/struct.Map.html) -> HashMap<String, T> [example](https://github.com/AlexPikalov/cdrs/blob/master/examples/all.rs#L350) |
| udt | Rust struct + custom [implementation into value](https://github.com/AlexPikalov/cdrs/blob/master/examples/all.rs#L467) |
