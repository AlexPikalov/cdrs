### Type relations between Rust (in CDRS approach) and Apache Cassandra

#### primitive types (`T`)

| Cassandra | Rust | Feature
|-----------|-------|-------|
| tinyint | i8 | v4, v5 |
| smallint | i16 | v4, v5 |
| int | i32 | all |
| bigint | i64 | all |
| ascii | String | all |
| text | String | all |
| varchar | String | all |
| boolean | bool | all |
| time | i64 | all |
| timestamp | i64 | all |
| float | f32 | all |
| double | f64 | all |
| uuid | [Uuid](https://doc.rust-lang.org/uuid/uuid/struct.Uuid.html) | all |
| counter | u64 | all |

#### complex types
| Cassandra | Rust + CDRS |
|-----------|-------------|
| list | List -> Vec<T> [example](https://github.com/AlexPikalov/cdrs/blob/master/examples/all.rs#L159) |
| set | List -> Vec<T> [example](https://github.com/AlexPikalov/cdrs/blob/master/examples/all.rs#L159)|
| map | Map -> HashMap<String, T> [example](https://github.com/AlexPikalov/cdrs/blob/master/examples/all.rs#L185) |
| udt | Rust struct + custom [implementation into value](https://github.com/AlexPikalov/cdrs/blob/master/examples/all.rs#L211) |
