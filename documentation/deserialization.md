### Mapping results into Rust structures

_TODO: provide more details and link to examples_

In ordert to query information from Cassandra DB and transform results to Rust types an structures each row in a query result should be transformed leveraging one of following traits provided by CDRS `cdrs::types::{AsRustType, AsRust, IntoRustByName, ByName, IntoRustByIndex, ByIndex}`.

- `AsRustType` may be used in order to transform such complex structures as Cassandra lists, sets, tuples. The Cassandra value in this case could non-set and null values.

- `AsRust` trait may be used for similar purposes as `AsRustType` but it assumes that Cassandra value is neither non-set nor null value. Otherwise it panics.

- `IntoRustByName` trait may be used to access a value as a Rust structure/type by name. Such as in case of rows where each column has its own name, and maps. These values may be as well non-set and null.

- `ByName` trait is the same as `IntoRustByName` but value should be neither non-set nor null. Otherwise it panics.

- `IntoRustByIndex` is the same as `IntoRustByName` but values could be accessed via column index basing on their order provided in query. These values may be as well non-set and null.

- `ByIndex` is the same as `IntoRustByIndex` but value can be neither non-set nor null. Otherwise it panics.

Relations between Cassandra and Rust types are described in [type-mapping.md](https://github.com/AlexPikalov/cdrs/blob/master/type-mapping.md). For details see examples.
