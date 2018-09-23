# Making queries

By default `Session` structure doesn't provide an API for making queries. Query functionality bacomes enabled after importing one or few of following traits:

```rust
use cdrs::query::QueryExecutor;
```

This trait provides an API for making plain queries and immediately receiving responses.

```rust
use cdrs::query::PrepareExecutor;
```

This trait enables query preparation on the server. After query preparation it's enough to exectute query via `cdrs::query::ExecExecutor` API prviding a query ID returned by `cdrs::query::PrepareExecutor`

```rust
use cdrs::query::ExecExecutor;
```

This trait provides an API for query exectution. `PrepareExecutor` and `ExecExecutor` APIs are considered in [next sections](./preparing-and-executing-queries.md).

```rust
use cdrs::query::BatchExecutor;
```

`BatchExecutor` provides functionality for executing multiple queries in a single request to Cluster. For more details refer to [Batch Queries](./batching-multiple-queries.md) section.

### `CDRSSession` trait

Each of traits enumered beyond provides just a piece of full query API. They can be used independently one from another though. However if the whole query functionality is needed in a programm `use cdrs::cluster::CDRSSession` should be considered instead.

`CDRSSession` source code looks following

```rust
pub trait CDRSSession<
  'a,
  T: CDRSTransport + 'static,
  M: r2d2::ManageConnection<Connection = cell::RefCell<T>, Error = error::Error>,
>:
  GetCompressor<'static>
  + GetConnection<T, M>
  + QueryExecutor<T, M>
  + PrepareExecutor<T, M>
  + ExecExecutor<T, M>
  + BatchExecutor<T, M>
{
}
```

It includes all the functionality related to making queries.

### `QueryExecutor` API

`QueryExecutor` trait provides various methods for immediate query execution.

```rust
session.query("INSERT INTO my.numbers (my_int, my_bigint) VALUES (1, 2)").unwrap();
```

`query` method receives a single argument which is a CQL query string. It returns `cdrs::error::Result` that in case of `SELECT` query can be mapped on corresponded Rust structure. See [CRUD example](../examples/crud_operations.rs) for details.

The same query could be made leveraging something that is called Values. It allows to have generic query strings independent from actuall values.

```rust
#[macro_use]
extern crate cdrs;
//...

const insert_numbers_query: &'static str = "INSERT INTO my.numbers (my_int, my_bigint) VALUES (?, ?)";
let values = query_values!(1 as i32, 1 as i64);

session.query_with_values(insert_numbers_query, values).unwrap();
```

However the full controll over the query can be achieved via `cdrs::query::QueryParamsBuilder`:

```rust
use cdrs::query::QueryParamsBuilder;
use cdrs::consistency::Consistency;

let query_params = QueryParamsBuilder::new()
  .consistency(Consistency::Any)
  .finalize();
session.query_with_params("SELECT * FROM my.store", query_params).unwrap();
```

`QueryParamsBuilder` allows to precise all possible parameters of a query: consistency, values, paging properties and others. To get all parameters please refer to CDRS API [docs](https://docs.rs/cdrs/2.0.0-beta.1/cdrs/query/struct.QueryParamsBuilder.html).

Usually developers don't need to use `query_with_params` as almost all functionality is provided by such ergonomic methods as `query_with_params`, `pager` etc.

### Reference

1. `QueryParamsBuilder` API docs https://docs.rs/cdrs/2.0.0-beta.1/cdrs/query/struct.QueryParamsBuilder.html.

2. The Cassandra Query Language (CQL) http://cassandra.apache.org/doc/4.0/cql/
