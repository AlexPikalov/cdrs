### Executing queries

CDRS `Session` implements `cdrs::query::QueryExecutor` trait that provides few options for immediate query execution:

```rust
// simple query
session.query("SELECT * from my.store").unwrap();

// simple query with tracing and warnings
let with_tracing = true;
let with_warnings = true;
session.query_tw("SELECT * FROM my.store", with_tracing, with_warnings).unwrap();

// query with query values
let values = query_values!(1 as i32, 1 as i64);
session.query_tw("INSERT INTO my.numbers (my_int, my_bigint) VALUES (?, ?)", values).unwrap();

// query with query values, tracing and warnings
let with_tracing = true;
let with_warnings = true;
let values = query_values!(1 as i32, 1 as i64);
session.query_tw("INSERT INTO my.numbers (my_int, my_bigint) VALUES (?, ?)", values, with_tracing, with_warnings).unwrap();

// query with query params
use cdrs::query::QueryParamsBuilder;
use cdrs::consistency::Consistency;

let mut params = QueryParamsBuilder::new();
params = params.consistency(Consistency::Any);
session.query_with_params("SELECT * FROM my.store", params.finalize()).unwrap();

// query with query params and tracing, warnings
use cdrs::query::QueryParamsBuilder;
use cdrs::consistency::Consistency;

let with_tracing = true;
let with_warnings = true;

let mut params = QueryParamsBuilder::new();
params = params.consistency(Consistency::Any);

session.query_with_params_tw("SELECT * FROM my.store", params.finalize(), with_tracing, with_warnings).unwrap();
```
