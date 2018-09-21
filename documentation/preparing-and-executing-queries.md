### Preparing queries

During preparing a query a server parses the query, saves parsing result into cache and returns back to a client an ID that could be further used for executing prepared statement with different parameters (such as values, consistency etc.). When a server executes prepared query it doesn't need to parse it so parsing step will be skipped.

CDRS `Session` implements `cdrs::query::PrepareExecutor` trait that provides few option for preparing query:

```rust
let prepared_query = session.prepare("INSERT INTO my.store (my_int, my_bigint) VALUES (?, ?)").unwrap();

// or with tracing and warnings
let with_tracing = true;
let with_warnings = true;

let prepred_query = session.prepare_tw("INSERT INTO my.store (my_int, my_bigint) VALUES (?, ?)", with_tracing, with_warnings).unwrap();
```

### Executing prepared queries

When query is prepared on the server client gets prepared query id of type `cdrs::query::PreparedQuery`. Having such id it's possible to execute prepared query using session methods from `cdrs::query::ExecExecutor`:

```rust
// execute prepared query without specifying any extra parameters or values
session.exec(&preparedQuery).unwrap();

// execute prepared query with tracing and warning information
let with_tracing = true;
let with_warnings = true;

session.exec_tw(&preparedQuery, with_tracing, with_warnings).unwrap();

// execute prepared query with values
let values_with_names = query_values!{"my_bigint" => bigint, "my_int" => int};

session.exec_with_values(&preparedQuery, values_with_names).unwrap();

// execute prepared query with values with warnings and tracing information
let with_tracing = true;
let with_warnings = true;

let values_with_names = query_values!{"my_bigint" => 1 as i64, "my_int" => 2 as i32};

session.exec_with_values_tw(&preparedQuery, values_with_names, with_tracing, with_warnings).unwrap();

// execute prepared query with parameters
use cdrs::query::QueryParamsBuilder;
use cdrs::consistency::Consistency;

let mut params = QueryParamsBuilder::new();
params = params.consistency(Consistency::Any);
session.exec_with_parameters(&preparedQuery, params.finalize()).unwrap();

// execute prepared query with parameters, tracing and warning information
use cdrs::query::QueryParamsBuilder;
use cdrs::consistency::Consistency;

let with_tracing = true;
let with_warnings = true;
let mut params = QueryParamsBuilder::new();
params = params.consistency(Consistency::Any);
session.exec_with_parameters_tw(&preparedQuery, params.finalize(), with_tracing, with_warnings).unwrap();
```
