### Describing Cassandra Cluster and starting new Session

In order to start any communication with Cassandra cluster that requires authentication there should be provided a list of Cassandra nodes (IP addresses of machines where Cassandra is installed and included into a cluster). To get more details how to configure multinode cluster refer, for instance, to [DataStax documentation](https://docs.datastax.com/en/cassandra/3.0/cassandra/initialize/initTOC.html).

```rust
use cdrs::cluster::Cluster;
let cluster = Cluster::new(vec!["youraddress_1:9042", "youraddress_2:9042"], authenticator);
```

First agrument is a Rust `Vec` of node addresses, the second argument could be any structure that implements `cdrs::authenticators::Authenticator` trait. This allows to use custom authentication strategies, but in this case developers should implement authenticators by themselves. Out of the box CDRS provides two types of authenticators:

- `cdrs::authenticators::NoneAuthenticator` that should be used if authentication is disabled ([Cassandra authenticator](http://cassandra.apache.org/doc/latest/configuration/cassandra_config_file.html#authenticator) is set to `AllowAllAuthenticator`) on server.

- `cdrs::authenticators::PasswordAuthenticator` that should be used if authentication is enabled on the server and [authenticator](http://cassandra.apache.org/doc/latest/configuration/cassandra_config_file.html#authenticator) is `PasswordAuthenticator`:

```rust
use cdrs::authenticators::PasswordAuthenticator;
let authenticator = PasswordAuthenticator::new("user", "pass");
```

When cluster nodes are described new `Session` could be established.
