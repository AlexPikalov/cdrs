Cassandra ports:

* 7199 - JMX (was 8080 pre Cassandra 0.8.xx)
* 7000 - Internode communication (not used if TLS enabled)
* 7001 - TLS Internode communication (used if TLS enabled)
* 9160 - Thrift client API
* 9042 - CQL native transport port

```rs
let client = Client::connect("127.0.0.1");
```
