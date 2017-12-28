### v 1.2.1

* Fixed buffer issue when UDT schema was changed https://github.com/AlexPikalov/cdrs/pull/191

### v 1.2.0

* Fixed reconnection when a node went down and evaluation of peer address returns an error
* Introduced `IntoCDRSBytes` trait and created the [auto derive](https://github.com/AlexPikalov/into-cdrs-value-derive). See [example](https://github.com/AlexPikalov/into-cdrs-value-derive/tree/master/example) on how to use derive for values needed to be converted into CDRS `Value` which could be passed into query builder as a value
* Performance of node health check was improved

### v 1.1.0

* Create `Blob` type. It's a wrapper for `Vec<u8>` which represents Cassandra
  blob type.

* Fix full event to simple event mapping.

* Derive `Clone` for `Bytes`
