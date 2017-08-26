#!/usr/bin/env bash

docker run -d -p 9042:9042 --name scylladb scylladb/scylla
# docker run --name some-scylla -d scylladb/scylla
