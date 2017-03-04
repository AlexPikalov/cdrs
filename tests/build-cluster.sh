#!/usr/bin/env bash

# The script builds Apache Cassandra two-node cluster
# The nodes could be reached by 127.0.0.1:9042 and 127.0.0.1:9043 address
docker stop cass1
docker rm cass1
docker run -d -p 9042:9042 --name cass1 cassandra:3.9

docker stop cass2
docker rm cass2
docker run -d -p 9043:9042 --name cass2 \
  -e CASSANDRA_SEEDS="$(docker inspect --format='{{ .NetworkSettings.IPAddress }}' cass1)" \
  cassandra:3.9
