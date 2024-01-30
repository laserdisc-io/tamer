#!/usr/bin/env bash

trap 'trap - SIGTERM && kill 0' SIGINT SIGTERM EXIT

export LOG_LEVEL=INFO
export KAFKA_BROKERS=localhost:9092
export KAFKA_SCHEMA_REGISTRY_URL=http://localhost:8081
export KAFKA_CLOSE_TIMEOUT=10s
export KAFKA_BUFFER_SIZE=50
export KAFKA_SINK_TOPIC=sink
export KAFKA_SINK_AUTO_CREATE=on
export KAFKA_SINK_PARTITIONS=1
export KAFKA_SINK_REPLICAS=1
export KAFKA_STATE_TOPIC=state
export KAFKA_STATE_AUTO_CREATE=on
export KAFKA_STATE_PARTITIONS=1
export KAFKA_STATE_REPLICAS=1
export KAFKA_GROUP_ID=groupid
export KAFKA_CLIENT_ID=clientid
export KAFKA_TRANSACTIONAL_ID=transactionid

SCRIPT_PATH=$(cd "$(dirname "${BASH_SOURCE[0]}")" || exit; pwd -P)
cd "$SCRIPT_PATH"/../.. || exit

sbt "example/runMain tamer.rest.support.RESTServer" &
sleep 15
sbt "example/runMain tamer.rest.RESTSimple" -jvm-debug 5005