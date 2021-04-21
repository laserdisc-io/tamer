#!/usr/bin/env bash

trap 'trap - SIGTERM && kill 0' SIGINT SIGTERM EXIT

export KAFKA_BROKERS=localhost:9092;
export KAFKA_SCHEMA_REGISTRY_URL=http://localhost:8081;
export KAFKA_CLOSE_TIMEOUT=10s;
export KAFKA_BUFFER_SIZE=50;
export KAFKA_SINK_TOPIC=sink;
export KAFKA_STATE_TOPIC=state;
export KAFKA_STATE_GROUP_ID=state-group;
export KAFKA_STATE_CLIENT_ID=state-client

SCRIPT_PATH=$(cd "$(dirname "${BASH_SOURCE[0]}")" || exit; pwd -P)
cd "$SCRIPT_PATH"/../.. || exit

sbt "example/runMain tamer.support.RestServer" &
sleep 15
sbt "example/runMain tamer.example.RestBasicAuth" -jvm-debug 5005