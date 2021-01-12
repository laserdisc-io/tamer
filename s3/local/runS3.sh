#!/usr/bin/env bash

export AWS_ACCESS_KEY_ID=minio
export AWS_SECRET_ACCESS_KEY=miniosecret
export KAFKA_BROKERS=localhost:9092;
export KAFKA_SCHEMA_REGISTRY_URL=http://localhost:8081;
export KAFKA_CLOSE_TIMEOUT=10s;
export KAFKA_BUFFER_SIZE=50;
export KAFKA_SINK_TOPIC=sink;
export KAFKA_STATE_TOPIC=state;
export KAFKA_STATE_GROUP_ID=state-group;
export KAFKA_STATE_CLIENT_ID=state-client

export LOG_LEVEL=info

SCRIPT_PATH=$(cd "$(dirname "${BASH_SOURCE[0]}")" || exit; pwd -P)
cd "$SCRIPT_PATH"/../.. || exit

sbt "example/runMain tamer.example.S3Simple" -jvm-debug 5005