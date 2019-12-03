#!/usr/bin/env bash

export LOG_LEVEL=INFO
export DATABASE_DRIVER=org.postgresql.Driver
export DATABASE_URL=jdbc:postgresql://localhost:5432/postgres
export DATABASE_USERNAME=postgres
export DATABASE_PASSWORD=mysecretpassword
export KAFKA_BROKERS=localhost:9092
export KAFKA_CLOSE_TIMEOUT="30 seconds"
export KAFKA_BUFFER_SIZE=4096
export KAFKA_SINK_TOPIC=sink-topic-11
export KAFKA_STATE_TOPIC=state-topic-11
export KAFKA_STATE_GROUP_ID=state-group-id-11
export KAFKA_STATE_CLIENT_ID=state-client-id-11


../target/universal/stage/bin/example
