#!/usr/bin/env bash

export AWS_ACCESS_KEY_ID=minio
export AWS_SECRET_ACCESS_KEY=miniosecret

SCRIPT_PATH=$(cd "$(dirname "${BASH_SOURCE[0]}")" || exit; pwd -P)
cd "$SCRIPT_PATH"/../.. || exit

sbt "example/runMain tamer.s3.S3Generalized" -jvm-debug 5005