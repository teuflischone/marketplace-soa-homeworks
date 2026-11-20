#!/bin/sh
set -eu
cd "$(dirname "$0")"

project="homework5-test-$$"
export GENERATOR_INTERVAL_SECONDS=0 AGGREGATION_INTERVAL_SECONDS=0
export PRODUCER_PORT="" AGGREGATION_PORT="" REGISTRY_PORT="" S3_PORT="" GRAFANA_PORT=""

compose() {
    docker compose -p "$project" --profile test "$@"
}

cleanup() {
    result=$?
    if [ "$result" -ne 0 ]; then
        compose logs --tail=40 setup clickhouse producer aggregation grafana s3 >&2 || true
    fi
    compose down -v --remove-orphans >/dev/null || true
    exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

compose build test
compose run --rm -T test
compose stop kafka-1
compose run --rm -T --no-deps test python -m tests.integration --pipeline-only
