#!/usr/bin/env bash

set -euo pipefail

RUNS="${1:-10}"
CONTAINER="${CLICKHOUSE_CONTAINER:-realtime-clickhouse}"
RUN_TAG="$(date +%Y%m%d%H%M%S)"

if ! [[ "$RUNS" =~ ^[1-9][0-9]*$ ]]; then
    echo "RUNS must be a positive integer."
    exit 1
fi


run_query() {
    local query_id="$1"
    local query="$2"

    printf '%s\n' "$query" |
        docker exec -i "$CONTAINER" sh -c \
        'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --query_id "$1" --format Null' \
        sh "$query_id"
}


run_route_filter() {
    local table="$1"
    local query_id="$2"

    local query="
SELECT
    count() AS searches,
    avg(min_price) AS average_price,
    avg(response_time_ms) AS average_response_time
FROM benchmark.${table}
WHERE origin = 'THR'
  AND destination = 'IST'
  AND event_time >= toDateTime64(
      '2026-01-01 00:00:00',
      3,
      'UTC'
  )
  AND event_time < toDateTime64(
      '2026-04-04 00:00:00',
      3,
      'UTC'
  )
"

    run_query "$query_id" "$query"
}


run_time_filter() {
    local table="$1"
    local query_id="$2"

    local query="
SELECT
    count() AS searches,
    avg(min_price) AS average_price,
    avg(response_time_ms) AS average_response_time
FROM benchmark.${table}
WHERE event_time >= toDateTime64(
      '2026-03-15 00:00:00',
      3,
      'UTC'
  )
  AND event_time < toDateTime64(
      '2026-03-16 00:00:00',
      3,
      'UTC'
  )
"

    run_query "$query_id" "$query"
}


echo "Warming up benchmark queries..."

run_route_filter \
    "events_route_first" \
    "warmup_${RUN_TAG}_route_route_first"

run_route_filter \
    "events_time_first" \
    "warmup_${RUN_TAG}_route_time_first"

run_time_filter \
    "events_route_first" \
    "warmup_${RUN_TAG}_time_route_first"

run_time_filter \
    "events_time_first" \
    "warmup_${RUN_TAG}_time_time_first"


echo "Running ${RUNS} measured iterations..."

for ((i = 1; i <= RUNS; i++)); do
    echo "Iteration ${i}/${RUNS}"

    # Alternate execution order to reduce ordering and cache bias.
    if ((i % 2 == 1)); then
        run_route_filter \
            "events_route_first" \
            "m3_${RUN_TAG}_route_route_first_${i}"

        run_route_filter \
            "events_time_first" \
            "m3_${RUN_TAG}_route_time_first_${i}"

        run_time_filter \
            "events_route_first" \
            "m3_${RUN_TAG}_time_route_first_${i}"

        run_time_filter \
            "events_time_first" \
            "m3_${RUN_TAG}_time_time_first_${i}"
    else
        run_route_filter \
            "events_time_first" \
            "m3_${RUN_TAG}_route_time_first_${i}"

        run_route_filter \
            "events_route_first" \
            "m3_${RUN_TAG}_route_route_first_${i}"

        run_time_filter \
            "events_time_first" \
            "m3_${RUN_TAG}_time_time_first_${i}"

        run_time_filter \
            "events_route_first" \
            "m3_${RUN_TAG}_time_route_first_${i}"
    fi
done


echo "Flushing ClickHouse query logs..."

docker exec "$CONTAINER" sh -c \
    'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --query "SYSTEM FLUSH LOGS"'


REPORT_QUERY="
SELECT
    multiIf(
        position(query_id, '_route_route_first_') > 0,
            'route filter / route-first',

        position(query_id, '_route_time_first_') > 0,
            'route filter / time-first',

        position(query_id, '_time_route_first_') > 0,
            'time filter / route-first',

        position(query_id, '_time_time_first_') > 0,
            'time filter / time-first',

        'unknown'
    ) AS benchmark_case,

    count() AS runs,

    round(
        median(query_duration_ms),
        2
    ) AS median_duration_ms,

    round(
        avg(query_duration_ms),
        2
    ) AS average_duration_ms,

    toUInt64(
        median(read_rows)
    ) AS median_read_rows,

    formatReadableSize(
        toUInt64(median(read_bytes))
    ) AS median_read_size,

    formatReadableSize(
        toUInt64(median(memory_usage))
    ) AS median_memory_usage

FROM system.query_log
WHERE type = 'QueryFinish'
  AND startsWith(
      query_id,
      'm3_${RUN_TAG}_'
  )
GROUP BY benchmark_case
ORDER BY benchmark_case
FORMAT PrettyCompact
"


echo
echo "Benchmark results for run ${RUN_TAG}:"

printf '%s\n' "$REPORT_QUERY" |
    docker exec -i "$CONTAINER" sh -c \
    'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD"'