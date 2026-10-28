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


run_event_id_lookup() {
    local table="$1"
    local query_id="$2"

    local query="
SELECT
    event_id,
    event_time,
    origin,
    destination
FROM benchmark.${table}
WHERE toString(event_id) =
      toString(reinterpretAsUUID(MD5('event-500000')))
SETTINGS
    use_query_condition_cache = 0,
    max_threads = 1
"

    run_query "$query_id" "$query"
}

echo "Warming up event-ID lookup queries..."

run_event_id_lookup \
    "events_time_first" \
    "warmup_${RUN_TAG}_baseline"

run_event_id_lookup \
    "events_time_first_bloom" \
    "warmup_${RUN_TAG}_bloom"


echo "Running ${RUNS} measured iterations..."

for ((i = 1; i <= RUNS; i++)); do
    echo "Iteration ${i}/${RUNS}"

    # Alternate execution order to reduce ordering and cache bias.
    if ((i % 2 == 1)); then
        run_event_id_lookup \
            "events_time_first" \
            "m3b_${RUN_TAG}_baseline_${i}"

        run_event_id_lookup \
            "events_time_first_bloom" \
            "m3b_${RUN_TAG}_bloom_${i}"
    else
        run_event_id_lookup \
            "events_time_first_bloom" \
            "m3b_${RUN_TAG}_bloom_${i}"

        run_event_id_lookup \
            "events_time_first" \
            "m3b_${RUN_TAG}_baseline_${i}"
    fi
done


echo "Flushing ClickHouse query logs..."

docker exec "$CONTAINER" sh -c \
    'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --query "SYSTEM FLUSH LOGS"'


REPORT_QUERY="
SELECT
    if(
        position(query_id, '_bloom_') > 0,
        'with Bloom filter',
        'without Bloom filter'
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
      'm3b_${RUN_TAG}_'
  )
GROUP BY benchmark_case
ORDER BY benchmark_case
FORMAT PrettyCompact
"


echo
echo "Bloom-filter benchmark results for run ${RUN_TAG}:"

printf '%s\n' "$REPORT_QUERY" |
    docker exec -i "$CONTAINER" sh -c \
    'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD"'