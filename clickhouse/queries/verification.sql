-- 1. Confirm that events are continuously arriving.
SELECT
    count() AS total_events,
    min(event_time) AS first_event_time,
    max(event_time) AS latest_event_time,
    max(ingested_at) AS latest_ingestion_time
FROM realtime.flight_search_events;

-- 2. Inspect recent raw events.
SELECT
    event_time,
    origin,
    destination,
    cabin_class,
    result_count,
    min_price,
    response_time_ms,
    device
FROM realtime.flight_search_events
ORDER BY event_time DESC
LIMIT 10;

-- 3. Query the incremental hourly aggregate.
SELECT
    hour,
    origin,
    destination,
    cabin_class,
    countMerge(search_count) AS searches,
    uniqCombined64Merge(unique_users) AS users,
    round(avgMerge(average_min_price), 2) AS avg_min_price,
    round(minMerge(lowest_min_price), 2) AS lowest_price,
    round(avgMerge(average_response_time_ms), 2) AS avg_response_ms,
    round(avgMerge(average_result_count), 2) AS avg_results
FROM realtime.route_metrics_hourly
GROUP BY
    hour,
    origin,
    destination,
    cabin_class
ORDER BY hour DESC, searches DESC
LIMIT 20;

-- 4. Check end-to-end ingestion delay.
SELECT
    quantile(0.50)(dateDiff('millisecond', event_time, ingested_at)) AS p50_ingestion_ms,
    quantile(0.95)(dateDiff('millisecond', event_time, ingested_at)) AS p95_ingestion_ms,
    max(dateDiff('millisecond', event_time, ingested_at)) AS max_ingestion_ms
FROM realtime.flight_search_events
WHERE event_time >= now() - INTERVAL 10 MINUTE;
