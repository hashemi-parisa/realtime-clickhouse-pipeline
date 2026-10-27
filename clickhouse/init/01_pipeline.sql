CREATE DATABASE IF NOT EXISTS realtime;

CREATE TABLE IF NOT EXISTS realtime.flight_search_events
(
    event_time DateTime64(3, 'UTC'),
    event_id UUID,
    user_id UInt64,
    session_id UUID,
    origin LowCardinality(String),
    destination LowCardinality(String),
    cabin_class LowCardinality(String),
    adults UInt8,
    children UInt8,
    result_count UInt16,
    min_price Float64,
    response_time_ms UInt32,
    device LowCardinality(String),
    source LowCardinality(String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (origin, destination, event_time, event_id)
TTL toDateTime(event_time) + INTERVAL 180 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS realtime.flight_search_events_queue
(
    event_time DateTime64(3, 'UTC'),
    event_id UUID,
    user_id UInt64,
    session_id UUID,
    origin String,
    destination String,
    cabin_class String,
    adults UInt8,
    children UInt8,
    result_count UInt16,
    min_price Float64,
    response_time_ms UInt32,
    device String,
    source String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'flight_search_events',
    kafka_group_name = 'clickhouse-flight-search-consumer-v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_thread_per_consumer = 1,
    kafka_handle_error_mode = 'stream';

CREATE MATERIALIZED VIEW IF NOT EXISTS realtime.flight_search_events_mv
TO realtime.flight_search_events
AS
SELECT
    event_time,
    event_id,
    user_id,
    session_id,
    origin,
    destination,
    cabin_class,
    adults,
    children,
    result_count,
    min_price,
    response_time_ms,
    device,
    source
FROM realtime.flight_search_events_queue;

CREATE TABLE IF NOT EXISTS realtime.route_metrics_hourly
(
    hour DateTime('UTC'),
    origin LowCardinality(String),
    destination LowCardinality(String),
    cabin_class LowCardinality(String),
    search_count AggregateFunction(count),
    unique_users AggregateFunction(uniqCombined64, UInt64),
    average_min_price AggregateFunction(avg, Float64),
    lowest_min_price AggregateFunction(min, Float64),
    average_response_time_ms AggregateFunction(avg, Float64),
    average_result_count AggregateFunction(avg, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, origin, destination, cabin_class);

CREATE MATERIALIZED VIEW IF NOT EXISTS realtime.route_metrics_hourly_mv
TO realtime.route_metrics_hourly
AS
SELECT
    toStartOfHour(event_time) AS hour,
    origin,
    destination,
    cabin_class,
    countState() AS search_count,
    uniqCombined64State(user_id) AS unique_users,
    avgState(min_price) AS average_min_price,
    minState(min_price) AS lowest_min_price,
    avgState(toFloat64(response_time_ms)) AS average_response_time_ms,
    avgState(toFloat64(result_count)) AS average_result_count
FROM realtime.flight_search_events
GROUP BY
    hour,
    origin,
    destination,
    cabin_class;
