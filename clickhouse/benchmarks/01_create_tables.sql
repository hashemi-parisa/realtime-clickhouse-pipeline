CREATE DATABASE IF NOT EXISTS benchmark;


/*
Route-first table

Expected to perform better when queries filter by:
1. origin
2. destination
3. event_time
*/
DROP TABLE IF EXISTS benchmark.events_route_first;

CREATE TABLE benchmark.events_route_first
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
    source LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (origin, destination, event_time, event_id)
SETTINGS index_granularity = 8192;


/*
Time-first table

Expected to perform better when queries primarily filter
by an event_time range across all routes.
*/
DROP TABLE IF EXISTS benchmark.events_time_first;

CREATE TABLE benchmark.events_time_first
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
    source LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, origin, destination, event_id)
SETTINGS index_granularity = 8192;