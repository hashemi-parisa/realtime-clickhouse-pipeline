-- Store malformed Kafka messages for investigation and possible replay.
CREATE TABLE IF NOT EXISTS realtime.flight_search_dead_letters
(
    failed_at DateTime64(3, 'UTC') DEFAULT now64(3),
    topic LowCardinality(String),
    partition UInt64,
    offset UInt64,
    kafka_timestamp Nullable(DateTime64(3, 'UTC')),
    raw_message String,
    error_message String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(failed_at)
ORDER BY (failed_at, topic, partition, offset)
TTL toDateTime(failed_at) + INTERVAL 30 DAY;


-- Replace the original raw-events view so that only valid messages
-- are inserted into the main raw table.
DROP VIEW IF EXISTS realtime.flight_search_events_mv;

CREATE MATERIALIZED VIEW realtime.flight_search_events_mv
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
FROM realtime.flight_search_events_queue
WHERE _error = '';


-- Route malformed messages to the dead-letter table.
CREATE MATERIALIZED VIEW IF NOT EXISTS realtime.flight_search_dead_letters_mv
TO realtime.flight_search_dead_letters
AS
SELECT
    now64(3) AS failed_at,
    _topic AS topic,
    _partition AS partition,
    _offset AS offset,
    _timestamp_ms AS kafka_timestamp,
    _raw_message AS raw_message,
    _error AS error_message
FROM realtime.flight_search_events_queue
WHERE _error != '';