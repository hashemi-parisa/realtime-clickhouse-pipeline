DROP TABLE IF EXISTS benchmark.events_time_first_bloom;


/*
Same schema and sorting key as events_time_first.

The only additional structure is a Bloom-filter skipping index
for exact event-ID lookups.
*/
CREATE TABLE benchmark.events_time_first_bloom
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

    INDEX idx_event_id_bloom toString(event_id)
        TYPE bloom_filter(0.01)
        GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, origin, destination, event_id)
SETTINGS index_granularity = 8192;


/*
Copy the exact same dataset used by the baseline table.
The Bloom-filter index is created while the rows are written.
*/
INSERT INTO benchmark.events_time_first_bloom
SELECT *
FROM benchmark.events_time_first;