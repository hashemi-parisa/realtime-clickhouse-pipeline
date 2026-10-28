# ClickHouse Ordering-Key and Bloom-Filter Benchmarks

This benchmark evaluates how ClickHouse table design affects query
pruning, data compression, read volume, memory usage, and execution time.

The experiments focus on two design decisions:

1. Selecting an `ORDER BY` expression based on expected query patterns.
2. Using a Bloom-filter data-skipping index for sparse, high-cardinality
   equality lookups.

The goal is not to identify one universally optimal design. Instead, the
experiments demonstrate the trade-offs involved in selecting sorting keys
and secondary indexes.

## Dataset

The benchmark generates 1,000,000 deterministic synthetic flight-search
events covering approximately 93 days:

- First event: `2026-01-01 00:00:00 UTC`
- Last event: `2026-04-03 14:13:12 UTC`
- ClickHouse version: `25.8.33`
- Number of measured executions per query: `10`
- Warm-up executions are excluded from reported results

All comparison tables contain exactly the same rows.

## Ordering-key comparison

### Route-first ordering

```sql
ORDER BY (origin, destination, event_time, event_id)
```

This design prioritizes queries that filter by origin and destination.

### Time-first ordering

```sql
ORDER BY (event_time, origin, destination, event_id)
```

This design prioritizes time-range queries across all routes.

## Storage results

| Table | Rows | Disk size | Compression ratio |
|---|---:|---:|---:|
| `events_route_first` | 1,000,000 | 39.43 MiB | 1.67 |
| `events_time_first` | 1,000,000 | 28.57 MiB | 2.30 |

For this deterministic dataset, the time-first layout used approximately
27.5% less disk space.

The generated timestamps follow a regular sequence, which benefits from
placing `event_time` first in the sorting key. This result is specific to
the generated data distribution and should not be treated as a universal
storage advantage.

## Benchmark 1: route filter

The route-oriented query filters a single origin-destination pair over
the complete time range:

```sql
WHERE origin = 'THR'
  AND destination = 'IST'
  AND event_time >= '2026-01-01 00:00:00'
  AND event_time <  '2026-04-04 00:00:00'
```

### Primary-index pruning

| Table | Selected granules |
|---|---:|
| `events_route_first` | 13 of 122 |
| `events_time_first` | 122 of 122 |

### Measured results

| Table | Median duration | Median rows read | Median bytes read | Median memory |
|---|---:|---:|---:|---:|
| `events_route_first` | 8 ms | 107,072 | 2.25 MiB | 2.53 MiB |
| `events_time_first` | 13 ms | 1,000,000 | 20.98 MiB | 4.74 MiB |

For this query, the route-first design:

- reduced median execution time by approximately 38.5%
- reduced rows read by approximately 89.3%
- reduced bytes read by approximately 89.3%
- reduced median memory usage by approximately 46.6%

## Benchmark 2: time filter

The time-oriented query filters one day across all routes:

```sql
WHERE event_time >= '2026-03-15 00:00:00'
  AND event_time <  '2026-03-16 00:00:00'
```

### Primary-index pruning

| Table | Selected granules |
|---|---:|
| `events_route_first` | 25 of 41 |
| `events_time_first` | 2 of 41 |

### Measured results

| Table | Median duration | Median rows read | Median bytes read | Median memory |
|---|---:|---:|---:|---:|
| `events_route_first` | 7.5 ms | 204,800 | 2.27 MiB | 1.51 MiB |
| `events_time_first` | 5 ms | 16,384 | 320 KiB | 716.16 KiB |

For this query, the time-first design:

- reduced median execution time by approximately 33.3%
- reduced rows read by 92%
- reduced bytes read by approximately 86.2%
- reduced median memory usage by approximately 53.7%

## Benchmark 3: exact event-ID lookup

The third benchmark evaluates a high-cardinality equality lookup on
`event_id`.

The baseline and indexed tables use the same sorting key:

```sql
ORDER BY (event_time, origin, destination, event_id)
```

The indexed table adds the following data-skipping index:

```sql
INDEX idx_event_id_bloom toString(event_id)
    TYPE bloom_filter(0.01)
    GRANULARITY 1
```

The lookup uses an event ID generated deterministically from
`event-500000`:

```sql
WHERE toString(event_id) =
      toString(reinterpretAsUUID(MD5('event-500000')))
```

The query condition cache was disabled to prevent repeated executions
from reusing previous predicate results. The benchmark also used one
query-processing thread to reduce execution variability:

```sql
SETTINGS
    use_query_condition_cache = 0,
    max_threads = 1
```

### Data-skipping results

| Table | Selected parts | Selected granules |
|---|---:|---:|
| `events_time_first` | 4 of 4 | 122 of 122 |
| `events_time_first_bloom` | 1 of 4 | 1 of 122 |

The Bloom filter eliminated approximately 99.18% of the data granules.

### Storage overhead

| Table | Rows | Disk size |
|---|---:|---:|
| `events_time_first` | 1,000,000 | 28.57 MiB |
| `events_time_first_bloom` | 1,000,000 | 29.77 MiB |

The Bloom-filter index added approximately 1.20 MiB, increasing the
table size by approximately 4.2%.

### Measured results

| Table | Median duration | Median rows read | Median bytes read | Median memory |
|---|---:|---:|---:|---:|
| Without Bloom filter | 25.5 ms | 1,000,000 | 15.27 MiB | 5.63 MiB |
| With Bloom filter | 3 ms | 8,192 | 141.29 KiB | 178.57 KiB |

For this exact-ID lookup, the Bloom-filter index:

- reduced median execution time by approximately 88.2%
- reduced rows read by approximately 99.18%
- reduced bytes read by approximately 99.1%
- reduced median memory usage by approximately 96.9%

This result demonstrates that a Bloom-filter skipping index can be
effective for sparse, high-cardinality equality lookups when the target
column is not a useful prefix of the primary sorting key.

## Interpretation

The experiments demonstrate several direct trade-offs:

- Route-first ordering is more effective for broad time-range queries
  that select a specific route.
- Time-first ordering is more effective for narrow time-range queries
  that scan all routes.
- Time-first ordering compressed this deterministic synthetic dataset
  more effectively.
- A Bloom-filter index can substantially reduce data reads for sparse,
  high-cardinality equality lookups.
- Bloom filters introduce additional storage and write overhead.
- Sorting keys should be selected based on dominant production query
  patterns rather than generic recommendations.
- Secondary indexes should be added only when their pruning benefit
  justifies their storage and ingestion cost.

## Reproducing the benchmarks

The complete streaming stack must be running before executing the
benchmarks:

```bash
docker compose up --build -d
```

### 1. Create the ordering-key comparison tables

```bash
docker exec -i realtime-clickhouse sh -c \
'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery' \
< clickhouse/benchmarks/01_create_tables.sql
```

### 2. Load the deterministic dataset

```bash
docker exec -i realtime-clickhouse sh -c \
'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery' \
< clickhouse/benchmarks/02_load_data.sql
```

### 3. Run the ordering-key benchmark

```bash
bash clickhouse/benchmarks/run_ordering_key_benchmark.sh 10
```

The script performs warm-up executions, alternates table execution order,
assigns unique query IDs, flushes the ClickHouse query log, and reports
median resource usage from `system.query_log`.

### 4. Create the Bloom-filter comparison table

```bash
docker exec -i realtime-clickhouse sh -c \
'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery' \
< clickhouse/benchmarks/03_create_bloom_table.sql
```

### 5. Run the Bloom-filter benchmark

```bash
bash clickhouse/benchmarks/run_bloom_filter_benchmark.sh 10
```

The Bloom-filter benchmark disables the query condition cache and uses
one query-processing thread so repeated predicate results do not hide
the difference between indexed and non-indexed tables.

## Repository files

```text
clickhouse/benchmarks/
├── 01_create_tables.sql
├── 02_load_data.sql
├── 03_create_bloom_table.sql
├── README.md
├── run_bloom_filter_benchmark.sh
└── run_ordering_key_benchmark.sh
```

## Limitations

These measurements were produced in a local Docker environment using
synthetic data. Absolute execution times depend on hardware, container
resources, background activity, cache state, and ClickHouse version.

The generated data contains regular patterns that may compress differently
from production data. Bloom-filter effectiveness also depends on value
distribution, index granularity, false-positive probability, and query
selectivity.

The relative pruning and read-volume results are more useful than the
absolute millisecond values. Results should be reproduced with
representative production-like data before making schema decisions.