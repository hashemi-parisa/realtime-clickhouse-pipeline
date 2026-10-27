# Real-Time Flight Search Analytics with Redpanda and ClickHouse

An end-to-end streaming analytics project that generates synthetic flight-search events, publishes them to a Kafka-compatible Redpanda topic, ingests valid events through the ClickHouse Kafka table engine, quarantines malformed messages, and incrementally builds hourly route metrics with materialized views.

The project is intentionally small enough to run locally, while demonstrating the same ingestion and analytical patterns used in larger data platforms.

## Architecture

```mermaid
flowchart LR
    A[Python producer] -->|JSON events| B[Redpanda topic]
    B --> C[ClickHouse Kafka table]

    C -->|Valid events| D[Raw-events materialized view]
    D --> E[MergeTree raw table]
    E --> F[Hourly materialized view]
    F --> G[AggregatingMergeTree metrics]

    C -->|Malformed events| H[Dead-letter materialized view]
    H --> I[Dead-letter MergeTree table]
```

## What the project demonstrates

- Kafka-compatible streaming with Redpanda
- Native ClickHouse ingestion using the Kafka table engine
- Durable raw-event storage using `MergeTree`
- Incremental hourly aggregation using `AggregatingMergeTree`
- Materialized views as the streaming transformation layer
- Data-quality routing for malformed events
- Dead-letter storage with Kafka topic, partition, offset, and parsing error
- Partitioning and ordering choices for time-series analytical data
- Reproducible synthetic data generation
- Container health checks and dependency ordering
- Automated Python tests with GitHub Actions

All generated data is synthetic. The schema is inspired by common travel analytics use cases and contains no company or customer data.

## Event model

Each event represents one flight search and includes:

- event and session identifiers
- event timestamp and synthetic user identifier
- origin, destination, and cabin class
- passenger counts
- number of available results and minimum displayed price
- response time, device, and acquisition source

The producer uses a fixed random seed by default, making the business-value distribution reproducible across runs. Event IDs and timestamps remain unique.

## Quick start

### 1. Create the local environment file

Linux or macOS:

```bash
cp .env.example .env
```

PowerShell:

```powershell
Copy-Item .env.example .env
```

Change `CLICKHOUSE_PASSWORD` in `.env` before starting the stack.

### 2. Start the complete pipeline

```bash
docker compose up --build -d
```

The startup sequence is:

1. Redpanda becomes healthy.
2. `redpanda-init` creates the `flight_search_events` topic with three partitions.
3. ClickHouse creates the raw, aggregate, and dead-letter tables and their materialized views.
4. The Python producer begins publishing synthetic events.

### 3. Watch the producer

```bash
docker compose logs -f producer
```

### 4. Verify data in ClickHouse

Open the ClickHouse client:

```bash
docker exec -it realtime-clickhouse clickhouse-client \
  --user parisa \
  --password YOUR_PASSWORD
```

Then run:

```sql
SELECT count(), min(event_time), max(event_time)
FROM realtime.flight_search_events;
```

The full verification query set, including dead-letter inspection and ingestion-health checks, is available in [`clickhouse/queries/verification.sql`](clickhouse/queries/verification.sql).

### 5. Test malformed-event handling

Publish a deliberately invalid event from Git Bash, Linux, or macOS:

```bash
printf '%s\n' '{"event_time":"not-a-date","event_id":"not-a-uuid","origin":"THR","destination":"IST"}' \
| docker exec -i realtime-redpanda rpk topic produce flight_search_events --brokers redpanda:9092
```

Inspect the captured message:

```sql
SELECT
    failed_at,
    topic,
    partition,
    offset,
    raw_message,
    error_message
FROM realtime.flight_search_dead_letters
ORDER BY failed_at DESC
LIMIT 5;
```

Malformed messages are excluded from the main raw table. Their payload, parsing error, and Kafka metadata are retained for 30 days for investigation or replay.

### 6. Open Redpanda Console

Redpanda Console is available at:

```text
http://localhost:8080
```

ClickHouse HTTP is available at `http://localhost:18123`, and the native protocol is exposed on port `19001`.

## Useful commands

```bash
# Show service health and status
docker compose ps

# Follow ClickHouse and producer logs
docker compose logs -f clickhouse producer

# Stop containers but keep data volumes
docker compose down

# Remove containers and local volumes for a clean restart
docker compose down -v
```

## Repository structure

```text
.
├── .github/workflows/python-tests.yml
├── clickhouse
│   ├── init/01_pipeline.sql
│   ├── init/02_data_quality.sql
│   └── queries/verification.sql
├── producer
│   ├── Dockerfile
│   ├── producer.py
│   ├── requirements.txt
│   └── test_producer.py
├── .env.example
├── docker-compose.yml
└── README.md
```

## Design decisions

### Native Kafka ingestion

ClickHouse consumes directly from Redpanda through a Kafka-engine table. A materialized view moves each consumed block into a durable `MergeTree` table, avoiding a separate custom consumer for the database write path.

### Raw and aggregate layers

Raw events remain available for debugging and new analytical questions. A second materialized view maintains hourly route aggregates so common route-level metrics do not need to scan the entire raw table.

### Data quality and dead-letter handling

The Kafka table uses `kafka_handle_error_mode = 'stream'`, which exposes parsing failures through the `_raw_message` and `_error` virtual columns. One materialized view filters valid events into the main raw table, while another routes malformed messages into a dedicated `MergeTree` dead-letter table together with their topic, partition, offset, and Kafka timestamp. This prevents a malformed message from stopping valid ingestion and preserves enough context for diagnosis and replay.

### Partition and sort keys

The raw table is partitioned monthly and ordered by route, event time, and event ID. This supports time-range pruning and route-oriented queries while retaining a unique tie-breaker for events sharing a timestamp.

### Synthetic data only

The producer models realistic route, price, device, and response-time distributions without using proprietary data. A fixed seed makes tests and demonstrations repeatable.

## Current scope and next milestones

Milestone 1 — end-to-end streaming pipeline:

- Redpanda topic initialization
- continuous synthetic event production
- Kafka Engine ingestion
- durable raw storage
- incremental hourly route metrics
- verification and ingestion-latency queries
- producer unit tests and CI

Milestone 2 — data-quality handling:

- valid and malformed event routing
- durable dead-letter storage
- Kafka metadata and parsing-error capture
- 30-day retention for malformed messages
- verification that valid ingestion continues after parsing failures

Planned next:

1. Add reproducible performance experiments for ordering keys and skip indexes.
2. Add a lightweight dashboard for route, price, and ingestion-latency metrics.
3. Document delivery semantics, failure cases, and recovery procedures.

## License

MIT
