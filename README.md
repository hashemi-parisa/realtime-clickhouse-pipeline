# Real-Time Flight Search Analytics with Redpanda and ClickHouse

An end-to-end streaming analytics project that generates synthetic flight-search events, publishes them to a Kafka-compatible Redpanda topic, ingests them through the ClickHouse Kafka table engine, and incrementally builds hourly route metrics with materialized views.

The project is intentionally small enough to run locally, while demonstrating the same ingestion and analytical patterns used in larger data platforms.

## Architecture

```mermaid
flowchart LR
    A[Python producer] -->|JSON events| B[Redpanda topic]
    B --> C[ClickHouse Kafka table]
    C --> D[Raw-events materialized view]
    D --> E[MergeTree raw table]
    E --> F[Hourly materialized view]
    F --> G[AggregatingMergeTree metrics]
```

## What the project demonstrates

- Kafka-compatible streaming with Redpanda
- Native ClickHouse ingestion using the Kafka table engine
- Durable raw-event storage using `MergeTree`
- Incremental hourly aggregation using `AggregatingMergeTree`
- Materialized views as the streaming transformation layer
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
3. ClickHouse creates the raw and aggregate tables and their materialized views.
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

The full verification query set is available in [`clickhouse/queries/verification.sql`](clickhouse/queries/verification.sql).

### 5. Open Redpanda Console

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

### Partition and sort keys

The raw table is partitioned monthly and ordered by route, event time, and event ID. This supports time-range pruning and route-oriented queries while retaining a unique tie-breaker for events sharing a timestamp.

### Synthetic data only

The producer models realistic route, price, device, and response-time distributions without using proprietary data. A fixed seed makes tests and demonstrations repeatable.

## Current scope and next milestones

Implemented in the first end-to-end milestone:

- Redpanda topic initialization
- continuous synthetic event production
- Kafka Engine ingestion
- durable raw storage
- incremental hourly route metrics
- verification and ingestion-latency queries
- producer unit tests and CI

Planned next:

1. Add data-quality handling and a dead-letter path for malformed events.
2. Add reproducible performance experiments for ordering keys and skip indexes.
3. Add a lightweight dashboard for route, price, and ingestion-latency metrics.
4. Document delivery semantics, failure cases, and recovery procedures.

## License

MIT
