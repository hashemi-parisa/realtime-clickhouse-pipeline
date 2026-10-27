"""Generate synthetic flight-search events and publish them to Redpanda."""

from __future__ import annotations

import json
import logging
import os
import random
import signal
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import KafkaError, Producer


LOGGER = logging.getLogger("flight-search-producer")
RUNNING = True

ROUTES = (
    ("THR", "IST", 210.0),
    ("THR", "DXB", 180.0),
    ("THR", "DOH", 195.0),
    ("THR", "FRA", 420.0),
    ("IKA", "CDG", 455.0),
    ("IKA", "LHR", 485.0),
)
DEVICES = ("web", "android", "ios")
SOURCES = ("organic", "direct", "campaign", "metasearch")
CABIN_CLASSES = ("economy", "business")


def generate_event(rng: random.Random, now: datetime | None = None) -> dict[str, Any]:
    """Return one deterministic-shape event with synthetic values."""
    event_time = now or datetime.now(timezone.utc)
    origin, destination, base_price = rng.choice(ROUTES)
    cabin_class = rng.choices(CABIN_CLASSES, weights=(0.9, 0.1), k=1)[0]

    price_multiplier = 2.6 if cabin_class == "business" else 1.0
    min_price = round(base_price * price_multiplier * rng.uniform(0.82, 1.28), 2)
    result_count = rng.randint(4, 80)

    return {
        "event_time": event_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "event_id": str(uuid.uuid4()),
        "user_id": rng.randint(1, 50_000),
        "session_id": str(uuid.uuid4()),
        "origin": origin,
        "destination": destination,
        "cabin_class": cabin_class,
        "adults": rng.choices((1, 2, 3, 4), weights=(0.64, 0.27, 0.06, 0.03), k=1)[0],
        "children": rng.choices((0, 1, 2), weights=(0.82, 0.14, 0.04), k=1)[0],
        "result_count": result_count,
        "min_price": min_price,
        "response_time_ms": max(40, int(rng.gauss(430, 125))),
        "device": rng.choices(DEVICES, weights=(0.48, 0.31, 0.21), k=1)[0],
        "source": rng.choice(SOURCES),
    }


def delivery_report(error: KafkaError | None, message: Any) -> None:
    if error is not None:
        LOGGER.error("Delivery failed: %s", error)


def stop(_signum: int, _frame: Any) -> None:
    global RUNNING
    RUNNING = False


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    brokers = os.getenv("KAFKA_BROKERS", "localhost:19092")
    topic = os.getenv("KAFKA_TOPIC", "flight_search_events")
    rate = max(1, int(os.getenv("EVENT_RATE_PER_SECOND", "10")))
    seed = int(os.getenv("RANDOM_SEED", "42"))

    producer = Producer(
        {
            "bootstrap.servers": brokers,
            "client.id": "synthetic-flight-search-producer",
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "snappy",
        }
    )
    rng = random.Random(seed)
    interval = 1.0 / rate
    sent = 0

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    LOGGER.info("Publishing synthetic events to %s on %s at %s events/second", topic, brokers, rate)

    while RUNNING:
        started = time.monotonic()
        event = generate_event(rng)

        while RUNNING:
            try:
                producer.produce(
                    topic=topic,
                    key=event["session_id"],
                    value=json.dumps(event, separators=(",", ":")),
                    on_delivery=delivery_report,
                )
                break
            except BufferError:
                producer.poll(0.5)

        producer.poll(0)
        sent += 1
        if sent % 100 == 0:
            LOGGER.info("Queued %s events", sent)

        elapsed = time.monotonic() - started
        time.sleep(max(0.0, interval - elapsed))

    LOGGER.info("Stopping producer and flushing outstanding messages")
    remaining = producer.flush(10)
    if remaining:
        LOGGER.warning("%s messages were still queued after flush timeout", remaining)


if __name__ == "__main__":
    main()
