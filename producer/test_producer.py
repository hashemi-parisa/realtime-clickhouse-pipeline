import random
import unittest
import uuid
from datetime import datetime, timezone

from producer import generate_event


class GenerateEventTests(unittest.TestCase):
    def test_event_shape_and_ranges(self):
        fixed_time = datetime(2026, 10, 25, 12, 30, 45, 123000, tzinfo=timezone.utc)
        event = generate_event(random.Random(42), now=fixed_time)

        self.assertEqual(event["event_time"], "2026-10-25 12:30:45.123")
        uuid.UUID(event["event_id"])
        uuid.UUID(event["session_id"])
        self.assertNotEqual(event["origin"], event["destination"])
        self.assertGreater(event["result_count"], 0)
        self.assertGreater(event["min_price"], 0)
        self.assertGreaterEqual(event["response_time_ms"], 40)
        self.assertIn(event["cabin_class"], {"economy", "business"})

    def test_seed_makes_business_values_reproducible(self):
        first = generate_event(random.Random(7))
        second = generate_event(random.Random(7))

        ignored = {"event_time", "event_id", "session_id"}
        first_values = {key: value for key, value in first.items() if key not in ignored}
        second_values = {key: value for key, value in second.items() if key not in ignored}
        self.assertEqual(first_values, second_values)


if __name__ == "__main__":
    unittest.main()
