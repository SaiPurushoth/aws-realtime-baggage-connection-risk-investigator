import os
import sys
import unittest
from collections import defaultdict
from datetime import UTC, datetime

sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "simulator"),
)

import producer  # noqa: E402
import seed_history  # noqa: E402


class TestSeedHistory(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.events, cls.summary = seed_history.generate_history(
            journey_count=750,
            history_days=7,
            random_seed=20261004,
            now=datetime(2026, 10, 4, 12, 0, tzinfo=UTC),
            run_id="UNIT-HISTORY",
        )
        cls.by_bag = defaultdict(list)
        for event in cls.events:
            cls.by_bag[event["bag_tag_id"]].append(event)

    def test_generates_requested_scale_and_synthetic_schema(self) -> None:
        self.assertEqual(750, len(self.by_bag))
        self.assertEqual(750, self.summary["journey_count"])
        self.assertTrue(3750 <= len(self.events) <= 6000)
        self.assertTrue(all(set(event) == producer.EVENT_KEYS for event in self.events))
        self.assertTrue(all("SYN" in event["bag_tag_id"] for event in self.events))
        self.assertTrue(all("SYN" in event["passenger_id"] for event in self.events))
        self.assertTrue(all(event["metadata"]["synthetic"] for event in self.events))

    def test_distribution_has_multiple_operational_dimensions(self) -> None:
        self.assertEqual(8, self.summary["inbound_flights"])
        self.assertEqual(12, self.summary["outbound_flights"])
        self.assertEqual(4, self.summary["transfer_zones"])
        self.assertGreaterEqual(self.summary["gates"], 10)
        durations = {
            event["metadata"]["planned_connection_minutes"]
            for event in self.events
        }
        self.assertGreater(len(durations), 20)

    def test_most_bags_connect_and_failures_are_occasional(self) -> None:
        self.assertGreater(self.summary["connection_success_percent"], 85)
        self.assertLess(self.summary["connection_success_percent"], 95)
        self.assertEqual(
            {
                "INBOUND_DELAY": 75,
                "ISOLATED_STALL": 30,
                "NORMAL": 615,
                "ZONE_CONGESTION": 30,
            },
            self.summary["patterns"],
        )
        stalled = [
            events
            for events in self.by_bag.values()
            if "TRANSFER_SORTER_SCAN" in {event["event_type"] for event in events}
            and "CONNECTING_AIRCRAFT_LOADED"
            not in {event["event_type"] for event in events}
        ]
        self.assertGreater(len(stalled), 50)
        self.assertLess(len(stalled), 120)

    def test_events_do_not_embed_agent_conclusions_or_risk_incidents(self) -> None:
        forbidden_metadata = {
            "scenario",
            "root_cause",
            "classification",
            "recommended_action",
            "connection_success",
        }
        self.assertTrue(
            all(
                forbidden_metadata.isdisjoint(event["metadata"])
                for event in self.events
            )
        )
        self.assertTrue(
            all(event["event_type"] in producer.LIFECYCLE for event in self.events)
        )

    def test_zone_congestion_is_visible_as_clustered_event_behavior(self) -> None:
        stalled_buckets = defaultdict(set)
        for bag_tag_id, events in self.by_bag.items():
            event_types = {event["event_type"] for event in events}
            if "CONNECTING_AIRCRAFT_LOADED" in event_types:
                continue
            sorter = next(
                (event for event in events if event["event_type"] == "TRANSFER_SORTER_SCAN"),
                None,
            )
            if sorter is None:
                continue
            bucket = sorter["event_time"][:15]
            stalled_buckets[(sorter["transfer_zone"], bucket)].add(bag_tag_id)

        self.assertTrue(any(len(bags) >= 3 for bags in stalled_buckets.values()))

    def test_journey_count_boundaries_are_enforced(self) -> None:
        with self.assertRaises(ValueError):
            seed_history.generate_history(journey_count=499)
        with self.assertRaises(ValueError):
            seed_history.generate_history(journey_count=1001)


if __name__ == "__main__":
    unittest.main()
