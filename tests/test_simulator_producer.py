import json
import os
import sys
import unittest
from datetime import UTC, datetime

sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "simulator"),
)

import producer  # noqa: E402


NOW = datetime(2026, 10, 4, 10, 0, 0, tzinfo=UTC)
RUN_ID = "UNIT-20261004"


class FakeKinesisClient:
    def __init__(self, fail_first_record_once: bool = False) -> None:
        self.calls = []
        self.fail_first_record_once = fail_first_record_once

    def put_records(self, **kwargs):
        self.calls.append(kwargs)
        results = []
        for index, _ in enumerate(kwargs["Records"]):
            if self.fail_first_record_once and len(self.calls) == 1 and index == 0:
                results.append(
                    {"ErrorCode": "ProvisionedThroughputExceededException"}
                )
            else:
                results.append(
                    {"SequenceNumber": str(index + 1), "ShardId": "shardId-000"}
                )
        return {"FailedRecordCount": sum("ErrorCode" in item for item in results), "Records": results}


class TestSimulatorProducer(unittest.TestCase):
    def generate(self, scenario: str):
        return producer.generate_scenario(
            scenario,
            now=NOW,
            run_id=RUN_ID,
            departure_seconds=75,
        )

    def test_normal_connection_has_complete_lifecycle_and_schema(self) -> None:
        events = self.generate("normal")

        self.assertEqual(7, len(events))
        self.assertEqual(
            list(producer.CONNECTION_JOURNEY),
            [e["event_type"] for e in events],
        )
        self.assertTrue(all(set(event) == producer.EVENT_KEYS for event in events))
        self.assertTrue(all("SYN" in event["bag_tag_id"] for event in events))
        self.assertTrue(all("SYN" in event["passenger_id"] for event in events))
        self.assertTrue(all(event["metadata"]["synthetic"] for event in events))
        self.assertTrue(
            all(event["priority_code"] == "NORMAL" for event in events)
        )

        departure = datetime.fromisoformat(
            events[0]["connection_departure_time"].replace("Z", "+00:00")
        )
        self.assertEqual(75, (departure - NOW).total_seconds())
        loaded = next(
            event
            for event in events
            if event["event_type"] == "CONNECTING_AIRCRAFT_LOADED"
        )
        loaded_at = datetime.fromisoformat(loaded["event_time"].replace("Z", "+00:00"))
        self.assertEqual(45, (departure - loaded_at).total_seconds())

    def test_isolated_stall_has_one_stalled_target_and_normal_peers(self) -> None:
        events = self.generate("isolated-stall")
        by_bag = self._by_bag(events)
        target = next(
            bag_events
            for bag_events in by_bag.values()
            if bag_events[0]["metadata"]["bag_role"] == "TARGET_STALLED"
        )
        peers = [
            bag_events
            for bag_events in by_bag.values()
            if bag_events[0]["metadata"]["bag_role"] == "CONNECTION_PEER"
        ]

        self.assertEqual(list(producer.LIFECYCLE[:5]), [e["event_type"] for e in target])
        self.assertEqual(4, len(peers))
        self.assertTrue(
            all(
                [e["event_type"] for e in peer]
                == list(producer.CONNECTION_JOURNEY)
                for peer in peers
            )
        )
        self.assertEqual(1, len({e["outbound_flight_id"] for e in events}))

    def test_zone_congestion_stalls_multiple_flights_in_one_zone(self) -> None:
        events = self.generate("zone-congestion")
        by_bag = self._by_bag(events)
        affected = [
            bag_events
            for bag_events in by_bag.values()
            if bag_events[0]["metadata"]["bag_role"] == "ZONE_AFFECTED"
        ]
        controls = [
            bag_events
            for bag_events in by_bag.values()
            if bag_events[0]["metadata"]["bag_role"] == "CONTROL_ZONE_NORMAL"
        ]

        self.assertEqual(12, len(by_bag))
        self.assertEqual(8, len(affected))
        self.assertEqual(4, len(controls))
        self.assertTrue(
            all(
                [e["event_type"] for e in bag_events]
                == list(producer.LIFECYCLE[:5])
                for bag_events in affected
            )
        )
        self.assertTrue(
            all(
                [e["event_type"] for e in bag_events]
                == list(producer.CONNECTION_JOURNEY)
                for bag_events in controls
            )
        )
        self.assertEqual(2, len({e["transfer_zone"] for e in events}))
        self.assertEqual(2, len({e["outbound_flight_id"] for e in events}))
        self.assertTrue(
            all(
                e["metadata"]["expected_risk_signal"]
                for bag_events in affected
                for e in bag_events
            )
        )
        self.assertTrue(
            all(
                not e["metadata"]["expected_risk_signal"]
                for bag_events in controls
                for e in bag_events
            )
        )

    def test_inbound_delay_keeps_baggage_progress_normal(self) -> None:
        events = self.generate("inbound-delay")
        by_bag = self._by_bag(events)

        self.assertEqual(3, len(by_bag))
        self.assertTrue(
            all(
                [e["event_type"] for e in bag_events]
                == list(producer.LIFECYCLE[:5])
                for bag_events in by_bag.values()
            )
        )
        self.assertTrue(all(e["priority_code"] == "HOT" for e in events))
        self.assertTrue(
            all(e["metadata"]["inbound_delay_minutes"] == 32 for e in events)
        )
        self.assertTrue(
            all(e["metadata"]["baggage_system_status"] == "NORMAL" for e in events)
        )
        self.assertTrue(
            all("scheduled_inbound_arrival_time" in e["metadata"] for e in events)
        )
        self.assertEqual(1, len({e["inbound_flight_id"] for e in events}))
        self.assertEqual(1, len({e["outbound_flight_id"] for e in events}))

    def test_each_run_has_isolated_flight_and_zone_identifiers(self) -> None:
        first = producer.generate_scenario(
            "isolated-stall", now=NOW, run_id="RUN-ONE", departure_seconds=75
        )
        second = producer.generate_scenario(
            "isolated-stall", now=NOW, run_id="RUN-TWO", departure_seconds=75
        )

        self.assertTrue(
            {e["inbound_flight_id"] for e in first}.isdisjoint(
                {e["inbound_flight_id"] for e in second}
            )
        )
        self.assertTrue(
            {e["outbound_flight_id"] for e in first}.isdisjoint(
                {e["outbound_flight_id"] for e in second}
            )
        )
        self.assertTrue(
            {e["transfer_zone"] for e in first}.isdisjoint(
                {e["transfer_zone"] for e in second}
            )
        )

    def test_publish_batches_and_partitions_by_bag_tag(self) -> None:
        events = self.generate("normal")
        client = FakeKinesisClient()

        published = producer.publish_events(client, events)

        self.assertEqual(len(events), published)
        self.assertEqual(1, len(client.calls))
        records = client.calls[0]["Records"]
        self.assertEqual(
            [event["bag_tag_id"] for event in events],
            [record["PartitionKey"] for record in records],
        )
        decoded = [json.loads(record["Data"]) for record in records]
        self.assertEqual(events, decoded)

    def test_publish_retries_only_failed_records(self) -> None:
        events = self.generate("normal")[:2]
        client = FakeKinesisClient(fail_first_record_once=True)

        published = producer.publish_events(client, events)

        self.assertEqual(2, published)
        self.assertEqual(2, len(client.calls))
        self.assertEqual(2, len(client.calls[0]["Records"]))
        self.assertEqual(1, len(client.calls[1]["Records"]))

    def test_departure_window_is_limited_to_synthetic_range(self) -> None:
        with self.assertRaises(ValueError):
            producer.generate_scenario("normal", now=NOW, departure_seconds=44)
        with self.assertRaises(ValueError):
            producer.generate_scenario("normal", now=NOW, departure_seconds=91)

    @staticmethod
    def _by_bag(events):
        result = {}
        for event in events:
            result.setdefault(event["bag_tag_id"], []).append(event)
        return result


if __name__ == "__main__":
    unittest.main()
