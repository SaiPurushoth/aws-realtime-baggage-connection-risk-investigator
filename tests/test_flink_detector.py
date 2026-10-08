import copy
import os
import sys
import unittest
from collections import defaultdict
from datetime import UTC, datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "simulator"))

from flink.detector_core import (  # noqa: E402
    ConnectionRiskDetector,
    format_timestamp,
    parse_timestamp,
)
import producer  # noqa: E402


NOW = datetime(2026, 10, 5, 6, 0, 0, tzinfo=UTC)
RUN_ID = "FLINK-UNIT-20261005"


class TestFlinkConnectionRiskDetector(unittest.TestCase):
    def scenario(self, name: str, departure_seconds: int = 75):
        return producer.generate_scenario(
            name,
            now=NOW,
            run_id=RUN_ID,
            departure_seconds=departure_seconds,
        )

    def run_in_event_order(self, events):
        detector = ConnectionRiskDetector(risk_buffer_seconds=30)
        incidents = []
        for event in sorted(events, key=lambda value: value["event_time"]):
            event_time_ms = parse_timestamp(event["event_time"], "event_time")
            incidents.extend(detector.advance_time(event_time_ms))
            incidents.extend(detector.process_event(event))
        if events:
            deadline_ms = max(
                parse_timestamp(event["estimated_departure_time"], "deadline")
                for event in events
            )
            incidents.extend(
                detector.advance_time(
                    max(detector.current_time_ms or 0, deadline_ms + 1_000)
                )
            )
        return detector, incidents

    def test_normal_connection_emits_no_risk(self) -> None:
        _, incidents = self.run_in_event_order(self.scenario("normal"))

        self.assertEqual([], incidents)

    def test_isolated_bag_stall_emits_one_risk_for_target(self) -> None:
        events = self.scenario("isolated-stall")
        _, incidents = self.run_in_event_order(events)

        self.assertEqual(1, len(incidents))
        target_bag = next(
            event["bag_tag_id"]
            for event in events
            if event["metadata"]["bag_role"] == "TARGET_STALLED"
        )
        self.assertEqual(target_bag, incidents[0]["bag_tag_id"])
        self.assertEqual("BAG_CONNECTION_RISK", incidents[0]["incident_type"])

    def test_zone_congestion_emits_one_risk_for_each_affected_bag(self) -> None:
        events = self.scenario("zone-congestion")
        _, incidents = self.run_in_event_order(events)

        affected_bags = {
            event["bag_tag_id"]
            for event in events
            if event["metadata"]["expected_risk_signal"]
        }
        self.assertEqual(8, len(incidents))
        self.assertEqual(affected_bags, {item["bag_tag_id"] for item in incidents})
        counts = defaultdict(int)
        for incident in incidents:
            counts[incident["bag_tag_id"]] += 1
        self.assertTrue(all(count == 1 for count in counts.values()))

    def test_inbound_delay_risks_critically_short_connection(self) -> None:
        events = self.scenario("inbound-delay", departure_seconds=45)
        _, incidents = self.run_in_event_order(events)

        self.assertEqual(3, len(incidents))
        self.assertTrue(
            all(incident["seconds_to_departure"] == 30 for incident in incidents)
        )
        self.assertTrue(
            all(
                incident["observed_context"]
                == {
                    "makeup_area_reached": False,
                    "aircraft_loaded": False,
                }
                for incident in incidents
            )
        )

    def test_aircraft_loaded_before_timer_emits_no_risk(self) -> None:
        events = self.scenario("normal")
        loaded = next(
            event
            for event in events
            if event["event_type"] == "CONNECTING_AIRCRAFT_LOADED"
        )
        transfer_events = [
            event
            for event in events
            if event["event_type"]
            in {
                "TRANSFER_ARRIVED",
                "TRANSFER_SORTER_SCAN",
                "CONNECTING_AIRCRAFT_LOADED",
            }
        ]
        detector, incidents = self.run_in_event_order(transfer_events)

        self.assertEqual([], incidents)
        self.assertTrue(detector.states[loaded["bag_tag_id"]].aircraft_loaded)

    def test_duplicate_events_and_repeated_time_advances_do_not_duplicate_risk(self) -> None:
        events = [
            event
            for event in self.scenario("isolated-stall")
            if event["metadata"]["bag_role"] == "TARGET_STALLED"
        ]
        duplicated = [item for event in events for item in (event, copy.deepcopy(event))]
        detector, incidents = self.run_in_event_order(duplicated)
        deadline_ms = parse_timestamp(
            events[0]["connection_departure_time"], "deadline"
        )
        incidents.extend(detector.advance_time(deadline_ms + 60_000))

        self.assertEqual(1, len(incidents))

    def test_out_of_order_events_do_not_regress_state_or_create_false_risk(self) -> None:
        events = self.scenario("normal")
        by_type = {event["event_type"]: event for event in events}
        detector = ConnectionRiskDetector(risk_buffer_seconds=30)
        detector.advance_time(NOW)

        detector.process_event(by_type["CONNECTING_AIRCRAFT_LOADED"])
        detector.process_event(by_type["TRANSFER_SORTER_SCAN"])
        detector.process_event(by_type["TRANSFER_ARRIVED"])
        deadline_ms = parse_timestamp(
            by_type["TRANSFER_ARRIVED"]["connection_departure_time"], "deadline"
        )
        incidents = detector.advance_time(deadline_ms)
        state = detector.states[by_type["TRANSFER_ARRIVED"]["bag_tag_id"]]

        self.assertEqual([], incidents)
        self.assertTrue(state.aircraft_loaded)
        self.assertEqual("CONNECTING_AIRCRAFT_LOADED", state.last_event_type)

    def test_latest_estimated_departure_reschedules_risk_timer(self) -> None:
        events = self.scenario("isolated-stall")
        target = [
            event
            for event in events
            if event["metadata"]["bag_role"] == "TARGET_STALLED"
        ]
        detector = ConnectionRiskDetector(risk_buffer_seconds=30)
        for event in target:
            detector.process_event(event)

        original_deadline = parse_timestamp(
            target[-1]["estimated_departure_time"], "deadline"
        )
        update = copy.deepcopy(target[-1])
        update["event_id"] += "-UPDATE"
        update["event_time"] = format_timestamp(
            parse_timestamp(update["event_time"], "event_time") + 1_000
        )
        extended_deadline = original_deadline + 45_000
        update["estimated_departure_time"] = format_timestamp(extended_deadline)
        detector.process_event(update)

        self.assertEqual([], detector.advance_time(original_deadline - 30_000))
        incidents = detector.advance_time(extended_deadline - 30_000)
        self.assertEqual(1, len(incidents))
        self.assertEqual(
            format_timestamp(extended_deadline),
            incidents[0]["connection_departure_time"],
        )

    def test_incident_contract_is_deterministic_and_contains_no_conclusion(self) -> None:
        events = self.scenario("isolated-stall")
        _, incidents = self.run_in_event_order(events)
        incident = incidents[0]

        self.assertEqual(
            f"{incident['bag_tag_id']}-{incident['outbound_flight_id']}-"
            "BAG_CONNECTION_RISK",
            incident["incident_id"],
        )
        self.assertEqual(
            {
                "incident_id",
                "bag_tag_id",
                "incident_type",
                "detected_at",
                "airport_code",
                "outbound_flight_id",
                "transfer_zone",
                "last_scan_type",
                "last_scan_location",
                "last_scan_time",
                "connection_departure_time",
                "seconds_to_departure",
                "observed_context",
            },
            set(incident),
        )
        serialized = str(incident).lower()
        self.assertNotIn("root_cause", serialized)
        self.assertNotIn("recommended_action", serialized)
        self.assertNotIn("isolated", serialized)
        self.assertNotIn("sorter_failure", serialized)

    def test_restored_emitted_state_prevents_replay_incident(self) -> None:
        events = [
            event
            for event in self.scenario("isolated-stall")
            if event["metadata"]["bag_role"] == "TARGET_STALLED"
        ]
        detector, incidents = self.run_in_event_order(events)
        self.assertEqual(1, len(incidents))

        restored = ConnectionRiskDetector(risk_buffer_seconds=30)
        restored.states = copy.deepcopy(detector.states)
        restored.current_time_ms = detector.current_time_ms
        replay_incidents = restored.process_events(copy.deepcopy(events))
        replay_incidents.extend(
            restored.advance_time((restored.current_time_ms or 0) + 60_000)
        )

        self.assertEqual([], replay_incidents)

    def test_makeup_area_before_timer_is_sufficient_progress(self) -> None:
        events = self.scenario("normal")
        before_loaded = [
            event
            for event in events
            if event["event_type"] != "CONNECTING_AIRCRAFT_LOADED"
        ]
        _, incidents = self.run_in_event_order(before_loaded)

        self.assertEqual([], incidents)

    def test_invalid_risk_buffer_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            ConnectionRiskDetector(risk_buffer_seconds=0)


if __name__ == "__main__":
    unittest.main()
