from __future__ import annotations

import json
import unittest
from typing import Any

from dashboard.service import (
    BagGuardService,
    OperationsSnapshot,
    ScenarioRun,
    departure_seconds,
    parse_evidence,
    parse_incident_context,
    time_sensitive_reason,
)


class _Payload:
    def __init__(self, body: dict[str, Any]) -> None:
        self._body = json.dumps(body).encode("utf-8")

    def read(self) -> bytes:
        return self._body


class _LambdaClient:
    def __init__(self, results: dict[str, Any]) -> None:
        self.results = results
        self.calls: list[dict[str, Any]] = []

    def invoke(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        request = json.loads(kwargs["Payload"])
        action = request["action"]
        return {
            "StatusCode": 200,
            "Payload": _Payload(
                {"ok": True, "action": action, "result": self.results[action]}
            ),
        }


class _KinesisClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def put_records(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        return {
            "FailedRecordCount": 0,
            "Records": [
                {"SequenceNumber": str(index), "ShardId": "shard-000"}
                for index, _ in enumerate(kwargs["Records"], start=1)
            ],
        }


class DashboardServiceTests(unittest.TestCase):
    def test_departure_countdown_accepts_clickhouse_naive_utc_timestamp(self) -> None:
        timeline = [{"estimated_departure_time": "2999-01-01 12:00:00.000"}]

        seconds = departure_seconds(timeline)

        self.assertIsInstance(seconds, int)
        self.assertGreater(seconds, 0)

    def test_departure_countdown_accepts_timezone_aware_timestamp(self) -> None:
        timeline = [{"estimated_departure_time": "2999-01-01T12:00:00.000Z"}]

        seconds = departure_seconds(timeline)

        self.assertIsInstance(seconds, int)
        self.assertGreater(seconds, 0)

    def test_scenario_publishes_synthetic_events_partitioned_by_bag(self) -> None:
        kinesis = _KinesisClient()
        service = BagGuardService(
            region_name="us-east-1",
            stream_name="bagguard-baggage-events-prod",
            kinesis_client=kinesis,
            lambda_client=_LambdaClient({}),
        )

        run = service.start_scenario("isolated-stall", departure_seconds=60)

        self.assertTrue(run.expected_risk)
        self.assertTrue(run.incident_id.endswith("-BAG_CONNECTION_RISK"))
        self.assertEqual(run.published_events, len(kinesis.calls[0]["Records"]))
        for record in kinesis.calls[0]["Records"]:
            event = json.loads(record["Data"])
            self.assertEqual(event["bag_tag_id"], record["PartitionKey"])
            self.assertTrue(event["metadata"]["synthetic"])

    def test_snapshot_reads_only_allow_listed_adapter_actions(self) -> None:
        run = ScenarioRun(
            scenario_key="isolated-stall",
            scenario_label="Isolated Bag Stall",
            run_id="RUN-1",
            bag_tag_id="BG-SYN-1",
            incident_id="BG-SYN-1-AI-SYN-802-BAG_CONNECTION_RISK",
            outbound_flight_id="AI-SYN-802",
            airport_code="BGA",
            transfer_zone="T2-RUN-1",
            expected_risk=True,
            published_events=5,
        )
        incident = {
            "incident_id": run.incident_id,
            "bag_tag_id": run.bag_tag_id,
            "incident_type": "BAG_CONNECTION_RISK",
            "outbound_flight_id": run.outbound_flight_id,
            "transfer_zone": run.transfer_zone,
            "context_json": json.dumps(
                {
                    "seconds_to_departure": 30,
                    "observed_context": {
                        "makeup_area_reached": False,
                        "aircraft_loaded": False,
                    },
                }
            ),
        }
        investigation = {
            "incident_id": run.incident_id,
            "evidence_json": '["Target stalled", "Peers progressed"]',
        }
        lambda_client = _LambdaClient(
            {
                "get_bag_timeline": [{"event_type": "TRANSFER_SORTER_SCAN"}],
                "get_recent_connection_risks": [incident],
                "get_investigation_result": investigation,
            }
        )
        service = BagGuardService(
            kinesis_client=_KinesisClient(), lambda_client=lambda_client
        )

        snapshot = service.get_snapshot(run)

        actions = [json.loads(call["Payload"])["action"] for call in lambda_client.calls]
        self.assertEqual(
            [
                "get_bag_timeline",
                "get_recent_connection_risks",
                "get_investigation_result",
            ],
            actions,
        )
        self.assertEqual(1, snapshot.flight_risk_bags)
        self.assertEqual(1, snapshot.flight_zone_risk_bags)
        self.assertEqual(1, snapshot.recent_zone_risk_count)
        self.assertEqual(30, parse_incident_context(snapshot.incident)["seconds_to_departure"])
        self.assertEqual(["Target stalled", "Peers progressed"], parse_evidence(investigation))
        self.assertIn("30 seconds", time_sensitive_reason(snapshot))

    def test_context_metrics_deduplicate_incidents_and_bags(self) -> None:
        run = ScenarioRun(
            "zone-congestion",
            "Transfer Zone Congestion",
            "RUN-2",
            "BG-1",
            "INC-1",
            "AI-802",
            "BGB",
            "T2",
            True,
            10,
        )
        risks = [
            {
                "incident_id": "INC-1",
                "bag_tag_id": "BG-1",
                "outbound_flight_id": "AI-802",
                "transfer_zone": "T2",
            },
            {
                "incident_id": "INC-1",
                "bag_tag_id": "BG-1",
                "outbound_flight_id": "AI-802",
                "transfer_zone": "T2",
            },
            {
                "incident_id": "INC-2",
                "bag_tag_id": "BG-2",
                "outbound_flight_id": "AI-803",
                "transfer_zone": "T2",
            },
        ]
        snapshot = OperationsSnapshot(run, [], risks[0], None, risks)

        self.assertEqual(1, snapshot.flight_risk_bags)
        self.assertEqual(1, snapshot.flight_zone_risk_bags)
        self.assertEqual(2, snapshot.recent_zone_risk_count)


if __name__ == "__main__":
    unittest.main()
