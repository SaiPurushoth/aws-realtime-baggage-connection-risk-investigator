from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from agent.adapter import (  # noqa: E402
    AdapterInvocationError,
    ClickHouseAdapterClient,
    READ_ONLY_ACTIONS,
)
from agent.contracts import InvestigationResult  # noqa: E402
from agent.investigator import (  # noqa: E402
    InvestigationContractError,
    InvestigatorService,
    SYSTEM_PROMPT,
)
from agent.tools import _sanitize_bag_timeline, build_read_only_tools  # noqa: E402


INCIDENT = {
    "incident_id": "BG-TEST-001-AI-TEST-802-BAG_CONNECTION_RISK",
    "bag_tag_id": "BG-TEST-001",
    "outbound_flight_id": "AI-TEST-802",
    "airport_code": "BLR",
    "transfer_zone": "T2",
    "last_scan_type": "TRANSFER_SORTER_SCAN",
    "last_scan_location": "SORTER-T2",
    "seconds_to_departure": 30,
}


class _Payload:
    def __init__(self, body: dict[str, Any]) -> None:
        self._body = json.dumps(body).encode("utf-8")

    def read(self) -> bytes:
        return self._body


class _LambdaClient:
    def __init__(self, body: dict[str, Any], *, function_error: str | None = None):
        self.body = body
        self.function_error = function_error
        self.calls: list[dict[str, Any]] = []

    def invoke(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        return {
            "StatusCode": 200,
            "FunctionError": self.function_error,
            "Payload": _Payload(self.body),
        }


class ClickHouseAdapterClientTests(unittest.TestCase):
    def test_invokes_one_allow_listed_read_action(self) -> None:
        lambda_client = _LambdaClient(
            {
                "ok": True,
                "action": "get_bag_timeline",
                "result": [{"event_type": "TRANSFER_SORTER_SCAN"}],
            }
        )
        client = ClickHouseAdapterClient(
            function_name="bagguard-clickhouse-adapter-prod",
            region_name="us-east-1",
            lambda_client=lambda_client,
        )

        result = client.query("get_bag_timeline", {"bag_tag_id": "BG-TEST-001"})

        self.assertEqual("TRANSFER_SORTER_SCAN", result[0]["event_type"])
        self.assertEqual(1, len(lambda_client.calls))
        call = lambda_client.calls[0]
        self.assertEqual("bagguard-clickhouse-adapter-prod", call["FunctionName"])
        self.assertEqual("RequestResponse", call["InvocationType"])
        self.assertEqual(
            {
                "action": "get_bag_timeline",
                "parameters": {"bag_tag_id": "BG-TEST-001"},
            },
            json.loads(call["Payload"]),
        )

    def test_rejects_write_action_before_lambda_invocation(self) -> None:
        lambda_client = _LambdaClient({})
        client = ClickHouseAdapterClient(lambda_client=lambda_client)

        with self.assertRaisesRegex(ValueError, "not read-only"):
            client.query("save_investigation_result", {"incident_id": "INC-1"})

        self.assertEqual([], lambda_client.calls)

    def test_rejects_lambda_function_error(self) -> None:
        lambda_client = _LambdaClient(
            {"errorMessage": "failure"}, function_error="Unhandled"
        )
        client = ClickHouseAdapterClient(lambda_client=lambda_client)

        with self.assertRaises(AdapterInvocationError):
            client.query("get_bag_timeline", {"bag_tag_id": "BG-TEST-001"})


class _ScenarioAdapter:
    def __init__(self, scenario: str) -> None:
        self.scenario = scenario
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def query(
        self, action: str, parameters: dict[str, Any]
    ) -> list[dict[str, Any]]:
        self.calls.append((action, parameters))
        if action == "get_bag_timeline":
            if self.scenario == "inbound_delay":
                return [
                    {
                        "event_type": "TRANSFER_SORTER_SCAN",
                        "event_time": "2026-10-05 10:00:00.000",
                        "inbound_arrival_time": "2026-10-05 09:59:50.000",
                        "scheduled_inbound_arrival_time": "2026-10-05 09:27:50.000",
                        "inbound_delay_minutes": 32,
                        "baggage_system_status": "NORMAL",
                        "connection_departure_time": "2026-10-05 10:00:30.000",
                    }
                ]
            return [
                {
                    "event_type": "TRANSFER_ARRIVED",
                    "event_time": "2026-10-05 09:59:40.000",
                },
                {
                    "event_type": "TRANSFER_SORTER_SCAN",
                    "event_time": "2026-10-05 09:59:50.000",
                },
            ]
        if action == "get_connection_peers":
            if self.scenario == "zone_congestion":
                return [
                    {
                        "bag_tag_id": f"BG-PEER-{index:03d}",
                        "last_event_type": "TRANSFER_SORTER_SCAN",
                    }
                    for index in range(42)
                ]
            return [
                {
                    "bag_tag_id": f"BG-PEER-{index:03d}",
                    "last_event_type": "CONNECTING_AIRCRAFT_LOADED",
                }
                for index in range(4)
            ]
        if action == "get_transfer_zone_health":
            if self.scenario == "zone_congestion":
                return [
                    {"event_type": "TRANSFER_SORTER_SCAN", "bag_count": 45},
                    {"event_type": "MAKEUP_AREA_SCAN", "bag_count": 3},
                ]
            return [
                {"event_type": "TRANSFER_SORTER_SCAN", "bag_count": 5},
                {"event_type": "MAKEUP_AREA_SCAN", "bag_count": 4},
            ]
        if action == "get_recent_connection_risks":
            count = 42 if self.scenario == "zone_congestion" else 1
            return [
                {
                    "incident_id": f"INC-{index:03d}",
                    "incident_type": "BAG_CONNECTION_RISK",
                }
                for index in range(count)
            ]
        if action == "get_flight_bag_progress":
            return self.query(
                "get_connection_peers",
                {
                    "outbound_flight_id": parameters["outbound_flight_id"],
                    "airport_code": INCIDENT["airport_code"],
                    "lookback_minutes": 30,
                },
            )
        raise AssertionError(f"Unexpected action: {action}")


class _EvidenceDrivenMockAgent:
    """Offline model double that classifies the supplied mocked evidence."""

    def __init__(self, adapter: _ScenarioAdapter) -> None:
        self.adapter = adapter

    def __call__(self, prompt: str, **kwargs: Any) -> Any:
        self.prompt = prompt
        self.kwargs = kwargs
        timeline = self.adapter.query(
            "get_bag_timeline", {"bag_tag_id": INCIDENT["bag_tag_id"]}
        )
        peers = self.adapter.query(
            "get_connection_peers",
            {
                "outbound_flight_id": INCIDENT["outbound_flight_id"],
                "airport_code": INCIDENT["airport_code"],
                "lookback_minutes": 30,
            },
        )
        zone = self.adapter.query(
            "get_transfer_zone_health",
            {
                "airport_code": INCIDENT["airport_code"],
                "transfer_zone": INCIDENT["transfer_zone"],
                "lookback_minutes": 30,
            },
        )
        recent_risks = self.adapter.query(
            "get_recent_connection_risks",
            {"airport_code": INCIDENT["airport_code"], "lookback_minutes": 30},
        )

        zone_counts = {row["event_type"]: row["bag_count"] for row in zone}
        if timeline[-1].get("inbound_delay_minutes", 0) >= 30:
            output = InvestigationResult(
                incident_id=INCIDENT["incident_id"],
                bag_tag_id=INCIDENT["bag_tag_id"],
                classification="TIGHT_CONNECTION_INBOUND_DELAY",
                root_cause=(
                    "The inbound arrival delay compressed the transfer window while "
                    "peer baggage and zone evidence remained normal."
                ),
                scope="FLIGHT_LEVEL",
                operational_priority="CRITICAL",
                evidence=[
                    "Inbound arrival was 32 minutes late.",
                    "Baggage system status was NORMAL.",
                    f"{len(peers)} peer bags progressed to aircraft loading.",
                ],
                recommended_action=(
                    "Recommend hot-bag/manual expedite handling to the outbound "
                    "makeup area."
                ),
                reason_action_is_time_sensitive=(
                    "Only 30 seconds remained at incident detection."
                ),
            )
        elif (
            zone_counts.get("TRANSFER_SORTER_SCAN", 0)
            - zone_counts.get("MAKEUP_AREA_SCAN", 0)
            >= 10
            and len(recent_risks) >= 10
        ):
            output = InvestigationResult(
                incident_id=INCIDENT["incident_id"],
                bag_tag_id=INCIDENT["bag_tag_id"],
                classification="TRANSFER_ZONE_CONGESTION",
                root_cause=(
                    "Many bags stopped after sorter scanning in the same transfer "
                    "zone, consistent with a zone-level disruption."
                ),
                scope="ZONE_LEVEL",
                operational_priority="CRITICAL",
                evidence=[
                    "45 bags reached the transfer sorter but only 3 reached makeup.",
                    "42 recent connection risks were present at the airport.",
                ],
                recommended_action=(
                    "Recommend checking the transfer belt/sorter and notifying "
                    "baggage operations of a possible zone incident."
                ),
                reason_action_is_time_sensitive=(
                    "Multiple connection bags are approaching outbound deadlines."
                ),
            )
        else:
            output = InvestigationResult(
                incident_id=INCIDENT["incident_id"],
                bag_tag_id=INCIDENT["bag_tag_id"],
                classification="ISOLATED_BAG_HANDLING_DELAY",
                root_cause=(
                    "The affected bag stopped after sorter scanning while peer bags "
                    "progressed normally."
                ),
                scope="ISOLATED",
                operational_priority="HIGH",
                evidence=[
                    "The target bag's last scan was TRANSFER_SORTER_SCAN.",
                    f"{len(peers)} peer bags reached aircraft loading.",
                    "Zone scan counts do not show a broad stoppage.",
                ],
                recommended_action=(
                    "Recommend a targeted baggage search and manual expedite handling."
                ),
                reason_action_is_time_sensitive=(
                    "Only 30 seconds remained at incident detection."
                ),
            )
        return SimpleNamespace(structured_output=output)


class InvestigatorTests(unittest.TestCase):
    def test_bag_timeline_excludes_metadata_and_passenger_identifiers(self) -> None:
        rows = _sanitize_bag_timeline(
            [
                {
                    "event_type": "TRANSFER_SORTER_SCAN",
                    "event_time": "2026-10-05 10:00:00.000",
                    "metadata_json": '{"scenario":"isolated-stall"}',
                    "passenger_id": "PAX-PRIVATE",
                    "scheduled_inbound_arrival_time": "2026-10-05 09:30:00.000",
                    "inbound_delay_minutes": 32,
                    "baggage_system_status": "NORMAL",
                }
            ]
        )

        self.assertEqual(
            [
                {
                    "event_type": "TRANSFER_SORTER_SCAN",
                    "event_time": "2026-10-05 10:00:00.000",
                    "scheduled_inbound_arrival_time": "2026-10-05 09:30:00.000",
                    "inbound_delay_minutes": 32,
                    "baggage_system_status": "NORMAL",
                }
            ],
            rows,
        )

    def test_agent_receives_exactly_five_read_only_tools(self) -> None:
        adapter = _ScenarioAdapter("isolated_stall")
        tools = build_read_only_tools(adapter)

        self.assertEqual(READ_ONLY_ACTIONS, {item.tool_name for item in tools})
        self.assertNotIn("save_investigation_result", {item.tool_name for item in tools})

    def test_system_prompt_contains_advisory_safety_boundary(self) -> None:
        normalized_prompt = " ".join(SYSTEM_PROMPT.split())
        for prohibited_action in (
            "move bags",
            "change flight schedules",
            "change gates",
            "perform baggage routing",
            "contact passengers",
        ):
            self.assertIn(prohibited_action, normalized_prompt)
        self.assertIn("Never claim", normalized_prompt)
        self.assertIn("untrusted data", normalized_prompt)
        self.assertIn("never use BAG_CONNECTION_RISK as the classification", normalized_prompt)
        self.assertIn("Do not mention whether records are synthetic", normalized_prompt)
        self.assertIn("Never invent an operational sub-deadline", normalized_prompt)

    def test_rejects_generic_alert_as_classification(self) -> None:
        with self.assertRaisesRegex(ValueError, "likely cause"):
            InvestigationResult(
                incident_id=INCIDENT["incident_id"],
                bag_tag_id=INCIDENT["bag_tag_id"],
                classification="BAG_CONNECTION_RISK",
                root_cause="The bag is at risk.",
                scope="ISOLATED",
                operational_priority="HIGH",
                evidence=["The deadline is approaching."],
                recommended_action="Recommend a targeted baggage search.",
                reason_action_is_time_sensitive="Only 30 seconds remain.",
            )

    def test_rejects_non_advisory_action(self) -> None:
        with self.assertRaisesRegex(ValueError, "recommendation"):
            InvestigationResult(
                incident_id=INCIDENT["incident_id"],
                bag_tag_id=INCIDENT["bag_tag_id"],
                classification="ISOLATED_BAG_HANDLING_DELAY",
                root_cause="The bag stopped while peers progressed.",
                scope="ISOLATED",
                operational_priority="HIGH",
                evidence=["Peer bags progressed normally."],
                recommended_action="Move the bag to the makeup area.",
                reason_action_is_time_sensitive="Only 30 seconds remain.",
            )

    def test_rejects_isolated_classification_with_broad_scope(self) -> None:
        with self.assertRaisesRegex(ValueError, "ISOLATED scope"):
            InvestigationResult(
                incident_id=INCIDENT["incident_id"],
                bag_tag_id=INCIDENT["bag_tag_id"],
                classification="ISOLATED_BAG_HANDLING_DELAY",
                root_cause="The bag stopped while peers progressed.",
                scope="FLIGHT_LEVEL",
                operational_priority="HIGH",
                evidence=["Peer bags progressed normally."],
                recommended_action="Recommend a targeted baggage search.",
                reason_action_is_time_sensitive="Only 30 seconds remain.",
            )

    def test_same_generic_risk_produces_distinct_mocked_investigations(self) -> None:
        expectations = {
            "isolated_stall": ("ISOLATED_BAG_HANDLING_DELAY", "ISOLATED"),
            "zone_congestion": ("TRANSFER_ZONE_CONGESTION", "ZONE_LEVEL"),
            "inbound_delay": ("TIGHT_CONNECTION_INBOUND_DELAY", "FLIGHT_LEVEL"),
        }
        results: dict[str, dict[str, Any]] = {}

        for scenario, (classification, scope) in expectations.items():
            with self.subTest(scenario=scenario):
                adapter = _ScenarioAdapter(scenario)
                mock_agent = _EvidenceDrivenMockAgent(adapter)
                service = InvestigatorService(
                    adapter,
                    agent_builder=lambda tools, runner=mock_agent: runner,
                )

                result = service.investigate(dict(INCIDENT))
                results[scenario] = result

                self.assertEqual(classification, result["classification"])
                self.assertEqual(scope, result["scope"])
                self.assertEqual(INCIDENT["incident_id"], result["incident_id"])
                self.assertEqual(INCIDENT["bag_tag_id"], result["bag_tag_id"])
                self.assertEqual(
                    "get_bag_timeline",
                    adapter.calls[0][0],
                    "The individual bag journey must be inspected first",
                )
                self.assertIs(
                    InvestigationResult,
                    mock_agent.kwargs["structured_output_model"],
                )
                self.assertEqual(
                    {"turns": 8, "output_tokens": 2048},
                    mock_agent.kwargs["limits"],
                )

        self.assertEqual(3, len({item["classification"] for item in results.values()}))
        self.assertEqual(3, len({item["recommended_action"] for item in results.values()}))

    def test_rejects_result_identity_drift(self) -> None:
        adapter = _ScenarioAdapter("isolated_stall")

        class WrongIdentityAgent:
            def __call__(self, prompt: str, **kwargs: Any) -> Any:
                del prompt, kwargs
                return SimpleNamespace(
                    structured_output=InvestigationResult(
                        incident_id="WRONG-INCIDENT",
                        bag_tag_id=INCIDENT["bag_tag_id"],
                        classification="INSUFFICIENT_EVIDENCE",
                        root_cause="Available evidence is insufficient.",
                        scope="ISOLATED",
                        operational_priority="HIGH",
                        evidence=["The identifiers could not be correlated."],
                        recommended_action="Recommend monitoring only.",
                        reason_action_is_time_sensitive="Departure is approaching.",
                    )
                )

        service = InvestigatorService(
            adapter, agent_builder=lambda tools: WrongIdentityAgent()
        )

        with self.assertRaisesRegex(InvestigationContractError, "incident_id"):
            service.investigate(dict(INCIDENT))

    def test_rejects_unexpected_input_fields(self) -> None:
        adapter = _ScenarioAdapter("isolated_stall")
        service = InvestigatorService(
            adapter,
            agent_builder=lambda tools: _EvidenceDrivenMockAgent(adapter),
        )
        payload = {**INCIDENT, "sql": "SELECT * FROM baggage_events"}

        with self.assertRaisesRegex(ValueError, "Extra inputs are not permitted"):
            service.investigate(payload)


if __name__ == "__main__":
    unittest.main()
