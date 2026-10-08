"""AWS-backed application service for the local BagGuard console."""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import boto3
from botocore.config import Config

from simulator import producer


ADAPTER_FUNCTION_NAME = "bagguard-clickhouse-adapter-prod"
RISK_INCIDENT_TYPE = "BAG_CONNECTION_RISK"
SCENARIOS = {
    "normal": "Normal Connection",
    "isolated-stall": "Isolated Bag Stall",
    "zone-congestion": "Transfer Zone Congestion",
    "inbound-delay": "Inbound Flight Delay",
}
JOURNEY_STAGES = (
    ("BAG_ACCEPTED", "Bag Accepted"),
    ("ORIGIN_AIRCRAFT_LOADED", "Origin Loaded"),
    ("TRANSFER_ARRIVED", "Transfer Arrived"),
    ("TRANSFER_SORTER_SCAN", "Sorter Scan"),
    ("MAKEUP_AREA_SCAN", "Makeup Area"),
    ("CONNECTING_AIRCRAFT_LOADED", "Connecting Aircraft Loaded"),
)


class DashboardServiceError(RuntimeError):
    """A controlled dashboard operation could not complete."""


@dataclass(frozen=True)
class ScenarioRun:
    scenario_key: str
    scenario_label: str
    run_id: str
    bag_tag_id: str
    incident_id: str | None
    outbound_flight_id: str
    airport_code: str
    transfer_zone: str
    expected_risk: bool
    published_events: int


@dataclass(frozen=True)
class OperationsSnapshot:
    run: ScenarioRun
    timeline: list[dict[str, Any]]
    incident: dict[str, Any] | None
    investigation: dict[str, Any] | None
    recent_risks: list[dict[str, Any]]

    @property
    def flight_risk_bags(self) -> int:
        return len(
            {
                row["bag_tag_id"]
                for row in self.recent_risks
                if row.get("outbound_flight_id") == self.run.outbound_flight_id
            }
        )

    @property
    def flight_zone_risk_bags(self) -> int:
        return len(
            {
                row["bag_tag_id"]
                for row in self.recent_risks
                if row.get("outbound_flight_id") == self.run.outbound_flight_id
                and row.get("transfer_zone") == self.run.transfer_zone
            }
        )

    @property
    def recent_zone_risk_count(self) -> int:
        return len(
            {
                row["incident_id"]
                for row in self.recent_risks
                if row.get("transfer_zone") == self.run.transfer_zone
            }
        )


class BagGuardService:
    """Publish synthetic scenarios and read operational state through Lambda."""

    def __init__(
        self,
        *,
        region_name: str | None = None,
        stream_name: str | None = None,
        adapter_function_name: str | None = None,
        kinesis_client: Any | None = None,
        lambda_client: Any | None = None,
    ) -> None:
        self.region_name = (
            region_name
            or os.environ.get("AWS_REGION")
            or os.environ.get("AWS_DEFAULT_REGION")
            or producer.DEFAULT_REGION
        )
        self.stream_name = stream_name or os.environ.get(
            "BAGGUARD_EVENT_STREAM_NAME", producer.STREAM_NAME
        )
        self.adapter_function_name = adapter_function_name or os.environ.get(
            "BAGGUARD_CLICKHOUSE_ADAPTER_FUNCTION_NAME",
            ADAPTER_FUNCTION_NAME,
        )
        client_config = Config(
            connect_timeout=3,
            read_timeout=20,
            retries={"max_attempts": 3, "mode": "standard"},
        )
        self._kinesis = kinesis_client or boto3.client(
            "kinesis", region_name=self.region_name, config=client_config
        )
        self._lambda = lambda_client or boto3.client(
            "lambda", region_name=self.region_name, config=client_config
        )

    def start_scenario(
        self,
        scenario_key: str,
        *,
        departure_seconds: int = 60,
    ) -> ScenarioRun:
        if scenario_key not in SCENARIOS:
            raise DashboardServiceError(f"Unknown scenario: {scenario_key}")
        run_id = datetime.now(UTC).strftime("CONSOLE-%Y%m%dT%H%M%S%f")[:-3]
        events = producer.generate_scenario(
            scenario_key,
            run_id=run_id,
            departure_seconds=departure_seconds,
        )
        grouped: dict[str, list[dict[str, Any]]] = {}
        for event in events:
            grouped.setdefault(event["bag_tag_id"], []).append(event)
        target_events = next(
            (
                bag_events
                for bag_events in grouped.values()
                if bag_events[0]["metadata"].get("expected_risk_signal")
            ),
            next(iter(grouped.values())),
        )
        target = target_events[0]
        expected_risk = bool(target["metadata"].get("expected_risk_signal"))
        incident_id = (
            f"{target['bag_tag_id']}-{target['outbound_flight_id']}"
            f"-{RISK_INCIDENT_TYPE}"
            if expected_risk
            else None
        )
        published = producer.publish_events(
            self._kinesis,
            events,
            stream_name=self.stream_name,
        )
        return ScenarioRun(
            scenario_key=scenario_key,
            scenario_label=SCENARIOS[scenario_key],
            run_id=run_id,
            bag_tag_id=target["bag_tag_id"],
            incident_id=incident_id,
            outbound_flight_id=target["outbound_flight_id"],
            airport_code=target["airport_code"],
            transfer_zone=target["transfer_zone"],
            expected_risk=expected_risk,
            published_events=published,
        )

    def get_snapshot(self, run: ScenarioRun) -> OperationsSnapshot:
        timeline = self._invoke_adapter(
            "get_bag_timeline", {"bag_tag_id": run.bag_tag_id}
        )
        recent_risks = self._invoke_adapter(
            "get_recent_connection_risks",
            {"airport_code": run.airport_code, "lookback_minutes": 60},
        )
        incident = next(
            (
                row
                for row in recent_risks
                if run.incident_id and row.get("incident_id") == run.incident_id
            ),
            None,
        )
        investigation = (
            self._invoke_adapter(
                "get_investigation_result", {"incident_id": run.incident_id}
            )
            if run.incident_id
            else None
        )
        return OperationsSnapshot(
            run=run,
            timeline=timeline,
            incident=incident,
            investigation=investigation,
            recent_risks=recent_risks,
        )

    def wait_for_timeline(
        self,
        run: ScenarioRun,
        *,
        timeout_seconds: int = 45,
        poll_seconds: float = 2,
    ) -> OperationsSnapshot:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            snapshot = self.get_snapshot(run)
            if snapshot.timeline:
                return snapshot
            time.sleep(poll_seconds)
        raise DashboardServiceError("Baggage events did not reach the analytical store")

    def _invoke_adapter(self, action: str, parameters: dict[str, Any]) -> Any:
        response = self._lambda.invoke(
            FunctionName=self.adapter_function_name,
            InvocationType="RequestResponse",
            Payload=json.dumps(
                {"action": action, "parameters": parameters},
                separators=(",", ":"),
            ).encode("utf-8"),
        )
        raw = response.get("Payload").read() if response.get("Payload") else b""
        try:
            body = json.loads(raw)
        except (TypeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DashboardServiceError("Adapter returned invalid JSON") from exc
        if response.get("StatusCode") != 200 or response.get("FunctionError"):
            raise DashboardServiceError(f"Adapter invocation failed: {action}")
        if (
            not isinstance(body, dict)
            or body.get("ok") is not True
            or body.get("action") != action
            or "result" not in body
        ):
            raise DashboardServiceError(f"Adapter rejected action: {action}")
        return body["result"]


def parse_incident_context(incident: dict[str, Any] | None) -> dict[str, Any]:
    if not incident:
        return {}
    context = incident.get("context_json")
    if isinstance(context, dict):
        return context
    if isinstance(context, str):
        try:
            parsed = json.loads(context)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def parse_evidence(investigation: dict[str, Any] | None) -> list[str]:
    if not investigation:
        return []
    evidence = investigation.get("evidence_json", [])
    if isinstance(evidence, list):
        return [str(item) for item in evidence]
    if isinstance(evidence, str):
        try:
            parsed = json.loads(evidence)
            return [str(item) for item in parsed] if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def time_sensitive_reason(snapshot: OperationsSnapshot) -> str:
    if snapshot.investigation and snapshot.investigation.get(
        "reason_action_is_time_sensitive"
    ):
        return str(snapshot.investigation["reason_action_is_time_sensitive"])
    context = parse_incident_context(snapshot.incident)
    seconds = context.get("seconds_to_departure")
    observed = context.get("observed_context", {})
    if isinstance(seconds, int):
        milestone = (
            "the bag had not reached makeup or aircraft loading"
            if not observed.get("makeup_area_reached")
            and not observed.get("aircraft_loaded")
            else "the connection remained inside the active intervention window"
        )
        return (
            f"Flink detected the risk with {seconds} seconds remaining, and "
            f"{milestone}."
        )
    return "The connection is inside the active operational intervention window."


def departure_seconds(timeline: list[dict[str, Any]]) -> int | None:
    if not timeline:
        return None
    raw = timeline[-1].get("estimated_departure_time") or timeline[-1].get(
        "connection_departure_time"
    )
    if not isinstance(raw, str):
        return None
    try:
        deadline = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if deadline.tzinfo is None:
        # ClickHouse DateTime64 values are UTC but its JSON output can omit the
        # offset. Normalize them before comparing with the aware UTC clock.
        deadline = deadline.replace(tzinfo=UTC)
    else:
        deadline = deadline.astimezone(UTC)
    return max(0, int((deadline - datetime.now(UTC)).total_seconds()))
