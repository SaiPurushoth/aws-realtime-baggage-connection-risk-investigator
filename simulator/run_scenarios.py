#!/usr/bin/env python3
"""Run the three BagGuard risk investigations through the deployed pipeline."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Sequence

try:
    from . import producer
except ImportError:
    import producer  # type: ignore[no-redef]


ADAPTER_FUNCTION_NAME = "bagguard-clickhouse-adapter-prod"
DEFAULT_TIMEOUT_SECONDS = 900
SCENARIOS = (
    ("isolated-stall", "ISOLATED BAG", "A"),
    ("zone-congestion", "TRANSFER ZONE CONGESTION", "B"),
    ("inbound-delay", "INBOUND FLIGHT DELAY", "C"),
)
TOOL_PATTERN = re.compile(r"tool_action_started action=([a-z_]+)")


class AdapterClient:
    """Invoke controlled adapter actions without exposing SQL."""

    def __init__(self, lambda_client: Any, function_name: str) -> None:
        self._lambda = lambda_client
        self._function_name = function_name

    def invoke(self, action: str, parameters: dict[str, Any]) -> Any:
        response = self._lambda.invoke(
            FunctionName=self._function_name,
            InvocationType="RequestResponse",
            Payload=json.dumps(
                {"action": action, "parameters": parameters},
                separators=(",", ":"),
            ).encode("utf-8"),
        )
        raw = response["Payload"].read()
        body = json.loads(raw)
        if response.get("FunctionError") or body.get("ok") is not True:
            raise RuntimeError(f"Adapter action failed: {action}")
        if body.get("action") != action or "result" not in body:
            raise RuntimeError(f"Adapter returned an invalid contract: {action}")
        return body["result"]


def _risk_candidates(events: Sequence[dict[str, Any]]) -> list[dict[str, str]]:
    candidates: dict[str, dict[str, str]] = {}
    for event in events:
        if not event["metadata"].get("expected_risk_signal"):
            continue
        bag_tag_id = event["bag_tag_id"]
        candidates[bag_tag_id] = {
            "bag_tag_id": bag_tag_id,
            "outbound_flight_id": event["outbound_flight_id"],
            "airport_code": event["airport_code"],
            "incident_id": (
                f"{bag_tag_id}-{event['outbound_flight_id']}"
                "-BAG_CONNECTION_RISK"
            ),
        }
    return list(candidates.values())


def _wait_for_scenario(
    adapter: AdapterClient,
    candidates: list[dict[str, str]],
    *,
    timeout_seconds: int,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    expected_ids = {item["incident_id"] for item in candidates}
    airport_codes = {item["airport_code"] for item in candidates}
    if len(airport_codes) != 1:
        raise RuntimeError("Each scenario must use exactly one synthetic airport")
    airport_code = next(iter(airport_codes))
    deadline = time.monotonic() + timeout_seconds
    last_progress: tuple[int, int] | None = None

    while time.monotonic() < deadline:
        risk_rows = adapter.invoke(
            "get_recent_connection_risks",
            {"airport_code": airport_code, "lookback_minutes": 60},
        )
        risks = {
            row["incident_id"]: row
            for row in risk_rows
            if row.get("incident_id") in expected_ids
        }
        results: dict[str, dict[str, Any]] = {}
        for incident_id in sorted(expected_ids):
            result = adapter.invoke(
                "get_investigation_result",
                {"incident_id": incident_id},
            )
            if result is not None:
                results[incident_id] = result

        progress = (len(risks), len(results))
        if progress != last_progress:
            print(
                f"  progress: {progress[0]}/{len(expected_ids)} Flink incidents, "
                f"{progress[1]}/{len(expected_ids)} investigations"
            )
            last_progress = progress
        if set(risks) == expected_ids and set(results) == expected_ids:
            if any(row.get("incident_type") != "BAG_CONNECTION_RISK" for row in risks.values()):
                raise RuntimeError("Flink emitted an unexpected incident type")
            return risks, results
        time.sleep(5)

    missing_risks = sorted(expected_ids - set(risks))
    missing_results = sorted(expected_ids - set(results))
    raise TimeoutError(
        "Scenario did not complete before timeout; "
        f"missing risks={missing_risks}, missing investigations={missing_results}"
    )


def _observed_tools(
    logs_client: Any | None,
    log_group: str | None,
    *,
    start_ms: int,
    end_ms: int,
) -> list[str]:
    if logs_client is None or not log_group:
        return []
    tools: list[str] = []
    token: str | None = None
    while True:
        request: dict[str, Any] = {
            "logGroupName": log_group,
            "startTime": start_ms,
            "endTime": end_ms,
            "filterPattern": '"tool_action_started"',
        }
        if token:
            request["nextToken"] = token
        response = logs_client.filter_log_events(**request)
        for event in response.get("events", []):
            match = TOOL_PATTERN.search(event.get("message", ""))
            if match and match.group(1) not in tools:
                tools.append(match.group(1))
        next_token = response.get("nextToken")
        if not next_token or next_token == token:
            return tools
        token = next_token


def _markdown(results: list[dict[str, Any]]) -> str:
    def clean(value: Any) -> str:
        return str(value).replace("|", "\\|").replace("\n", " ")

    lines = [
        "| Scenario | Flink detection | Agent tools | Root cause | Scope | Recommended intervention |",
        "|---|---|---|---|---|---|",
    ]
    for result in results:
        investigation = result["representative_investigation"]
        lines.append(
            "| "
            + " | ".join(
                clean(value)
                for value in (
                    result["scenario"],
                    result["flink_detection"],
                    ", ".join(result["agent_tools"]) or "See AgentCore trace",
                    investigation["root_cause"],
                    investigation["scope"],
                    investigation["recommended_action"],
                )
            )
            + " |"
        )
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run all three BagGuard scenarios through the deployed pipeline"
    )
    parser.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"))
    parser.add_argument("--stream-name", default=producer.STREAM_NAME)
    parser.add_argument("--adapter-function", default=ADAPTER_FUNCTION_NAME)
    parser.add_argument("--agent-log-group")
    parser.add_argument("--departure-seconds", type=int, default=60)
    parser.add_argument("--timeout-seconds", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--run-id")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("simulator/scenario_results.json"),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        import boto3
    except ImportError:
        print("boto3 is required", file=sys.stderr)
        return 2

    base_run_id = producer._safe_run_id(
        args.run_id or datetime.now(UTC).strftime("E2E-%Y%m%dT%H%M%S")
    )
    kinesis = boto3.client("kinesis", region_name=args.region)
    lambda_client = boto3.client("lambda", region_name=args.region)
    logs_client = (
        boto3.client("logs", region_name=args.region)
        if args.agent_log_group
        else None
    )
    adapter = AdapterClient(lambda_client, args.adapter_function)
    comparisons: list[dict[str, Any]] = []

    for scenario_name, display_name, suffix in SCENARIOS:
        run_id = f"{base_run_id}-{suffix}"
        scenario_start_ms = int(time.time() * 1000)
        events = producer.generate_scenario(
            scenario_name,
            run_id=run_id,
            departure_seconds=args.departure_seconds,
        )
        candidates = _risk_candidates(events)
        print(f"\n=== Scenario {suffix}: {display_name} ===")
        producer.print_journey(events, producer.SCENARIO_ALIASES[scenario_name])
        published = producer.publish_events(
            kinesis,
            events,
            stream_name=args.stream_name,
        )
        print(f"Published {published} baggage events; awaiting the risk timer.")
        risks, investigations = _wait_for_scenario(
            adapter,
            candidates,
            timeout_seconds=args.timeout_seconds,
        )
        scenario_end_ms = int(time.time() * 1000) + 5_000
        tools = _observed_tools(
            logs_client,
            args.agent_log_group,
            start_ms=scenario_start_ms,
            end_ms=scenario_end_ms,
        )
        representative_id = candidates[0]["incident_id"]
        comparisons.append(
            {
                "scenario": display_name,
                "run_id": run_id,
                "candidate_bags": len(candidates),
                "flink_incidents_observed": len(risks),
                "investigations_observed": len(investigations),
                "flink_detection": risks[representative_id]["incident_type"],
                "agent_tools": tools,
                "representative_incident": risks[representative_id],
                "representative_investigation": investigations[representative_id],
            }
        )

    output = {
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "region": args.region,
        "stream_name": args.stream_name,
        "scenarios": comparisons,
        "comparison_markdown": _markdown(comparisons),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2) + "\n", encoding="utf-8")
    print("\n=== Comparison ===")
    print(output["comparison_markdown"])
    print(f"\nSaved evidence to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
