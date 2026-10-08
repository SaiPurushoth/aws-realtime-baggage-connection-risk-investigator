"""Dispatch generic BagGuard risk incidents to AgentCore and persist results.

This Lambda contains no baggage-investigation rules. It validates transport
contracts, invokes the investigator runtime, and coordinates idempotent result
persistence through the existing ClickHouse adapter Lambda.
"""

from __future__ import annotations

import base64
import json
import logging
import os
from datetime import UTC, datetime
from typing import Any

import boto3
from botocore.config import Config


LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

AGENT_RUNTIME_ARN = os.environ.get("AGENT_RUNTIME_ARN", "")
AGENT_RUNTIME_QUALIFIER = os.environ.get("AGENT_RUNTIME_QUALIFIER", "DEFAULT")
ADAPTER_FUNCTION_NAME = os.environ.get(
    "CLICKHOUSE_ADAPTER_FUNCTION_NAME",
    "bagguard-clickhouse-adapter-prod",
)

INPUT_FIELDS = frozenset(
    {
        "incident_id",
        "bag_tag_id",
        "outbound_flight_id",
        "airport_code",
        "transfer_zone",
        "last_scan_type",
        "last_scan_location",
        "seconds_to_departure",
    }
)
RESULT_FIELDS = frozenset(
    {
        "incident_id",
        "bag_tag_id",
        "classification",
        "root_cause",
        "scope",
        "operational_priority",
        "evidence",
        "recommended_action",
        "reason_action_is_time_sensitive",
    }
)
ALLOWED_SCOPES = frozenset(
    {"ISOLATED", "FLIGHT_LEVEL", "ZONE_LEVEL", "AIRPORT_LEVEL"}
)
ALLOWED_PRIORITIES = frozenset({"LOW", "MEDIUM", "HIGH", "CRITICAL"})

_agentcore_client: Any = None
_lambda_client: Any = None


class DispatchError(RuntimeError):
    """A record could not be safely dispatched or persisted."""


def _log(level: int, event: str, **fields: Any) -> None:
    LOGGER.log(level, json.dumps({"event": event, **fields}, default=str))


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    request_id = getattr(context, "aws_request_id", "local")
    records = event.get("Records") if isinstance(event, dict) else None
    if not isinstance(records, list):
        raise DispatchError("Dispatcher accepts only Kinesis batch events")

    failures: list[str] = []
    for record in records:
        sequence_number = str(
            record.get("kinesis", {}).get("sequenceNumber")
            or record.get("eventID")
            or "unknown"
        )
        try:
            incident = _decode_and_validate_incident(record)
            outcome = _dispatch_incident(incident)
            _log(
                logging.INFO,
                "risk_incident_dispatched",
                request_id=request_id,
                sequence_number=sequence_number,
                incident_id=incident["incident_id"],
                outcome=outcome,
            )
        except Exception as exc:  # Return record-level failure to the mapping.
            failures.append(sequence_number)
            _log(
                logging.ERROR,
                "risk_incident_dispatch_failed",
                request_id=request_id,
                sequence_number=sequence_number,
                error_type=type(exc).__name__,
            )

    return {
        "batchItemFailures": [
            {"itemIdentifier": identifier} for identifier in failures
        ]
    }


def _decode_and_validate_incident(record: dict[str, Any]) -> dict[str, Any]:
    if record.get("eventSource") != "aws:kinesis":
        raise DispatchError("Only Kinesis records are accepted")
    encoded = record.get("kinesis", {}).get("data")
    if not isinstance(encoded, str):
        raise DispatchError("Kinesis record data must be base64 text")
    try:
        payload = json.loads(base64.b64decode(encoded, validate=True))
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DispatchError("Kinesis record contains invalid JSON") from exc
    if not isinstance(payload, dict):
        raise DispatchError("Risk incident must be a JSON object")
    if payload.get("incident_type") != "BAG_CONNECTION_RISK":
        raise DispatchError("Only BAG_CONNECTION_RISK incidents are accepted")

    incident = {field: payload.get(field) for field in INPUT_FIELDS}
    for field in INPUT_FIELDS - {"seconds_to_departure"}:
        value = incident[field]
        if not isinstance(value, str) or not value.strip():
            raise DispatchError(f"{field} must be non-empty text")
    seconds = incident["seconds_to_departure"]
    if isinstance(seconds, bool) or not isinstance(seconds, int):
        raise DispatchError("seconds_to_departure must be an integer")

    expected_id = (
        f"{incident['bag_tag_id']}-{incident['outbound_flight_id']}"
        "-BAG_CONNECTION_RISK"
    )
    if incident["incident_id"] != expected_id:
        raise DispatchError("incident_id is not the deterministic risk identifier")
    return incident


def _dispatch_incident(incident: dict[str, Any]) -> str:
    existing = _invoke_adapter(
        "get_investigation_result",
        {"incident_id": incident["incident_id"]},
    )
    if existing is not None:
        if not isinstance(existing, dict):
            raise DispatchError("Adapter returned an invalid existing result")
        if existing.get("incident_id") != incident["incident_id"]:
            raise DispatchError("Adapter returned a mismatched existing result")
        return "already_persisted"

    result = _invoke_agentcore(incident)
    _validate_result(result, incident)
    save_result = _invoke_adapter(
        "save_investigation_result",
        {
            "incident_id": result["incident_id"],
            "bag_tag_id": result["bag_tag_id"],
            "investigated_at": datetime.now(UTC).isoformat(timespec="milliseconds"),
            "classification": result["classification"],
            "root_cause": result["root_cause"],
            "scope": result["scope"],
            "recommended_action": result["recommended_action"],
            "operational_priority": result["operational_priority"],
            "evidence_json": result["evidence"],
        },
    )
    if not isinstance(save_result, dict) or save_result != {
        "saved": True,
        "incident_id": incident["incident_id"],
    }:
        raise DispatchError("Adapter did not confirm investigation persistence")
    return "persisted"


def _invoke_agentcore(incident: dict[str, Any]) -> dict[str, Any]:
    global _agentcore_client
    if not AGENT_RUNTIME_ARN:
        raise DispatchError("AGENT_RUNTIME_ARN is not configured")
    if _agentcore_client is None:
        _agentcore_client = boto3.client(
            "bedrock-agentcore",
            config=Config(
                connect_timeout=3,
                read_timeout=90,
                retries={"max_attempts": 2, "mode": "standard"},
            ),
        )
    response = _agentcore_client.invoke_agent_runtime(
        agentRuntimeArn=AGENT_RUNTIME_ARN,
        qualifier=AGENT_RUNTIME_QUALIFIER,
        contentType="application/json",
        accept="application/json",
        payload=json.dumps(incident, separators=(",", ":")).encode("utf-8"),
    )
    if response.get("statusCode") != 200:
        raise DispatchError("AgentCore returned a non-success status")
    stream = response.get("response")
    raw = stream.read() if stream is not None else b""
    try:
        result = json.loads(raw)
    except (TypeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DispatchError("AgentCore returned invalid JSON") from exc
    if not isinstance(result, dict):
        raise DispatchError("AgentCore result must be a JSON object")
    return result


def _validate_result(result: dict[str, Any], incident: dict[str, Any]) -> None:
    if frozenset(result) != RESULT_FIELDS:
        raise DispatchError("AgentCore result fields do not match the contract")
    if result["incident_id"] != incident["incident_id"]:
        raise DispatchError("AgentCore result incident_id does not match")
    if result["bag_tag_id"] != incident["bag_tag_id"]:
        raise DispatchError("AgentCore result bag_tag_id does not match")
    for field in (
        "classification",
        "root_cause",
        "recommended_action",
        "reason_action_is_time_sensitive",
    ):
        if not isinstance(result[field], str) or not result[field].strip():
            raise DispatchError(f"AgentCore result {field} must be non-empty text")
    if result["scope"] not in ALLOWED_SCOPES:
        raise DispatchError("AgentCore result has an invalid scope")
    if result["operational_priority"] not in ALLOWED_PRIORITIES:
        raise DispatchError("AgentCore result has an invalid operational priority")
    evidence = result["evidence"]
    if (
        not isinstance(evidence, list)
        or not 1 <= len(evidence) <= 20
        or any(not isinstance(item, str) or not item.strip() for item in evidence)
    ):
        raise DispatchError("AgentCore result evidence must be non-empty text items")


def _invoke_adapter(action: str, parameters: dict[str, Any]) -> Any:
    global _lambda_client
    if _lambda_client is None:
        _lambda_client = boto3.client(
            "lambda",
            config=Config(
                connect_timeout=3,
                read_timeout=20,
                retries={"max_attempts": 2, "mode": "standard"},
            ),
        )
    response = _lambda_client.invoke(
        FunctionName=ADAPTER_FUNCTION_NAME,
        InvocationType="RequestResponse",
        Payload=json.dumps(
            {"action": action, "parameters": parameters},
            separators=(",", ":"),
        ).encode("utf-8"),
    )
    payload = response.get("Payload")
    raw = payload.read() if payload is not None else b""
    try:
        body = json.loads(raw)
    except (TypeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DispatchError("Adapter returned invalid JSON") from exc
    if response.get("StatusCode") != 200 or response.get("FunctionError"):
        raise DispatchError(f"Adapter invocation failed for {action}")
    if (
        not isinstance(body, dict)
        or body.get("ok") is not True
        or body.get("action") != action
        or "result" not in body
    ):
        raise DispatchError(f"Adapter returned an invalid contract for {action}")
    return body["result"]
