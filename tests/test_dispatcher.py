from __future__ import annotations

import base64
import importlib.util
import json
import os
import sys
import unittest
from types import SimpleNamespace
from typing import Any


DISPATCHER_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "lambdas",
    "dispatcher",
    "lambda_function.py",
)
SPEC = importlib.util.spec_from_file_location("bagguard_dispatcher", DISPATCHER_PATH)
assert SPEC is not None and SPEC.loader is not None
dispatcher = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = dispatcher
SPEC.loader.exec_module(dispatcher)


INCIDENT = {
    "incident_id": "BG-TEST-001-AI-TEST-802-BAG_CONNECTION_RISK",
    "bag_tag_id": "BG-TEST-001",
    "incident_type": "BAG_CONNECTION_RISK",
    "detected_at": "2026-10-05T12:00:00.000Z",
    "outbound_flight_id": "AI-TEST-802",
    "airport_code": "BLR",
    "transfer_zone": "T2",
    "last_scan_type": "TRANSFER_SORTER_SCAN",
    "last_scan_location": "SORTER-T2",
    "seconds_to_departure": 30,
}

RESULT = {
    "incident_id": INCIDENT["incident_id"],
    "bag_tag_id": INCIDENT["bag_tag_id"],
    "classification": "ISOLATED_BAG_HANDLING_DELAY",
    "root_cause": "The bag stopped while peer bags progressed.",
    "scope": "ISOLATED",
    "operational_priority": "HIGH",
    "evidence": ["The last event was TRANSFER_SORTER_SCAN."],
    "recommended_action": "Recommend a targeted baggage search.",
    "reason_action_is_time_sensitive": "Only 30 seconds remain.",
}


class _Body:
    def __init__(self, value: Any) -> None:
        self._raw = json.dumps(value).encode("utf-8")

    def read(self) -> bytes:
        return self._raw


class _AgentCoreClient:
    def __init__(self, result: dict[str, Any]) -> None:
        self.result = result
        self.calls: list[dict[str, Any]] = []

    def invoke_agent_runtime(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        return {"statusCode": 200, "response": _Body(self.result)}


class _LambdaClient:
    def __init__(self, *, existing: dict[str, Any] | None = None) -> None:
        self.existing = existing
        self.calls: list[dict[str, Any]] = []

    def invoke(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        request = json.loads(kwargs["Payload"])
        action = request["action"]
        if action == "get_investigation_result":
            result: Any = self.existing
        elif action == "save_investigation_result":
            result = {
                "saved": True,
                "incident_id": request["parameters"]["incident_id"],
            }
        else:
            raise AssertionError(action)
        return {
            "StatusCode": 200,
            "Payload": _Body({"ok": True, "action": action, "result": result}),
        }


def _event(payload: dict[str, Any], sequence: str = "100") -> dict[str, Any]:
    return {
        "Records": [
            {
                "eventSource": "aws:kinesis",
                "eventID": f"shard:{sequence}",
                "kinesis": {
                    "sequenceNumber": sequence,
                    "data": base64.b64encode(json.dumps(payload).encode()).decode(),
                },
            }
        ]
    }


class DispatcherTests(unittest.TestCase):
    def setUp(self) -> None:
        dispatcher.AGENT_RUNTIME_ARN = "arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime/test"
        self.agent = _AgentCoreClient(dict(RESULT))
        self.adapter = _LambdaClient()
        dispatcher._agentcore_client = self.agent
        dispatcher._lambda_client = self.adapter

    def tearDown(self) -> None:
        dispatcher._agentcore_client = None
        dispatcher._lambda_client = None

    def test_dispatches_valid_risk_and_persists_structured_result(self) -> None:
        response = dispatcher.lambda_handler(
            _event(INCIDENT), SimpleNamespace(aws_request_id="request-1")
        )

        self.assertEqual({"batchItemFailures": []}, response)
        self.assertEqual(1, len(self.agent.calls))
        sent = json.loads(self.agent.calls[0]["payload"])
        self.assertNotIn("incident_type", sent)
        self.assertEqual(INCIDENT["incident_id"], sent["incident_id"])
        actions = [json.loads(call["Payload"])["action"] for call in self.adapter.calls]
        self.assertEqual(
            ["get_investigation_result", "save_investigation_result"], actions
        )
        save = json.loads(self.adapter.calls[-1]["Payload"])["parameters"]
        self.assertEqual(RESULT["evidence"], save["evidence_json"])

    def test_existing_result_skips_agent_and_save(self) -> None:
        existing = {
            "incident_id": INCIDENT["incident_id"],
            "bag_tag_id": INCIDENT["bag_tag_id"],
        }
        dispatcher._lambda_client = _LambdaClient(existing=existing)

        response = dispatcher.lambda_handler(
            _event(INCIDENT), SimpleNamespace(aws_request_id="request-2")
        )

        self.assertEqual({"batchItemFailures": []}, response)
        self.assertEqual([], self.agent.calls)
        self.assertEqual(1, len(dispatcher._lambda_client.calls))

    def test_rejects_non_deterministic_incident_id(self) -> None:
        invalid = {**INCIDENT, "incident_id": "random-id"}

        response = dispatcher.lambda_handler(
            _event(invalid), SimpleNamespace(aws_request_id="request-3")
        )

        self.assertEqual(
            {"batchItemFailures": [{"itemIdentifier": "100"}]}, response
        )
        self.assertEqual([], self.agent.calls)
        self.assertEqual([], self.adapter.calls)

    def test_rejects_agent_identity_drift_without_saving(self) -> None:
        dispatcher._agentcore_client = _AgentCoreClient(
            {**RESULT, "incident_id": "different-id"}
        )

        response = dispatcher.lambda_handler(
            _event(INCIDENT), SimpleNamespace(aws_request_id="request-4")
        )

        self.assertEqual(
            {"batchItemFailures": [{"itemIdentifier": "100"}]}, response
        )
        actions = [json.loads(call["Payload"])["action"] for call in self.adapter.calls]
        self.assertEqual(["get_investigation_result"], actions)


if __name__ == "__main__":
    unittest.main()
