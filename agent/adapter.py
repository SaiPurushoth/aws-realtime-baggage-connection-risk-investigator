"""Narrow client for the allow-listed ClickHouse adapter Lambda actions."""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Protocol

import boto3
from botocore.config import Config


DEFAULT_ADAPTER_FUNCTION = "bagguard-clickhouse-adapter-prod"
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
if not LOGGER.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("%(levelname)s %(name)s %(message)s"))
    LOGGER.addHandler(_handler)
LOGGER.propagate = False
READ_ONLY_ACTIONS = frozenset(
    {
        "get_bag_timeline",
        "get_connection_peers",
        "get_transfer_zone_health",
        "get_flight_bag_progress",
        "get_recent_connection_risks",
    }
)


class AdapterInvocationError(RuntimeError):
    """The ClickHouse adapter rejected or could not complete a query."""


class ReadOnlyAdapter(Protocol):
    """Interface used by investigator tools and mocked scenario tests."""

    def query(
        self, action: str, parameters: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Run one allow-listed read-only adapter action."""


class ClickHouseAdapterClient:
    """Invoke the adapter Lambda without exposing SQL or write operations."""

    def __init__(
        self,
        *,
        function_name: str | None = None,
        region_name: str | None = None,
        lambda_client: Any | None = None,
    ) -> None:
        self.function_name = (
            function_name
            or os.environ.get("BAGGUARD_CLICKHOUSE_ADAPTER_FUNCTION_NAME")
            or DEFAULT_ADAPTER_FUNCTION
        )
        region = (
            region_name
            or os.environ.get("AWS_REGION")
            or os.environ.get("AWS_DEFAULT_REGION")
            or "us-east-1"
        )
        self._lambda = lambda_client or boto3.client(
            "lambda",
            region_name=region,
            config=Config(
                connect_timeout=3,
                read_timeout=15,
                retries={"max_attempts": 3, "mode": "standard"},
            ),
        )

    def query(
        self, action: str, parameters: dict[str, Any]
    ) -> list[dict[str, Any]]:
        if action not in READ_ONLY_ACTIONS:
            raise ValueError(f"Adapter action is not read-only and allow-listed: {action}")
        if not isinstance(parameters, dict):
            raise TypeError("parameters must be an object")

        payload = json.dumps(
            {"action": action, "parameters": parameters},
            separators=(",", ":"),
        ).encode("utf-8")
        LOGGER.info(
            "tool_action_started action=%s parameter_names=%s",
            action,
            ",".join(sorted(parameters)),
        )
        response = self._lambda.invoke(
            FunctionName=self.function_name,
            InvocationType="RequestResponse",
            Payload=payload,
        )
        response_payload = response.get("Payload")
        raw_body = response_payload.read() if response_payload is not None else b""
        try:
            body = json.loads(raw_body or b"{}")
        except (TypeError, json.JSONDecodeError) as exc:
            raise AdapterInvocationError(
                "ClickHouse adapter returned invalid JSON"
            ) from exc

        status_code = response.get("StatusCode")
        if response.get("FunctionError") or status_code != 200:
            raise AdapterInvocationError(
                f"ClickHouse adapter invocation failed for action {action}"
            )
        if not isinstance(body, dict) or body.get("ok") is not True:
            raise AdapterInvocationError(
                f"ClickHouse adapter rejected action {action}"
            )
        if body.get("action") != action or not isinstance(body.get("result"), list):
            raise AdapterInvocationError(
                f"ClickHouse adapter returned an invalid contract for action {action}"
            )
        rows = body["result"]
        if not all(isinstance(row, dict) for row in rows):
            raise AdapterInvocationError(
                f"ClickHouse adapter returned invalid rows for action {action}"
            )
        LOGGER.info(
            "tool_action_completed action=%s row_count=%d",
            action,
            len(rows),
        )
        return rows
