"""BagGuard ClickHouse adapter Lambda.

The handler accepts Kinesis batches from the two BagGuard streams or a small,
explicit set of operational actions. It never accepts caller-provided SQL.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import socket
import time
from datetime import UTC, datetime
from typing import Any
from urllib import error, parse, request


LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "bagguard")
CLICKHOUSE_SECRET_ARN = os.environ.get("CLICKHOUSE_SECRET_ARN", "")
BAGGAGE_EVENTS_STREAM_NAME = os.environ.get(
    "BAGGAGE_EVENTS_STREAM_NAME", "bagguard-baggage-events-prod"
)
RISK_INCIDENTS_STREAM_NAME = os.environ.get(
    "RISK_INCIDENTS_STREAM_NAME", "bagguard-risk-incidents-prod"
)
HTTP_TIMEOUT_SECONDS = float(os.environ.get("CLICKHOUSE_HTTP_TIMEOUT_SECONDS", "5"))
SECRET_CACHE_SECONDS = int(os.environ.get("SECRET_CACHE_SECONDS", "300"))

BAGGAGE_EVENT_FIELDS = (
    "event_id",
    "bag_tag_id",
    "passenger_id",
    "itinerary_id",
    "inbound_flight_id",
    "outbound_flight_id",
    "airport_code",
    "event_type",
    "event_time",
    "scan_location",
    "transfer_zone",
    "current_gate",
    "connection_departure_time",
    "scheduled_departure_time",
    "estimated_departure_time",
    "inbound_arrival_time",
    "bag_status",
    "priority_code",
    "metadata_json",
)

RISK_INCIDENT_FIELDS = (
    "incident_id",
    "bag_tag_id",
    "incident_type",
    "detected_at",
    "outbound_flight_id",
    "airport_code",
    "transfer_zone",
    "minutes_to_departure",
    "last_scan_type",
    "last_scan_location",
    "context_json",
)

INVESTIGATION_RESULT_FIELDS = (
    "incident_id",
    "bag_tag_id",
    "investigated_at",
    "classification",
    "root_cause",
    "scope",
    "recommended_action",
    "operational_priority",
    "evidence_json",
)

STREAM_TARGETS = {
    BAGGAGE_EVENTS_STREAM_NAME: (
        "bagguard.baggage_events",
        BAGGAGE_EVENT_FIELDS,
        frozenset({"metadata_json"}),
    ),
    RISK_INCIDENTS_STREAM_NAME: (
        "bagguard.baggage_risk_incidents",
        RISK_INCIDENT_FIELDS,
        frozenset({"context_json"}),
    ),
}

_secrets_client: Any = None
_secret_cache: dict[str, Any] | None = None
_secret_cache_expires_at = 0.0


class ValidationError(ValueError):
    """Raised when an invocation does not match an allowed contract."""


class ClickHouseError(RuntimeError):
    """Raised when ClickHouse cannot complete a fixed adapter operation."""


def _log(level: int, event: str, **fields: Any) -> None:
    LOGGER.log(level, json.dumps({"event": event, **fields}, default=str))


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    request_id = getattr(context, "aws_request_id", "local")
    if isinstance(event, dict) and isinstance(event.get("Records"), list):
        return _handle_kinesis(event["Records"], request_id)

    return _handle_action(event, request_id)


def _handle_kinesis(records: list[dict[str, Any]], request_id: str) -> dict[str, Any]:
    grouped: dict[str, list[tuple[str, dict[str, Any]]]] = {}
    failures: list[str] = []

    for record in records:
        sequence_number = str(
            record.get("kinesis", {}).get("sequenceNumber")
            or record.get("eventID")
            or "unknown"
        )
        try:
            stream_name = _stream_name(record)
            table, fields, json_fields = STREAM_TARGETS[stream_name]
            payload = _decode_kinesis_payload(record)
            row = _normalize_row(payload, fields, json_fields)
            grouped.setdefault(table, []).append((sequence_number, row))
        except (KeyError, TypeError, ValueError, ValidationError) as exc:
            failures.append(sequence_number)
            _log(
                logging.ERROR,
                "kinesis_record_rejected",
                request_id=request_id,
                sequence_number=sequence_number,
                error_type=type(exc).__name__,
            )

    inserted = 0
    for table, batch in grouped.items():
        try:
            _insert_rows(table, [row for _, row in batch])
            inserted += len(batch)
            _log(
                logging.INFO,
                "clickhouse_batch_inserted",
                request_id=request_id,
                table=table,
                record_count=len(batch),
            )
        except ClickHouseError as exc:
            failures.extend(identifier for identifier, _ in batch)
            _log(
                logging.ERROR,
                "clickhouse_batch_failed",
                request_id=request_id,
                table=table,
                record_count=len(batch),
                error_type=type(exc).__name__,
            )

    unique_failures = list(dict.fromkeys(failures))
    _log(
        logging.INFO,
        "kinesis_batch_complete",
        request_id=request_id,
        received=len(records),
        inserted=inserted,
        failed=len(unique_failures),
    )
    return {
        "batchItemFailures": [
            {"itemIdentifier": identifier} for identifier in unique_failures
        ]
    }


def _stream_name(record: dict[str, Any]) -> str:
    if record.get("eventSource") != "aws:kinesis":
        raise ValidationError("Only Kinesis records are accepted in batch mode")
    source_arn = record.get("eventSourceARN") or record.get("eventSourceArn")
    if not isinstance(source_arn, str) or "/" not in source_arn:
        raise ValidationError("Kinesis record is missing eventSourceARN")
    stream_name = source_arn.rsplit("/", 1)[-1]
    if stream_name not in STREAM_TARGETS:
        raise ValidationError("Kinesis stream is not allow-listed")
    return stream_name


def _decode_kinesis_payload(record: dict[str, Any]) -> dict[str, Any]:
    encoded = record.get("kinesis", {}).get("data")
    if not isinstance(encoded, str):
        raise ValidationError("Kinesis record data must be base64 text")
    decoded = base64.b64decode(encoded, validate=True)
    payload = json.loads(decoded.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValidationError("Kinesis record must contain a JSON object")
    return payload


def _normalize_row(
    payload: dict[str, Any],
    fields: tuple[str, ...],
    json_fields: frozenset[str],
) -> dict[str, Any]:
    if fields == RISK_INCIDENT_FIELDS:
        payload = _normalize_risk_incident_contract(payload)
    if (
        "metadata_json" in fields
        and "metadata_json" not in payload
        and "metadata" in payload
    ):
        payload = {**payload, "metadata_json": payload["metadata"]}
    missing = [field for field in fields if field not in payload or payload[field] is None]
    if missing:
        raise ValidationError(f"Missing required fields: {', '.join(missing)}")

    row = {field: payload[field] for field in fields}
    if "minutes_to_departure" in row and (
        isinstance(row["minutes_to_departure"], bool)
        or not isinstance(row["minutes_to_departure"], int)
    ):
        raise ValidationError("minutes_to_departure must be an integer")
    for field in json_fields:
        value = row[field]
        if not isinstance(value, str):
            row[field] = json.dumps(value, separators=(",", ":"), sort_keys=True)
    return row


def _normalize_risk_incident_contract(
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Map the detector contract to the existing analytical table contract."""

    normalized = dict(payload)
    if "minutes_to_departure" not in normalized:
        seconds = normalized.get("seconds_to_departure")
        if isinstance(seconds, bool) or not isinstance(seconds, int):
            raise ValidationError("seconds_to_departure must be an integer")
        normalized["minutes_to_departure"] = (
            (seconds + 59) // 60 if seconds >= 0 else seconds // 60
        )

    if "context_json" not in normalized:
        observed_context = normalized.get("observed_context")
        if not isinstance(observed_context, dict):
            raise ValidationError("observed_context must be an object")
        normalized["context_json"] = {
            "observed_context": observed_context,
            "last_scan_time": normalized.get("last_scan_time"),
            "connection_departure_time": normalized.get(
                "connection_departure_time"
            ),
            "seconds_to_departure": normalized.get("seconds_to_departure"),
        }
    return normalized


def _handle_action(event: dict[str, Any], request_id: str) -> dict[str, Any]:
    if not isinstance(event, dict):
        raise ValidationError("Action invocation must be a JSON object")
    if "sql" in event:
        raise ValidationError("Arbitrary SQL is not supported")

    action = event.get("action")
    parameters = event.get("parameters", {})
    if not isinstance(action, str) or action not in ACTION_HANDLERS:
        raise ValidationError("Unknown or missing action")
    if not isinstance(parameters, dict):
        raise ValidationError("parameters must be a JSON object")

    _log(logging.INFO, "action_started", request_id=request_id, action=action)
    result = ACTION_HANDLERS[action](parameters)
    _log(logging.INFO, "action_completed", request_id=request_id, action=action)
    return {"ok": True, "action": action, "result": result}


def _health_check(parameters: dict[str, Any]) -> dict[str, Any]:
    _require_parameters(parameters, required=frozenset())
    rows = _query_rows(
        "SELECT version() AS version, currentDatabase() AS database, "
        "1 AS healthy FORMAT JSONEachRow"
    )
    return rows[0]


def _get_bag_timeline(parameters: dict[str, Any]) -> list[dict[str, Any]]:
    _require_parameters(parameters, required=frozenset({"bag_tag_id"}))
    bag_tag_id = _string_parameter(parameters, "bag_tag_id", 128)
    return _query_rows(
        """SELECT event_id, bag_tag_id, passenger_id, itinerary_id,
inbound_flight_id, outbound_flight_id, airport_code, event_type, event_time,
scan_location, transfer_zone, current_gate, connection_departure_time,
scheduled_departure_time, estimated_departure_time, inbound_arrival_time,
bag_status, priority_code,
JSONExtractString(metadata_json, 'scheduled_inbound_arrival_time')
  AS scheduled_inbound_arrival_time,
JSONExtractInt(metadata_json, 'inbound_delay_minutes') AS inbound_delay_minutes,
JSONExtractString(metadata_json, 'baggage_system_status')
  AS baggage_system_status
FROM bagguard.baggage_events
WHERE bag_tag_id = {bag_tag_id:String}
ORDER BY event_time ASC, event_id ASC
LIMIT 500
FORMAT JSONEachRow""",
        {"bag_tag_id": bag_tag_id},
    )


def _get_connection_peers(parameters: dict[str, Any]) -> list[dict[str, Any]]:
    required = frozenset({"outbound_flight_id", "airport_code", "lookback_minutes"})
    _require_parameters(parameters, required=required)
    query_parameters = {
        "outbound_flight_id": _string_parameter(parameters, "outbound_flight_id", 64),
        "airport_code": _string_parameter(parameters, "airport_code", 16),
        "lookback_minutes": _lookback_parameter(parameters),
    }
    return _query_rows(
        """SELECT bag_tag_id,
argMax(event_type, event_time) AS last_event_type,
max(event_time) AS last_event_time,
argMax(scan_location, event_time) AS last_scan_location,
argMax(transfer_zone, event_time) AS transfer_zone,
argMax(bag_status, event_time) AS bag_status
FROM bagguard.baggage_events
WHERE outbound_flight_id = {outbound_flight_id:String}
  AND airport_code = {airport_code:String}
  AND event_time >= now64(3) - toIntervalMinute({lookback_minutes:UInt32})
GROUP BY bag_tag_id
ORDER BY last_event_time DESC
LIMIT 1000
FORMAT JSONEachRow""",
        query_parameters,
    )


def _get_transfer_zone_health(parameters: dict[str, Any]) -> list[dict[str, Any]]:
    required = frozenset({"airport_code", "transfer_zone", "lookback_minutes"})
    _require_parameters(parameters, required=required)
    query_parameters = {
        "airport_code": _string_parameter(parameters, "airport_code", 16),
        "transfer_zone": _string_parameter(parameters, "transfer_zone", 64),
        "lookback_minutes": _lookback_parameter(parameters),
    }
    return _query_rows(
        """SELECT event_type, count() AS event_count,
uniqExact(bag_tag_id) AS bag_count, max(event_time) AS last_event_time
FROM bagguard.baggage_events
WHERE airport_code = {airport_code:String}
  AND transfer_zone = {transfer_zone:String}
  AND event_time >= now64(3) - toIntervalMinute({lookback_minutes:UInt32})
GROUP BY event_type
ORDER BY event_type ASC
FORMAT JSONEachRow""",
        query_parameters,
    )


def _get_recent_connection_risks(
    parameters: dict[str, Any],
) -> list[dict[str, Any]]:
    required = frozenset({"airport_code", "lookback_minutes"})
    _require_parameters(parameters, required=required)
    query_parameters = {
        "airport_code": _string_parameter(parameters, "airport_code", 16),
        "lookback_minutes": _lookback_parameter(parameters),
    }
    return _query_rows(
        """SELECT incident_id, bag_tag_id, incident_type, detected_at,
outbound_flight_id, airport_code, transfer_zone, minutes_to_departure,
last_scan_type, last_scan_location, context_json
FROM bagguard.baggage_risk_incidents
WHERE airport_code = {airport_code:String}
  AND detected_at >= now64(3) - toIntervalMinute({lookback_minutes:UInt32})
ORDER BY detected_at DESC, incident_id DESC
LIMIT 500
FORMAT JSONEachRow""",
        query_parameters,
    )


def _get_flight_bag_progress(parameters: dict[str, Any]) -> list[dict[str, Any]]:
    _require_parameters(parameters, required=frozenset({"outbound_flight_id"}))
    outbound_flight_id = _string_parameter(parameters, "outbound_flight_id", 64)
    return _query_rows(
        """SELECT bag_tag_id,
argMax(event_type, event_time) AS last_event_type,
max(event_time) AS last_event_time,
argMax(scan_location, event_time) AS last_scan_location,
argMax(transfer_zone, event_time) AS transfer_zone,
argMax(bag_status, event_time) AS bag_status
FROM bagguard.baggage_events
WHERE outbound_flight_id = {outbound_flight_id:String}
GROUP BY bag_tag_id
ORDER BY last_event_time DESC
LIMIT 2000
FORMAT JSONEachRow""",
        {"outbound_flight_id": outbound_flight_id},
    )


def _get_investigation_result(parameters: dict[str, Any]) -> dict[str, Any] | None:
    _require_parameters(parameters, required=frozenset({"incident_id"}))
    incident_id = _string_parameter(parameters, "incident_id", 128)
    rows = _query_rows(
        """SELECT incident_id, bag_tag_id, investigated_at, classification,
root_cause, scope, recommended_action, operational_priority, evidence_json
FROM bagguard.investigation_results
WHERE incident_id = {incident_id:String}
ORDER BY investigated_at DESC
LIMIT 1
FORMAT JSONEachRow""",
        {"incident_id": incident_id},
    )
    return rows[0] if rows else None


def _get_average_transfer_progression_by_zone(
    parameters: dict[str, Any],
) -> list[dict[str, Any]]:
    required = frozenset({"airport_code", "lookback_minutes"})
    _require_parameters(parameters, required=required)
    query_parameters = {
        "airport_code": _string_parameter(parameters, "airport_code", 16),
        "lookback_minutes": _lookback_parameter(parameters),
    }
    return _query_rows(
        """SELECT transfer_zone, count() AS completed_bags,
round(avg(dateDiff('second', transfer_arrived_at, loaded_at)) / 60, 2)
  AS average_progression_minutes,
round(quantileExact(0.5)(dateDiff('second', transfer_arrived_at, loaded_at)) / 60, 2)
  AS median_progression_minutes
FROM
(
  SELECT bag_tag_id, argMax(transfer_zone, event_time) AS transfer_zone,
  minIf(event_time, event_type = 'TRANSFER_ARRIVED') AS transfer_arrived_at,
  minIf(event_time, event_type = 'CONNECTING_AIRCRAFT_LOADED') AS loaded_at
  FROM bagguard.baggage_events
  WHERE airport_code = {airport_code:String}
    AND event_time >= now64(3) - toIntervalMinute({lookback_minutes:UInt32})
  GROUP BY bag_tag_id
  HAVING countIf(event_type = 'TRANSFER_ARRIVED') > 0
     AND countIf(event_type = 'CONNECTING_AIRCRAFT_LOADED') > 0
)
GROUP BY transfer_zone
ORDER BY transfer_zone ASC
FORMAT JSONEachRow""",
        query_parameters,
    )


def _get_risk_frequency_by_transfer_zone(
    parameters: dict[str, Any],
) -> list[dict[str, Any]]:
    required = frozenset({"airport_code", "lookback_minutes"})
    _require_parameters(parameters, required=required)
    query_parameters = {
        "airport_code": _string_parameter(parameters, "airport_code", 16),
        "lookback_minutes": _lookback_parameter(parameters),
    }
    return _query_rows(
        """SELECT transfer_zone, count() AS transfer_bags,
countIf(connection_load_events = 0) AS bags_without_connection_load,
round(countIf(connection_load_events = 0) * 100.0 / count(), 2)
  AS risk_frequency_percent
FROM
(
  SELECT bag_tag_id, argMax(transfer_zone, event_time) AS transfer_zone,
  countIf(event_type = 'CONNECTING_AIRCRAFT_LOADED') AS connection_load_events
  FROM bagguard.baggage_events
  WHERE airport_code = {airport_code:String}
    AND connection_departure_time >= now64(3) - toIntervalMinute({lookback_minutes:UInt32})
    AND connection_departure_time <= now64(3)
  GROUP BY bag_tag_id
  HAVING countIf(event_type = 'TRANSFER_ARRIVED') > 0
)
GROUP BY transfer_zone
ORDER BY transfer_zone ASC
FORMAT JSONEachRow""",
        query_parameters,
    )


def _get_connection_success_rate(
    parameters: dict[str, Any],
) -> list[dict[str, Any]]:
    required = frozenset({"airport_code", "lookback_minutes", "window_minutes"})
    _require_parameters(parameters, required=required)
    window_minutes = parameters.get("window_minutes")
    if (
        isinstance(window_minutes, bool)
        or not isinstance(window_minutes, int)
        or not 15 <= window_minutes <= 1440
    ):
        raise ValidationError("window_minutes must be an integer from 15 to 1440")
    query_parameters = {
        "airport_code": _string_parameter(parameters, "airport_code", 16),
        "lookback_minutes": _lookback_parameter(parameters),
        "window_minutes": window_minutes,
    }
    return _query_rows(
        """SELECT
toDateTime(
  intDiv(toUnixTimestamp(connection_departure), {window_minutes:UInt32} * 60)
    * {window_minutes:UInt32} * 60,
  'UTC'
) AS window_start,
count() AS transfer_bags,
countIf(connection_load_events > 0) AS connected_bags,
round(countIf(connection_load_events > 0) * 100.0 / count(), 2)
  AS connection_success_percent
FROM
(
  SELECT bag_tag_id, max(connection_departure_time) AS connection_departure,
  countIf(event_type = 'CONNECTING_AIRCRAFT_LOADED') AS connection_load_events
  FROM bagguard.baggage_events
  WHERE airport_code = {airport_code:String}
    AND connection_departure_time >= now64(3) - toIntervalMinute({lookback_minutes:UInt32})
    AND connection_departure_time <= now64(3)
  GROUP BY bag_tag_id
  HAVING countIf(event_type = 'TRANSFER_ARRIVED') > 0
)
GROUP BY window_start
ORDER BY window_start ASC
FORMAT JSONEachRow""",
        query_parameters,
    )


def _save_investigation_result(parameters: dict[str, Any]) -> dict[str, Any]:
    required = frozenset(
        {
            "incident_id",
            "bag_tag_id",
            "classification",
            "root_cause",
            "scope",
            "recommended_action",
            "operational_priority",
            "evidence_json",
        }
    )
    _require_parameters(
        parameters,
        required=required,
        optional=frozenset({"investigated_at"}),
    )
    row = {
        "incident_id": _string_parameter(parameters, "incident_id", 128),
        "bag_tag_id": _string_parameter(parameters, "bag_tag_id", 128),
        "investigated_at": (
            _string_parameter(parameters, "investigated_at", 64)
            if "investigated_at" in parameters
            else datetime.now(UTC).isoformat(timespec="milliseconds")
        ),
        "classification": _string_parameter(parameters, "classification", 256),
        "root_cause": _string_parameter(parameters, "root_cause", 4096),
        "scope": _string_parameter(parameters, "scope", 256),
        "recommended_action": _string_parameter(
            parameters, "recommended_action", 8192
        ),
        "operational_priority": _string_parameter(
            parameters, "operational_priority", 128
        ),
        "evidence_json": parameters["evidence_json"],
    }
    row = _normalize_row(
        row,
        INVESTIGATION_RESULT_FIELDS,
        frozenset({"evidence_json"}),
    )
    _insert_rows("bagguard.investigation_results", [row])
    return {"saved": True, "incident_id": row["incident_id"]}


ACTION_HANDLERS = {
    "health_check": _health_check,
    "get_bag_timeline": _get_bag_timeline,
    "get_connection_peers": _get_connection_peers,
    "get_transfer_zone_health": _get_transfer_zone_health,
    "get_recent_connection_risks": _get_recent_connection_risks,
    "get_flight_bag_progress": _get_flight_bag_progress,
    "get_investigation_result": _get_investigation_result,
    "get_average_transfer_progression_by_zone": (
        _get_average_transfer_progression_by_zone
    ),
    "get_risk_frequency_by_transfer_zone": _get_risk_frequency_by_transfer_zone,
    "get_connection_success_rate": _get_connection_success_rate,
    "save_investigation_result": _save_investigation_result,
}


def _require_parameters(
    parameters: dict[str, Any],
    required: frozenset[str],
    optional: frozenset[str] = frozenset(),
) -> None:
    keys = frozenset(parameters)
    missing = required - keys
    unexpected = keys - required - optional
    if missing:
        raise ValidationError(f"Missing parameters: {', '.join(sorted(missing))}")
    if unexpected:
        raise ValidationError(f"Unexpected parameters: {', '.join(sorted(unexpected))}")


def _string_parameter(parameters: dict[str, Any], name: str, max_length: int) -> str:
    value = parameters.get(name)
    if not isinstance(value, str) or not value.strip() or len(value) > max_length:
        raise ValidationError(f"{name} must be non-empty text up to {max_length} characters")
    return value


def _lookback_parameter(parameters: dict[str, Any]) -> int:
    value = parameters.get("lookback_minutes")
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 10080:
        raise ValidationError("lookback_minutes must be an integer from 1 to 10080")
    return value


def _insert_rows(table: str, rows: list[dict[str, Any]]) -> None:
    allowed_tables = {
        "bagguard.baggage_events",
        "bagguard.baggage_risk_incidents",
        "bagguard.investigation_results",
    }
    if table not in allowed_tables:
        raise ValidationError("Insert target is not allow-listed")
    if not rows:
        return
    body = "\n".join(
        json.dumps(row, separators=(",", ":"), ensure_ascii=False) for row in rows
    ).encode("utf-8")
    _clickhouse_request(
        f"INSERT INTO {table} FORMAT JSONEachRow",
        body=body,
        settings={"date_time_input_format": "best_effort"},
    )


def _query_rows(
    query: str, parameters: dict[str, str | int] | None = None
) -> list[dict[str, Any]]:
    response = _clickhouse_request(query, query_parameters=parameters or {})
    if not response.strip():
        return []
    try:
        return [json.loads(line) for line in response.splitlines() if line.strip()]
    except json.JSONDecodeError as exc:
        raise ClickHouseError("ClickHouse returned invalid JSON") from exc


def _clickhouse_request(
    query: str,
    *,
    body: bytes = b"",
    query_parameters: dict[str, str | int] | None = None,
    settings: dict[str, str] | None = None,
) -> str:
    credentials = _get_secret()
    url_parameters: dict[str, str] = {
        "query": query,
        "database": CLICKHOUSE_DATABASE,
        **(settings or {}),
    }
    for name, value in (query_parameters or {}).items():
        url_parameters[f"param_{name}"] = str(value)

    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/?{parse.urlencode(url_parameters)}"
    headers = {
        "Content-Type": "application/x-ndjson; charset=utf-8",
        "X-ClickHouse-User": credentials["username"],
        "X-ClickHouse-Key": credentials["password"],
        "X-ClickHouse-Database": CLICKHOUSE_DATABASE,
    }

    last_error: Exception | None = None
    for attempt in range(1, 4):
        try:
            http_request = request.Request(url, data=body, headers=headers, method="POST")
            with request.urlopen(http_request, timeout=HTTP_TIMEOUT_SECONDS) as response:
                return response.read().decode("utf-8")
        except error.HTTPError as exc:
            detail = exc.read(2048).decode("utf-8", errors="replace")
            if exc.code < 500:
                raise ClickHouseError(f"ClickHouse rejected the request ({exc.code})") from exc
            last_error = ClickHouseError(
                f"ClickHouse server error ({exc.code}): {detail[:256]}"
            )
        except (error.URLError, TimeoutError, socket.timeout) as exc:
            last_error = exc

        if attempt < 3:
            time.sleep(0.2 * (2 ** (attempt - 1)))

    raise ClickHouseError("ClickHouse request failed after bounded retries") from last_error


def _get_secret() -> dict[str, str]:
    global _secrets_client, _secret_cache, _secret_cache_expires_at

    now = time.monotonic()
    if _secret_cache is not None and now < _secret_cache_expires_at:
        return _secret_cache

    if not CLICKHOUSE_SECRET_ARN:
        raise ClickHouseError("CLICKHOUSE_SECRET_ARN is not configured")
    if _secrets_client is None:
        import boto3

        _secrets_client = boto3.client("secretsmanager")

    response = _secrets_client.get_secret_value(SecretId=CLICKHOUSE_SECRET_ARN)
    try:
        secret = json.loads(response["SecretString"])
        username = secret["username"]
        password = secret["password"]
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise ClickHouseError("ClickHouse secret has an invalid structure") from exc
    if not isinstance(username, str) or not isinstance(password, str):
        raise ClickHouseError("ClickHouse secret credentials must be strings")

    _secret_cache = {"username": username, "password": password}
    _secret_cache_expires_at = now + SECRET_CACHE_SECONDS
    return _secret_cache
