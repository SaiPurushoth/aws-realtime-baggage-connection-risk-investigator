"""Deterministic connection-risk state machine shared by tests and PyFlink.

The state machine deliberately detects only time-based connection risk. It does
not infer a root cause, operational scope, or recommended intervention.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Iterable


INCIDENT_TYPE = "BAG_CONNECTION_RISK"
DEFAULT_RISK_BUFFER_SECONDS = 30

LIFECYCLE_RANK = {
    "BAG_ACCEPTED": 0,
    "ORIGIN_SORTED": 1,
    "ORIGIN_AIRCRAFT_LOADED": 2,
    "TRANSFER_ARRIVED": 3,
    "TRANSFER_SORTER_SCAN": 4,
    "MAKEUP_AREA_SCAN": 5,
    "CONNECTING_AIRCRAFT_LOADED": 6,
    "DESTINATION_ARRIVED": 7,
}


class InvalidBaggageEvent(ValueError):
    """Raised when a baggage event cannot safely drive detector state."""


@dataclass
class BagRiskState:
    """State held independently for one ``bag_tag_id``."""

    last_event_type: str | None = None
    last_scan_time_ms: int | None = None
    last_scan_location: str | None = None
    transfer_zone: str | None = None
    airport_code: str | None = None
    outbound_flight_id: str | None = None
    connection_departure_time_ms: int | None = None
    estimated_departure_time_ms: int | None = None
    effective_departure_time_ms: int | None = None
    context_event_time_ms: int | None = None
    transfer_arrived: bool = False
    makeup_area_reached: bool = False
    aircraft_loaded: bool = False
    incident_emitted: bool = False
    risk_timer_ms: int | None = None


def parse_timestamp(value: Any, field_name: str) -> int:
    """Parse an ISO-8601 timestamp into UTC epoch milliseconds."""

    if not isinstance(value, str) or not value.strip():
        raise InvalidBaggageEvent(f"{field_name} must be a non-empty ISO timestamp")
    normalized = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise InvalidBaggageEvent(f"{field_name} is not a valid ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise InvalidBaggageEvent(f"{field_name} must include a UTC offset")
    return int(parsed.astimezone(UTC).timestamp() * 1000)


def format_timestamp(timestamp_ms: int) -> str:
    """Format epoch milliseconds as a canonical UTC ISO-8601 timestamp."""

    return (
        datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def effective_departure_from_event(event: dict[str, Any]) -> tuple[int, int | None]:
    """Return the operational departure deadline and optional estimate.

    The estimated departure on the latest event is authoritative when present;
    otherwise the connection departure is used.
    """

    connection_ms = parse_timestamp(
        event.get("connection_departure_time"), "connection_departure_time"
    )
    estimate_value = event.get("estimated_departure_time")
    estimated_ms = (
        parse_timestamp(estimate_value, "estimated_departure_time")
        if estimate_value not in (None, "")
        else None
    )
    return estimated_ms if estimated_ms is not None else connection_ms, estimated_ms


def update_state(
    state: BagRiskState,
    event: dict[str, Any],
    *,
    risk_buffer_seconds: int = DEFAULT_RISK_BUFFER_SECONDS,
) -> BagRiskState:
    """Apply one event without allowing older events to regress latest context."""

    bag_tag_id = _required_string(event, "bag_tag_id")
    del bag_tag_id  # Validated here; the key is owned by the caller.
    event_type = _required_string(event, "event_type")
    if event_type not in LIFECYCLE_RANK:
        raise InvalidBaggageEvent(f"Unsupported event_type: {event_type}")
    event_time_ms = parse_timestamp(event.get("event_time"), "event_time")

    if event_type == "TRANSFER_ARRIVED":
        state.transfer_arrived = True
    elif event_type == "MAKEUP_AREA_SCAN":
        state.makeup_area_reached = True
    elif event_type == "CONNECTING_AIRCRAFT_LOADED":
        state.aircraft_loaded = True
        state.makeup_area_reached = True

    if _is_newer_scan(state, event_type, event_time_ms):
        state.last_event_type = event_type
        state.last_scan_time_ms = event_time_ms
        state.last_scan_location = _optional_string(event.get("scan_location"))

    if state.context_event_time_ms is None or event_time_ms >= state.context_event_time_ms:
        effective_ms, estimated_ms = effective_departure_from_event(event)
        state.context_event_time_ms = event_time_ms
        state.connection_departure_time_ms = parse_timestamp(
            event.get("connection_departure_time"), "connection_departure_time"
        )
        state.estimated_departure_time_ms = estimated_ms
        state.effective_departure_time_ms = effective_ms
        state.transfer_zone = _required_string(event, "transfer_zone")
        state.airport_code = _required_string(event, "airport_code")
        state.outbound_flight_id = _required_string(event, "outbound_flight_id")

    if state.transfer_arrived and not state.incident_emitted:
        if state.makeup_area_reached or state.aircraft_loaded:
            state.risk_timer_ms = None
        elif state.effective_departure_time_ms is not None:
            state.risk_timer_ms = state.effective_departure_time_ms - (
                risk_buffer_seconds * 1000
            )
    return state


def should_emit_incident(state: BagRiskState, timer_timestamp_ms: int) -> bool:
    """Return whether the active risk timer still represents an unsafe bag."""

    return bool(
        not state.incident_emitted
        and state.transfer_arrived
        and not state.makeup_area_reached
        and not state.aircraft_loaded
        and state.risk_timer_ms == timer_timestamp_ms
        and state.effective_departure_time_ms is not None
        and state.outbound_flight_id
        and state.airport_code
    )


def build_incident(
    bag_tag_id: str,
    state: BagRiskState,
    *,
    detected_at_ms: int,
) -> dict[str, Any]:
    """Build the generic deterministic incident contract."""

    if state.effective_departure_time_ms is None or not state.outbound_flight_id:
        raise InvalidBaggageEvent("Incomplete state cannot produce a risk incident")
    if state.last_scan_time_ms is None or not state.last_event_type:
        raise InvalidBaggageEvent("Missing last scan cannot produce a risk incident")

    seconds_to_departure = int(
        (state.effective_departure_time_ms - detected_at_ms) / 1000
    )
    return {
        "incident_id": (
            f"{bag_tag_id}-{state.outbound_flight_id}-{INCIDENT_TYPE}"
        ),
        "bag_tag_id": bag_tag_id,
        "incident_type": INCIDENT_TYPE,
        "detected_at": format_timestamp(detected_at_ms),
        "airport_code": state.airport_code,
        "outbound_flight_id": state.outbound_flight_id,
        "transfer_zone": state.transfer_zone or "",
        "last_scan_type": state.last_event_type,
        "last_scan_location": state.last_scan_location or "",
        "last_scan_time": format_timestamp(state.last_scan_time_ms),
        "connection_departure_time": format_timestamp(
            state.effective_departure_time_ms
        ),
        "seconds_to_departure": seconds_to_departure,
        "observed_context": {
            "makeup_area_reached": state.makeup_area_reached,
            "aircraft_loaded": state.aircraft_loaded,
        },
    }


class ConnectionRiskDetector:
    """In-memory timer harness with the same semantics as the PyFlink operator."""

    def __init__(self, risk_buffer_seconds: int = DEFAULT_RISK_BUFFER_SECONDS):
        if risk_buffer_seconds <= 0:
            raise ValueError("risk_buffer_seconds must be positive")
        self.risk_buffer_seconds = risk_buffer_seconds
        self.states: dict[str, BagRiskState] = {}
        self.current_time_ms: int | None = None

    def process_event(self, event: dict[str, Any]) -> list[dict[str, Any]]:
        bag_tag_id = _required_string(event, "bag_tag_id")
        state = self.states.setdefault(bag_tag_id, BagRiskState())
        update_state(
            state,
            event,
            risk_buffer_seconds=self.risk_buffer_seconds,
        )
        if (
            state.risk_timer_ms is not None
            and self.current_time_ms is not None
            and state.risk_timer_ms <= self.current_time_ms
        ):
            return self._fire(bag_tag_id, state, self.current_time_ms)
        return []

    def advance_time(self, value: datetime | int) -> list[dict[str, Any]]:
        target_ms = (
            int(value.astimezone(UTC).timestamp() * 1000)
            if isinstance(value, datetime)
            else value
        )
        if self.current_time_ms is not None and target_ms < self.current_time_ms:
            raise ValueError("processing time cannot move backwards")
        self.current_time_ms = target_ms

        incidents: list[dict[str, Any]] = []
        due = sorted(
            (
                (state.risk_timer_ms, bag_tag_id, state)
                for bag_tag_id, state in self.states.items()
                if state.risk_timer_ms is not None
                and state.risk_timer_ms <= target_ms
            ),
            key=lambda item: (item[0], item[1]),
        )
        for timer_ms, bag_tag_id, state in due:
            incidents.extend(self._fire(bag_tag_id, state, timer_ms))
        return incidents

    def process_events(
        self, events: Iterable[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        incidents: list[dict[str, Any]] = []
        for event in events:
            incidents.extend(self.process_event(event))
        return incidents

    @staticmethod
    def _fire(
        bag_tag_id: str,
        state: BagRiskState,
        detected_at_ms: int,
    ) -> list[dict[str, Any]]:
        timer_ms = state.risk_timer_ms
        if timer_ms is None or not should_emit_incident(state, timer_ms):
            state.risk_timer_ms = None
            return []
        incident = build_incident(
            bag_tag_id,
            state,
            detected_at_ms=detected_at_ms,
        )
        state.incident_emitted = True
        state.risk_timer_ms = None
        return [incident]


def _is_newer_scan(
    state: BagRiskState, event_type: str, event_time_ms: int
) -> bool:
    if state.last_scan_time_ms is None or event_time_ms > state.last_scan_time_ms:
        return True
    if event_time_ms < state.last_scan_time_ms:
        return False
    current_rank = LIFECYCLE_RANK.get(state.last_event_type or "", -1)
    return LIFECYCLE_RANK[event_type] >= current_rank


def _required_string(event: dict[str, Any], field_name: str) -> str:
    value = event.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise InvalidBaggageEvent(f"{field_name} must be a non-empty string")
    return value.strip()


def _optional_string(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None
