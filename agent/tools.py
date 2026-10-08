"""Strands tools for read-only baggage operational evidence."""

from __future__ import annotations

from typing import Any

from strands import tool

try:
    from .adapter import ReadOnlyAdapter
except ImportError:  # AgentCore CodeZip executes app.py as a top-level module.
    from adapter import ReadOnlyAdapter  # type: ignore[no-redef]


def _validate_lookback(lookback_minutes: int) -> int:
    if (
        isinstance(lookback_minutes, bool)
        or not isinstance(lookback_minutes, int)
        or not 1 <= lookback_minutes <= 10080
    ):
        raise ValueError("lookback_minutes must be an integer from 1 to 10080")
    return lookback_minutes


_BAG_TIMELINE_FIELDS = frozenset(
    {
        "event_id",
        "bag_tag_id",
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
        "scheduled_inbound_arrival_time",
        "inbound_delay_minutes",
        "baggage_system_status",
        "bag_status",
        "priority_code",
    }
)


def _sanitize_bag_timeline(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Expose operational evidence, excluding free-form metadata and PII."""

    return [
        {key: value for key, value in row.items() if key in _BAG_TIMELINE_FIELDS}
        for row in rows
    ]


def build_read_only_tools(adapter: ReadOnlyAdapter) -> list[Any]:
    """Create only the five approved investigative tools."""

    @tool
    def get_bag_timeline(bag_tag_id: str) -> list[dict[str, Any]]:
        """Get the ordered lifecycle scans for one bag.

        Args:
            bag_tag_id: Synthetic or operational bag tag identifier to inspect.
        """

        rows = adapter.query("get_bag_timeline", {"bag_tag_id": bag_tag_id})
        return _sanitize_bag_timeline(rows)

    @tool
    def get_connection_peers(
        outbound_flight_id: str,
        airport_code: str,
        lookback_minutes: int,
    ) -> list[dict[str, Any]]:
        """Get latest progression for bags sharing an outbound connection.

        Args:
            outbound_flight_id: Connecting outbound flight identifier.
            airport_code: Connection airport code.
            lookback_minutes: Recent evidence window from 1 to 10080 minutes.
        """

        return adapter.query(
            "get_connection_peers",
            {
                "outbound_flight_id": outbound_flight_id,
                "airport_code": airport_code,
                "lookback_minutes": _validate_lookback(lookback_minutes),
            },
        )

    @tool
    def get_transfer_zone_health(
        airport_code: str,
        transfer_zone: str,
        lookback_minutes: int,
    ) -> list[dict[str, Any]]:
        """Get recent event and bag counts for one transfer zone.

        Args:
            airport_code: Connection airport code.
            transfer_zone: Transfer zone identifier.
            lookback_minutes: Recent evidence window from 1 to 10080 minutes.
        """

        return adapter.query(
            "get_transfer_zone_health",
            {
                "airport_code": airport_code,
                "transfer_zone": transfer_zone,
                "lookback_minutes": _validate_lookback(lookback_minutes),
            },
        )

    @tool
    def get_flight_bag_progress(
        outbound_flight_id: str,
    ) -> list[dict[str, Any]]:
        """Get latest baggage progress for an outbound flight.

        Args:
            outbound_flight_id: Connecting outbound flight identifier.
        """

        return adapter.query(
            "get_flight_bag_progress",
            {"outbound_flight_id": outbound_flight_id},
        )

    @tool
    def get_recent_connection_risks(
        airport_code: str,
        lookback_minutes: int,
    ) -> list[dict[str, Any]]:
        """Get recent generic connection-risk incidents at an airport.

        Args:
            airport_code: Connection airport code.
            lookback_minutes: Recent evidence window from 1 to 10080 minutes.
        """

        return adapter.query(
            "get_recent_connection_risks",
            {
                "airport_code": airport_code,
                "lookback_minutes": _validate_lookback(lookback_minutes),
            },
        )

    return [
        get_bag_timeline,
        get_connection_peers,
        get_transfer_zone_health,
        get_flight_bag_progress,
        get_recent_connection_risks,
    ]
