#!/usr/bin/env python3
"""Generate and publish synthetic BagGuard baggage journeys."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any, Iterable, Sequence


STREAM_NAME = "bagguard-baggage-events-prod"
DEFAULT_REGION = "us-east-1"
DEFAULT_DEPARTURE_SECONDS = 75

LIFECYCLE = (
    "BAG_ACCEPTED",
    "ORIGIN_SORTED",
    "ORIGIN_AIRCRAFT_LOADED",
    "TRANSFER_ARRIVED",
    "TRANSFER_SORTER_SCAN",
    "MAKEUP_AREA_SCAN",
    "CONNECTING_AIRCRAFT_LOADED",
    "DESTINATION_ARRIVED",
)
CONNECTION_JOURNEY = LIFECYCLE[:-1]

SCENARIO_ALIASES = {
    "normal": "NORMAL_CONNECTION",
    "isolated-stall": "ISOLATED_BAG_STALL",
    "zone-congestion": "TRANSFER_ZONE_CONGESTION",
    "inbound-delay": "INBOUND_FLIGHT_DELAY",
}

EVENT_OFFSETS_SECONDS = {
    "BAG_ACCEPTED": -240,
    "ORIGIN_SORTED": -210,
    "ORIGIN_AIRCRAFT_LOADED": -180,
    "TRANSFER_ARRIVED": 0,
    "TRANSFER_SORTER_SCAN": 10,
    "MAKEUP_AREA_SCAN": 20,
    "CONNECTING_AIRCRAFT_LOADED": 30,
}

SCAN_LOCATIONS = {
    "BAG_ACCEPTED": "ACCEPTANCE-01",
    "ORIGIN_SORTED": "ORIGIN-SORT-01",
    "ORIGIN_AIRCRAFT_LOADED": "ORIGIN-RAMP-01",
    "TRANSFER_ARRIVED": "TRANSFER-INFEED",
    "TRANSFER_SORTER_SCAN": "SORTER",
    "MAKEUP_AREA_SCAN": "MAKEUP",
    "CONNECTING_AIRCRAFT_LOADED": "GATE",
    "DESTINATION_ARRIVED": "DESTINATION-BELT-01",
}

BAG_STATUSES = {
    "BAG_ACCEPTED": "ACCEPTED",
    "ORIGIN_SORTED": "ORIGIN_SORTED",
    "ORIGIN_AIRCRAFT_LOADED": "IN_TRANSIT",
    "TRANSFER_ARRIVED": "AT_TRANSFER_AIRPORT",
    "TRANSFER_SORTER_SCAN": "IN_TRANSFER",
    "MAKEUP_AREA_SCAN": "AT_MAKEUP_AREA",
    "CONNECTING_AIRCRAFT_LOADED": "LOADED",
    "DESTINATION_ARRIVED": "ARRIVED",
}

EVENT_KEYS = {
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
    "metadata",
}


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def _run_id(now: datetime) -> str:
    return now.astimezone(UTC).strftime("%Y%m%dT%H%M%S%f")[:-3]


def _safe_run_id(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9-]", "-", value).strip("-")
    if not sanitized or len(sanitized) > 40:
        raise ValueError("run_id must contain 1-40 letters, numbers, or hyphens")
    return sanitized.upper()


def generate_scenario(
    scenario: str,
    *,
    now: datetime | None = None,
    run_id: str | None = None,
    departure_seconds: int = DEFAULT_DEPARTURE_SECONDS,
) -> list[dict[str, Any]]:
    """Return one complete synthetic scenario as event dictionaries."""

    if scenario in SCENARIO_ALIASES:
        scenario = SCENARIO_ALIASES[scenario]
    if scenario not in SCENARIO_ALIASES.values():
        raise ValueError(f"Unsupported scenario: {scenario}")
    if not 45 <= departure_seconds <= 90:
        raise ValueError("departure_seconds must be between 45 and 90")

    scenario_now = (now or datetime.now(UTC)).astimezone(UTC)
    scenario_run_id = _safe_run_id(run_id or _run_id(scenario_now))
    connection_departure = scenario_now + timedelta(seconds=departure_seconds)
    normal_inbound_arrival = scenario_now - timedelta(seconds=20)
    airport_code = {
        "NORMAL_CONNECTION": "BGR",
        "ISOLATED_BAG_STALL": "BGA",
        "TRANSFER_ZONE_CONGESTION": "BGB",
        "INBOUND_FLIGHT_DELAY": "BGC",
    }[scenario]
    inbound_flight_id = f"AI-SYN-501-{scenario_run_id}"
    primary_outbound = f"AI-SYN-802-{scenario_run_id}"
    secondary_outbound = f"AI-SYN-803-{scenario_run_id}"
    primary_zone = f"T2-{scenario_run_id}"
    control_zone = f"T3-{scenario_run_id}"

    bag_specs: list[dict[str, Any]]
    if scenario == "NORMAL_CONNECTION":
        bag_specs = [
            {
                "index": 1,
                "role": "TARGET",
                "events": CONNECTION_JOURNEY,
                "inbound_flight_id": inbound_flight_id,
                "outbound_flight_id": primary_outbound,
                "transfer_zone": primary_zone,
                "gate": "G-SYN-12",
                "priority_code": "NORMAL",
                "inbound_arrival": normal_inbound_arrival,
                "expected_risk_signal": False,
            }
        ]
    elif scenario == "ISOLATED_BAG_STALL":
        bag_specs = [
            {
                "index": 1,
                "role": "TARGET_STALLED",
                "events": LIFECYCLE[:5],
                "inbound_flight_id": inbound_flight_id,
                "outbound_flight_id": primary_outbound,
                "transfer_zone": primary_zone,
                "gate": "G-SYN-12",
                "priority_code": "HOT",
                "inbound_arrival": normal_inbound_arrival,
                "expected_risk_signal": True,
            }
        ]
        bag_specs.extend(
            {
                "index": index,
                "role": "CONNECTION_PEER",
                "events": CONNECTION_JOURNEY,
                "inbound_flight_id": inbound_flight_id,
                "outbound_flight_id": primary_outbound,
                "transfer_zone": primary_zone,
                "gate": "G-SYN-12",
                "priority_code": "NORMAL",
                "inbound_arrival": normal_inbound_arrival,
                "expected_risk_signal": False,
            }
            for index in range(2, 6)
        )
    elif scenario == "TRANSFER_ZONE_CONGESTION":
        bag_specs = [
            {
                "index": index,
                "role": "ZONE_AFFECTED",
                "events": LIFECYCLE[:5],
                "inbound_flight_id": inbound_flight_id,
                "outbound_flight_id": (
                    primary_outbound if index <= 4 else secondary_outbound
                ),
                "transfer_zone": primary_zone,
                "gate": "G-SYN-12" if index <= 4 else "G-SYN-14",
                "priority_code": "HOT",
                "inbound_arrival": normal_inbound_arrival,
                "expected_risk_signal": True,
            }
            for index in range(1, 9)
        ]
        bag_specs.extend(
            {
                "index": index,
                "role": "CONTROL_ZONE_NORMAL",
                "events": CONNECTION_JOURNEY,
                "inbound_flight_id": inbound_flight_id,
                "outbound_flight_id": (
                    primary_outbound if index <= 10 else secondary_outbound
                ),
                "transfer_zone": control_zone,
                "gate": "G-SYN-12" if index <= 10 else "G-SYN-14",
                "priority_code": "NORMAL",
                "inbound_arrival": normal_inbound_arrival,
                "expected_risk_signal": False,
            }
            for index in range(9, 13)
        )
    else:
        delayed_arrival = scenario_now - timedelta(seconds=5)
        bag_specs = [
            {
                "index": index,
                "role": "INBOUND_DELAY_COHORT",
                "events": LIFECYCLE[:5],
                "inbound_flight_id": inbound_flight_id,
                "outbound_flight_id": primary_outbound,
                "transfer_zone": primary_zone,
                "gate": "G-SYN-12",
                "priority_code": "HOT",
                "inbound_arrival": delayed_arrival,
                "scheduled_inbound_arrival": delayed_arrival
                - timedelta(minutes=32),
                "inbound_delay_minutes": 32,
                "baggage_system_status": "NORMAL",
                "expected_risk_signal": True,
            }
            for index in range(1, 4)
        ]

    events: list[dict[str, Any]] = []
    for spec in bag_specs:
        events.extend(
            _build_bag_events(
                scenario=scenario,
                scenario_run_id=scenario_run_id,
                scenario_now=scenario_now,
                connection_departure=connection_departure,
                departure_seconds=departure_seconds,
                airport_code=airport_code,
                **spec,
            )
        )

    return sorted(events, key=lambda event: (event["event_time"], event["bag_tag_id"]))


def _build_bag_events(
    *,
    scenario: str,
    scenario_run_id: str,
    scenario_now: datetime,
    connection_departure: datetime,
    departure_seconds: int,
    airport_code: str,
    index: int,
    role: str,
    events: Sequence[str],
    inbound_flight_id: str,
    outbound_flight_id: str,
    transfer_zone: str,
    gate: str,
    priority_code: str,
    inbound_arrival: datetime,
    expected_risk_signal: bool,
    scheduled_inbound_arrival: datetime | None = None,
    inbound_delay_minutes: int = 0,
    baggage_system_status: str = "NORMAL",
) -> list[dict[str, Any]]:
    bag_suffix = f"{index:04d}"
    bag_tag_id = f"BG-SYN-{scenario_run_id}-{bag_suffix}"
    passenger_id = f"PAX-SYN-{scenario_run_id}-{bag_suffix}"
    itinerary_id = f"ITIN-SYN-{scenario_run_id}-{bag_suffix}"

    metadata = {
        "synthetic": True,
        "data_classification": "SYNTHETIC_ONLY",
        "scenario": scenario,
        "scenario_run_id": scenario_run_id,
        "bag_role": role,
        "expected_risk_signal": expected_risk_signal,
        "compressed_timeframe": True,
        "connection_departure_seconds_from_start": departure_seconds,
        "connection_risk_threshold_seconds": 30,
        "timing_note": (
            "Compressed synthetic timing; not a real airport "
            "SLA or minimum connection time."
        ),
        "baggage_system_status": baggage_system_status,
        "inbound_delay_minutes": inbound_delay_minutes,
    }
    if scheduled_inbound_arrival is not None:
        metadata["scheduled_inbound_arrival_time"] = _iso(
            scheduled_inbound_arrival
        )

    generated: list[dict[str, Any]] = []
    for event_index, event_type in enumerate(events, start=1):
        if event_type == "DESTINATION_ARRIVED":
            event_time = connection_departure + timedelta(seconds=90)
        else:
            event_time = scenario_now + timedelta(
                seconds=EVENT_OFFSETS_SECONDS[event_type]
            )
        generated.append(
            {
                "event_id": (
                    f"EVT-SYN-{scenario_run_id}-{bag_suffix}-{event_index:02d}"
                ),
                "bag_tag_id": bag_tag_id,
                "passenger_id": passenger_id,
                "itinerary_id": itinerary_id,
                "inbound_flight_id": inbound_flight_id,
                "outbound_flight_id": outbound_flight_id,
                "airport_code": airport_code,
                "event_type": event_type,
                "event_time": _iso(event_time),
                "scan_location": _scan_location(
                    event_type, airport_code, transfer_zone, gate
                ),
                "transfer_zone": transfer_zone,
                "current_gate": gate,
                "connection_departure_time": _iso(connection_departure),
                "scheduled_departure_time": _iso(connection_departure),
                "estimated_departure_time": _iso(connection_departure),
                "inbound_arrival_time": _iso(inbound_arrival),
                "bag_status": BAG_STATUSES[event_type],
                "priority_code": priority_code,
                "metadata": dict(metadata),
            }
        )
    return generated


def _scan_location(
    event_type: str,
    airport_code: str,
    transfer_zone: str,
    gate: str,
) -> str:
    base = SCAN_LOCATIONS[event_type]
    if event_type in {
        "TRANSFER_ARRIVED",
        "TRANSFER_SORTER_SCAN",
        "MAKEUP_AREA_SCAN",
    }:
        return f"{airport_code}-{transfer_zone}-{base}"
    if event_type == "CONNECTING_AIRCRAFT_LOADED":
        return f"{airport_code}-{gate}-{base}"
    return f"{airport_code}-{base}"


def publish_events(
    client: Any,
    events: Sequence[dict[str, Any]],
    *,
    stream_name: str = STREAM_NAME,
    max_attempts: int = 3,
) -> int:
    """Publish events with PutRecords, retrying only failed entries."""

    records = []
    for event in events:
        payload = json.dumps(event, separators=(",", ":"), ensure_ascii=False).encode(
            "utf-8"
        )
        if len(payload) > 1_000_000:
            raise ValueError(f"Event {event['event_id']} exceeds the Kinesis limit")
        records.append({"Data": payload, "PartitionKey": event["bag_tag_id"]})

    published = 0
    for batch in _record_batches(records):
        pending = batch
        for attempt in range(1, max_attempts + 1):
            response = client.put_records(StreamName=stream_name, Records=pending)
            results = response.get("Records", [])
            if len(results) != len(pending):
                raise RuntimeError("Kinesis returned an invalid PutRecords response")

            failed = [
                record
                for record, result in zip(pending, results, strict=True)
                if result.get("ErrorCode")
            ]
            published += len(pending) - len(failed)
            if not failed:
                break
            if attempt == max_attempts:
                raise RuntimeError(
                    f"Kinesis rejected {len(failed)} records after {max_attempts} attempts"
                )
            pending = failed
            time.sleep(0.2 * (2 ** (attempt - 1)))

    return published


def _record_batches(
    records: Sequence[dict[str, Any]],
) -> Iterable[list[dict[str, Any]]]:
    batch: list[dict[str, Any]] = []
    batch_bytes = 0
    for record in records:
        record_bytes = len(record["Data"]) + len(record["PartitionKey"].encode("utf-8"))
        if batch and (len(batch) == 500 or batch_bytes + record_bytes > 5_000_000):
            yield batch
            batch = []
            batch_bytes = 0
        batch.append(record)
        batch_bytes += record_bytes
    if batch:
        yield batch


def print_journey(events: Sequence[dict[str, Any]], scenario: str) -> None:
    print(f"BagGuard synthetic scenario: {scenario}")
    print(
        "Timing: compressed synthetic timeframe; "
        "not a real airport SLA or minimum connection time."
    )
    if events:
        departure_seconds = events[0]["metadata"][
            "connection_departure_seconds_from_start"
        ]
        print(
            f"Connection departure: {events[0]['connection_departure_time']} "
            f"({departure_seconds} seconds from scenario start)"
        )
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        grouped[event["bag_tag_id"]].append(event)

    for bag_tag_id, bag_events in sorted(grouped.items()):
        role = bag_events[0]["metadata"]["bag_role"]
        print(f"\n{bag_tag_id} [{role}]")
        for event in sorted(bag_events, key=lambda item: item["event_time"]):
            print(
                f"  {event['event_time']}  {event['event_type']:<30} "
                f"{event['scan_location']}"
            )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Publish synthetic BagGuard airport baggage events"
    )
    parser.add_argument("--scenario", choices=sorted(SCENARIO_ALIASES), required=True)
    parser.add_argument("--stream-name", default=STREAM_NAME)
    parser.add_argument(
        "--region",
        default=os.environ.get("AWS_REGION")
        or os.environ.get("AWS_DEFAULT_REGION")
        or DEFAULT_REGION,
    )
    parser.add_argument("--run-id")
    parser.add_argument(
        "--departure-seconds", type=int, default=DEFAULT_DEPARTURE_SECONDS
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the synthetic journey without publishing to Kinesis",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    scenario = SCENARIO_ALIASES[args.scenario]
    try:
        events = generate_scenario(
            scenario,
            run_id=args.run_id,
            departure_seconds=args.departure_seconds,
        )
    except ValueError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    print_journey(events, scenario)
    if args.dry_run:
        print(f"\nDry run: {len(events)} events were not published.")
        return 0

    try:
        import boto3
    except ImportError:
        print(
            "boto3 is required. Install simulator/requirements.txt first.",
            file=sys.stderr,
        )
        return 2

    client = boto3.client("kinesis", region_name=args.region)
    published = publish_events(client, events, stream_name=args.stream_name)
    print(
        f"\nPublished {published} synthetic events to {args.stream_name} "
        f"in {args.region}."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
