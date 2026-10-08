#!/usr/bin/env python3
"""Seed synthetic historical BagGuard journeys through Kinesis."""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any, Sequence

from producer import (
    BAG_STATUSES,
    EVENT_KEYS,
    LIFECYCLE,
    STREAM_NAME,
    publish_events,
)


DEFAULT_JOURNEYS = 750
DEFAULT_HISTORY_DAYS = 7
DEFAULT_RANDOM_SEED = 20261004

INBOUND_FLIGHTS = tuple(f"AI-SYN-{number}" for number in range(501, 509))
OUTBOUND_FLIGHTS = tuple(f"AI-SYN-{number}" for number in range(801, 813))
TRANSFER_ZONES = ("T1-SYN", "T2-SYN", "T3-SYN", "T4-SYN")
GATES = tuple(f"G-SYN-{number:02d}" for number in range(1, 19))

SCAN_LOCATION_TEMPLATES = {
    "BAG_ACCEPTED": "{airport}-SYN-ACCEPTANCE-{lane:02d}",
    "ORIGIN_SORTED": "{airport}-SYN-ORIGIN-SORT-{lane:02d}",
    "ORIGIN_AIRCRAFT_LOADED": "{airport}-SYN-ORIGIN-RAMP-{lane:02d}",
    "TRANSFER_ARRIVED": "{airport}-SYN-TRANSFER-INFEED-{zone}",
    "TRANSFER_SORTER_SCAN": "{airport}-SYN-SORTER-{zone}",
    "MAKEUP_AREA_SCAN": "{airport}-SYN-MAKEUP-{zone}",
    "CONNECTING_AIRCRAFT_LOADED": "{airport}-SYN-GATE-{gate}",
    "DESTINATION_ARRIVED": "{airport}-SYN-DESTINATION-BELT-{lane:02d}",
}


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def _default_run_id(now: datetime) -> str:
    return now.astimezone(UTC).strftime("%Y%m%dT%H%M%S")


def generate_history(
    *,
    journey_count: int = DEFAULT_JOURNEYS,
    history_days: int = DEFAULT_HISTORY_DAYS,
    random_seed: int = DEFAULT_RANDOM_SEED,
    now: datetime | None = None,
    run_id: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Create deterministic synthetic historical baggage events."""

    if not 500 <= journey_count <= 1000:
        raise ValueError("journey_count must be between 500 and 1000")
    if not 2 <= history_days <= 30:
        raise ValueError("history_days must be between 2 and 30")

    generated_at = (now or datetime.now(UTC)).astimezone(UTC)
    history_run_id = _sanitize_run_id(run_id or _default_run_id(generated_at))
    rng = random.Random(random_seed)

    scenario_counts = _scenario_counts(journey_count)
    patterns = [
        pattern
        for pattern, count in scenario_counts.items()
        for _ in range(count)
    ]
    rng.shuffle(patterns)
    congestion_clusters = _congestion_clusters(
        rng, generated_at, history_days, max(3, scenario_counts["ZONE_CONGESTION"] // 10)
    )

    all_events: list[dict[str, Any]] = []
    connected_bags = 0
    durations: list[int] = []
    pattern_summary: Counter[str] = Counter()

    for index, pattern in enumerate(patterns, start=1):
        journey = _build_historical_journey(
            rng=rng,
            index=index,
            pattern=pattern,
            generated_at=generated_at,
            history_days=history_days,
            history_run_id=history_run_id,
            random_seed=random_seed,
            congestion_clusters=congestion_clusters,
        )
        all_events.extend(journey["events"])
        connected_bags += int(journey["connected"])
        durations.append(journey["planned_connection_minutes"])
        pattern_summary[pattern] += 1

    all_events.sort(key=lambda event: (event["event_time"], event["bag_tag_id"]))
    summary = {
        "history_run_id": history_run_id,
        "journey_count": journey_count,
        "event_count": len(all_events),
        "connected_bags": connected_bags,
        "connection_success_percent": round(connected_bags * 100 / journey_count, 2),
        "patterns": dict(sorted(pattern_summary.items())),
        "inbound_flights": len({event["inbound_flight_id"] for event in all_events}),
        "outbound_flights": len({event["outbound_flight_id"] for event in all_events}),
        "transfer_zones": len({event["transfer_zone"] for event in all_events}),
        "gates": len({event["current_gate"] for event in all_events}),
        "connection_duration_min_minutes": min(durations),
        "connection_duration_max_minutes": max(durations),
    }
    return all_events, summary


def _scenario_counts(journey_count: int) -> dict[str, int]:
    isolated = round(journey_count * 0.04)
    congestion = round(journey_count * 0.04)
    inbound_delay = round(journey_count * 0.10)
    normal = journey_count - isolated - congestion - inbound_delay
    return {
        "NORMAL": normal,
        "INBOUND_DELAY": inbound_delay,
        "ISOLATED_STALL": isolated,
        "ZONE_CONGESTION": congestion,
    }


def _congestion_clusters(
    rng: random.Random,
    generated_at: datetime,
    history_days: int,
    cluster_count: int,
) -> list[dict[str, Any]]:
    clusters = []
    for index in range(cluster_count):
        zone = TRANSFER_ZONES[index % len(TRANSFER_ZONES)]
        departure = generated_at - timedelta(
            seconds=rng.uniform(6 * 3600, (history_days - 0.25) * 86400)
        )
        outbound_flights = rng.sample(OUTBOUND_FLIGHTS, k=3)
        clusters.append(
            {
                "id": f"ZC-{index + 1:02d}",
                "zone": zone,
                "departure": departure,
                "outbound_flights": outbound_flights,
            }
        )
    return clusters


def _build_historical_journey(
    *,
    rng: random.Random,
    index: int,
    pattern: str,
    generated_at: datetime,
    history_days: int,
    history_run_id: str,
    random_seed: int,
    congestion_clusters: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    if pattern == "ZONE_CONGESTION":
        cluster = congestion_clusters[index % len(congestion_clusters)]
        departure = cluster["departure"] + timedelta(minutes=rng.uniform(-8, 8))
        transfer_zone = cluster["zone"]
        outbound_flight_id = rng.choice(cluster["outbound_flights"])
    else:
        departure = generated_at - timedelta(
            seconds=rng.uniform(4 * 3600, (history_days - 0.25) * 86400)
        )
        transfer_zone = rng.choice(TRANSFER_ZONES)
        outbound_flight_id = rng.choice(OUTBOUND_FLIGHTS)

    gate_index = OUTBOUND_FLIGHTS.index(outbound_flight_id) % len(GATES)
    current_gate = GATES[gate_index]
    inbound_flight_id = rng.choice(INBOUND_FLIGHTS)
    planned_connection_minutes = round(rng.triangular(55, 120, 78))
    scheduled_inbound_arrival = departure - timedelta(
        minutes=planned_connection_minutes
    )

    inbound_delay_minutes = 0
    if pattern == "INBOUND_DELAY":
        inbound_delay_minutes = round(rng.triangular(20, 48, 32))
    else:
        inbound_delay_minutes = round(rng.uniform(0, 6))
    inbound_arrival = scheduled_inbound_arrival + timedelta(
        minutes=inbound_delay_minutes
    )
    transfer_arrived = inbound_arrival + timedelta(minutes=rng.uniform(3, 7))

    connected = pattern == "NORMAL"
    if pattern == "INBOUND_DELAY":
        available_minutes = (departure - transfer_arrived).total_seconds() / 60
        connected = available_minutes >= 16 and rng.random() < 0.80

    event_types: Sequence[str]
    if pattern in {"ISOLATED_STALL", "ZONE_CONGESTION"}:
        event_types = LIFECYCLE[:5]
    elif connected:
        event_types = LIFECYCLE
    else:
        event_types = LIFECYCLE[:6]

    accepted = scheduled_inbound_arrival - timedelta(minutes=rng.uniform(130, 190))
    origin_sorted = accepted + timedelta(minutes=rng.uniform(8, 20))
    origin_loaded = origin_sorted + timedelta(minutes=rng.uniform(12, 30))
    sorter_scan = transfer_arrived + timedelta(minutes=rng.uniform(3, 9))

    available_after_sorter = max(
        6.0, (departure - sorter_scan).total_seconds() / 60
    )
    progression_minutes = min(
        rng.triangular(10, 36, 21), max(5.0, available_after_sorter - 3)
    )
    makeup_scan = sorter_scan + timedelta(minutes=progression_minutes * 0.60)
    connected_loaded = sorter_scan + timedelta(minutes=progression_minutes)
    if not connected:
        makeup_scan = sorter_scan + timedelta(minutes=rng.uniform(7, 18))
    destination_arrived = departure + timedelta(minutes=rng.uniform(75, 180))

    timestamps = {
        "BAG_ACCEPTED": accepted,
        "ORIGIN_SORTED": origin_sorted,
        "ORIGIN_AIRCRAFT_LOADED": origin_loaded,
        "TRANSFER_ARRIVED": transfer_arrived,
        "TRANSFER_SORTER_SCAN": sorter_scan,
        "MAKEUP_AREA_SCAN": makeup_scan,
        "CONNECTING_AIRCRAFT_LOADED": connected_loaded,
        "DESTINATION_ARRIVED": destination_arrived,
    }

    bag_suffix = f"{index:06d}"
    bag_tag_id = f"BG-SYN-HIST-{history_run_id}-{bag_suffix}"
    metadata = {
        "synthetic": True,
        "data_classification": "SYNTHETIC_TEST_DATA_ONLY",
        "history_run_id": history_run_id,
        "generator": "bagguard-history-seeder-v1",
        "random_seed": random_seed,
        "scheduled_inbound_arrival_time": _iso(scheduled_inbound_arrival),
        "planned_connection_minutes": planned_connection_minutes,
        "timing_note": (
            "Synthetic historical distribution; not a real airport SLA or "
            "minimum connection time."
        ),
    }
    lane = 1 + (index % 12)
    events = []
    for event_index, event_type in enumerate(event_types, start=1):
        scan_location = SCAN_LOCATION_TEMPLATES[event_type].format(
            airport="BLR",
            lane=lane,
            zone=transfer_zone,
            gate=current_gate,
        )
        event = {
            "event_id": (
                f"EVT-SYN-HIST-{history_run_id}-{bag_suffix}-{event_index:02d}"
            ),
            "bag_tag_id": bag_tag_id,
            "passenger_id": f"PAX-SYN-HIST-{history_run_id}-{bag_suffix}",
            "itinerary_id": f"ITIN-SYN-HIST-{history_run_id}-{bag_suffix}",
            "inbound_flight_id": inbound_flight_id,
            "outbound_flight_id": outbound_flight_id,
            "airport_code": "BLR",
            "event_type": event_type,
            "event_time": _iso(timestamps[event_type]),
            "scan_location": scan_location,
            "transfer_zone": transfer_zone,
            "current_gate": current_gate,
            "connection_departure_time": _iso(departure),
            "scheduled_departure_time": _iso(departure),
            "estimated_departure_time": _iso(departure),
            "inbound_arrival_time": _iso(inbound_arrival),
            "bag_status": BAG_STATUSES[event_type],
            "priority_code": (
                "HOT"
                if inbound_delay_minutes >= 20 or planned_connection_minutes < 60
                else "NORMAL"
            ),
            "metadata": dict(metadata),
        }
        if set(event) != EVENT_KEYS:
            raise RuntimeError("Generated historical event does not match schema")
        events.append(event)

    return {
        "events": events,
        "connected": connected,
        "planned_connection_minutes": planned_connection_minutes,
    }


def _sanitize_run_id(value: str) -> str:
    sanitized = "".join(character for character in value.upper() if character.isalnum() or character == "-")
    if not sanitized or len(sanitized) > 32:
        raise ValueError("run_id must contain 1-32 letters, numbers, or hyphens")
    return sanitized


def print_summary(summary: dict[str, Any]) -> None:
    print("BagGuard synthetic historical seed")
    print(
        "Data classification: synthetic scenario data only; distributions "
        "are not real airport SLA/MCT values."
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Seed synthetic BagGuard baggage history through Kinesis"
    )
    parser.add_argument("--journeys", type=int, default=DEFAULT_JOURNEYS)
    parser.add_argument("--history-days", type=int, default=DEFAULT_HISTORY_DAYS)
    parser.add_argument("--seed", type=int, default=DEFAULT_RANDOM_SEED)
    parser.add_argument("--run-id")
    parser.add_argument("--stream-name", default=STREAM_NAME)
    parser.add_argument(
        "--region",
        default=os.environ.get("AWS_REGION")
        or os.environ.get("AWS_DEFAULT_REGION")
        or "us-east-1",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        events, summary = generate_history(
            journey_count=args.journeys,
            history_days=args.history_days,
            random_seed=args.seed,
            run_id=args.run_id,
        )
    except ValueError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    print_summary(summary)
    if args.dry_run:
        print(f"Dry run: {len(events)} events were not published.")
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
        f"Published {published} synthetic events from {summary['journey_count']} "
        f"journeys to {args.stream_name} in {args.region}."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
