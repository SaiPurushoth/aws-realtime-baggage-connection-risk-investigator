# BagGuard synthetic event producer

The producer creates synthetic airport baggage events only. Every identifier
contains `SYN`, and no real passenger information or airport operational data
is used.

Install its pinned dependency:

```bash
.venv/bin/python -m pip install -r simulator/requirements.txt
source .venv/bin/activate
```

Run a scenario:

```bash
python simulator/producer.py --scenario normal
python simulator/producer.py --scenario isolated-stall
python simulator/producer.py --scenario zone-congestion
python simulator/producer.py --scenario inbound-delay
```

Use `--dry-run` to print without publishing. The connection departure is 75
seconds from scenario generation by default and can be set from 45 through 90
seconds with `--departure-seconds`. This is compressed synthetic scenario
timing and is not a real airport service-level agreement or minimum connection
time.

## Historical seed

Seed 500–1000 synthetic journeys through the same Kinesis ingestion path:

```bash
python simulator/seed_history.py --journeys 750 --history-days 7
```

The default deterministic distribution is mostly successful connections with
smaller populations of isolated stalls, clustered transfer-zone congestion,
and inbound delays. It spans several synthetic flights, zones, gates, and
connection durations. The seeder publishes only baggage lifecycle events to
`bagguard-baggage-events-prod`; it never calls AgentCore or writes risk incidents.

The stored metadata contains generation provenance and scheduled timing only.
It does not contain root-cause classifications, recommendations, or future
agent conclusions.
