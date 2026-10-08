# BagGuard connection-risk detector

This module contains the deterministic PyFlink detector that reads synthetic
baggage lifecycle events from `bagguard-baggage-events-prod`, keys records by
`bag_tag_id`, and writes generic `BAG_CONNECTION_RISK` incidents to
`bagguard-risk-incidents-prod`.

The detector correlates events, maintains per-bag state, tracks time, and emits
one deterministic risk incident. It does not infer root cause, affected scope,
or an operational intervention.

The Kinesis sink provides at-least-once delivery during checkpoint recovery.
The keyed `incident_emitted` state prevents duplicate logical detections, while
the deterministic `incident_id` is the downstream idempotency key if a sink
write is replayed after a failure.

## Detection rule

After `TRANSFER_ARRIVED`, the active timer is:

```text
effective departure time - RISK_BUFFER_SECONDS
```

`estimated_departure_time` from the latest event is the effective departure
when available; otherwise `connection_departure_time` is used. A later event
can therefore cancel and replace the timer. Reaching `MAKEUP_AREA_SCAN` or
`CONNECTING_AIRCRAFT_LOADED` before the timer suppresses the incident.

Processing-time timers are intentional: a stalled bag emits no subsequent
event, so an event-time watermark might not advance to fire its timer. Event
timestamps still control latest-scan and latest-flight-context selection so an
older, out-of-order record cannot regress state.

`RISK_BUFFER_SECONDS` defaults to `30`. This compressed threshold is
configurable and is not an airport SLA or minimum connection time.

## Runtime versions

- Amazon Managed Service for Apache Flink runtime: Apache Flink `2.3.0`
- Python: `3.12`
- PyFlink: `apache-flink==2.3.0`
- Kinesis source/sink connector: `6.0.1-2.0`

The connector version uses the Flink 2.x FLIP-27 source and FLIP-143 sink APIs.
The removed legacy `FlinkKinesisConsumer` is not used.

## Configuration

```text
INPUT_STREAM                      default bagguard-baggage-events-prod
OUTPUT_STREAM                     default bagguard-risk-incidents-prod
AWS_REGION                        default us-east-1
SOURCE_STREAM_ARN                 injected by CDK for the source connector
AWS_ACCOUNT_ID                    fallback used to construct the source ARN
RISK_BUFFER_SECONDS               default 30
KINESIS_STARTING_POSITION         default LATEST; or TRIM_HORIZON
CHECKPOINT_INTERVAL_MS            default 60000
FLINK_PARALLELISM                 default 1
```

The Managed Flink execution role supplies credentials through the default AWS
credential provider chain. Do not configure static AWS keys.

## Build preparation

Use Python 3.12 for PyFlink. Maven builds the exact supported Kinesis connector
and its dependencies into one shaded JAR, as required when a PyFlink
application has multiple Java dependencies. The packaging script creates the
deployable ZIP:

```bash
python3.12 -m venv .venv-flink
.venv-flink/bin/pip install -r flink/requirements.txt
scripts/package_flink.sh
```
