# BagGuard investigator

`bagguard-investigator-prod` is the Strands-based **Real-Time Baggage
Connection Risk Investigator** hosted by Amazon Bedrock AgentCore Runtime.

Flink has already emitted `BAG_CONNECTION_RISK` before this component is
called. The investigator explains the most likely operational cause from the
bag timeline and, when needed, peer, flight, zone, and recent-risk evidence.
It does not re-detect risk and cannot modify operational systems.

## Runtime contract

The AgentCore entrypoint is `agent.app`. It accepts:

```json
{
  "incident_id": "BG-001-AI-802-BAG_CONNECTION_RISK",
  "bag_tag_id": "BG-001",
  "outbound_flight_id": "AI-802",
  "airport_code": "BLR",
  "transfer_zone": "T2",
  "last_scan_type": "TRANSFER_SORTER_SCAN",
  "last_scan_location": "SORTER-T2",
  "seconds_to_departure": 30
}
```

The response is validated with `InvestigationResult` and contains only the
specified incident and bag identifiers, classification, root cause, scope,
priority, evidence, recommended action, and its time sensitivity.

## Read-only tools

The agent receives exactly five tools:

- `get_bag_timeline`
- `get_connection_peers`
- `get_transfer_zone_health`
- `get_flight_bag_progress`
- `get_recent_connection_risks`

Every tool invokes `bagguard-clickhouse-adapter-prod`. The client rejects all
other actions, including `save_investigation_result`, and never accepts SQL.
The future runtime role needs only `lambda:InvokeFunction` for that one Lambda
and `bedrock:InvokeModelWithResponseStream` for the configured model.

## Configuration

```text
AWS_REGION=us-east-1
BAGGUARD_MODEL_ID=global.amazon.nova-2-lite-v1:0
BAGGUARD_CLICKHOUSE_ADAPTER_FUNCTION_NAME=bagguard-clickhouse-adapter-prod
```

Install dependencies with `pip install -r agent/requirements.txt`. Running
`python -m agent.app` starts the local AgentCore-compatible server. No runtime
or other AWS infrastructure is created by this module.
