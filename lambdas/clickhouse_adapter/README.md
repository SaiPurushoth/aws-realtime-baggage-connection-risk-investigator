# ClickHouse adapter Lambda

`bagguard-clickhouse-adapter-prod` provides two invocation modes:

- Kinesis batches from the baggage-event and risk-incident streams are decoded,
  validated, grouped by destination table, and inserted with one ClickHouse HTTP
  request per batch.
- Direct invocations use an explicit action allow-list. Queries are fixed in the
  adapter and caller values are supplied through ClickHouse typed query
  parameters. Arbitrary SQL is never accepted.

The function runs inside the BagGuard VPC with `bagguard-adapter-sg-prod`. It
connects only to the ClickHouse private address on port 8123 and to the private
Secrets Manager VPC endpoint on port 443.

Action invocation shape:

```json
{
  "action": "health_check",
  "parameters": {}
}
```

Allowed actions:

- `health_check`
- `get_bag_timeline`
- `get_connection_peers`
- `get_transfer_zone_health`
- `get_recent_connection_risks`
- `get_flight_bag_progress`
- `get_investigation_result`
- `get_average_transfer_progression_by_zone`
- `get_risk_frequency_by_transfer_zone`
- `get_connection_success_rate`
- `save_investigation_result`

Historical risk frequency is an operational event metric: bags whose connection
departure is in the past and which have no
`CONNECTING_AIRCRAFT_LOADED` event. It does not assert a root cause and is not a
substitute for a Flink `BAG_CONNECTION_RISK` incident.

The two event-source mappings read from `TRIM_HORIZON`, batch up to 500 records
for as long as five seconds, report individual failed records, bisect failed
batches, retry three times for up to one hour, and send exhausted failures to
the encrypted adapter failure queue.
