# BagGuard agent dispatcher

`bagguard-agent-dispatcher-prod` consumes generic `BAG_CONNECTION_RISK`
incidents from Kinesis, invokes the BagGuard AgentCore Runtime, validates the
strict structured result, and asks the ClickHouse adapter to persist it.

The function contains no baggage root-cause or intervention logic. It has no
VPC attachment, database endpoint, database credentials, EC2 permission, or
direct ClickHouse access. Its only downstream permissions are invocation of
the named AgentCore Runtime and `bagguard-clickhouse-adapter-prod`.

Before invoking AgentCore, the dispatcher asks the adapter whether the
deterministic `incident_id` already has a stored result. A retry therefore
acknowledges an existing result instead of inserting another row.
