# BagGuard cost controls

These scripts start and stop only the continuously running BagGuard compute.
They never delete EBS, Kinesis streams, the AgentCore runtime, CloudFormation
stacks, or logs.

```bash
make platform-start
make platform-status
make platform-stop
```

`platform-start` waits for EC2 status checks, a successful ClickHouse query through
the adapter Lambda, both Kinesis streams, Managed Flink `RUNNING`, the adapter
Lambda `Active`, and AgentCore `READY`.

`platform-stop` gracefully stops Managed Flink before EC2. The graceful Flink stop
is intentionally not forced, allowing the service to take its normal snapshot.

Optional overrides:

```text
AWS_REGION
BAGGUARD_STACK_NAME
BAGGUARD_CLICKHOUSE_INSTANCE_ID
BAGGUARD_FLINK_APPLICATION_NAME
BAGGUARD_BAGGAGE_STREAM_NAME
BAGGUARD_RISK_STREAM_NAME
BAGGUARD_ADAPTER_FUNCTION_NAME
BAGGUARD_AGENT_RUNTIME_ID
BAGGUARD_AGENT_RUNTIME_NAME
BAGGUARD_START_TIMEOUT_SECONDS
BAGGUARD_HEALTH_TIMEOUT_SECONDS
```

## Full-stack lifecycle

Routine cost control should use `platform-start` and `platform-stop`. Those commands
preserve data and retained infrastructure.

The full-stack commands are a separate, destructive lifecycle:

```bash
make stack-create
make stack-destroy STACK_CONFIRM=DELETE-163856954803-us-east-1-BagGuardProductionStack
make stack-recreate STACK_CONFIRM=DELETE-163856954803-us-east-1-BagGuardProductionStack
```

`stack-create` validates the configured account and Region, runs tests,
validates and deploys AgentCore, packages Flink, deploys `BagGuardProductionStack`,
starts ClickHouse and Flink, and verifies the complete environment.

`stack-destroy` stops compute, deletes `BagGuardProductionStack`, explicitly removes
its retained artifact bucket, ClickHouse secret, queues, and log groups, then
deletes the AgentCore stack and its runtime log group. The account- and Region-specific confirmation
token is mandatory. This permanently deletes ClickHouse data, EBS, streams,
logs, and investigation history. The shared `CDKToolkit` bootstrap stack is
not application infrastructure and is intentionally retained.

These lifecycle scripts are orchestration glue; they do not provision the AWS
Glue service. CDK bootstrap resources and the shared bootstrap asset cache are
account-level tooling and remain in place.

`stack-recreate` runs the destructive deletion followed by full creation in
one command. If creation fails after deletion, the old data cannot be
recovered unless a separate backup exists.
