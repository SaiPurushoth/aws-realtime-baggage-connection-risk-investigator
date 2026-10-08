# Repository guidance

## Product intent

BagGuard is a real-time airport operations solution. It detects checked baggage at risk of missing a connecting flight while intervention is still possible. It is not a post-departure lost-baggage tracker.

Use the full name **Real-Time Baggage Connection Risk Investigator** in formal documentation and **BagGuard** elsewhere.

## Safety boundary

- Use synthetic passenger, baggage, flight, and airport data only.
- The investigator produces evidence and recommendations only.
- Never add a path that changes airline, airport, flight, baggage, or passenger systems.
- Agent-facing ClickHouse tools must remain read-only and limited to allow-listed parameterized queries.
- Persist investigation results outside the agent tool boundary through a narrowly scoped application operation.
- Never commit credentials, tokens, private keys, connection strings, or real personal data.

## Architecture invariants

- Kinesis is the event backbone.
- Flink owns deterministic, stateful risk detection and emits `BAG_CONNECTION_RISK`.
- Do not encode an unverified root cause in a Flink incident.
- ClickHouse is the unified real-time and historical analytical serving layer.
- AgentCore investigates likely cause from stored context; it does not replace deterministic detection.
- Stream consumers and ClickHouse writes must tolerate at-least-once delivery.
- Resource names use `bagguard` plus an explicit environment value.
- CloudWatch log groups follow `/bagguard/{environment}/...`.
- ClickHouse must remain inaccessible from the public internet; port `8123` accepts traffic only from the adapter Lambda security group.
- EC2 administration uses Systems Manager. Do not add SSH access.
- Do not introduce a NAT gateway, public load balancer, container orchestrator, managed relational database, DynamoDB, Athena, OpenSearch, or table service without an approved architecture change.

## Engineering expectations

- Target Python 3.14 for CDK, Lambda, and AgentCore where supported, and Python 3.12 for Managed Apache Flink 2.3.
- Keep schemas versioned and shared through a single contract source.
- Use UTC internally and make event-time semantics explicit.
- Use stable identifiers, idempotent writes, bounded retries, timeouts, and correlation IDs.
- Prefer structured logs and metrics over free-form logging.
- Keep dependencies pinned and compatible with ARM64 where applicable.
- Default to least-privilege IAM and encrypted transport/storage.
- Add tests for every lifecycle transition, duplicate, late event, replay, timeout, and failure-recovery path.
- Keep cost controls, log retention, and stop/start behavior explicit in infrastructure code.

## Change workflow

1. Read `README.md` and the nearest module documentation before editing.
2. Preserve architecture and safety invariants unless the task explicitly changes them.
3. Update contracts and fixtures before implementations that depend on them.
4. Run the narrowest relevant tests, followed by the repository test target.
5. Run CDK synthesis for infrastructure changes when implementation exists.
6. Report assumptions, verification performed, and any cost or security impact.

Deployment is never implied by an implementation task. Deploy only when explicitly requested and after the target account, Region, environment, and change plan are confirmed.
