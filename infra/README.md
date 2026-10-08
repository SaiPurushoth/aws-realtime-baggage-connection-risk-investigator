# BagGuard foundation infrastructure

This CDK application defines `BagGuardProductionStack`, containing:

- `bagguard-vpc-prod` with one public subnet, one Availability Zone, and no NAT gateway;
- an Amazon Linux 2023 ARM64 `t4g.medium` EC2 instance;
- a 40 GB encrypted gp3 root volume backing `/var/lib/clickhouse`;
- ClickHouse Server and Client installed from the official stable RPM repository;
- Systems Manager administration with no SSH ingress or EC2 key pair;
- generated application credentials in `bagguard/clickhouse/prod`;
- `bagguard-clickhouse-adapter-prod` running on Python 3.14 ARM64 inside the VPC;
- `bagguard-agent-dispatcher-prod` consuming connection-risk incidents and
  coordinating AgentCore investigation result persistence;
- separate ClickHouse, adapter, and Secrets Manager endpoint security groups;
- a private Secrets Manager interface endpoint with adapter-only HTTPS access;
- an encrypted 14-day SQS failure queue and seven-day CloudWatch log retention;
- two AWS KMS-encrypted, on-demand Kinesis data streams for baggage events and risk incidents;
- a private, encrypted `bagguard-flink-artifacts-prod-{account}-{region}` application bucket; and
- the `bagguard` database and three initial MergeTree tables.

The instance is placed in the public subnet so it can reach the official package repository and Systems Manager endpoints without a NAT gateway. ClickHouse is not publicly reachable: the security group permits TCP 8123 only when the source has `bagguard-adapter-sg-prod`. The native TCP port 9000 and SSH have no ingress rules.

The instance role has the AWS-managed Systems Manager core policy and resource-scoped read access to the single ClickHouse secret. The adapter role can read only that secret and the two named streams, send failure metadata only to its queue, manage its VPC network interfaces, and write Lambda logs. The application user receives only `SELECT` and `INSERT` on `bagguard.*`; the default ClickHouse user is restricted to loopback addresses.

The adapter reads batches of up to 500 records with a five-second batching window. Partial batch failures, bounded retries, batch bisection, and the failure queue protect the stream checkpoint while keeping ClickHouse writes batched. Direct invocations expose only the documented controlled actions; caller-provided SQL is rejected.

The dispatcher reads one risk incident per invocation, validates its
deterministic identifier, checks for an existing investigation result, invokes
only the exported BagGuard AgentCore Runtime, validates its strict response,
and invokes only the ClickHouse adapter for persistence. It runs outside the
VPC and has no database endpoint or credential. Failed Kinesis records receive
three bounded retries before their metadata is retained in the encrypted
dispatcher failure queue.

## Local commands

From the repository root:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r infra/requirements.txt
make synth
make diff
```

`cdk diff --no-change-set` avoids creating a CloudFormation change set. Deployment is a separate, explicitly authorized operation.
