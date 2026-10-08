#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/_bagguard_common.sh
source "$SCRIPT_DIR/_bagguard_common.sh"

print_aws_context
printf 'CDK stack:   %s\n\n' "$BAGGUARD_STACK_NAME"

instance_id="$(clickhouse_instance_id)"
ec2_state="$(clickhouse_ec2_state "$instance_id")"
printf 'ClickHouse EC2     %-12s %s\n' "$ec2_state" "$instance_id"
if [[ "$ec2_state" == "running" ]]; then
    if adapter_health; then
        pass "ClickHouse query health through adapter"
    else
        fail "ClickHouse query health through adapter"
    fi
else
    log "ClickHouse health check skipped while EC2 is ${ec2_state}"
fi

flink_state="$(flink_status)"
printf 'Managed Flink      %s\n' "$flink_state"

for stream_name in "$BAGGUARD_BAGGAGE_STREAM_NAME" "$BAGGUARD_RISK_STREAM_NAME"; do
    status="$(stream_status "$stream_name")"
    if [[ "$status" == "ACTIVE" ]]; then
        pass "Kinesis ${stream_name}: ${status}"
    else
        fail "Kinesis ${stream_name}: ${status}"
    fi
done

lambda_state="$(adapter_lambda_status)"
if [[ "$lambda_state" == "Active" ]]; then
    pass "Adapter Lambda: ${lambda_state}"
else
    fail "Adapter Lambda: ${lambda_state}"
fi

runtime_id="$(agent_runtime_id)"
runtime_state="$(agent_runtime_status "$runtime_id")"
if [[ "$runtime_state" == "READY" ]]; then
    pass "AgentCore runtime: ${runtime_state} (${runtime_id})"
else
    fail "AgentCore runtime: ${runtime_state} (${runtime_id})"
fi

if [[ "$ec2_state" == "stopped" && "$flink_state" == "READY" ]]; then
    printf '\nOverall: STOPPED — retained services and data are intact.\n'
elif [[ "$ec2_state" == "running" && "$flink_state" == "RUNNING" ]]; then
    printf '\nOverall: RUNNING\n'
else
    printf '\nOverall: PARTIAL or TRANSITIONING\n'
fi
