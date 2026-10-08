#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/_bagguard_common.sh
source "$SCRIPT_DIR/_bagguard_common.sh"

print_aws_context
printf 'CDK stack:   %s\n' "$BAGGUARD_STACK_NAME"

"$SCRIPT_DIR/clickhouse-start.sh"

wait_for_value \
    stream_status ACTIVE 120 "Baggage event stream" \
    "$BAGGUARD_BAGGAGE_STREAM_NAME"
wait_for_value \
    stream_status ACTIVE 120 "Risk incident stream" \
    "$BAGGUARD_RISK_STREAM_NAME"

"$SCRIPT_DIR/flink-start.sh"

wait_for_value \
    flink_status RUNNING "$BAGGUARD_START_TIMEOUT_SECONDS" "Managed Flink"
wait_for_value \
    stream_status ACTIVE 120 "Baggage event stream" \
    "$BAGGUARD_BAGGAGE_STREAM_NAME"
wait_for_value \
    stream_status ACTIVE 120 "Risk incident stream" \
    "$BAGGUARD_RISK_STREAM_NAME"
wait_for_value \
    adapter_lambda_status Active 120 "ClickHouse adapter Lambda"
wait_for_success \
    "$BAGGUARD_HEALTH_TIMEOUT_SECONDS" \
    "ClickHouse adapter health check" \
    adapter_health

runtime_id="$(agent_runtime_id)"
wait_for_value \
    agent_runtime_status READY 300 "AgentCore runtime" "$runtime_id"

pass "BagGuard is ready"
