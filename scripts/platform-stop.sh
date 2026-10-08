#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/_bagguard_common.sh
source "$SCRIPT_DIR/_bagguard_common.sh"

print_aws_context
printf 'CDK stack:   %s\n' "$BAGGUARD_STACK_NAME"

"$SCRIPT_DIR/flink-stop.sh"
"$SCRIPT_DIR/clickhouse-stop.sh"

pass "BagGuard compute is stopped"
log "Retained: EBS, Kinesis, AgentCore Runtime, CloudFormation, and logs"
