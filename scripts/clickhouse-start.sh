#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/_bagguard_common.sh
source "$SCRIPT_DIR/_bagguard_common.sh"

instance_id="$(clickhouse_instance_id)"
state="$(clickhouse_ec2_state "$instance_id")"
log "ClickHouse EC2 ${instance_id} is ${state}"

case "$state" in
    stopped)
        log "Starting ClickHouse EC2"
        aws_cli ec2 start-instances --instance-ids "$instance_id" >/dev/null
        ;;
    stopping)
        log "Waiting for the current stop operation to finish"
        aws_cli ec2 wait instance-stopped --instance-ids "$instance_id"
        aws_cli ec2 start-instances --instance-ids "$instance_id" >/dev/null
        ;;
    pending|running)
        ;;
    *)
        die "ClickHouse EC2 cannot be started from state: ${state}"
        ;;
esac

aws_cli ec2 wait instance-running --instance-ids "$instance_id"
pass "ClickHouse EC2 state: running"
aws_cli ec2 wait instance-status-ok --instance-ids "$instance_id"
pass "ClickHouse EC2 status checks: ok"
wait_for_success \
    "$BAGGUARD_HEALTH_TIMEOUT_SECONDS" \
    "ClickHouse query health through ${BAGGUARD_ADAPTER_FUNCTION_NAME}" \
    adapter_health
