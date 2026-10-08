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
        pass "ClickHouse EC2 is already stopped"
        exit 0
        ;;
    stopping)
        ;;
    pending|running)
        log "Stopping ClickHouse EC2 without deleting its EBS volume"
        aws_cli ec2 stop-instances --instance-ids "$instance_id" >/dev/null
        ;;
    *)
        die "ClickHouse EC2 cannot be stopped from state: ${state}"
        ;;
esac

aws_cli ec2 wait instance-stopped --instance-ids "$instance_id"
pass "ClickHouse EC2 state: stopped"
