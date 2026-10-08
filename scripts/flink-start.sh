#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/_bagguard_common.sh
source "$SCRIPT_DIR/_bagguard_common.sh"

status="$(flink_status)"
log "Managed Flink ${BAGGUARD_FLINK_APPLICATION_NAME} is ${status}"

case "$status" in
    RUNNING)
        pass "Managed Flink is already RUNNING"
        exit 0
        ;;
    READY)
        log "Starting Managed Flink"
        aws_cli kinesisanalyticsv2 start-application \
            --application-name "$BAGGUARD_FLINK_APPLICATION_NAME" >/dev/null
        ;;
    STOPPING)
        wait_for_value \
            flink_status READY "$BAGGUARD_START_TIMEOUT_SECONDS" "Managed Flink"
        aws_cli kinesisanalyticsv2 start-application \
            --application-name "$BAGGUARD_FLINK_APPLICATION_NAME" >/dev/null
        ;;
    STARTING|UPDATING|AUTOSCALING)
        ;;
    *)
        die "Managed Flink cannot be started from status: ${status}"
        ;;
esac

wait_for_value \
    flink_status RUNNING "$BAGGUARD_START_TIMEOUT_SECONDS" "Managed Flink"
