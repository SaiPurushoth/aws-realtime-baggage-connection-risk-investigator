#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/_bagguard_common.sh
source "$SCRIPT_DIR/_bagguard_common.sh"

status="$(flink_status)"
log "Managed Flink ${BAGGUARD_FLINK_APPLICATION_NAME} is ${status}"

case "$status" in
    READY)
        pass "Managed Flink is already stopped"
        exit 0
        ;;
    STOPPING)
        ;;
    STARTING|UPDATING|AUTOSCALING)
        wait_for_value \
            flink_status RUNNING "$BAGGUARD_START_TIMEOUT_SECONDS" "Managed Flink"
        log "Gracefully stopping Managed Flink"
        aws_cli kinesisanalyticsv2 stop-application \
            --application-name "$BAGGUARD_FLINK_APPLICATION_NAME" \
            --no-force >/dev/null
        ;;
    RUNNING)
        log "Gracefully stopping Managed Flink"
        aws_cli kinesisanalyticsv2 stop-application \
            --application-name "$BAGGUARD_FLINK_APPLICATION_NAME" \
            --no-force >/dev/null
        ;;
    *)
        die "Managed Flink cannot be stopped from status: ${status}"
        ;;
esac

wait_for_value \
    flink_status READY "$BAGGUARD_START_TIMEOUT_SECONDS" "Managed Flink"
