#!/usr/bin/env bash
# Shared helpers for BagGuard cost-control scripts. Source this file; do not run it.

BAGGUARD_STACK_NAME="${BAGGUARD_STACK_NAME:-BagGuardProductionStack}"
BAGGUARD_FLINK_APPLICATION_NAME="${BAGGUARD_FLINK_APPLICATION_NAME:-bagguard-risk-detector-prod}"
BAGGUARD_BAGGAGE_STREAM_NAME="${BAGGUARD_BAGGAGE_STREAM_NAME:-bagguard-baggage-events-prod}"
BAGGUARD_RISK_STREAM_NAME="${BAGGUARD_RISK_STREAM_NAME:-bagguard-risk-incidents-prod}"
BAGGUARD_ADAPTER_FUNCTION_NAME="${BAGGUARD_ADAPTER_FUNCTION_NAME:-bagguard-clickhouse-adapter-prod}"
BAGGUARD_AGENT_RUNTIME_NAME="${BAGGUARD_AGENT_RUNTIME_NAME:-BagGuard_bagguard_investigator_prod}"
BAGGUARD_START_TIMEOUT_SECONDS="${BAGGUARD_START_TIMEOUT_SECONDS:-900}"
BAGGUARD_HEALTH_TIMEOUT_SECONDS="${BAGGUARD_HEALTH_TIMEOUT_SECONDS:-420}"

log() {
    printf '[BagGuard] %s\n' "$*"
}

pass() {
    printf 'PASS  %s\n' "$*"
}

warn() {
    printf 'WARN  %s\n' "$*" >&2
}

fail() {
    printf 'FAIL  %s\n' "$*" >&2
}

die() {
    fail "$*"
    exit 1
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

require_command aws
require_command python3

resolve_region() {
    local configured_region
    if [[ -n "${AWS_REGION:-}" ]]; then
        printf '%s\n' "$AWS_REGION"
        return
    fi
    if [[ -n "${AWS_DEFAULT_REGION:-}" ]]; then
        printf '%s\n' "$AWS_DEFAULT_REGION"
        return
    fi
    configured_region="$(aws configure get region 2>/dev/null || true)"
    [[ -n "$configured_region" ]] || die "AWS region is not configured"
    printf '%s\n' "$configured_region"
}

BAGGUARD_REGION="$(resolve_region)"
export AWS_REGION="$BAGGUARD_REGION"
export AWS_DEFAULT_REGION="$BAGGUARD_REGION"
AWS_CLI=(aws --region "$BAGGUARD_REGION" --no-cli-pager)

aws_cli() {
    "${AWS_CLI[@]}" "$@"
}

account_id() {
    aws_cli sts get-caller-identity --query Account --output text
}

stack_output() {
    local output_key="$1"
    local value
    value="$(
        aws_cli cloudformation describe-stacks \
            --stack-name "$BAGGUARD_STACK_NAME" \
            --query "Stacks[0].Outputs[?OutputKey=='${output_key}'].OutputValue | [0]" \
            --output text
    )"
    [[ -n "$value" && "$value" != "None" ]] || die \
        "CloudFormation output ${output_key} was not found in ${BAGGUARD_STACK_NAME}"
    printf '%s\n' "$value"
}

clickhouse_instance_id() {
    if [[ -n "${BAGGUARD_CLICKHOUSE_INSTANCE_ID:-}" ]]; then
        printf '%s\n' "$BAGGUARD_CLICKHOUSE_INSTANCE_ID"
    else
        stack_output ClickHouseInstanceId
    fi
}

clickhouse_ec2_state() {
    local instance_id="${1:-$(clickhouse_instance_id)}"
    aws_cli ec2 describe-instances \
        --instance-ids "$instance_id" \
        --query 'Reservations[0].Instances[0].State.Name' \
        --output text
}

flink_status() {
    aws_cli kinesisanalyticsv2 describe-application \
        --application-name "$BAGGUARD_FLINK_APPLICATION_NAME" \
        --query 'ApplicationDetail.ApplicationStatus' \
        --output text
}

stream_status() {
    local stream_name="$1"
    aws_cli kinesis describe-stream-summary \
        --stream-name "$stream_name" \
        --query 'StreamDescriptionSummary.StreamStatus' \
        --output text
}

adapter_lambda_status() {
    aws_cli lambda get-function-configuration \
        --function-name "$BAGGUARD_ADAPTER_FUNCTION_NAME" \
        --query State \
        --output text
}

agent_runtime_id() {
    local runtime_id
    if [[ -n "${BAGGUARD_AGENT_RUNTIME_ID:-}" ]]; then
        printf '%s\n' "$BAGGUARD_AGENT_RUNTIME_ID"
        return
    fi
    runtime_id="$(
        aws_cli bedrock-agentcore-control list-agent-runtimes \
            --query "agentRuntimes[?agentRuntimeName=='${BAGGUARD_AGENT_RUNTIME_NAME}'] | [0].agentRuntimeId" \
            --output text
    )"
    [[ -n "$runtime_id" && "$runtime_id" != "None" ]] || die \
        "AgentCore runtime ${BAGGUARD_AGENT_RUNTIME_NAME} was not found"
    printf '%s\n' "$runtime_id"
}

agent_runtime_status() {
    local runtime_id="${1:-$(agent_runtime_id)}"
    aws_cli bedrock-agentcore-control get-agent-runtime \
        --agent-runtime-id "$runtime_id" \
        --query status \
        --output text
}

adapter_health() {
    local response_file
    local invoke_status
    response_file="$(mktemp "${TMPDIR:-/tmp}/bagguard-adapter-health.XXXXXX")"
    if ! invoke_status="$(
        aws_cli lambda invoke \
            --function-name "$BAGGUARD_ADAPTER_FUNCTION_NAME" \
            --cli-binary-format raw-in-base64-out \
            --payload '{"action":"health_check","parameters":{}}' \
            --query StatusCode \
            --output text \
            "$response_file" 2>/dev/null
    )"; then
        rm -f "$response_file"
        return 1
    fi
    if [[ "$invoke_status" != "200" ]]; then
        rm -f "$response_file"
        return 1
    fi
    if python3 - "$response_file" <<'PY'
import json
import pathlib
import sys

body = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
result = body.get("result", {})
healthy = (
    body.get("ok") is True
    and body.get("action") == "health_check"
    and result.get("healthy") == 1
    and bool(result.get("version"))
)
raise SystemExit(0 if healthy else 1)
PY
    then
        rm -f "$response_file"
        return 0
    fi
    rm -f "$response_file"
    return 1
}

wait_for_value() {
    local function_name="$1"
    local expected="$2"
    local timeout_seconds="$3"
    local description="$4"
    shift 4
    local deadline=$((SECONDS + timeout_seconds))
    local value=""
    while ((SECONDS < deadline)); do
        value="$("$function_name" "$@" 2>/dev/null || true)"
        if [[ "$value" == "$expected" ]]; then
            pass "${description}: ${value}"
            return 0
        fi
        log "Waiting for ${description}; current state: ${value:-unavailable}"
        sleep 10
    done
    fail "${description} did not reach ${expected} within ${timeout_seconds}s"
    return 1
}

wait_for_success() {
    local timeout_seconds="$1"
    local description="$2"
    local function_name="$3"
    shift 3
    local deadline=$((SECONDS + timeout_seconds))
    while ((SECONDS < deadline)); do
        if "$function_name" "$@"; then
            pass "$description"
            return 0
        fi
        log "Waiting for ${description}"
        sleep 10
    done
    fail "${description} did not pass within ${timeout_seconds}s"
    return 1
}

print_aws_context() {
    printf 'AWS account: %s\n' "$(account_id)"
    printf 'AWS region:  %s\n' "$BAGGUARD_REGION"
}
