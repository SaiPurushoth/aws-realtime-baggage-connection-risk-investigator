#!/usr/bin/env bash
# Shared helpers for full BagGuard stack creation and destruction.

STACK_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BAGGUARD_REPOSITORY_ROOT="$(cd "${STACK_SCRIPT_DIR}/.." && pwd)"

# shellcheck source=scripts/_bagguard_common.sh
source "${STACK_SCRIPT_DIR}/_bagguard_common.sh"

BAGGUARD_AGENTCORE_PROJECT_DIR="${BAGGUARD_REPOSITORY_ROOT}/agentcore"
BAGGUARD_AGENTCORE_CDK_DIR="${BAGGUARD_AGENTCORE_PROJECT_DIR}/agentcore/cdk"
BAGGUARD_AGENTCORE_TARGETS_FILE="${BAGGUARD_AGENTCORE_PROJECT_DIR}/agentcore/aws-targets.json"
BAGGUARD_AGENTCORE_SPEC_FILE="${BAGGUARD_AGENTCORE_PROJECT_DIR}/agentcore/agentcore.json"
BAGGUARD_JSII_CACHE="${BAGGUARD_REPOSITORY_ROOT}/.cache/jsii"
BAGGUARD_NPM_CACHE="${BAGGUARD_REPOSITORY_ROOT}/.cache/npm"
BAGGUARD_UV_CACHE="${BAGGUARD_REPOSITORY_ROOT}/.cache/uv"

read_agentcore_target() {
    local field="$1"
    python3 - "$BAGGUARD_AGENTCORE_TARGETS_FILE" "$field" <<'PY'
import json
import pathlib
import sys

targets = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
if len(targets) != 1:
    raise SystemExit("BagGuard full-stack lifecycle requires exactly one AgentCore target")
value = targets[0].get(sys.argv[2])
if not isinstance(value, str) or not value:
    raise SystemExit(f"AgentCore target is missing {sys.argv[2]}")
print(value)
PY
}

read_agentcore_project_name() {
    python3 - "$BAGGUARD_AGENTCORE_SPEC_FILE" <<'PY'
import json
import pathlib
import sys

spec = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
name = spec.get("name")
if not isinstance(name, str) or not name:
    raise SystemExit("AgentCore project name is missing")
print(name)
PY
}

BAGGUARD_AGENTCORE_TARGET_NAME="${BAGGUARD_AGENTCORE_TARGET_NAME:-$(read_agentcore_target name)}"
BAGGUARD_TARGET_ACCOUNT="${BAGGUARD_TARGET_ACCOUNT:-$(read_agentcore_target account)}"
BAGGUARD_TARGET_REGION="${BAGGUARD_TARGET_REGION:-$(read_agentcore_target region)}"
BAGGUARD_AGENTCORE_PROJECT_NAME="${BAGGUARD_AGENTCORE_PROJECT_NAME:-$(read_agentcore_project_name)}"
BAGGUARD_AGENTCORE_STACK_NAME="${BAGGUARD_AGENTCORE_STACK_NAME:-AgentCore-${BAGGUARD_AGENTCORE_PROJECT_NAME//_/-}-${BAGGUARD_AGENTCORE_TARGET_NAME//_/-}}"

assert_deployment_target() {
    local actual_account
    actual_account="$(account_id)"
    [[ "$actual_account" == "$BAGGUARD_TARGET_ACCOUNT" ]] || die \
        "Refusing account ${actual_account}; configured target is ${BAGGUARD_TARGET_ACCOUNT}"
    [[ "$BAGGUARD_REGION" == "$BAGGUARD_TARGET_REGION" ]] || die \
        "Refusing region ${BAGGUARD_REGION}; configured target is ${BAGGUARD_TARGET_REGION}"
}

print_stack_context() {
    print_aws_context
    printf 'Application stack: %s\n' "$BAGGUARD_STACK_NAME"
    printf 'AgentCore stack:   %s\n' "$BAGGUARD_AGENTCORE_STACK_NAME"
}

stack_exists() {
    local stack_name="$1"
    aws_cli cloudformation describe-stacks \
        --stack-name "$stack_name" >/dev/null 2>&1
}

stack_status() {
    local stack_name="$1"
    aws_cli cloudformation describe-stacks \
        --stack-name "$stack_name" \
        --query 'Stacks[0].StackStatus' \
        --output text
}

stack_output_optional() {
    local stack_name="$1"
    local output_key="$2"
    local value
    value="$(
        aws_cli cloudformation describe-stacks \
            --stack-name "$stack_name" \
            --query "Stacks[0].Outputs[?OutputKey=='${output_key}'].OutputValue | [0]" \
            --output text 2>/dev/null || true
    )"
    if [[ -n "$value" && "$value" != "None" ]]; then
        printf '%s\n' "$value"
    fi
}

delete_cloudformation_stack() {
    local stack_name="$1"
    if ! stack_exists "$stack_name"; then
        pass "CloudFormation stack is already absent: ${stack_name}"
        return
    fi

    log "Deleting CloudFormation stack ${stack_name}"
    aws_cli cloudformation delete-stack --stack-name "$stack_name"
    if ! aws_cli cloudformation wait stack-delete-complete --stack-name "$stack_name"; then
        fail "CloudFormation could not delete ${stack_name}"
        aws_cli cloudformation describe-stack-events \
            --stack-name "$stack_name" \
            --max-items 20 \
            --query 'StackEvents[?ResourceStatus==`DELETE_FAILED`].[LogicalResourceId,ResourceStatusReason]' \
            --output table || true
        return 1
    fi
    pass "CloudFormation stack deleted: ${stack_name}"
}

confirmation_token() {
    printf 'DELETE-%s-%s-%s\n' \
        "$BAGGUARD_TARGET_ACCOUNT" "$BAGGUARD_TARGET_REGION" "$BAGGUARD_STACK_NAME"
}

require_destroy_confirmation() {
    local supplied="$1"
    local expected
    expected="$(confirmation_token)"
    [[ "$supplied" == "$expected" ]] || die \
        "Full deletion requires --confirm ${expected}"
}

run_main_cdk() {
    mkdir -p "$BAGGUARD_JSII_CACHE"
    (
        cd "$BAGGUARD_REPOSITORY_ROOT"
        env \
            JSII_RUNTIME_PACKAGE_CACHE_ROOT="$BAGGUARD_JSII_CACHE" \
            CDK_DEFAULT_ACCOUNT="$BAGGUARD_TARGET_ACCOUNT" \
            CDK_DEFAULT_REGION="$BAGGUARD_TARGET_REGION" \
            cdk "$@"
    )
}

run_agentcore_cdk() {
    local cdk_binary="${BAGGUARD_AGENTCORE_CDK_DIR}/node_modules/.bin/cdk"
    [[ -x "$cdk_binary" ]] || die \
        "AgentCore CDK dependencies are missing; run npm ci in ${BAGGUARD_AGENTCORE_CDK_DIR}"
    (
        cd "$BAGGUARD_AGENTCORE_CDK_DIR"
        mkdir -p "$BAGGUARD_UV_CACHE"
        env UV_CACHE_DIR="$BAGGUARD_UV_CACHE" "$cdk_binary" "$@"
    )
}
