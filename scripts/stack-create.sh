#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/_bagguard_stack_common.sh
source "$SCRIPT_DIR/_bagguard_stack_common.sh"

for command_name in aws python3 cdk npm mvn zip unzip agentcore; do
    require_command "$command_name"
done
[[ -x "$BAGGUARD_REPOSITORY_ROOT/.venv/bin/python" ]] || die \
    "Create .venv and install infra/requirements.txt before full-stack creation"

assert_deployment_target
print_stack_context

if [[ "${BAGGUARD_SKIP_TESTS:-0}" != "1" ]]; then
    log "Running repository tests"
    make -C "$BAGGUARD_REPOSITORY_ROOT" test
fi

log "Validating AgentCore configuration"
(
    cd "$BAGGUARD_AGENTCORE_PROJECT_DIR"
    agentcore validate
)

log "Installing pinned AgentCore CDK dependencies"
(
    cd "$BAGGUARD_AGENTCORE_CDK_DIR"
    mkdir -p "$BAGGUARD_NPM_CACHE"
    npm_config_cache="$BAGGUARD_NPM_CACHE" npm ci
    npm run build
)

log "Packaging the Managed Flink application"
"$SCRIPT_DIR/package_flink.sh"

log "Bootstrapping the target account and region if required"
run_main_cdk bootstrap \
    "aws://${BAGGUARD_TARGET_ACCOUNT}/${BAGGUARD_TARGET_REGION}"

log "Deploying AgentCore before the importing application stack"
run_agentcore_cdk deploy "$BAGGUARD_AGENTCORE_STACK_NAME" \
    --require-approval never

log "Deploying the BagGuard application stack"
run_main_cdk deploy "$BAGGUARD_STACK_NAME" \
    --require-approval never

log "Starting and verifying BagGuard compute"
"$SCRIPT_DIR/platform-start.sh"
"$SCRIPT_DIR/platform-status.sh"

pass "BagGuard full stack is deployed, running, and verified"
