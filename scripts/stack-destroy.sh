#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/_bagguard_stack_common.sh
source "$SCRIPT_DIR/_bagguard_stack_common.sh"

confirmation=""
while (($#)); do
    case "$1" in
        --confirm)
            (($# >= 2)) || die "--confirm requires a value"
            confirmation="$2"
            shift 2
            ;;
        *)
            die "Unknown argument: $1"
            ;;
    esac
done

require_destroy_confirmation "$confirmation"
assert_deployment_target
print_stack_context

printf '\nPERMANENT DELETION ENABLED\n'
printf 'ClickHouse data, EBS, streams, secrets, queues, logs, Lambdas,\n'
printf 'Flink, AgentCore, VPC resources, and both application stacks will be deleted.\n\n'

artifact_bucket=""
secret_identifier="bagguard/clickhouse/prod"
adapter_dlq_url=""
dispatcher_dlq_url=""
bucket_deployment_function=""
agent_runtime_identifier="$(agent_runtime_id 2>/dev/null || true)"

if stack_exists "$BAGGUARD_STACK_NAME"; then
    artifact_bucket="$(stack_output_optional "$BAGGUARD_STACK_NAME" FlinkArtifactBucketName)"
    secret_identifier="$(stack_output_optional "$BAGGUARD_STACK_NAME" ClickHouseSecretArn)"
    secret_identifier="${secret_identifier:-bagguard/clickhouse/prod}"
    adapter_dlq_url="$(stack_output_optional "$BAGGUARD_STACK_NAME" ClickHouseAdapterDlqUrl)"
    dispatcher_dlq_url="$(stack_output_optional "$BAGGUARD_STACK_NAME" AgentDispatcherDlqUrl)"
    bucket_deployment_function="$(
        aws_cli cloudformation list-stack-resources \
            --stack-name "$BAGGUARD_STACK_NAME" \
            --query "StackResourceSummaries[?ResourceType=='AWS::Lambda::Function' && contains(LogicalResourceId, 'CustomCDKBucketDeployment')].PhysicalResourceId | [0]" \
            --output text
    )"

    log "Stopping continuously running compute before deletion"
    "$SCRIPT_DIR/platform-stop.sh"
else
    warn "Application stack is absent; cleaning known retained resource names"
fi

delete_cloudformation_stack "$BAGGUARD_STACK_NAME"

delete_stream_if_present() {
    local stream_name="$1"
    case "$stream_name" in
        "$BAGGUARD_BAGGAGE_STREAM_NAME"|"$BAGGUARD_RISK_STREAM_NAME")
            ;;
        *)
            die "Refusing unexpected Kinesis stream: ${stream_name}"
            ;;
    esac
    if aws_cli kinesis describe-stream-summary \
        --stream-name "$stream_name" >/dev/null 2>&1; then
        aws_cli kinesis delete-stream \
            --stream-name "$stream_name" \
            --enforce-consumer-deletion
        aws_cli kinesis wait stream-not-exists --stream-name "$stream_name"
        pass "Deleted retained Kinesis stream: ${stream_name}"
    fi
}

delete_log_group_if_present() {
    local log_group_name="$1"
    local count
    count="$(
        aws_cli logs describe-log-groups \
            --log-group-name-prefix "$log_group_name" \
            --query "length(logGroups[?logGroupName=='${log_group_name}'])" \
            --output text
    )"
    if [[ "$count" == "1" ]]; then
        aws_cli logs delete-log-group --log-group-name "$log_group_name"
        pass "Deleted retained log group: ${log_group_name}"
    fi
}

delete_queue_if_present() {
    local queue_name="$1"
    local known_url="$2"
    local queue_url="$known_url"
    if [[ -z "$queue_url" ]]; then
        queue_url="$(
            aws_cli sqs get-queue-url \
                --queue-name "$queue_name" \
                --query QueueUrl \
                --output text 2>/dev/null || true
        )"
    fi
    if [[ -n "$queue_url" && "$queue_url" != "None" ]]; then
        [[ "$queue_url" == "https://sqs.${BAGGUARD_TARGET_REGION}.amazonaws.com/${BAGGUARD_TARGET_ACCOUNT}/${queue_name}" ]] || die \
            "Refusing unexpected queue URL: ${queue_url}"
        aws_cli sqs delete-queue --queue-url "$queue_url"
        pass "Deleted retained queue: ${queue_name}"
        return 0
    fi
    return 1
}

if aws_cli secretsmanager describe-secret \
    --secret-id "$secret_identifier" >/dev/null 2>&1; then
    case "$secret_identifier" in
        bagguard/clickhouse/prod|arn:aws:secretsmanager:"$BAGGUARD_TARGET_REGION":"$BAGGUARD_TARGET_ACCOUNT":secret:bagguard/clickhouse/prod-*)
            ;;
        *)
            die "Refusing unexpected secret: ${secret_identifier}"
            ;;
    esac
    aws_cli secretsmanager delete-secret \
        --secret-id "$secret_identifier" \
        --force-delete-without-recovery >/dev/null
    pass "Requested permanent deletion of the ClickHouse application secret"
fi

delete_stream_if_present "$BAGGUARD_BAGGAGE_STREAM_NAME"
delete_stream_if_present "$BAGGUARD_RISK_STREAM_NAME"

delete_log_group_if_present "/bagguard/prod/flink/risk-detector"
delete_log_group_if_present "/bagguard/prod/clickhouse-adapter"
delete_log_group_if_present "/bagguard/prod/agent-dispatcher"
if [[ -n "$bucket_deployment_function" && "$bucket_deployment_function" != "None" ]]; then
    [[ "$bucket_deployment_function" == BagGuardProductionStack-CustomCDKBucketDeployment* ]] || die \
        "Refusing unexpected bucket-deployment function: ${bucket_deployment_function}"
    delete_log_group_if_present "/aws/lambda/${bucket_deployment_function}"
fi

queue_deleted=0
if delete_queue_if_present \
    "bagguard-clickhouse-adapter-dlq-prod" "$adapter_dlq_url"; then
    queue_deleted=1
fi
if delete_queue_if_present \
    "bagguard-agent-dispatcher-dlq-prod" "$dispatcher_dlq_url"; then
    queue_deleted=1
fi

if [[ -n "$artifact_bucket" ]]; then
    [[ "$artifact_bucket" == "bagguard-flink-artifacts-prod-${BAGGUARD_TARGET_ACCOUNT}-${BAGGUARD_TARGET_REGION}" ]] || die \
        "Refusing unexpected artifact bucket: ${artifact_bucket}"
    if aws_cli s3api head-bucket --bucket "$artifact_bucket" >/dev/null 2>&1; then
        log "Emptying and deleting retained artifact bucket ${artifact_bucket}"
        aws_cli s3 rb "s3://${artifact_bucket}" --force
        pass "Deleted retained Flink artifact bucket"
    fi
fi

delete_cloudformation_stack "$BAGGUARD_AGENTCORE_STACK_NAME"

if [[ -n "$agent_runtime_identifier" && "$agent_runtime_identifier" != "None" ]]; then
    [[ "$agent_runtime_identifier" == "${BAGGUARD_AGENT_RUNTIME_NAME}-"* ]] || die \
        "Refusing unexpected AgentCore runtime ID: ${agent_runtime_identifier}"
    delete_log_group_if_present \
        "/aws/bedrock-agentcore/runtimes/${agent_runtime_identifier}-DEFAULT"
fi

if ((queue_deleted)); then
    log "Waiting for deleted SQS names to become reusable"
    for remaining in 55 50 45 40 35 30 25 20 15 10 5 0; do
        log "SQS name reuse wait: ${remaining}s remaining"
        sleep 5
    done
fi

for attempt in {1..24}; do
    if ! aws_cli secretsmanager describe-secret \
        --secret-id "$secret_identifier" >/dev/null 2>&1; then
        break
    fi
    ((attempt == 24)) && die "Secret deletion did not complete in time"
    log "Waiting for permanent secret deletion"
    sleep 5
done

stack_exists "$BAGGUARD_STACK_NAME" && die \
    "Application stack still exists after deletion"
stack_exists "$BAGGUARD_AGENTCORE_STACK_NAME" && die \
    "AgentCore stack still exists after deletion"

pass "BagGuard application resources and stacks are fully deleted"
log "The shared CDKToolkit bootstrap stack is intentionally retained"
