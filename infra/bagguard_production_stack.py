import hashlib
from pathlib import Path

from aws_cdk import (
    Arn,
    ArnComponents,
    ArnFormat,
    Aws,
    CfnOutput,
    Duration,
    Fn,
    RemovalPolicy,
    Stack,
    Tags,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_kinesis as kinesis,
    aws_kinesisanalyticsv2 as kinesisanalyticsv2,
    aws_lambda as lambda_,
    aws_lambda_event_sources as lambda_event_sources,
    aws_logs as logs,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    aws_secretsmanager as secretsmanager,
    aws_sqs as sqs,
)
from constructs import Construct


RESOURCE_PREFIX = "bagguard"
ENVIRONMENT = "prod"
RESOURCE_OWNER = "BaggageOperations"
CLICKHOUSE_DATABASE = "bagguard"
CLICKHOUSE_SECRET_NAME = f"{RESOURCE_PREFIX}/clickhouse/{ENVIRONMENT}"
CLICKHOUSE_USERNAME = "bagguard_app"
VPC_CIDR = "10.42.0.0/24"
BAGGAGE_EVENTS_STREAM_NAME = f"{RESOURCE_PREFIX}-baggage-events-{ENVIRONMENT}"
RISK_INCIDENTS_STREAM_NAME = f"{RESOURCE_PREFIX}-risk-incidents-{ENVIRONMENT}"
CLICKHOUSE_ADAPTER_FUNCTION_NAME = (
    f"{RESOURCE_PREFIX}-clickhouse-adapter-{ENVIRONMENT}"
)
FLINK_APPLICATION_NAME = f"{RESOURCE_PREFIX}-risk-detector-{ENVIRONMENT}"
AGENT_DISPATCHER_FUNCTION_NAME = (
    f"{RESOURCE_PREFIX}-agent-dispatcher-{ENVIRONMENT}"
)
AGENT_RUNTIME_ARN_EXPORT = (
    "AgentCore-BagGuard-production-bagguard-investigator-prod-RuntimeArn"
)


class BagGuardProductionStack(Stack):
    """Streaming ingestion and single-node ClickHouse foundation for BagGuard."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs: object) -> None:
        super().__init__(scope, construct_id, **kwargs)

        Tags.of(self).add("Project", "BagGuard")
        Tags.of(self).add("Environment", ENVIRONMENT)
        Tags.of(self).add("Workload", "BaggageConnectionRisk")
        Tags.of(self).add("Owner", RESOURCE_OWNER)

        baggage_events_stream = kinesis.Stream(
            self,
            "BaggageEventsStream",
            stream_name=BAGGAGE_EVENTS_STREAM_NAME,
            stream_mode=kinesis.StreamMode.ON_DEMAND,
            encryption=kinesis.StreamEncryption.MANAGED,
        )

        risk_incidents_stream = kinesis.Stream(
            self,
            "RiskIncidentsStream",
            stream_name=RISK_INCIDENTS_STREAM_NAME,
            stream_mode=kinesis.StreamMode.ON_DEMAND,
            encryption=kinesis.StreamEncryption.MANAGED,
        )

        flink_artifact_directory = (
            Path(__file__).resolve().parents[1] / "flink" / "dist"
        )
        flink_artifact_path = (
            flink_artifact_directory / "bagguard-risk-detector.zip"
        )
        flink_artifact_digest = hashlib.sha256(
            flink_artifact_path.read_bytes()
        ).hexdigest()[:16]
        flink_artifact_prefix = (
            "applications/bagguard-risk-detector/"
            f"{flink_artifact_digest}"
        )
        flink_artifact_key = (
            f"{flink_artifact_prefix}/bagguard-risk-detector.zip"
        )

        flink_artifact_bucket = s3.Bucket(
            self,
            "FlinkArtifactBucket",
            bucket_name=(
                f"{RESOURCE_PREFIX}-flink-artifacts-{ENVIRONMENT}-"
                f"{Aws.ACCOUNT_ID}-{Aws.REGION}"
            ),
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN,
        )
        flink_artifact_deployment = s3deploy.BucketDeployment(
            self,
            "DeployFlinkArtifact",
            sources=[
                s3deploy.Source.asset(
                    str(flink_artifact_directory)
                )
            ],
            destination_bucket=flink_artifact_bucket,
            destination_key_prefix=flink_artifact_prefix,
            prune=False,
        )

        flink_log_group = logs.LogGroup(
            self,
            "FlinkLogGroup",
            log_group_name=f"/{RESOURCE_PREFIX}/{ENVIRONMENT}/flink/risk-detector",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.RETAIN,
        )
        flink_log_stream = logs.LogStream(
            self,
            "FlinkLogStream",
            log_group=flink_log_group,
            log_stream_name="application",
            removal_policy=RemovalPolicy.RETAIN,
        )
        flink_log_group_arn = Arn.format(
            components=ArnComponents(
                service="logs",
                resource="log-group",
                resource_name=flink_log_group.log_group_name,
                arn_format=ArnFormat.COLON_RESOURCE_NAME,
            ),
            stack=self,
        )
        flink_log_stream_arn = (
            f"{flink_log_group_arn}:log-stream:"
            f"{flink_log_stream.log_stream_name}"
        )

        flink_role = iam.Role(
            self,
            "FlinkExecutionRole",
            role_name=f"{RESOURCE_PREFIX}-risk-detector-role-{ENVIRONMENT}",
            assumed_by=iam.ServicePrincipal("kinesisanalytics.amazonaws.com"),
            description="Least-privilege execution role for BagGuard risk detection",
        )
        flink_role.add_to_policy(
            iam.PolicyStatement(
                sid="DiscoverInputShards",
                actions=["kinesis:ListShards"],
                resources=["*"],
            )
        )
        flink_role.add_to_policy(
            iam.PolicyStatement(
                sid="ReadBaggageEvents",
                actions=[
                    "kinesis:DescribeStream",
                    "kinesis:DescribeStreamSummary",
                    "kinesis:GetRecords",
                    "kinesis:GetShardIterator",
                ],
                resources=[baggage_events_stream.stream_arn],
            )
        )
        flink_role.add_to_policy(
            iam.PolicyStatement(
                sid="WriteRiskIncidents",
                actions=[
                    "kinesis:DescribeStream",
                    "kinesis:DescribeStreamSummary",
                    "kinesis:PutRecord",
                    "kinesis:PutRecords",
                ],
                resources=[risk_incidents_stream.stream_arn],
            )
        )
        flink_role.add_to_policy(
            iam.PolicyStatement(
                sid="UseKinesisManagedEncryption",
                actions=["kms:Decrypt", "kms:GenerateDataKey"],
                resources=["*"],
                conditions={
                    "StringEquals": {
                        "kms:CallerAccount": Aws.ACCOUNT_ID,
                        "kms:ViaService": f"kinesis.{Aws.REGION}.amazonaws.com",
                    }
                },
            )
        )
        flink_role.add_to_policy(
            iam.PolicyStatement(
                sid="ReadApplicationArtifact",
                actions=["s3:GetObject", "s3:GetObjectVersion"],
                resources=[flink_artifact_bucket.arn_for_objects(flink_artifact_key)],
            )
        )
        flink_role.add_to_policy(
            iam.PolicyStatement(
                sid="DescribeFlinkLogGroups",
                actions=["logs:DescribeLogGroups"],
                resources=[
                    Arn.format(
                        components=ArnComponents(
                            service="logs",
                            resource="log-group",
                            resource_name="*",
                            arn_format=ArnFormat.COLON_RESOURCE_NAME,
                        ),
                        stack=self,
                    )
                ],
            )
        )
        flink_role.add_to_policy(
            iam.PolicyStatement(
                sid="UseFlinkLogStream",
                actions=["logs:DescribeLogStreams", "logs:PutLogEvents"],
                resources=[
                    f"{flink_log_group_arn}:log-stream:*",
                    flink_log_stream_arn,
                ],
            )
        )

        flink_application = kinesisanalyticsv2.CfnApplication(
            self,
            "RiskDetectorApplication",
            application_name=FLINK_APPLICATION_NAME,
            application_description=(
                "BagGuard deterministic real-time baggage connection risk detector"
            ),
            application_mode="STREAMING",
            runtime_environment="FLINK-2_3",
            service_execution_role=flink_role.role_arn,
            application_configuration=(
                kinesisanalyticsv2.CfnApplication.ApplicationConfigurationProperty(
                    application_code_configuration=(
                        kinesisanalyticsv2.CfnApplication.ApplicationCodeConfigurationProperty(
                            code_content=(
                                kinesisanalyticsv2.CfnApplication.CodeContentProperty(
                                    s3_content_location=(
                                        kinesisanalyticsv2.CfnApplication.S3ContentLocationProperty(
                                            bucket_arn=flink_artifact_bucket.bucket_arn,
                                            file_key=flink_artifact_key,
                                        )
                                    )
                                )
                            ),
                            code_content_type="ZIPFILE",
                        )
                    ),
                    application_snapshot_configuration=(
                        kinesisanalyticsv2.CfnApplication.ApplicationSnapshotConfigurationProperty(
                            snapshots_enabled=True
                        )
                    ),
                    environment_properties=(
                        kinesisanalyticsv2.CfnApplication.EnvironmentPropertiesProperty(
                            property_groups=[
                                kinesisanalyticsv2.CfnApplication.PropertyGroupProperty(
                                    property_group_id=(
                                        "kinesis.analytics.flink.run.options"
                                    ),
                                    property_map={
                                        "python": "main.py",
                                        "pyFiles": "detector_core.py",
                                        "jarfile": (
                                            "lib/bagguard-pyflink-dependencies.jar"
                                        ),
                                    },
                                ),
                                kinesisanalyticsv2.CfnApplication.PropertyGroupProperty(
                                    property_group_id="bagguard.runtime",
                                    property_map={
                                        "INPUT_STREAM": baggage_events_stream.stream_name,
                                        "OUTPUT_STREAM": risk_incidents_stream.stream_name,
                                        "RISK_BUFFER_SECONDS": "30",
                                        "AWS_REGION": Aws.REGION,
                                        "SOURCE_STREAM_ARN": (
                                            baggage_events_stream.stream_arn
                                        ),
                                        "KINESIS_STARTING_POSITION": "LATEST",
                                        "CHECKPOINT_INTERVAL_MS": "60000",
                                        "FLINK_PARALLELISM": "1",
                                    },
                                ),
                            ]
                        )
                    ),
                    flink_application_configuration=(
                        kinesisanalyticsv2.CfnApplication.FlinkApplicationConfigurationProperty(
                            checkpoint_configuration=(
                                kinesisanalyticsv2.CfnApplication.CheckpointConfigurationProperty(
                                    configuration_type="DEFAULT"
                                )
                            ),
                            monitoring_configuration=(
                                kinesisanalyticsv2.CfnApplication.MonitoringConfigurationProperty(
                                    configuration_type="CUSTOM",
                                    log_level="INFO",
                                    metrics_level="APPLICATION",
                                )
                            ),
                            parallelism_configuration=(
                                kinesisanalyticsv2.CfnApplication.ParallelismConfigurationProperty(
                                    configuration_type="CUSTOM",
                                    parallelism=1,
                                    parallelism_per_kpu=1,
                                    auto_scaling_enabled=False,
                                )
                            ),
                        )
                    ),
                )
            ),
            run_configuration=(
                kinesisanalyticsv2.CfnApplication.RunConfigurationProperty(
                    application_restore_configuration=(
                        kinesisanalyticsv2.CfnApplication.ApplicationRestoreConfigurationProperty(
                            application_restore_type="SKIP_RESTORE_FROM_SNAPSHOT"
                        )
                    )
                )
            ),
        )
        flink_application.node.add_dependency(flink_artifact_deployment)

        flink_logging = (
            kinesisanalyticsv2.CfnApplicationCloudWatchLoggingOption(
                self,
                "RiskDetectorLogging",
                application_name=flink_application.ref,
                cloud_watch_logging_option=(
                    kinesisanalyticsv2.CfnApplicationCloudWatchLoggingOption.CloudWatchLoggingOptionProperty(
                        log_stream_arn=flink_log_stream_arn
                    )
                ),
            )
        )
        flink_logging.node.add_dependency(flink_application)

        vpc = ec2.Vpc(
            self,
            "BagGuardVpc",
            vpc_name=f"{RESOURCE_PREFIX}-vpc-{ENVIRONMENT}",
            ip_addresses=ec2.IpAddresses.cidr(VPC_CIDR),
            max_azs=1,
            nat_gateways=0,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                )
            ],
        )

        clickhouse_security_group = ec2.SecurityGroup(
            self,
            "ClickHouseSecurityGroup",
            vpc=vpc,
            security_group_name=f"{RESOURCE_PREFIX}-clickhouse-sg-{ENVIRONMENT}",
            description="BagGuard ClickHouse HTTP access",
            allow_all_outbound=True,
        )

        adapter_security_group = ec2.SecurityGroup(
            self,
            "AdapterSecurityGroup",
            vpc=vpc,
            security_group_name=f"{RESOURCE_PREFIX}-adapter-sg-{ENVIRONMENT}",
            description="BagGuard ClickHouse adapter access",
            allow_all_outbound=False,
        )

        clickhouse_security_group.add_ingress_rule(
            peer=adapter_security_group,
            connection=ec2.Port.tcp(8123),
            description="ClickHouse HTTP from adapter only",
        )
        adapter_security_group.add_egress_rule(
            peer=clickhouse_security_group,
            connection=ec2.Port.tcp(8123),
            description="ClickHouse HTTP only",
        )

        secrets_endpoint_security_group = ec2.SecurityGroup(
            self,
            "SecretsEndpointSecurityGroup",
            vpc=vpc,
            security_group_name=(
                f"{RESOURCE_PREFIX}-secrets-endpoint-sg-{ENVIRONMENT}"
            ),
            description="Private Secrets Manager endpoint access",
            allow_all_outbound=False,
        )
        secrets_endpoint_security_group.add_ingress_rule(
            peer=adapter_security_group,
            connection=ec2.Port.tcp(443),
            description="Secrets Manager HTTPS from adapter only",
        )
        adapter_security_group.add_egress_rule(
            peer=secrets_endpoint_security_group,
            connection=ec2.Port.tcp(443),
            description="Secrets Manager HTTPS only",
        )
        secrets_endpoint = vpc.add_interface_endpoint(
            "SecretsManagerEndpoint",
            service=ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER,
            subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            security_groups=[secrets_endpoint_security_group],
            private_dns_enabled=True,
            open=False,
        )

        clickhouse_secret = secretsmanager.Secret(
            self,
            "ClickHouseApplicationSecret",
            secret_name=CLICKHOUSE_SECRET_NAME,
            description="BagGuard ClickHouse application credentials",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template=(
                    f'{{"username":"{CLICKHOUSE_USERNAME}",'
                    f'"database":"{CLICKHOUSE_DATABASE}","port":8123}}'
                ),
                generate_string_key="password",
                password_length=32,
                exclude_punctuation=True,
            ),
        )
        clickhouse_secret.apply_removal_policy(RemovalPolicy.RETAIN)

        instance_role = iam.Role(
            self,
            "ClickHouseInstanceRole",
            role_name=f"{RESOURCE_PREFIX}-clickhouse-role-{ENVIRONMENT}",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            description="BagGuard ClickHouse EC2 role",
        )
        instance_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "AmazonSSMManagedInstanceCore"
            )
        )
        clickhouse_secret.grant_read(instance_role)

        user_data = self._clickhouse_user_data(clickhouse_secret.secret_arn)

        clickhouse_instance = ec2.Instance(
            self,
            "ClickHouseInstance",
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            instance_type=ec2.InstanceType("t4g.medium"),
            machine_image=ec2.MachineImage.latest_amazon_linux2023(
                cpu_type=ec2.AmazonLinuxCpuType.ARM_64
            ),
            security_group=clickhouse_security_group,
            role=instance_role,
            user_data=user_data,
            require_imdsv2=True,
            block_devices=[
                ec2.BlockDevice(
                    device_name="/dev/xvda",
                    volume=ec2.BlockDeviceVolume.ebs(
                        volume_size=40,
                        volume_type=ec2.EbsDeviceVolumeType.GP3,
                        encrypted=True,
                        delete_on_termination=True,
                    ),
                )
            ],
        )
        Tags.of(clickhouse_instance).add(
            "Name", f"{RESOURCE_PREFIX}-clickhouse-{ENVIRONMENT}"
        )

        adapter_log_group = logs.LogGroup(
            self,
            "ClickHouseAdapterLogGroup",
            log_group_name=(
                f"/{RESOURCE_PREFIX}/{ENVIRONMENT}/clickhouse-adapter"
            ),
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.RETAIN,
        )
        adapter_dlq = sqs.Queue(
            self,
            "ClickHouseAdapterDlq",
            queue_name=(
                f"{RESOURCE_PREFIX}-clickhouse-adapter-dlq-{ENVIRONMENT}"
            ),
            encryption=sqs.QueueEncryption.SQS_MANAGED,
            retention_period=Duration.days(14),
        )
        adapter_dlq.apply_removal_policy(RemovalPolicy.RETAIN)
        adapter_function = lambda_.Function(
            self,
            "ClickHouseAdapterFunction",
            function_name=CLICKHOUSE_ADAPTER_FUNCTION_NAME,
            description="BagGuard batched ingestion and controlled ClickHouse actions",
            runtime=lambda_.Runtime.PYTHON_3_14,
            architecture=lambda_.Architecture.ARM_64,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(
                str(
                    Path(__file__).resolve().parents[1]
                    / "lambdas"
                    / "clickhouse_adapter"
                ),
                exclude=["README.md", "__pycache__", "*.pyc"],
            ),
            memory_size=512,
            timeout=Duration.seconds(30),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            allow_public_subnet=True,
            security_groups=[adapter_security_group],
            log_group=adapter_log_group,
            logging_format=lambda_.LoggingFormat.JSON,
            environment={
                "CLICKHOUSE_HOST": clickhouse_instance.instance_private_ip,
                "CLICKHOUSE_PORT": "8123",
                "CLICKHOUSE_DATABASE": CLICKHOUSE_DATABASE,
                "CLICKHOUSE_SECRET_ARN": clickhouse_secret.secret_arn,
                "BAGGAGE_EVENTS_STREAM_NAME": baggage_events_stream.stream_name,
                "RISK_INCIDENTS_STREAM_NAME": risk_incidents_stream.stream_name,
                "CLICKHOUSE_HTTP_TIMEOUT_SECONDS": "5",
                "SECRET_CACHE_SECONDS": "300",
            },
        )
        adapter_function.node.add_dependency(secrets_endpoint)
        clickhouse_secret.grant_read(adapter_function)

        event_source_dlq = lambda_event_sources.SqsDlq(adapter_dlq)
        for stream in (baggage_events_stream, risk_incidents_stream):
            adapter_function.add_event_source(
                lambda_event_sources.KinesisEventSource(
                    stream,
                    starting_position=lambda_.StartingPosition.TRIM_HORIZON,
                    batch_size=500,
                    max_batching_window=Duration.seconds(5),
                    bisect_batch_on_error=True,
                    report_batch_item_failures=True,
                    retry_attempts=3,
                    max_record_age=Duration.hours(1),
                    on_failure=event_source_dlq,
                )
            )

        agent_runtime_arn = Fn.import_value(AGENT_RUNTIME_ARN_EXPORT)
        dispatcher_log_group = logs.LogGroup(
            self,
            "AgentDispatcherLogGroup",
            log_group_name=f"/{RESOURCE_PREFIX}/{ENVIRONMENT}/agent-dispatcher",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.RETAIN,
        )
        dispatcher_dlq = sqs.Queue(
            self,
            "AgentDispatcherDlq",
            queue_name=f"{RESOURCE_PREFIX}-agent-dispatcher-dlq-{ENVIRONMENT}",
            encryption=sqs.QueueEncryption.SQS_MANAGED,
            retention_period=Duration.days(14),
        )
        dispatcher_dlq.apply_removal_policy(RemovalPolicy.RETAIN)
        dispatcher_function = lambda_.Function(
            self,
            "AgentDispatcherFunction",
            function_name=AGENT_DISPATCHER_FUNCTION_NAME,
            description=(
                "Dispatch BagGuard risk incidents to AgentCore and coordinate "
                "result persistence"
            ),
            runtime=lambda_.Runtime.PYTHON_3_14,
            architecture=lambda_.Architecture.ARM_64,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset(
                str(
                    Path(__file__).resolve().parents[1]
                    / "lambdas"
                    / "dispatcher"
                ),
                exclude=["README.md", "__pycache__", "*.pyc"],
            ),
            memory_size=512,
            timeout=Duration.seconds(120),
            log_group=dispatcher_log_group,
            logging_format=lambda_.LoggingFormat.JSON,
            environment={
                "AGENT_RUNTIME_ARN": agent_runtime_arn,
                "AGENT_RUNTIME_QUALIFIER": "DEFAULT",
                "CLICKHOUSE_ADAPTER_FUNCTION_NAME": (
                    adapter_function.function_name
                ),
            },
        )
        dispatcher_function.add_to_role_policy(
            iam.PolicyStatement(
                sid="InvokeOnlyBagGuardAgentRuntime",
                actions=["bedrock-agentcore:InvokeAgentRuntime"],
                resources=[
                    agent_runtime_arn,
                    Fn.join(
                        "",
                        [agent_runtime_arn, "/runtime-endpoint/DEFAULT"],
                    ),
                ],
            )
        )
        adapter_function.grant_invoke(dispatcher_function)
        dispatcher_function.add_event_source(
            lambda_event_sources.KinesisEventSource(
                risk_incidents_stream,
                starting_position=lambda_.StartingPosition.LATEST,
                batch_size=1,
                max_batching_window=Duration.seconds(1),
                bisect_batch_on_error=True,
                report_batch_item_failures=True,
                retry_attempts=3,
                max_record_age=Duration.hours(1),
                on_failure=lambda_event_sources.SqsDlq(dispatcher_dlq),
            )
        )

        CfnOutput(
            self,
            "ClickHouseInstanceId",
            value=clickhouse_instance.instance_id,
            description="ClickHouse EC2 instance ID",
        )
        CfnOutput(
            self,
            "ClickHousePrivateIp",
            value=clickhouse_instance.instance_private_ip,
            description="ClickHouse private IPv4 address",
        )
        CfnOutput(
            self,
            "ClickHouseSecretArn",
            value=clickhouse_secret.secret_arn,
            description="ClickHouse application credential secret ARN",
        )
        CfnOutput(
            self,
            "ClickHouseDatabaseName",
            value=CLICKHOUSE_DATABASE,
            description="ClickHouse database name",
        )
        CfnOutput(
            self,
            "BaggageEventsStreamName",
            value=baggage_events_stream.stream_name,
            description="Baggage events Kinesis stream name",
        )
        CfnOutput(
            self,
            "BaggageEventsStreamArn",
            value=baggage_events_stream.stream_arn,
            description="Baggage events Kinesis stream ARN",
        )
        CfnOutput(
            self,
            "RiskIncidentsStreamName",
            value=risk_incidents_stream.stream_name,
            description="Risk incidents Kinesis stream name",
        )
        CfnOutput(
            self,
            "RiskIncidentsStreamArn",
            value=risk_incidents_stream.stream_arn,
            description="Risk incidents Kinesis stream ARN",
        )
        CfnOutput(
            self,
            "ClickHouseAdapterFunctionName",
            value=adapter_function.function_name,
            description="ClickHouse adapter Lambda function name",
        )
        CfnOutput(
            self,
            "ClickHouseAdapterFunctionArn",
            value=adapter_function.function_arn,
            description="ClickHouse adapter Lambda function ARN",
        )
        CfnOutput(
            self,
            "ClickHouseAdapterDlqUrl",
            value=adapter_dlq.queue_url,
            description="ClickHouse adapter event-source failure queue URL",
        )
        CfnOutput(
            self,
            "FlinkApplicationName",
            value=FLINK_APPLICATION_NAME,
            description="Managed Service for Apache Flink application name",
        )
        CfnOutput(
            self,
            "FlinkApplicationArn",
            value=Arn.format(
                components=ArnComponents(
                    service="kinesisanalytics",
                    resource="application",
                    resource_name=FLINK_APPLICATION_NAME,
                    arn_format=ArnFormat.SLASH_RESOURCE_NAME,
                ),
                stack=self,
            ),
            description="Managed Service for Apache Flink application ARN",
        )
        CfnOutput(
            self,
            "FlinkArtifactBucketName",
            value=flink_artifact_bucket.bucket_name,
            description="S3 bucket containing the Managed Flink application artifact",
        )
        CfnOutput(
            self,
            "FlinkArtifactKey",
            value=flink_artifact_key,
            description="S3 object key for the Managed Flink application artifact",
        )
        CfnOutput(
            self,
            "FlinkLogGroupName",
            value=flink_log_group.log_group_name,
            description="CloudWatch log group for the risk detector",
        )
        CfnOutput(
            self,
            "AgentDispatcherFunctionName",
            value=dispatcher_function.function_name,
            description="AgentCore dispatcher Lambda function name",
        )
        CfnOutput(
            self,
            "AgentDispatcherFunctionArn",
            value=dispatcher_function.function_arn,
            description="AgentCore dispatcher Lambda function ARN",
        )
        CfnOutput(
            self,
            "AgentDispatcherDlqUrl",
            value=dispatcher_dlq.queue_url,
            description="Agent dispatcher event-source failure queue URL",
        )

    @staticmethod
    def _clickhouse_user_data(secret_arn: str) -> ec2.UserData:
        user_data = ec2.UserData.for_linux()
        user_data.add_commands(
            "set -euo pipefail",
            "dnf install -y jq dnf-plugins-core",
            (
                "dnf config-manager --add-repo "
                "https://packages.clickhouse.com/rpm/clickhouse.repo"
            ),
            "dnf install -y clickhouse-server clickhouse-client",
            "install -d -m 0755 /etc/clickhouse-server/config.d /etc/clickhouse-server/users.d",
            """cat > /etc/clickhouse-server/config.d/listen.xml <<'EOF'
<clickhouse>
  <listen_host>0.0.0.0</listen_host>
</clickhouse>
EOF""",
            """cat > /etc/clickhouse-server/users.d/default-local-only.xml <<'EOF'
<clickhouse>
  <users>
    <default>
      <networks replace="replace">
        <ip>127.0.0.1</ip>
        <ip>::1</ip>
      </networks>
    </default>
  </users>
</clickhouse>
EOF""",
            "systemctl enable --now clickhouse-server",
            """for attempt in $(seq 1 30); do
  if clickhouse-client --query "SELECT 1" >/dev/null 2>&1; then
    break
  fi
  if [ "$attempt" -eq 30 ]; then
    echo "ClickHouse did not become ready" >&2
    exit 1
  fi
  sleep 2
done""",
            (
                "SECRET_JSON=$(aws secretsmanager get-secret-value "
                f"--secret-id '{secret_arn}' --region '{Aws.REGION}' "
                "--query SecretString --output text)"
            ),
            "CLICKHOUSE_PASSWORD=$(printf '%s' \"$SECRET_JSON\" | jq -er '.password')",
            (
                "PASSWORD_SHA256=$(printf '%s' \"$CLICKHOUSE_PASSWORD\" "
                "| sha256sum | awk '{print $1}')"
            ),
            """cat > /tmp/bagguard-init.sql <<SQL
CREATE DATABASE IF NOT EXISTS bagguard;

CREATE TABLE IF NOT EXISTS bagguard.baggage_events
(
    event_id String,
    bag_tag_id String,
    passenger_id String,
    itinerary_id String,
    inbound_flight_id String,
    outbound_flight_id String,
    airport_code String,
    event_type String,
    event_time DateTime64(3, 'UTC'),
    scan_location String,
    transfer_zone String,
    current_gate String,
    connection_departure_time DateTime64(3, 'UTC'),
    scheduled_departure_time DateTime64(3, 'UTC'),
    estimated_departure_time DateTime64(3, 'UTC'),
    inbound_arrival_time DateTime64(3, 'UTC'),
    bag_status String,
    priority_code String,
    metadata_json String
)
ENGINE = MergeTree
ORDER BY (bag_tag_id, event_time, event_id);

CREATE TABLE IF NOT EXISTS bagguard.baggage_risk_incidents
(
    incident_id String,
    bag_tag_id String,
    incident_type String,
    detected_at DateTime64(3, 'UTC'),
    outbound_flight_id String,
    airport_code String,
    transfer_zone String,
    minutes_to_departure Int32,
    last_scan_type String,
    last_scan_location String,
    context_json String
)
ENGINE = MergeTree
ORDER BY (bag_tag_id, detected_at, incident_id);

CREATE TABLE IF NOT EXISTS bagguard.investigation_results
(
    incident_id String,
    bag_tag_id String,
    investigated_at DateTime64(3, 'UTC'),
    classification String,
    root_cause String,
    scope String,
    recommended_action String,
    operational_priority String,
    evidence_json String
)
ENGINE = MergeTree
ORDER BY (incident_id, investigated_at);

CREATE USER IF NOT EXISTS bagguard_app
IDENTIFIED WITH sha256_hash BY '$PASSWORD_SHA256'
HOST IP '10.42.0.0/24';

GRANT SELECT, INSERT ON bagguard.* TO bagguard_app;
SQL""",
            "chmod 0600 /tmp/bagguard-init.sql",
            "clickhouse-client --multiquery < /tmp/bagguard-init.sql",
            "shred -u /tmp/bagguard-init.sql",
            "unset CLICKHOUSE_PASSWORD PASSWORD_SHA256 SECRET_JSON",
        )
        return user_data
