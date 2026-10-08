import os
import sys
import unittest

import aws_cdk as cdk
from aws_cdk.assertions import Match, Template

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "infra"))

from bagguard_production_stack import BagGuardProductionStack  # noqa: E402


class TestClickHouseStack(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        app = cdk.App()
        stack = BagGuardProductionStack(app, "BagGuardProductionStack")
        cls.template = Template.from_stack(stack)

    def test_instance_shape_and_storage(self) -> None:
        self.template.has_resource_properties(
            "AWS::EC2::Instance",
            {
                "InstanceType": "t4g.medium",
                "BlockDeviceMappings": Match.array_with(
                    [
                        Match.object_like(
                            {
                                "DeviceName": "/dev/xvda",
                                "Ebs": Match.object_like(
                                    {
                                        "DeleteOnTermination": True,
                                        "Encrypted": True,
                                        "VolumeSize": 40,
                                        "VolumeType": "gp3",
                                    }
                                ),
                            }
                        )
                    ]
                ),
            },
        )

    def test_minimal_public_network_has_no_nat_gateway(self) -> None:
        self.template.resource_count_is("AWS::EC2::VPC", 1)
        self.template.resource_count_is("AWS::EC2::Subnet", 1)
        self.template.resource_count_is("AWS::EC2::NatGateway", 0)
        self.template.has_resource_properties(
            "AWS::EC2::Subnet",
            {"MapPublicIpOnLaunch": True},
        )
        self.template.has_resource_properties(
            "AWS::EC2::VPC",
            {
                "Tags": Match.array_with(
                    [{"Key": "Name", "Value": "bagguard-vpc-prod"}]
                )
            },
        )

    def test_named_persistent_resources_follow_production_convention(self) -> None:
        self.template.has_resource_properties(
            "AWS::IAM::Role",
            {"RoleName": "bagguard-clickhouse-role-prod"},
        )
        bucket = next(
            resource["Properties"]
            for resource in self.template.to_json()["Resources"].values()
            if resource["Type"] == "AWS::S3::Bucket"
        )
        self.assertEqual(
            [
                "bagguard-flink-artifacts-prod-",
                {"Ref": "AWS::AccountId"},
                "-",
                {"Ref": "AWS::Region"},
            ],
            bucket["BucketName"]["Fn::Join"][1],
        )

    def test_no_ssh_or_public_clickhouse_ingress(self) -> None:
        template = self.template.to_json()
        ingress_rules = [
            resource["Properties"]
            for resource in template["Resources"].values()
            if resource["Type"] == "AWS::EC2::SecurityGroupIngress"
        ]

        clickhouse_ingress = [
            rule
            for rule in ingress_rules
            if rule["FromPort"] == 8123 and rule["ToPort"] == 8123
        ]
        self.assertEqual(1, len(clickhouse_ingress))
        self.assertIn("SourceSecurityGroupId", clickhouse_ingress[0])
        self.assertNotIn("CidrIp", clickhouse_ingress[0])
        for rule in ingress_rules:
            self.assertNotEqual(22, rule["FromPort"])
            self.assertNotEqual(9000, rule["FromPort"])
            self.assertNotEqual("0.0.0.0/0", rule.get("CidrIp"))

    def test_secret_name_and_outputs(self) -> None:
        self.template.has_resource_properties(
            "AWS::SecretsManager::Secret",
            {"Name": "bagguard/clickhouse/prod"},
        )
        outputs = self.template.to_json()["Outputs"]
        self.assertEqual(
            {
                "ClickHouseInstanceId",
                "ClickHousePrivateIp",
                "ClickHouseSecretArn",
                "ClickHouseDatabaseName",
                "BaggageEventsStreamName",
                "BaggageEventsStreamArn",
                "RiskIncidentsStreamName",
                "RiskIncidentsStreamArn",
                "ClickHouseAdapterFunctionName",
                "ClickHouseAdapterFunctionArn",
                "ClickHouseAdapterDlqUrl",
                "FlinkApplicationName",
                "FlinkApplicationArn",
                "FlinkArtifactBucketName",
                "FlinkArtifactKey",
                "FlinkLogGroupName",
                "AgentDispatcherFunctionName",
                "AgentDispatcherFunctionArn",
                "AgentDispatcherDlqUrl",
            },
            set(outputs),
        )

    def test_managed_flink_application_is_minimum_footprint(self) -> None:
        self.template.has_resource_properties(
            "AWS::KinesisAnalyticsV2::Application",
            {
                "ApplicationName": "bagguard-risk-detector-prod",
                "ApplicationMode": "STREAMING",
                "RuntimeEnvironment": "FLINK-2_3",
                "ApplicationConfiguration": Match.object_like(
                    {
                        "FlinkApplicationConfiguration": Match.object_like(
                            {
                                "ParallelismConfiguration": {
                                    "ConfigurationType": "CUSTOM",
                                    "Parallelism": 1,
                                    "ParallelismPerKPU": 1,
                                    "AutoScalingEnabled": False,
                                }
                            }
                        ),
                        "ApplicationSnapshotConfiguration": {
                            "SnapshotsEnabled": True
                        },
                    }
                ),
            },
        )

    def test_flink_runtime_properties_and_artifact_bucket(self) -> None:
        template = self.template.to_json()
        application = next(
            resource["Properties"]
            for resource in template["Resources"].values()
            if resource["Type"] == "AWS::KinesisAnalyticsV2::Application"
        )
        groups = {
            group["PropertyGroupId"]: group["PropertyMap"]
            for group in application["ApplicationConfiguration"][
                "EnvironmentProperties"
            ]["PropertyGroups"]
        }
        self.assertEqual("main.py", groups["kinesis.analytics.flink.run.options"]["python"])
        self.assertEqual(
            "detector_core.py",
            groups["kinesis.analytics.flink.run.options"]["pyFiles"],
        )
        self.assertEqual(
            "lib/bagguard-pyflink-dependencies.jar",
            groups["kinesis.analytics.flink.run.options"]["jarfile"],
        )
        runtime = groups["bagguard.runtime"]
        self.assertIn("Ref", runtime["INPUT_STREAM"])
        self.assertIn("Ref", runtime["OUTPUT_STREAM"])
        self.assertEqual("30", runtime["RISK_BUFFER_SECONDS"])
        self.assertEqual("LATEST", runtime["KINESIS_STARTING_POSITION"])
        self.template.has_resource_properties(
            "AWS::S3::Bucket",
            {
                "BucketEncryption": Match.any_value(),
                "PublicAccessBlockConfiguration": {
                    "BlockPublicAcls": True,
                    "BlockPublicPolicy": True,
                    "IgnorePublicAcls": True,
                    "RestrictPublicBuckets": True,
                },
            },
        )

    def test_flink_has_logging_and_scoped_data_plane_iam(self) -> None:
        self.template.has_resource_properties(
            "AWS::Logs::LogGroup",
            {
                "LogGroupName": "/bagguard/prod/flink/risk-detector",
                "RetentionInDays": 7,
            },
        )
        self.template.resource_count_is(
            "AWS::KinesisAnalyticsV2::ApplicationCloudWatchLoggingOption", 1
        )
        template_text = str(self.template.to_json())
        for action in (
            "kinesis:GetRecords",
            "kinesis:GetShardIterator",
            "kinesis:PutRecord",
            "kinesis:PutRecords",
            "s3:GetObject",
            "logs:PutLogEvents",
        ):
            self.assertIn(action, template_text)

    def test_kinesis_streams_are_on_demand_and_encrypted(self) -> None:
        template = self.template.to_json()
        streams = [
            resource["Properties"]
            for resource in template["Resources"].values()
            if resource["Type"] == "AWS::Kinesis::Stream"
        ]

        self.assertEqual(
            {"bagguard-baggage-events-prod", "bagguard-risk-incidents-prod"},
            {stream["Name"] for stream in streams},
        )
        for stream in streams:
            self.assertEqual(
                {"StreamMode": "ON_DEMAND"}, stream["StreamModeDetails"]
            )
            self.assertEqual("KMS", stream["StreamEncryption"]["EncryptionType"])
            self.assertEqual(
                "alias/aws/kinesis", stream["StreamEncryption"]["KeyId"]
            )

    def test_clickhouse_adapter_lambda_and_event_sources(self) -> None:
        self.template.has_resource_properties(
            "AWS::Lambda::Function",
            {
                "FunctionName": "bagguard-clickhouse-adapter-prod",
                "Runtime": "python3.14",
                "Architectures": ["arm64"],
                "MemorySize": 512,
                "Timeout": 30,
                "VpcConfig": Match.object_like(
                    {
                        "SecurityGroupIds": Match.any_value(),
                        "SubnetIds": Match.any_value(),
                    }
                ),
            },
        )
        self.template.resource_count_is("AWS::Lambda::EventSourceMapping", 3)
        self.template.has_resource_properties(
            "AWS::Lambda::EventSourceMapping",
            {
                "BatchSize": 500,
                "BisectBatchOnFunctionError": True,
                "FunctionResponseTypes": ["ReportBatchItemFailures"],
                "MaximumBatchingWindowInSeconds": 5,
                "MaximumRecordAgeInSeconds": 3600,
                "MaximumRetryAttempts": 3,
                "StartingPosition": "TRIM_HORIZON",
                "DestinationConfig": Match.object_like(
                    {"OnFailure": Match.any_value()}
                ),
            },
        )

    def test_private_secret_access_and_operational_resources(self) -> None:
        self.template.has_resource_properties(
            "AWS::EC2::VPCEndpoint",
            {
                "PrivateDnsEnabled": True,
                "ServiceName": Match.any_value(),
                "VpcEndpointType": "Interface",
            },
        )
        endpoint = next(
            resource
            for resource in self.template.to_json()["Resources"].values()
            if resource["Type"] == "AWS::EC2::VPCEndpoint"
        )
        self.assertIn("secretsmanager", str(endpoint["Properties"]["ServiceName"]))
        self.template.has_resource_properties(
            "AWS::Logs::LogGroup",
            {
                "LogGroupName": "/bagguard/prod/clickhouse-adapter",
                "RetentionInDays": 7,
            },
        )
        self.template.has_resource_properties(
            "AWS::SQS::Queue",
            {
                "QueueName": "bagguard-clickhouse-adapter-dlq-prod",
                "SqsManagedSseEnabled": True,
                "MessageRetentionPeriod": 1209600,
            },
        )

    def test_agent_dispatcher_is_thin_and_least_privilege(self) -> None:
        self.template.has_resource_properties(
            "AWS::Lambda::Function",
            {
                "FunctionName": "bagguard-agent-dispatcher-prod",
                "Runtime": "python3.14",
                "Architectures": ["arm64"],
                "MemorySize": 512,
                "Timeout": 120,
                "Environment": {
                    "Variables": Match.object_like(
                        {
                            "AGENT_RUNTIME_QUALIFIER": "DEFAULT",
                            "CLICKHOUSE_ADAPTER_FUNCTION_NAME": {
                                "Ref": Match.any_value()
                            },
                        }
                    )
                },
            },
        )
        self.template.has_resource_properties(
            "AWS::Lambda::EventSourceMapping",
            {
                "BatchSize": 1,
                "BisectBatchOnFunctionError": True,
                "FunctionResponseTypes": ["ReportBatchItemFailures"],
                "MaximumBatchingWindowInSeconds": 1,
                "MaximumRecordAgeInSeconds": 3600,
                "MaximumRetryAttempts": 3,
                "StartingPosition": "LATEST",
                "DestinationConfig": Match.object_like(
                    {"OnFailure": Match.any_value()}
                ),
            },
        )
        template_text = str(self.template.to_json())
        self.assertIn("bedrock-agentcore:InvokeAgentRuntime", template_text)
        self.assertIn("/runtime-endpoint/DEFAULT", template_text)
        self.assertIn("bagguard-clickhouse-adapter-prod", template_text)
        self.assertNotIn("CLICKHOUSE_HOST", str(
            next(
                resource["Properties"]["Environment"]["Variables"]
                for resource in self.template.to_json()["Resources"].values()
                if resource["Type"] == "AWS::Lambda::Function"
                and resource["Properties"].get("FunctionName")
                == "bagguard-agent-dispatcher-prod"
            )
        ))

    def test_instance_secret_access_is_resource_scoped(self) -> None:
        self.template.has_resource_properties(
            "AWS::IAM::Policy",
            {
                "PolicyDocument": {
                    "Statement": Match.array_with(
                        [
                            Match.object_like(
                                {
                                    "Action": [
                                        "secretsmanager:GetSecretValue",
                                        "secretsmanager:DescribeSecret",
                                    ],
                                    "Effect": "Allow",
                                    "Resource": Match.object_like(
                                        {"Ref": Match.any_value()}
                                    ),
                                }
                            )
                        ]
                    )
                }
            },
        )


if __name__ == "__main__":
    unittest.main()
