#!/usr/bin/env python3

import os

import aws_cdk as cdk

from bagguard_production_stack import BagGuardProductionStack


app = cdk.App()

BagGuardProductionStack(
    app,
    "BagGuardProductionStack",
    env=cdk.Environment(
        account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
        region=os.environ.get("CDK_DEFAULT_REGION", "us-east-1"),
    ),
    description="BagGuard streaming and ClickHouse foundation",
)

app.synth()
