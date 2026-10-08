from __future__ import annotations

import os
import unittest
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIRECTORY = REPOSITORY_ROOT / "scripts"


class CostControlScriptsTest(unittest.TestCase):
    def test_required_scripts_are_executable(self) -> None:
        for name in (
            "clickhouse-start.sh",
            "clickhouse-stop.sh",
            "flink-start.sh",
            "flink-stop.sh",
            "platform-start.sh",
            "platform-stop.sh",
            "platform-status.sh",
            "stack-create.sh",
            "stack-destroy.sh",
            "stack-recreate.sh",
        ):
            path = SCRIPTS_DIRECTORY / name
            self.assertTrue(path.is_file(), name)
            self.assertTrue(os.access(path, os.X_OK), name)

    def test_stop_scripts_do_not_delete_retained_resources(self) -> None:
        content = "\n".join(
            (SCRIPTS_DIRECTORY / name).read_text(encoding="utf-8").lower()
            for name in ("clickhouse-stop.sh", "flink-stop.sh", "platform-stop.sh")
        )
        for forbidden in (
            "terminate-instances",
            "delete-application",
            "delete-stream",
            "delete-stack",
            "delete-agent-runtime",
            "delete-log-group",
        ):
            self.assertNotIn(forbidden, content)

    def test_orchestration_order_and_health_checks(self) -> None:
        start = (SCRIPTS_DIRECTORY / "platform-start.sh").read_text(encoding="utf-8")
        stop = (SCRIPTS_DIRECTORY / "platform-stop.sh").read_text(encoding="utf-8")

        self.assertLess(start.index("clickhouse-start.sh"), start.index("flink-start.sh"))
        self.assertIn("adapter_health", start)
        self.assertIn("stream_status ACTIVE", start)
        self.assertIn("agent_runtime_status READY", start)
        self.assertLess(stop.index("flink-stop.sh"), stop.index("clickhouse-stop.sh"))

    def test_makefile_exposes_cost_and_scenario_targets(self) -> None:
        makefile = (REPOSITORY_ROOT / "Makefile").read_text(encoding="utf-8")
        for target in (
            "platform-start",
            "platform-stop",
            "platform-status",
            "scenario-normal",
            "scenario-isolated",
            "scenario-zone",
            "scenario-delay",
            "stack-create",
            "stack-destroy",
            "stack-recreate",
        ):
            self.assertIn(f"\n{target}:\n", makefile)

    def test_dashboard_launch_exposes_repository_packages(self) -> None:
        makefile = (REPOSITORY_ROOT / "Makefile").read_text(encoding="utf-8")
        dashboard_target = makefile.split("\ndashboard:\n", 1)[1].split("\n\n", 1)[0]
        self.assertIn('PYTHONPATH="$(CURDIR)"', dashboard_target)

    def test_full_stack_deletion_requires_scoped_confirmation(self) -> None:
        common = (SCRIPTS_DIRECTORY / "_bagguard_stack_common.sh").read_text(
            encoding="utf-8"
        )
        destroy = (SCRIPTS_DIRECTORY / "stack-destroy.sh").read_text(
            encoding="utf-8"
        )
        self.assertIn("require_destroy_confirmation", destroy)
        self.assertIn("DELETE-%s-%s-%s", common)
        self.assertIn(
            'delete_stream_if_present "$BAGGUARD_BAGGAGE_STREAM_NAME"', destroy
        )
        self.assertIn(
            'delete_stream_if_present "$BAGGUARD_RISK_STREAM_NAME"', destroy
        )
        self.assertIn("/aws/bedrock-agentcore/runtimes/", destroy)
        self.assertIn("CustomCDKBucketDeployment", destroy)
        self.assertIn('delete_cloudformation_stack "$BAGGUARD_STACK_NAME"', destroy)
        self.assertIn(
            'delete_cloudformation_stack "$BAGGUARD_AGENTCORE_STACK_NAME"',
            destroy,
        )
        self.assertLess(
            destroy.index('delete_cloudformation_stack "$BAGGUARD_STACK_NAME"'),
            destroy.index(
                'delete_cloudformation_stack "$BAGGUARD_AGENTCORE_STACK_NAME"'
            ),
        )

    def test_full_stack_create_deploys_dependency_first(self) -> None:
        create = (SCRIPTS_DIRECTORY / "stack-create.sh").read_text(
            encoding="utf-8"
        )
        self.assertLess(
            create.index('run_agentcore_cdk deploy "$BAGGUARD_AGENTCORE_STACK_NAME"'),
            create.index('run_main_cdk deploy "$BAGGUARD_STACK_NAME"'),
        )
        self.assertIn('"$SCRIPT_DIR/platform-start.sh"', create)
        self.assertIn('"$SCRIPT_DIR/platform-status.sh"', create)


if __name__ == "__main__":
    unittest.main()
