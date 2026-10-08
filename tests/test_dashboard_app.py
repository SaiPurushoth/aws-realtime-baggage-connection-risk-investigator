from __future__ import annotations

import unittest
from pathlib import Path


try:
    from streamlit.testing.v1 import AppTest
except ImportError:  # The UI dependency is installed from dashboard/requirements.txt.
    AppTest = None


@unittest.skipIf(AppTest is None, "dashboard dependencies are not installed")
class DashboardAppTests(unittest.TestCase):
    def test_initial_console_renders_all_scenario_controls(self) -> None:
        app_path = Path(__file__).resolve().parents[1] / "dashboard" / "app.py"
        app = AppTest.from_file(str(app_path))

        app.run(timeout=20)

        self.assertEqual([], list(app.exception))
        self.assertEqual(
            [
                "Normal Connection",
                "Isolated Bag Stall",
                "Transfer Zone Congestion",
                "Inbound Flight Delay",
            ],
            [button.label for button in app.button],
        )
        self.assertIn(
            "Choose a scenario to populate the operations console.",
            [message.value for message in app.info],
        )
        self.assertIn("Live Data Path", [heading.value for heading in app.subheader])
        rendered_markdown = "\n".join(element.value for element in app.markdown)
        self.assertIn("Event Producer", rendered_markdown)
        self.assertIn("Amazon Bedrock AgentCore", rendered_markdown)
        self.assertIn("ClickHouse on private EC2", rendered_markdown)


if __name__ == "__main__":
    unittest.main()
