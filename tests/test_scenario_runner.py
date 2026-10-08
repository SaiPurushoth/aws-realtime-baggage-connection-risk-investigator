from __future__ import annotations

import os
import sys
import unittest
from datetime import UTC, datetime


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "simulator"))

import producer  # noqa: E402
import run_scenarios  # noqa: E402


NOW = datetime(2026, 10, 5, 10, 0, tzinfo=UTC)


class ScenarioRunnerTests(unittest.TestCase):
    def test_candidates_come_from_scenario_data(self) -> None:
        expected_counts = {
            "isolated-stall": 1,
            "zone-congestion": 8,
            "inbound-delay": 3,
        }
        for scenario, expected_count in expected_counts.items():
            with self.subTest(scenario=scenario):
                events = producer.generate_scenario(
                    scenario,
                    now=NOW,
                    run_id=f"RUN-{scenario}",
                    departure_seconds=60,
                )
                candidates = run_scenarios._risk_candidates(events)
                self.assertEqual(expected_count, len(candidates))
                self.assertEqual(
                    expected_count,
                    len({item["incident_id"] for item in candidates}),
                )
                self.assertTrue(
                    all(
                        item["incident_id"].endswith("-BAG_CONNECTION_RISK")
                        for item in candidates
                    )
                )

    def test_comparison_uses_observed_investigation_values(self) -> None:
        rows = [
            {
                "scenario": "ISOLATED BAG",
                "flink_detection": "BAG_CONNECTION_RISK",
                "agent_tools": ["get_bag_timeline", "get_connection_peers"],
                "representative_investigation": {
                    "root_cause": "observed root cause",
                    "scope": "ISOLATED",
                    "recommended_action": "Recommend observed action",
                },
            }
        ]

        markdown = run_scenarios._markdown(rows)

        self.assertIn("observed root cause", markdown)
        self.assertIn("Recommend observed action", markdown)
        self.assertIn("get_bag_timeline", markdown)


if __name__ == "__main__":
    unittest.main()
