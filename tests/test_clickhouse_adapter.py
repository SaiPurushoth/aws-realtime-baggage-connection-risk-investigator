import base64
import json
import os
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(__file__), "..", "lambdas", "clickhouse_adapter"
    ),
)

import lambda_function as adapter  # noqa: E402


class TestClickHouseAdapter(unittest.TestCase):
    def setUp(self) -> None:
        self.context = SimpleNamespace(aws_request_id="test-request")

    @staticmethod
    def baggage_event(event_id: str) -> dict:
        return {
            "event_id": event_id,
            "bag_tag_id": "BG-00192",
            "passenger_id": "P-SYNTHETIC-1",
            "itinerary_id": "ITIN-1",
            "inbound_flight_id": "AI-501",
            "outbound_flight_id": "AI-802",
            "airport_code": "DEL",
            "event_type": "TRANSFER_SORTER_SCAN",
            "event_time": "2026-10-04T10:24:00.000Z",
            "scan_location": "SORTER-T2",
            "transfer_zone": "T2",
            "current_gate": "G12",
            "connection_departure_time": "2026-10-04T10:30:00.000Z",
            "scheduled_departure_time": "2026-10-04T10:30:00.000Z",
            "estimated_departure_time": "2026-10-04T10:30:00.000Z",
            "inbound_arrival_time": "2026-10-04T10:07:00.000Z",
            "bag_status": "IN_TRANSFER",
            "priority_code": "HOT",
            "metadata": {"synthetic": True},
        }

    @staticmethod
    def risk_incident(incident_id: str) -> dict:
        return {
            "incident_id": incident_id,
            "bag_tag_id": "BG-00192",
            "incident_type": "BAG_CONNECTION_RISK",
            "detected_at": "2026-10-04T10:24:00.000Z",
            "outbound_flight_id": "AI-802",
            "airport_code": "DEL",
            "transfer_zone": "T2",
            "minutes_to_departure": 6,
            "last_scan_type": "TRANSFER_SORTER_SCAN",
            "last_scan_location": "SORTER-T2",
            "context_json": {"synthetic": True},
        }

    @staticmethod
    def flink_risk_incident(incident_id: str) -> dict:
        return {
            "incident_id": incident_id,
            "bag_tag_id": "BG-00192",
            "incident_type": "BAG_CONNECTION_RISK",
            "detected_at": "2026-10-04T10:29:30.000Z",
            "outbound_flight_id": "AI-802",
            "airport_code": "DEL",
            "transfer_zone": "T2",
            "last_scan_type": "TRANSFER_SORTER_SCAN",
            "last_scan_location": "SORTER-T2",
            "last_scan_time": "2026-10-04T10:24:00.000Z",
            "connection_departure_time": "2026-10-04T10:30:00.000Z",
            "seconds_to_departure": 30,
            "observed_context": {
                "makeup_area_reached": False,
                "aircraft_loaded": False,
            },
        }

    @staticmethod
    def kinesis_record(
        payload: dict,
        sequence_number: str,
        stream_name: str = "bagguard-baggage-events-prod",
    ) -> dict:
        encoded = base64.b64encode(json.dumps(payload).encode()).decode()
        return {
            "eventSource": "aws:kinesis",
            "eventSourceARN": (
                "arn:aws:kinesis:us-east-1:123456789012:stream/"
                f"{stream_name}"
            ),
            "kinesis": {"sequenceNumber": sequence_number, "data": encoded},
        }

    def test_kinesis_records_use_one_batch_insert(self) -> None:
        records = [
            self.kinesis_record(self.baggage_event("evt-1"), "1"),
            self.kinesis_record(self.baggage_event("evt-2"), "2"),
        ]
        with patch.object(adapter, "_insert_rows") as insert_rows:
            result = adapter.lambda_handler({"Records": records}, self.context)

        self.assertEqual({"batchItemFailures": []}, result)
        insert_rows.assert_called_once()
        table, rows = insert_rows.call_args.args
        self.assertEqual("bagguard.baggage_events", table)
        self.assertEqual(2, len(rows))
        self.assertEqual('{"synthetic":true}', rows[0]["metadata_json"])

    def test_risk_incidents_batch_to_the_incident_table(self) -> None:
        records = [
            self.kinesis_record(
                self.risk_incident("risk-1"),
                "10",
                "bagguard-risk-incidents-prod",
            ),
            self.kinesis_record(
                self.risk_incident("risk-2"),
                "11",
                "bagguard-risk-incidents-prod",
            ),
        ]
        with patch.object(adapter, "_insert_rows") as insert_rows:
            result = adapter.lambda_handler({"Records": records}, self.context)

        self.assertEqual({"batchItemFailures": []}, result)
        insert_rows.assert_called_once()
        self.assertEqual("bagguard.baggage_risk_incidents", insert_rows.call_args.args[0])
        self.assertEqual(2, len(insert_rows.call_args.args[1]))

    def test_flink_incident_contract_maps_to_analytical_table(self) -> None:
        record = self.kinesis_record(
            self.flink_risk_incident("risk-flink-1"),
            "12",
            "bagguard-risk-incidents-prod",
        )
        with patch.object(adapter, "_insert_rows") as insert_rows:
            result = adapter.lambda_handler({"Records": [record]}, self.context)

        self.assertEqual({"batchItemFailures": []}, result)
        row = insert_rows.call_args.args[1][0]
        self.assertEqual(1, row["minutes_to_departure"])
        context = json.loads(row["context_json"])
        self.assertEqual(30, context["seconds_to_departure"])
        self.assertFalse(context["observed_context"]["makeup_area_reached"])

    def test_invalid_record_is_reported_without_per_record_requests(self) -> None:
        invalid = self.baggage_event("evt-invalid")
        del invalid["bag_tag_id"]
        records = [
            self.kinesis_record(invalid, "1"),
            self.kinesis_record(self.baggage_event("evt-2"), "2"),
        ]
        with patch.object(adapter, "_insert_rows") as insert_rows:
            result = adapter.lambda_handler({"Records": records}, self.context)

        self.assertEqual([{"itemIdentifier": "1"}], result["batchItemFailures"])
        insert_rows.assert_called_once()
        self.assertEqual(1, len(insert_rows.call_args.args[1]))

    def test_health_check(self) -> None:
        expected = {"version": "26.9.10.4", "database": "bagguard", "healthy": 1}
        with patch.object(adapter, "_query_rows", return_value=[expected]) as query:
            result = adapter.lambda_handler(
                {"action": "health_check", "parameters": {}}, self.context
            )

        self.assertTrue(result["ok"])
        self.assertEqual(expected, result["result"])
        query.assert_called_once()

    def test_action_values_use_typed_query_parameters(self) -> None:
        with patch.object(adapter, "_query_rows", return_value=[]) as query:
            adapter.lambda_handler(
                {
                    "action": "get_connection_peers",
                    "parameters": {
                        "outbound_flight_id": "AI-802' OR 1=1 --",
                        "airport_code": "DEL",
                        "lookback_minutes": 60,
                    },
                },
                self.context,
            )

        sql, parameters = query.call_args.args
        self.assertIn("{outbound_flight_id:String}", sql)
        self.assertNotIn("AI-802", sql)
        self.assertEqual("AI-802' OR 1=1 --", parameters["outbound_flight_id"])

    def test_arbitrary_sql_is_rejected(self) -> None:
        with self.assertRaises(adapter.ValidationError):
            adapter.lambda_handler(
                {"action": "health_check", "parameters": {}, "sql": "SELECT 1"},
                self.context,
            )

    def test_unknown_parameters_are_rejected(self) -> None:
        with self.assertRaises(adapter.ValidationError):
            adapter.lambda_handler(
                {
                    "action": "get_bag_timeline",
                    "parameters": {"bag_tag_id": "BG-1", "sql": "SELECT 1"},
                },
                self.context,
            )

    def test_action_allow_list_is_exact(self) -> None:
        self.assertEqual(
            {
                "health_check",
                "get_bag_timeline",
                "get_connection_peers",
                "get_transfer_zone_health",
                "get_recent_connection_risks",
                "get_flight_bag_progress",
                "get_investigation_result",
                "get_average_transfer_progression_by_zone",
                "get_risk_frequency_by_transfer_zone",
                "get_connection_success_rate",
                "save_investigation_result",
            },
            set(adapter.ACTION_HANDLERS),
        )

    def test_historical_analytics_use_typed_parameters(self) -> None:
        with patch.object(adapter, "_query_rows", return_value=[]) as query:
            adapter.lambda_handler(
                {
                    "action": "get_connection_success_rate",
                    "parameters": {
                        "airport_code": "BLR",
                        "lookback_minutes": 10080,
                        "window_minutes": 360,
                    },
                },
                self.context,
            )

        sql, parameters = query.call_args.args
        self.assertIn("{airport_code:String}", sql)
        self.assertIn("{lookback_minutes:UInt32}", sql)
        self.assertIn("{window_minutes:UInt32}", sql)
        self.assertEqual(360, parameters["window_minutes"])


if __name__ == "__main__":
    unittest.main()
