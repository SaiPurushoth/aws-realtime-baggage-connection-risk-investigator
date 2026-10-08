#!/usr/bin/env python3
"""BagGuard PyFlink connection-risk detector.

The job uses processing-time timers at an absolute flight deadline so a stalled
bag can produce an incident even when no later baggage event advances an event
time watermark. Event timestamps still determine which scan and departure
context are the latest when records arrive out of order.
"""

from __future__ import annotations

import json
import os
from dataclasses import fields
from typing import Any, Iterator

try:
    from detector_core import (
        BagRiskState,
        DEFAULT_RISK_BUFFER_SECONDS,
        build_incident,
        should_emit_incident,
        update_state,
    )
except ModuleNotFoundError:
    from flink.detector_core import (  # type: ignore[no-redef]
        BagRiskState,
        DEFAULT_RISK_BUFFER_SECONDS,
        build_incident,
        should_emit_incident,
        update_state,
    )


SOURCE_STREAM_NAME = "bagguard-baggage-events-prod"
SINK_STREAM_NAME = "bagguard-risk-incidents-prod"
DEFAULT_REGION = "us-east-1"
RUNTIME_PROPERTY_GROUP = "bagguard.runtime"


def parse_json_record(value: str) -> dict[str, Any]:
    payload = json.loads(value)
    if not isinstance(payload, dict):
        raise ValueError("Kinesis baggage record must be a JSON object")
    return payload


def serialize_incident(value: dict[str, Any]) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _setting(
    runtime_properties: dict[str, str], name: str, default: str
) -> str:
    return os.environ.get(name, runtime_properties.get(name, default))


def _positive_int_setting(
    runtime_properties: dict[str, str], name: str, default: int
) -> int:
    raw_value = _setting(runtime_properties, name, str(default))
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def _source_stream_arn(
    region: str, runtime_properties: dict[str, str]
) -> str:
    configured = _setting(runtime_properties, "SOURCE_STREAM_ARN", "")
    if configured:
        return configured
    account_id = _setting(runtime_properties, "AWS_ACCOUNT_ID", "")
    if not account_id:
        raise ValueError("SOURCE_STREAM_ARN or AWS_ACCOUNT_ID must be configured")
    stream_name = _setting(
        runtime_properties, "INPUT_STREAM", SOURCE_STREAM_NAME
    )
    return f"arn:aws:kinesis:{region}:{account_id}:stream/{stream_name}"


def _load_pyflink() -> dict[str, Any]:
    """Import PyFlink only when constructing the executable job."""

    try:
        from pyflink.common import Types, WatermarkStrategy
        from pyflink.common.serialization import SimpleStringSchema
        from pyflink.datastream import RuntimeExecutionMode, StreamExecutionEnvironment
        from pyflink.datastream.checkpointing_mode import CheckpointingMode
        from pyflink.datastream.connectors.base import Source
        from pyflink.datastream.connectors.kinesis import (
            KinesisStreamsSink,
            PartitionKeyGenerator,
        )
        from pyflink.datastream.functions import KeyedProcessFunction
        from pyflink.datastream.state import ValueStateDescriptor
        from pyflink.java_gateway import get_gateway
    except ImportError as exc:
        raise RuntimeError(
            "PyFlink is required to run the streaming job; install "
            "flink/requirements.txt under Python 3.12"
        ) from exc

    return locals()


def _load_runtime_properties(pyflink: dict[str, Any]) -> dict[str, str]:
    """Load the allow-listed Managed Flink application property group."""

    gateway = pyflink["get_gateway"]()
    application_properties = (
        gateway.jvm.com.amazonaws.services.kinesisanalytics.runtime
        .KinesisAnalyticsRuntime.getApplicationProperties()
    )
    group = application_properties.get(RUNTIME_PROPERTY_GROUP)
    if group is None:
        return {}
    return {
        str(name): str(group.getProperty(name))
        for name in group.stringPropertyNames()
    }


def create_process_function(pyflink: dict[str, Any], risk_buffer_seconds: int):
    KeyedProcessFunction = pyflink["KeyedProcessFunction"]
    ValueStateDescriptor = pyflink["ValueStateDescriptor"]
    Types = pyflink["Types"]

    class BagConnectionRiskProcessFunction(KeyedProcessFunction):
        """Keyed state and processing-time timer for one bag."""

        def __init__(self, buffer_seconds: int):
            self.buffer_seconds = buffer_seconds
            self._states: dict[str, Any] = {}

        def open(self, runtime_context) -> None:
            string_fields = {
                "last_event_type",
                "last_scan_location",
                "transfer_zone",
                "airport_code",
                "outbound_flight_id",
            }
            boolean_fields = {
                "transfer_arrived",
                "makeup_area_reached",
                "aircraft_loaded",
                "incident_emitted",
            }
            for field in fields(BagRiskState):
                if field.name in string_fields:
                    type_info = Types.STRING()
                elif field.name in boolean_fields:
                    type_info = Types.BOOLEAN()
                else:
                    type_info = Types.LONG()
                descriptor = ValueStateDescriptor(field.name, type_info)
                self._states[field.name] = runtime_context.get_state(descriptor)

        def process_element(self, event, ctx) -> Iterator[dict[str, Any]]:
            bag_tag_id = event.get("bag_tag_id")
            state = self._read_state()
            old_timer_ms = state.risk_timer_ms
            update_state(
                state,
                event,
                risk_buffer_seconds=self.buffer_seconds,
            )
            timer_service = ctx.timer_service()

            if old_timer_ms is not None and old_timer_ms != state.risk_timer_ms:
                timer_service.delete_processing_time_timer(old_timer_ms)

            now_ms = timer_service.current_processing_time()
            if state.risk_timer_ms is not None:
                if state.risk_timer_ms <= now_ms:
                    if should_emit_incident(state, state.risk_timer_ms):
                        incident = build_incident(
                            bag_tag_id,
                            state,
                            detected_at_ms=now_ms,
                        )
                        state.incident_emitted = True
                        state.risk_timer_ms = None
                        self._write_state(state)
                        yield incident
                        return
                    state.risk_timer_ms = None
                else:
                    timer_service.register_processing_time_timer(
                        state.risk_timer_ms
                    )
            self._write_state(state)

        def on_timer(self, timestamp, ctx) -> Iterator[dict[str, Any]]:
            state = self._read_state()
            if not should_emit_incident(state, timestamp):
                return
            incident = build_incident(
                ctx.get_current_key(),
                state,
                detected_at_ms=timestamp,
            )
            state.incident_emitted = True
            state.risk_timer_ms = None
            self._write_state(state)
            yield incident

        def _read_state(self) -> BagRiskState:
            values = {
                field.name: self._states[field.name].value()
                for field in fields(BagRiskState)
            }
            for field_name in (
                "transfer_arrived",
                "makeup_area_reached",
                "aircraft_loaded",
                "incident_emitted",
            ):
                values[field_name] = bool(values[field_name])
            return BagRiskState(**values)

        def _write_state(self, state: BagRiskState) -> None:
            for field in fields(BagRiskState):
                value = getattr(state, field.name)
                if value is None:
                    self._states[field.name].clear()
                else:
                    self._states[field.name].update(value)

    return BagConnectionRiskProcessFunction(risk_buffer_seconds)


def build_kinesis_source(
    pyflink: dict[str, Any],
    *,
    stream_arn: str,
    region: str,
    starting_position: str,
):
    """Wrap the Flink 2.x FLIP-27 Kinesis source for PyFlink."""

    gateway = pyflink["get_gateway"]()
    jvm = gateway.jvm
    source_config = jvm.org.apache.flink.configuration.Configuration()
    source_options = (
        jvm.org.apache.flink.connector.kinesis.source.config
        .KinesisSourceConfigOptions
    )
    try:
        initial_position = getattr(source_options.InitialPosition, starting_position)
    except Exception as exc:
        raise ValueError(
            "KINESIS_STARTING_POSITION must be LATEST or TRIM_HORIZON"
        ) from exc
    source_config.set(source_options.STREAM_INITIAL_POSITION, initial_position)
    source_config.setString("aws.region", region)

    schema = pyflink["SimpleStringSchema"]()
    j_source = (
        jvm.org.apache.flink.connector.kinesis.source.KinesisStreamsSource
        .builder()
        .setStreamArn(stream_arn)
        .setSourceConfig(source_config)
        .setDeserializationSchema(schema._j_deserialization_schema)
        .build()
    )
    return pyflink["Source"](j_source)


def main() -> None:
    pyflink = _load_pyflink()
    runtime_properties = _load_runtime_properties(pyflink)
    region = _setting(runtime_properties, "AWS_REGION", DEFAULT_REGION)
    sink_stream = _setting(
        runtime_properties, "OUTPUT_STREAM", SINK_STREAM_NAME
    )
    starting_position = _setting(
        runtime_properties, "KINESIS_STARTING_POSITION", "LATEST"
    ).upper()
    risk_buffer_seconds = _positive_int_setting(
        runtime_properties, "RISK_BUFFER_SECONDS", DEFAULT_RISK_BUFFER_SECONDS
    )
    checkpoint_interval_ms = _positive_int_setting(
        runtime_properties, "CHECKPOINT_INTERVAL_MS", 60_000
    )
    parallelism = _positive_int_setting(
        runtime_properties, "FLINK_PARALLELISM", 1
    )

    env = pyflink["StreamExecutionEnvironment"].get_execution_environment()
    env.set_runtime_mode(pyflink["RuntimeExecutionMode"].STREAMING)
    env.set_parallelism(parallelism)
    env.enable_checkpointing(checkpoint_interval_ms)
    env.get_checkpoint_config().set_checkpointing_mode(
        pyflink["CheckpointingMode"].EXACTLY_ONCE
    )

    source = build_kinesis_source(
        pyflink,
        stream_arn=_source_stream_arn(region, runtime_properties),
        region=region,
        starting_position=starting_position,
    )
    raw_events = env.from_source(
        source,
        pyflink["WatermarkStrategy"].no_watermarks(),
        "bagguard-kinesis-baggage-events",
        type_info=pyflink["Types"].STRING(),
    ).uid("bagguard-kinesis-baggage-events-v1")

    events = raw_events.map(
        parse_json_record,
        output_type=pyflink["Types"].PICKLED_BYTE_ARRAY(),
    ).name("parse-baggage-event-json")

    incidents = (
        events.key_by(
            lambda event: event["bag_tag_id"],
            key_type=pyflink["Types"].STRING(),
        )
        .process(
            create_process_function(pyflink, risk_buffer_seconds),
            output_type=pyflink["Types"].PICKLED_BYTE_ARRAY(),
        )
        .name("detect-bag-connection-risk")
        .uid("detect-bag-connection-risk-v1")
    )

    serialized = incidents.map(
        serialize_incident,
        output_type=pyflink["Types"].STRING(),
    ).name("serialize-risk-incident-json")

    sink = (
        pyflink["KinesisStreamsSink"]
        .builder()
        .set_kinesis_client_properties({"aws.region": region})
        .set_stream_name(sink_stream)
        .set_serialization_schema(pyflink["SimpleStringSchema"]())
        .set_partition_key_generator(pyflink["PartitionKeyGenerator"].fixed())
        .set_fail_on_error(True)
        .set_max_batch_size(100)
        .set_max_time_in_buffer_ms(1_000)
        .build()
    )
    serialized.sink_to(sink).name("bagguard-kinesis-risk-incidents").uid(
        "bagguard-kinesis-risk-incidents-v1"
    )

    env.execute("bagguard-risk-detector-prod")


if __name__ == "__main__":
    main()
