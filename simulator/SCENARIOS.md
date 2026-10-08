# BagGuard end-to-end scenarios

These scenarios publish synthetic baggage lifecycle events through the normal
BagGuard path. They do not write risk incidents or investigation conclusions.

```text
producer -> baggage-events Kinesis -> Flink -> risk-incidents Kinesis
         -> dispatcher -> AgentCore -> ClickHouse adapter -> ClickHouse
```

Run all three scenarios:

```bash
.venv/bin/python simulator/run_scenarios.py \
  --agent-log-group /aws/bedrock-agentcore/runtimes/<runtime-id>-DEFAULT
```

The runner creates a unique flight, transfer zone, and bag cohort for each
execution. It waits for the Flink timer, verifies that every expected signal is
`BAG_CONNECTION_RISK`, waits for the persisted investigations, reads the
AgentCore tool-action trace, and writes the observed comparison to
`simulator/scenario_results.json`.

The scenario data contains only observable operational conditions:

- one stalled bag alongside normally progressing peers;
- multiple stalled bags in one zone alongside normally progressing bags in a
  control zone;
- a late inbound arrival, a shortened connection window, and normally
  progressing transfer scans.

No scenario name, expected scope, root cause, or recommendation is passed to
Flink or the investigator as a conclusion.
