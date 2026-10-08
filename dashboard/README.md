# BagGuard Operations Console

The Operations Console is a local Streamlit interface for BagGuard. It uses
locally configured AWS credentials to publish synthetic scenario events to
Kinesis and invokes `bagguard-clickhouse-adapter-prod` for every data read. It
does not connect directly to ClickHouse and is not deployed to AWS.

Install dependencies:

```bash
.venv/bin/python -m pip install -r dashboard/requirements.txt
```

Run locally:

```bash
AWS_REGION=us-east-1 \
BAGGUARD_EVENT_STREAM_NAME=bagguard-baggage-events-prod \
BAGGUARD_CLICKHOUSE_ADAPTER_FUNCTION_NAME=bagguard-clickhouse-adapter-prod \
.venv/bin/streamlit run dashboard/app.py
```

The repository configuration binds Streamlit to `127.0.0.1:8501`. It is not
published on the LAN or internet.

The four scenario buttons publish synthetic data only. For a risk scenario,
the console polls the adapter until the corresponding structured investigation
is available. The UI displays evidence and recommendations but never requests
or displays model chain-of-thought.
