"""Amazon Bedrock AgentCore Runtime entrypoint for BagGuard."""

from __future__ import annotations

from typing import Any

from bedrock_agentcore.runtime import BedrockAgentCoreApp

try:
    from .adapter import ClickHouseAdapterClient
    from .investigator import InvestigatorService
except ImportError:  # AgentCore CodeZip executes app.py as a top-level module.
    from adapter import ClickHouseAdapterClient  # type: ignore[no-redef]
    from investigator import InvestigatorService  # type: ignore[no-redef]


app = BedrockAgentCoreApp()
service = InvestigatorService(ClickHouseAdapterClient())


@app.entrypoint
def invoke(payload: dict[str, Any], context: Any) -> dict[str, Any]:
    """Investigate one generic baggage connection-risk incident."""

    del context
    return service.investigate(payload)


if __name__ == "__main__":
    app.run()
