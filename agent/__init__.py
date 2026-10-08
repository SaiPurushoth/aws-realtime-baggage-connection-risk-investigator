"""BagGuard real-time baggage connection risk investigator."""

from .contracts import InvestigationInput, InvestigationResult
from .investigator import AGENT_NAME, AGENT_ROLE, SYSTEM_PROMPT, InvestigatorService

__all__ = [
    "AGENT_NAME",
    "AGENT_ROLE",
    "SYSTEM_PROMPT",
    "InvestigationInput",
    "InvestigationResult",
    "InvestigatorService",
]
