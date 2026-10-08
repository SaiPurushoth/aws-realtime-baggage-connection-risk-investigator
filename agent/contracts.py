"""Validated invocation and response contracts for the investigator."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class InvestigationInput(BaseModel):
    """Generic risk incident passed from the dispatcher to the investigator."""

    model_config = ConfigDict(extra="forbid", strict=True)

    incident_id: str = Field(min_length=1, max_length=256)
    bag_tag_id: str = Field(min_length=1, max_length=128)
    outbound_flight_id: str = Field(min_length=1, max_length=64)
    airport_code: str = Field(min_length=1, max_length=16)
    transfer_zone: str = Field(min_length=1, max_length=64)
    last_scan_type: str = Field(min_length=1, max_length=128)
    last_scan_location: str = Field(min_length=1, max_length=256)
    seconds_to_departure: int


class InvestigationResult(BaseModel):
    """Strict structured response returned by the Strands agent."""

    model_config = ConfigDict(extra="forbid", strict=True)

    incident_id: str = Field(min_length=1, max_length=256)
    bag_tag_id: str = Field(min_length=1, max_length=128)
    classification: str = Field(min_length=1, max_length=256)
    root_cause: str = Field(min_length=1, max_length=4096)
    scope: Literal["ISOLATED", "FLIGHT_LEVEL", "ZONE_LEVEL", "AIRPORT_LEVEL"]
    operational_priority: Literal["LOW", "MEDIUM", "HIGH", "CRITICAL"]
    evidence: list[str] = Field(min_length=1, max_length=20)
    recommended_action: str = Field(min_length=1, max_length=8192)
    reason_action_is_time_sensitive: str = Field(min_length=1, max_length=4096)

    @field_validator("classification")
    @classmethod
    def classification_must_explain_cause(cls, value: str) -> str:
        if value.strip().upper() == "BAG_CONNECTION_RISK":
            raise ValueError(
                "classification must explain the likely cause, not repeat the alert"
            )
        return value

    @field_validator("reason_action_is_time_sensitive")
    @classmethod
    def action_must_be_time_sensitive(cls, value: str) -> str:
        normalized = value.casefold()
        if "not time-sensitive" in normalized or "not time sensitive" in normalized:
            raise ValueError(
                "the recommendation must acknowledge the active departure deadline"
            )
        return value

    @field_validator("recommended_action")
    @classmethod
    def action_must_be_advisory(cls, value: str) -> str:
        if "recommend" not in value.casefold():
            raise ValueError(
                "recommended_action must explicitly be phrased as a recommendation"
            )
        return value

    @model_validator(mode="after")
    def isolated_classification_must_have_isolated_scope(self) -> "InvestigationResult":
        if "ISOLATED" in self.classification.upper() and self.scope != "ISOLATED":
            raise ValueError(
                "an isolated classification must use ISOLATED scope"
            )
        return self
