"""Pydantic schemas for inbound OBM payloads and outbound responses.

These schemas are deliberately permissive. The OBM mapping file is a
collection-policy definition, not a guaranteed runtime POST sample, so the parser must accept
multiple envelope shapes (single record, list, or wrapper). Strict validation of
identity fields is configurable via ``STRICT_VALIDATION``.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class IngestionAcceptedResponse(BaseModel):
    status: str = "ok"
    request_id: str
    accepted: bool = True
    record_count: int
    raw_payload_ref: Optional[str] = None
    quarantined: bool = False


class IngestionErrorResponse(BaseModel):
    status: str = "error"
    request_id: str
    accepted: bool = False
    error: str
    details: Optional[List[Any]] = None


class HealthResponse(BaseModel):
    status: str = "ok"
    service: str
    version: str


class NormalizedRecord(BaseModel):
    """Internal normalized representation. Not used as a wire schema, but useful for tests."""

    model_config = ConfigDict(extra="allow")

    request_id: str
    received_at: str
    source: str = "obm"
    datasource: Optional[str] = None
    class_name: str
    target_table: Optional[str] = None
    common: Dict[str, Any] = Field(default_factory=dict)
    metrics: Dict[str, Any] = Field(default_factory=dict)
    extra_metrics: Dict[str, Any] = Field(default_factory=dict)
    raw_payload_ref: Optional[str] = None
