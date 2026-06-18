"""Coordinates the full receive-archive-normalize-persist flow.

Failures during normalization never block ingestion: the raw payload is always
archived first, and only the normalized output (or quarantine writes) can fail
softly, so mapping bugs never lose data — the audit archive can be replayed.

The service is source-agnostic: the ``normalizer`` and the ``identity_check``
predicate are injected by whichever ingest source mounted the route.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Protocol

from ..core.config import Settings
from ..core.logging import get_logger
from .storage_service import (
    NormalizedSink,
    archive_raw_payload,
    write_quarantine,
)


logger = get_logger(__name__)


class Normalizer(Protocol):
    def normalize(
        self,
        payload: Any,
        request_id: str,
        raw_payload_ref: Optional[str] = None,
        received_at: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]: ...


# A source-provided predicate deciding whether a normalized record carries a
# strong enough identity to persist (vs. quarantine). Defaults to "always
# strong" when a source does not supply one.
IdentityCheck = Callable[[Dict[str, Any]], bool]


@dataclass
class IngestionResult:
    request_id: str
    accepted: bool
    record_count: int
    raw_payload_ref: Optional[str]
    quarantined: bool
    quarantine_ref: Optional[str] = None
    normalization_error: Optional[str] = None
    strict_validation_failed: bool = False
    validation_errors: Optional[List[Dict[str, Any]]] = None


class IngestionService:
    def __init__(
        self,
        settings: Settings,
        normalizer: Normalizer,
        sink: NormalizedSink,
        identity_check: Optional[IdentityCheck] = None,
    ):
        self._settings = settings
        self._normalizer = normalizer
        self._sink = sink
        self._identity_check = identity_check or (lambda record: True)

    def ingest(
        self,
        request_id: str,
        raw_body: bytes,
        parsed_payload: Any,
    ) -> IngestionResult:
        now = datetime.now(timezone.utc)

        raw_ref = archive_raw_payload(self._settings, request_id, raw_body, now=now)

        try:
            records = self._normalizer.normalize(
                payload=parsed_payload,
                request_id=request_id,
                raw_payload_ref=raw_ref,
                received_at=now,
            )
        except Exception as exc:  # noqa: BLE001 — payload shape is unknown by design
            logger.exception(
                "normalization_failed",
                extra={"request_id": request_id, "raw_payload_ref": raw_ref},
            )
            quarantine_ref = write_quarantine(
                self._settings,
                request_id,
                reason="normalization_exception",
                body=raw_body,
                metadata={"error": str(exc), "raw_payload_ref": raw_ref},
                now=now,
            )
            return IngestionResult(
                request_id=request_id,
                accepted=True,
                record_count=0,
                raw_payload_ref=raw_ref,
                quarantined=True,
                quarantine_ref=quarantine_ref,
                normalization_error=str(exc),
            )

        weak_records = [r for r in records if not self._identity_check(r)]

        if weak_records and self._settings.strict_validation:
            quarantine_ref = write_quarantine(
                self._settings,
                request_id,
                reason="strict_validation_failed_missing_identity",
                body=raw_body,
                metadata={
                    "raw_payload_ref": raw_ref,
                    "weak_record_count": len(weak_records),
                    "total_records": len(records),
                },
                now=now,
            )
            return IngestionResult(
                request_id=request_id,
                accepted=False,
                record_count=len(records),
                raw_payload_ref=raw_ref,
                quarantined=True,
                quarantine_ref=quarantine_ref,
                strict_validation_failed=True,
                validation_errors=[
                    {
                        "loc": ["common"],
                        "msg": "missing required identity field (node_fqdn or node_short_name)",
                        "type": "value_error.missing",
                    }
                ],
            )

        if weak_records and not self._settings.strict_validation:
            write_quarantine(
                self._settings,
                request_id,
                reason="weak_identity_non_strict",
                body=raw_body,
                metadata={
                    "raw_payload_ref": raw_ref,
                    "weak_record_count": len(weak_records),
                    "total_records": len(records),
                },
                now=now,
            )

        try:
            self._sink.write(records)
        except Exception as exc:  # noqa: BLE001 — fail soft so raw payload is still saved
            logger.exception(
                "normalized_sink_write_failed",
                extra={"request_id": request_id, "raw_payload_ref": raw_ref},
            )
            quarantine_ref = write_quarantine(
                self._settings,
                request_id,
                reason="sink_write_failed",
                body=raw_body,
                metadata={"error": str(exc), "raw_payload_ref": raw_ref},
                now=now,
            )
            return IngestionResult(
                request_id=request_id,
                accepted=True,
                record_count=len(records),
                raw_payload_ref=raw_ref,
                quarantined=True,
                quarantine_ref=quarantine_ref,
                normalization_error=str(exc),
            )

        return IngestionResult(
            request_id=request_id,
            accepted=True,
            record_count=len(records),
            raw_payload_ref=raw_ref,
            quarantined=bool(weak_records),
        )
