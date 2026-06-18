"""File-based storage for raw, normalized, and quarantined payloads.

The first production version intentionally avoids a database. Raw payloads are dated
JSON files, normalized output is append-only JSONL, and quarantined records sit in a
parallel dated tree. A ``NormalizedSink`` protocol leaves room for a DB sink later.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Protocol

from ..core.config import Settings


class NormalizedSink(Protocol):
    def write(self, records: List[Dict[str, Any]]) -> None: ...


def _date_partition(now: datetime) -> tuple[str, str, str]:
    return f"{now.year:04d}", f"{now.month:02d}", f"{now.day:02d}"


def archive_raw_payload(
    settings: Settings, request_id: str, body: bytes, now: datetime | None = None
) -> str:
    """Store the raw request body verbatim. Returns a stable relative path used for audit refs."""
    now = now or datetime.now(timezone.utc)
    y, m, d = _date_partition(now)
    target_dir = settings.raw_payload_dir / y / m / d
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / f"{request_id}.json"
    target_path.write_bytes(body)
    return f"raw/{y}/{m}/{d}/{request_id}.json"


def append_normalized_records(
    settings: Settings, records: List[Dict[str, Any]]
) -> None:
    """Append one JSON line per record to the normalized JSONL log."""
    if not records:
        return
    settings.normalized_jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    with settings.normalized_jsonl_path.open("a", encoding="utf-8") as fp:
        for record in records:
            fp.write(json.dumps(record, ensure_ascii=False, default=str))
            fp.write("\n")


def write_quarantine(
    settings: Settings,
    request_id: str,
    reason: str,
    body: bytes,
    metadata: Dict[str, Any] | None = None,
    now: datetime | None = None,
) -> str:
    """Persist a payload whose normalization failed or whose identity is too weak.

    The quarantine file holds the parsed metadata alongside the raw body so an operator
    can replay/repair without going back to the audit archive.
    """
    now = now or datetime.now(timezone.utc)
    y, m, d = _date_partition(now)
    target_dir = settings.quarantine_dir / y / m / d
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / f"{request_id}.json"
    document = {
        "request_id": request_id,
        "reason": reason,
        "received_at": now.isoformat().replace("+00:00", "Z"),
        "metadata": metadata or {},
        "raw_body_b64": body.decode("utf-8", errors="replace"),
    }
    target_path.write_text(
        json.dumps(document, ensure_ascii=False, default=str), encoding="utf-8"
    )
    return f"quarantine/{y}/{m}/{d}/{request_id}.json"


class JsonlSink:
    """Default ``NormalizedSink`` implementation; appends to the configured JSONL file."""

    def __init__(self, settings: Settings):
        self._settings = settings

    def write(self, records: List[Dict[str, Any]]) -> None:
        append_normalized_records(self._settings, records)
