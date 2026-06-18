"""File-based storage for raw, normalized, quarantined, and staged payloads.

Three audit/output paths, all file-based so the service stays stateless and
horizontally scalable:

* ``archive_raw_payload`` / ``write_quarantine`` — one file per request, always
  safe across replicas.
* ``StagingSink`` — writes ``{meta, data}`` files into the NFS ``pending/`` dir
  that the Airflow ``generic_postgres_writer`` DAG scans. One file per request
  (per target table), written atomically (temp + ``os.replace``) so the writer
  never reads a half-written file. This is the production output path.
* ``JsonlSink`` — appends to a single local JSONL file. Dev / single-node only;
  a shared JSONL file is unsafe under multiple replicas.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Protocol, Tuple

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


def write_pending_file_atomic(
    pending_dir: Path, file_stem: str, payload: Dict[str, Any]
) -> Path:
    """Atomically write one ``{meta, data}`` staging file into the pending dir.

    Writes to a hidden ``.<stem>.<uuid>.tmp`` in the same directory, then
    ``os.replace`` to ``<stem>.json``. ``os.replace`` is atomic within a single
    filesystem (the shared NFS export), so the Airflow writer — which globs
    ``*.json`` — never observes a partial file, and the ``.tmp`` is never matched.
    """
    pending_dir.mkdir(parents=True, exist_ok=True)
    final_path = pending_dir / f"{file_stem}.json"
    tmp_path = pending_dir / f".{file_stem}.{uuid.uuid4().hex}.tmp"
    with tmp_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False)
    os.replace(tmp_path, final_path)
    return final_path


# Builder turns normalized records into a list of (file_stem, {meta,data}) tuples.
StagingBuilder = Callable[[List[Dict[str, Any]]], List[Tuple[str, Dict[str, Any]]]]


class StagingSink:
    """``NormalizedSink`` that drops staging files into the NFS pending/ dir.

    The source-specific shaping (grouping by target table, building ``meta``) is
    injected as ``builder`` so this sink stays source-agnostic.
    """

    def __init__(self, settings: Settings, builder: StagingBuilder):
        self._settings = settings
        self._builder = builder

    def write(self, records: List[Dict[str, Any]]) -> None:
        if not records:
            return
        for file_stem, payload in self._builder(records):
            if not payload.get("data"):
                continue
            write_pending_file_atomic(self._settings.pending_dir, file_stem, payload)


class JsonlSink:
    """``NormalizedSink`` for local/dev use; appends to the configured JSONL file."""

    def __init__(self, settings: Settings):
        self._settings = settings

    def write(self, records: List[Dict[str, Any]]) -> None:
        append_normalized_records(self._settings, records)


class CompositeSink:
    """Fan a single ``write`` out to several sinks (e.g. staging + jsonl)."""

    def __init__(self, sinks: List[NormalizedSink]):
        self._sinks = sinks

    def write(self, records: List[Dict[str, Any]]) -> None:
        for sink in self._sinks:
            sink.write(records)
