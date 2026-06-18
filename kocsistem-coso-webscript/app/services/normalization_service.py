"""Translate OBM payloads into the canonical internal record shape.

The normalizer is intentionally tolerant. The provided JSON file is a collection-policy
definition, not a guaranteed runtime sample, so we accept multiple envelope shapes and
fall back to prefix-based class inference when no explicit class is present. Unknown
metrics are preserved under ``extra_metrics`` rather than dropped, so future mapping
additions can re-process the audit archive without data loss.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


_CANDIDATE_RECORD_KEYS = ("records", "data", "items", "payload", "metrics_records")
_CANDIDATE_METRIC_KEYS = ("metrics", "metric_values", "values", "data")
_CLASS_KEYS = ("class_name", "class", "metric_class")
_DATASOURCE_KEYS = ("datasource", "data_source")
_REQUIRED_IDENTITY_FIELDS = ("node_fqdn", "node_short_name")


class NormalizationConfig:
    """In-memory view of the mapping JSON. Loaded once at startup."""

    def __init__(self, mapping: Dict[str, Any]):
        self.raw = mapping
        self.common_fields_map: Dict[str, str] = mapping.get("common_fields_map", {})
        self.class_table_map: Dict[str, str] = mapping.get("class_table_map", {})
        self.classes: Dict[str, Dict[str, Any]] = mapping.get("classes", {})
        self.metric_map: Dict[str, Dict[str, str]] = mapping.get("metric_map", {})
        self.class_prefix_map: Dict[str, str] = mapping.get("class_prefix_map", {})
        self.datasource_default: str = mapping.get("datasource", "SCOPE")

    @classmethod
    def from_path(cls, path: Path) -> "NormalizationConfig":
        with path.open("r", encoding="utf-8") as fp:
            return cls(json.load(fp))


class Normalizer:
    def __init__(self, config: NormalizationConfig):
        self._config = config

    # ---- envelope handling --------------------------------------------------

    def _iter_records(self, payload: Any) -> Iterable[Dict[str, Any]]:
        """Yield candidate record dicts from any of the supported envelope shapes."""
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    yield item
            return

        if isinstance(payload, dict):
            for key in _CANDIDATE_RECORD_KEYS:
                inner = payload.get(key)
                if isinstance(inner, list):
                    for item in inner:
                        if isinstance(item, dict):
                            yield item
                    return
                if isinstance(inner, dict):
                    yield inner
                    return
            yield payload

    # ---- metric extraction --------------------------------------------------

    def _extract_metric_pairs(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Pull metric KV pairs from common wrappers, or fall back to OBM-prefixed top-level keys."""
        for key in _CANDIDATE_METRIC_KEYS:
            value = record.get(key)
            if isinstance(value, dict):
                return value

        prefixes = tuple(self._config.class_prefix_map.keys()) or (
            "GBL_",
            "BYDSK_",
            "FS_",
            "BYCPU_",
            "BYNETIF_",
        )
        return {k: v for k, v in record.items() if isinstance(k, str) and k.startswith(prefixes)}

    # ---- class detection ---------------------------------------------------

    def _detect_class(self, record: Dict[str, Any], metric_pairs: Dict[str, Any]) -> str:
        for key in _CLASS_KEYS:
            value = record.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip().upper()

        # Prefix-based fallback. Counts metric keys per known prefix; majority wins.
        counts: Dict[str, int] = {}
        for metric_key in metric_pairs.keys():
            if not isinstance(metric_key, str):
                continue
            for prefix, class_name in self._config.class_prefix_map.items():
                if metric_key.startswith(prefix):
                    counts[class_name] = counts.get(class_name, 0) + 1
                    break
        if counts:
            return max(counts.items(), key=lambda kv: kv[1])[0]

        return "UNKNOWN"

    # ---- common fields ------------------------------------------------------

    def _extract_common(self, record: Dict[str, Any]) -> Dict[str, Any]:
        common: Dict[str, Any] = {}
        for raw_key, normalized_key in self._config.common_fields_map.items():
            if raw_key in record:
                common[normalized_key] = record[raw_key]
        return common

    # ---- target table -------------------------------------------------------

    def _resolve_target_table(self, class_name: str) -> Optional[str]:
        class_def = self._config.classes.get(class_name)
        if class_def:
            target_key = class_def.get("target_key")
            if target_key:
                return self._config.class_table_map.get(target_key)
        return None

    # ---- metric mapping -----------------------------------------------------

    def _map_metrics(
        self, class_name: str, metric_pairs: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        class_map = self._config.metric_map.get(class_name, {})
        mapped: Dict[str, Any] = {}
        extra: Dict[str, Any] = {}
        for raw_key, value in metric_pairs.items():
            normalized_name = class_map.get(raw_key)
            if normalized_name:
                mapped[normalized_name] = value
            else:
                extra[raw_key] = value
        return mapped, extra

    # ---- public entrypoint --------------------------------------------------

    def normalize(
        self,
        payload: Any,
        request_id: str,
        raw_payload_ref: Optional[str] = None,
        received_at: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        received_at = received_at or datetime.now(timezone.utc)
        received_iso = received_at.isoformat().replace("+00:00", "Z")

        results: List[Dict[str, Any]] = []
        for record in self._iter_records(payload):
            metric_pairs = self._extract_metric_pairs(record)
            class_name = self._detect_class(record, metric_pairs)
            common = self._extract_common(record)
            mapped, extra = self._map_metrics(class_name, metric_pairs)
            target_table = self._resolve_target_table(class_name)

            datasource = next(
                (record[k] for k in _DATASOURCE_KEYS if k in record),
                self._config.datasource_default,
            )

            results.append(
                {
                    "request_id": request_id,
                    "received_at": received_iso,
                    "source": "obm",
                    "datasource": datasource,
                    "class_name": class_name,
                    "target_table": target_table,
                    "common": common,
                    "metrics": mapped,
                    "extra_metrics": extra,
                    "raw_payload_ref": raw_payload_ref,
                }
            )
        return results


def record_has_strong_identity(record: Dict[str, Any]) -> bool:
    """Strict-mode gate: at least one strong identity field must be present."""
    common = record.get("common", {})
    return any(common.get(field) for field in _REQUIRED_IDENTITY_FIELDS)
