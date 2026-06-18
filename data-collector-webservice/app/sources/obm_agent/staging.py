"""Shape normalized OBM records into Airflow staging files.

Output is the exact ``{meta, data}`` contract the ``generic_postgres_writer``
DAG consumes (see ``docs/STAGING_FORMAT.md`` at the repo root). One file is
produced per target table per request; the ``StagingSink`` writes each atomically
into the NFS ``pending/`` dir.

Schema stability
----------------
The writer infers a table's column list from the **first** record in a file and
applies it to every row. A single request can yield several records for the same
table with different metric keys (e.g. GLOBAL and CONFIGURATION both land in
``opsb_agent_node``). To keep the column set stable across records *and across
requests*, every row is emitted with the full known column superset for its table
(missing values are ``NULL``), derived once from the mapping file. ``sql/`` ships
explicit typed DDL for these tables; if absent, the writer auto-creates them as
TEXT.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Tuple

from . import MAPPING_PATH, SOURCE_NAME

# Append-only time-series: each (node, timestamp) reading is a new row. No
# conflict_target → fastest direct COPY path, fully safe under many replicas.
# Dedup/typing can be tightened later via explicit DDL + a conflict_target.
_DEFAULT_METHOD = "copy"

# Metadata columns added to every staged row, on top of common + metric columns.
_META_COLUMNS = (
    "class_name",
    "datasource",
    "source",
    "received_at",
    "request_id",
    "raw_payload_ref",
)
_JSON_COLUMNS = ["extra_metrics"]
_COLUMN_TYPES = {
    "timestamp_utc_s": "BIGINT",
    "extra_metrics": "JSONB",
}


def _load_mapping() -> Dict[str, Any]:
    with MAPPING_PATH.open("r", encoding="utf-8") as fp:
        return json.load(fp)


def _build_table_schemas(mapping: Dict[str, Any]) -> Dict[str, List[str]]:
    """Compute the full ordered column superset for each target table.

    columns = all common identity fields + union of mapped metric names for every
    class that targets the table + fixed meta columns + extra_metrics.
    """
    classes = mapping.get("classes", {})
    class_table_map = mapping.get("class_table_map", {})
    metric_map = mapping.get("metric_map", {})
    common_cols = list(dict.fromkeys(mapping.get("common_fields_map", {}).values()))

    table_metric_cols: Dict[str, List[str]] = {}
    for class_name, class_def in classes.items():
        table = class_table_map.get(class_def.get("target_key"))
        if not table:
            continue
        cols = table_metric_cols.setdefault(table, [])
        for mapped_name in metric_map.get(class_name, {}).values():
            if mapped_name not in cols:
                cols.append(mapped_name)

    schemas: Dict[str, List[str]] = {}
    for table, metric_cols in table_metric_cols.items():
        ordered = list(common_cols)
        ordered += [c for c in metric_cols if c not in ordered]
        ordered += [c for c in _META_COLUMNS if c not in ordered]
        ordered += [c for c in _JSON_COLUMNS if c not in ordered]
        schemas[table] = ordered
    return schemas


_MAPPING = _load_mapping()
TABLE_SCHEMAS: Dict[str, List[str]] = _build_table_schemas(_MAPPING)


def _row_for(record: Dict[str, Any], columns: List[str]) -> Dict[str, Any]:
    flat: Dict[str, Any] = {}
    flat.update(record.get("common", {}) or {})
    flat.update(record.get("metrics", {}) or {})
    flat["class_name"] = record.get("class_name")
    flat["datasource"] = record.get("datasource")
    flat["source"] = record.get("source", SOURCE_NAME)
    flat["received_at"] = record.get("received_at")
    flat["request_id"] = record.get("request_id")
    flat["raw_payload_ref"] = record.get("raw_payload_ref")
    flat["extra_metrics"] = record.get("extra_metrics") or {}
    # Project onto the stable column superset (missing → None).
    return {col: flat.get(col) for col in columns}


def _meta_for(table: str, columns: List[str]) -> Dict[str, Any]:
    return {
        "table": table,
        "method": _DEFAULT_METHOD,
        "json_columns": [c for c in _JSON_COLUMNS if c in columns],
        "column_types": {k: v for k, v in _COLUMN_TYPES.items() if k in columns},
        "add_updated_at": False,
        "source": SOURCE_NAME,
    }


def build_staging_payloads(
    records: List[Dict[str, Any]],
) -> List[Tuple[str, Dict[str, Any]]]:
    """Group normalized records by target table and build one staging file each.

    Records without a resolved ``target_table`` (unknown class) are skipped here —
    they were still archived raw and, if weak, quarantined upstream.
    """
    if not records:
        return []

    request_id = str(records[0].get("request_id", "unknown"))

    by_table: Dict[str, List[Dict[str, Any]]] = {}
    for record in records:
        table = record.get("target_table")
        if not table:
            continue
        by_table.setdefault(table, []).append(record)

    files: List[Tuple[str, Dict[str, Any]]] = []
    for table, table_records in by_table.items():
        # Use the mapping-derived schema; fall back to the union of keys actually
        # present if this table is not in the mapping (e.g. a brand-new class).
        columns = TABLE_SCHEMAS.get(table) or _discover_columns(table_records)
        rows = [_row_for(rec, columns) for rec in table_records]
        payload = {"meta": _meta_for(table, columns), "data": rows}
        file_stem = f"{SOURCE_NAME}_{table}_{request_id}"
        files.append((file_stem, payload))
    return files


def _discover_columns(records: List[Dict[str, Any]]) -> List[str]:
    """Union of common + metric keys across records, plus the fixed meta columns."""
    cols: List[str] = []
    for rec in records:
        for key in list((rec.get("common") or {}).keys()) + list((rec.get("metrics") or {}).keys()):
            if key not in cols:
                cols.append(key)
    cols += [c for c in _META_COLUMNS if c not in cols]
    cols += [c for c in _JSON_COLUMNS if c not in cols]
    return cols
