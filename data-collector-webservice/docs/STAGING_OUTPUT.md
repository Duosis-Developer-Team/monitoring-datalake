# Staging output

How an inbound OBM payload becomes PostgreSQL rows, with **no writer changes**.

## Pipeline

1. **Receive** — `POST /api/v1/obm/metrics` (mTLS). The raw body is archived to
   `$RAW_PAYLOAD_DIR/YYYY/MM/DD/<request_id>.json` before anything else.
2. **Normalize** (`app/sources/obm_agent/normalization.py`) — each record is
   mapped to the canonical shape: `{common, metrics, extra_metrics, class_name,
   target_table, ...}`. Unknown metrics are preserved in `extra_metrics`.
3. **Stage** (`app/sources/obm_agent/staging.py`) — records are grouped by
   `target_table` and flattened into rows, then written as `{meta, data}` files.
4. **Load** — the Airflow `generic_postgres_writer` DAG scans `pending/`, reads
   `meta`, writes `data`, and deletes the file. See
   [../../docs/STAGING_FORMAT.md](../../docs/STAGING_FORMAT.md).

## File naming & atomicity

```
{STAGING_FOLDER_PATH}/pending/obm_agent_<table>_<request_id>.json
```

`request_id` is a UUID, so names never collide across replicas. Each file is
written to a hidden `.<name>.<uuid>.tmp` and `os.replace`d into place — atomic on
the shared NFS export, so the writer (which globs `*.json`) never reads a partial
file.

## Row shaping

Each row is the flattened union of:

- all `common` identity fields (e.g. `node_fqdn`, `node_short_name`,
  `timestamp_utc_s`),
- the mapped metric columns for the record's class,
- metadata: `class_name`, `datasource`, `source`, `received_at`, `request_id`,
  `raw_payload_ref`,
- `extra_metrics` (a JSONB column holding everything unmapped).

To keep the writer-inferred column set stable across records and requests, every
row is emitted with the **full known column superset** for its table (missing
values are `NULL`). The superset is derived once from the mapping file.

## Target tables & `meta`

| Class(es) | Table |
|---|---|
| GLOBAL, CONFIGURATION | `opsb_agent_node` |
| DISK | `opsb_agent_disk` |
| FILESYSTEM | `opsb_agent_filesys` |
| CPU | `opsb_agent_cpu` |
| NETIF | `opsb_agent_netif` |

`meta` per file:

```json
{
  "table": "opsb_agent_node",
  "method": "copy",
  "json_columns": ["extra_metrics"],
  "column_types": {"timestamp_utc_s": "BIGINT", "extra_metrics": "JSONB"},
  "add_updated_at": false,
  "source": "obm_agent"
}
```

`method: copy` with no `conflict_target` is the append-only, fastest path —
each collection cycle is a new time-series row, and it is fully replica-safe.

## Typed tables (recommended)

The writer auto-creates these tables as TEXT if absent. Apply
[`../../sql/05_obm_agent.sql`](../../sql/05_obm_agent.sql) first to get numeric
column types (better for Grafana) and query indexes. The column names/sets there
must stay in sync with the mapping-derived superset; if you add metric mappings,
update the DDL.

## Adding a new source

Add `app/sources/<name>/` with its own `normalization.py`, `staging.py`,
`routes.py`, and mapping, then mount its router and wire its sink in
`app/main.py`. The core services, storage sinks, and the Airflow writer are
untouched.
