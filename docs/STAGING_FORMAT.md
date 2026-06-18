# Staging File Format

Collectors write JSON files into `{staging_folder_path}/pending/`. Each file is
self-describing: it carries both the data and the metadata the writer needs to
load it. The writer reads `meta`, writes `data`, and deletes the file on
success.

## File naming

```
<dag_id>_<run_id>.json
```

`run_id` is sanitised (`:`, `+`, `/` replaced with `_`) so it is filesystem
safe.

## Top-level structure

```json
{
  "meta": { ... },
  "data": [ { ... }, { ... } ]
}
```

## `meta` fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `table` | string | yes | Target table name |
| `method` | string | yes | `insert`, `upsert`, or `copy` |
| `conflict_target` | string[] | for `upsert`; optional for `copy` | Columns forming the conflict key |
| `column_types` | object | no | Explicit column→SQL-type overrides, e.g. `{"clock": "BIGINT"}` |
| `json_columns` | string[] | no | Columns serialized as JSONB |
| `add_updated_at` | bool | no (default `true`) | Whether to add an `updated_at` column on table creation |
| `source` | string | no | Originating DAG id, for logging/tracing |

Additional keys (e.g. `window_from`, `window_till`) may be present for
traceability; the writer ignores unknown keys.

## `data` field

A JSON array of flat objects. Every object should share the same keys; the
writer infers the column list from the first record.

- Keys map directly to column names.
- Columns listed in `json_columns` are serialized to JSONB.
- Columns ending in `_at` default to `TIMESTAMPTZ` (unless overridden in
  `column_types`).
- All other columns default to `TEXT` (unless overridden).

## Method behaviour

| `method` | `conflict_target` | Behaviour |
|----------|-------------------|-----------|
| `insert` | — | Plain batched INSERT; errors on conflict |
| `upsert` | required | INSERT ... ON CONFLICT DO UPDATE |
| `copy` | absent | Direct COPY, append-only, fastest |
| `copy` | present | COPY into UNLOGGED staging, then INSERT ... SELECT ... ON CONFLICT DO NOTHING |

## Example: inventory (upsert)

```json
{
  "meta": {
    "table": "zabbix_inventory",
    "method": "upsert",
    "conflict_target": ["hostid"],
    "json_columns": ["secondary_ips", "host_groups", "templates",
                     "interfaces", "macros", "tags"],
    "source": "zabbix_data_collector_v2"
  },
  "data": [
    {
      "hostid": "10668",
      "name": "web-server-01",
      "status": "Enabled",
      "primary_ip": "<ip>",
      "secondary_ips": ["<ip>"],
      "host_groups": ["Linux Servers"],
      "interfaces": [ { "...": "..." } ],
      "macros": [ { "...": "..." } ],
      "tags": [ { "...": "..." } ],
      "collected_at": "2026-05-15T12:00:00Z"
    }
  ]
}
```

## Example: history (copy + staging)

```json
{
  "meta": {
    "table": "zabbix_history",
    "method": "copy",
    "conflict_target": ["itemid", "clock", "ns"],
    "column_types": {
      "itemid": "BIGINT", "hostid": "BIGINT", "clock": "BIGINT",
      "ns": "INTEGER", "value": "TEXT", "value_type": "SMALLINT"
    },
    "add_updated_at": false,
    "json_columns": [],
    "source": "zabbix_history_collector",
    "window_from": 1781259240,
    "window_till": 1781259300
  },
  "data": [
    {
      "itemid": "411602", "hostid": "27713",
      "clock": "1781259243", "ns": "534138498",
      "value": "0", "value_type": 3
    }
  ]
}
```

## Error handling

On failure, the writer does **not** delete the file. Instead it annotates it
with `_error` and `_error_at` keys and leaves it in `pending/`, so the next
writer run retries it. Inspect these keys to debug a stuck file.
