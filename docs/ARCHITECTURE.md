# Architecture

## Design principle: collector/writer decoupling

The pipeline is built around one core idea: **collectors and the writer never
talk to each other directly.** They communicate only through self-describing
files on a shared NFS directory.

A collector's only responsibilities are:
1. Pull data from a source (Zabbix).
2. Shape it into a list of records.
3. Write a JSON file containing both the data and the metadata describing how
   it should be written to PostgreSQL.

The writer's only responsibilities are:
1. Scan the staging directory.
2. For each file, read the metadata and write the data accordingly.
3. Delete the file on success.

Because the file is self-describing, the writer is fully generic. Adding a new
data source never requires touching the writer.

## Data flow

```
Zabbix API
    │
    │  (collector pulls + shapes data)
    ▼
{staging_folder_path}/pending/<dag_id>_<run_id>.json
    │
    │  (writer polls every 5 min)
    ▼
PostgreSQL  ──▶  file deleted on success
                 file annotated with _error on failure (kept for retry)
```

## Staging file format

Each pending file has two top-level keys: `meta` and `data`. See
[`STAGING_FORMAT.md`](STAGING_FORMAT.md) for the full schema. In short:

```json
{
  "meta": {
    "table": "target_table",
    "method": "upsert",
    "conflict_target": ["pk_col"],
    "json_columns": ["col_a"],
    "column_types": {"clock": "BIGINT"},
    "add_updated_at": true,
    "source": "collector_dag_id"
  },
  "data": [ { "...": "..." } ]
}
```

## Write methods

The writer supports three methods, chosen per-file via `meta.method`:

### `insert`
Plain batched `INSERT`. Fails on primary-key conflict. Use for tables where
duplicates cannot occur.

### `upsert`
`INSERT ... ON CONFLICT (conflict_target) DO UPDATE SET ...`. Each row updates
the existing row on conflict. Used for the inventory table, where a host's
current state should overwrite its previous state.

### `copy`
PostgreSQL's `COPY` protocol — the fastest bulk-load path. Two sub-modes:

- **No `conflict_target`** → direct `COPY` into the target table. Append-only,
  no duplicate handling. Fastest.
- **With `conflict_target`** → **staging pattern**:
  1. Create an `UNLOGGED` staging table from the target's structure
     (`LIKE target INCLUDING DEFAULTS EXCLUDING CONSTRAINTS EXCLUDING INDEXES`).
  2. `COPY` raw data into staging (no constraints, maximum speed).
  3. `INSERT INTO target SELECT FROM staging ON CONFLICT (...) DO NOTHING`.
  4. Drop the staging table.

  This keeps COPY's speed while still preventing duplicates — the correct
  choice for high-volume time-series data.

## Inventory collector (`zabbix_data_collector_v2`)

Pulls host inventory in three Zabbix API calls to avoid an expensive single
join:
1. Base host data + interfaces + groups + templates (paginated).
2. Macros for all host IDs in one call.
3. Tags for all host IDs in one call.

Results are merged in memory. Interfaces are split into a `primary_ip`
(interface with `main=1`) and a `secondary_ips` list. Secret macros (type 1)
are masked. Written with `method: upsert` keyed on `hostid`.

### Pagination safety

Before paging, the collector queries the total host count via
`countOutput: true` and loops `while offset < total`, advancing by the actual
returned batch size. This prevents the infinite-loop / runaway behaviour that
a naive `while True` loop can exhibit when multiple DAG runs overlap.
`max_active_runs=1` further guarantees a single active run.

## History collector (`zabbix_history_collector`)

Collects time-series values into a single vertical (long-format) table.

### value_type handling

Zabbix's `history.get` cannot return all value types in one call — the
`history` parameter selects a single type (0=float, 1=char, 2=log,
3=unsigned, 4=text). The collector first builds an item→value_type map via
`item.get`, then issues one `history.get` per value type. All values land in a
single `value TEXT` column, tagged with a `value_type SMALLINT` column.

### Gap prevention (overlap) and duplicate prevention

Two independent problems, solved together:

- **Window gaps** (a missed or delayed run) are avoided by deriving the window
  from Airflow's `data_interval`. With catchup enabled, missed intervals are
  backfilled automatically.
- **Boundary loss** (a record sitting exactly on a window edge) is avoided by
  deliberately overlapping windows: each run reads
  `[data_interval_start - overlap_seconds, data_interval_end]`.

The overlap inevitably produces duplicate rows across adjacent runs. These are
absorbed by the writer's `ON CONFLICT (itemid, clock, ns) DO NOTHING`. The
primary key `(itemid, clock, ns)` is unique because a single item never emits
two values at the same nanosecond. Items with very different intervals (e.g.
1-minute vs 8-hour) are handled transparently, since each value carries its
own `clock`.

## Database considerations

The history table starts as a **plain PostgreSQL table** so the data shape can
be analysed before committing to a partitioning or hypertable strategy. Once
the value_type distribution and row volume are understood, it can be converted
to a TimescaleDB hypertable (`create_hypertable`) or native range partitions
on `clock` without data loss.

For high-volume writes, `upsert` row-by-row is avoided in favour of the
COPY + staging pattern described above.

## Airflow version compatibility

All DAGs use fallback imports so they parse on both Airflow 3.x and 2.x:

```python
try:
    from airflow.sdk import DAG, Variable, task        # Airflow 3.x
except ImportError:
    from airflow import DAG                            # Airflow 2.x
    from airflow.decorators import task
    from airflow.models import Variable
```

A `_var()` helper wraps `Variable.get()` to handle the `default=` (3.x) vs
`default_var=` (2.x) keyword difference.

### Helper-function placement

In Airflow 3.x, only task instances may live inside the `with DAG() as dag:`
block. Helper functions (`_ensure_table`, `_upsert`, `_copy`, etc.) must be
defined **outside** the DAG block, or the parser silently rejects the DAG and
it never appears in the UI.
