# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

A monitoring-to-datalake pipeline. It collects metrics and inventory from
infrastructure monitoring sources (Zabbix 7.x, OpenText OBM agents) and lands
them in a PostgreSQL datalake using Apache Airflow. The repo holds **two
subsystems**. They share no code, but they converge on the same NFS
`pending/` directory and the same PostgreSQL writer:

1. **`dags/` + `sql/` + `airflow/`** — the Airflow pipeline that pulls from the
   Zabbix API (and now also loads whatever the webservice drops in `pending/`).
2. **`data-collector-webservice/`** — a standalone, source-agnostic FastAPI
   service that *receives* pushed metric payloads over mTLS HTTPS, audits them,
   normalizes them, and writes the same `{meta, data}` staging files into the
   shared NFS `pending/` dir for the writer DAG to load. Its first ingest source
   is `obm_agent` (OpenText OBM); future sources plug in under
   `app/sources/<name>/`. It is built to run as **multiple replicas** on the
   Airflow Kubernetes cluster.

Most prose docs are under `docs/` (pipeline) and
`data-collector-webservice/docs/` (webservice). Note: code comments and DAG
docstrings are in **Turkish**; match that language when editing them. Do **not**
reintroduce customer names (e.g. "kocsistem"/"coso") anywhere — the webservice
was deliberately de-branded to `data-collector-webservice`.

## Subsystem 1: Airflow Zabbix → PostgreSQL pipeline

### The core design: collector/writer decoupling via self-describing files

Collectors and the writer **never call each other**. They communicate only
through JSON files dropped in `{staging_folder_path}/pending/`. This is the most
important thing to understand before changing anything:

- A **collector DAG** pulls from Zabbix, shapes records, and writes one
  self-describing JSON file (`{meta, data}`) where `meta` declares the target
  table, write method, conflict key, column types, etc. Collectors know nothing
  about PostgreSQL.
- The single **`generic_postgres_writer` DAG** (runs every 5 min) scans
  `pending/`, reads each file's `meta`, writes `data` accordingly, then deletes
  the file. On failure it does **not** delete — it annotates the file with
  `_error`/`_error_at` and leaves it for the next run to retry. The writer knows
  nothing about Zabbix.

**Consequence: to add a new data source, write a new collector that emits a
correctly-formatted file. Never modify the writer for a new source.** The full
file schema is in [docs/STAGING_FORMAT.md](docs/STAGING_FORMAT.md).

### Write methods (chosen per-file via `meta.method`)

- `insert` — batched INSERT, errors on conflict.
- `upsert` — INSERT ... ON CONFLICT DO UPDATE; requires `conflict_target`. Used
  for inventory (current host state overwrites previous).
- `copy` — fastest. With no `conflict_target`: direct append-only COPY. With a
  `conflict_target`: COPY into an `UNLOGGED` staging table, then
  `INSERT ... SELECT ... ON CONFLICT DO NOTHING`, then drop staging. This is the
  high-volume time-series path (used for history).

The writer **auto-creates target tables** from the first record's keys.
Column types: `meta.column_types` override > `json_columns`→JSONB >
`*_at`→TIMESTAMPTZ > everything else TEXT. The `sql/` files are the canonical
schema/index/grant reference, but the writer will create tables itself if absent.

### The collectors

- **`zabbix_data_collector_v2`** (`@hourly`) — host inventory. Three API calls
  (hosts+interfaces+groups+templates paginated, then macros, then tags) merged
  in memory to avoid one expensive join. Secret macros are masked. `upsert` on
  `hostid`. Pagination uses `countOutput` + `while offset < total` (never
  `while True`) plus `max_active_runs=1` to prevent runaway loops on overlapping
  runs.
- **`zabbix_history_collector`** (`*/5`) — time-series. `history.get` returns one
  value_type per call, so it builds an item→value_type map first, then issues one
  call per type; all values land in one long-format `zabbix_history` table tagged
  with `value_type`. PK is `(itemid, clock, ns)`. Each run reads
  `[data_interval_start - overlap_seconds, data_interval_end]` — the deliberate
  overlap prevents boundary loss; the resulting duplicates are absorbed by the
  writer's `ON CONFLICT DO NOTHING`.
- **`zabbix_metadata_collector`** (`@hourly`) — id↔name lookups. Pulls **all**
  templates (`template.get`) and **all** items (`item.get`, `webitems=True`, no
  monitored filter) and emits two `upsert` files: `zabbix_templates`
  (`templateid`) and `zabbix_items` (`itemid`, with `name`, `key_`, `units`,
  `value_type`/`value_type_name`, `hostid`). This is the lookup layer that makes
  `zabbix_history`'s raw values human-readable (`history.itemid → items.name`,
  `items.hostid → inventory.name`) and feeds the planned host-category Grafana
  dashboards.

### Editing DAGs — two hard constraints

- **Airflow 2.x/3.x dual compatibility.** Every DAG uses `try/except ImportError`
  import fallbacks (`airflow.sdk` for 3.x, `airflow`/`airflow.decorators`/
  `airflow.models` for 2.x). Use the existing `_var()` helper for `Variable.get`
  — it handles the `default=` (3.x) vs `default_var=` (2.x) keyword difference.
  Preserve this pattern in any new DAG.
- **Helper functions go OUTSIDE the `with DAG() as dag:` block.** In Airflow 3.x,
  putting non-task helpers inside the block makes the parser silently reject the
  DAG so it never appears in the UI. Only `@task` instances belong inside.

### Configuration (never hardcode credentials in DAGs)

Everything environment-specific lives in Airflow **Connections** and
**Variables**, documented in [docs/CONFIGURATION.md](docs/CONFIGURATION.md):
- Connection `zabbix_api_conn` (HTTP) — all three collectors. If Login is empty, the
  Password field is treated as a pre-issued API token instead of doing
  `user.login`.
- Postgres connection id is itself a Variable (`pg_writer_conn_id`, default
  `postgres_default` — usually needs overriding).
- Shared Variable `staging_folder_path` is the NFS root holding `pending/`.

There is no build/test tooling for the DAGs in this repo; they are deployed by
copying `dags/*.py` into the Airflow DAGs folder (KubernetesExecutor). See
[docs/SETUP.md](docs/SETUP.md). `airflow/values.example.yaml` is a sanitized Helm
reference.

## Subsystem 2: data-collector-webservice (source-agnostic FastAPI receiver)

A FastAPI app that accepts pushed metric JSON over mTLS HTTPS, archives the raw
bytes, normalizes, and writes Airflow staging files. Self-contained Python
package with its own `requirements.txt` and pytest suite under
`data-collector-webservice/`.

### The big idea: it feeds the *same* writer as the DAGs

The webservice is the push-side counterpart to the Zabbix collectors. Instead of
its own DB code, its **`StagingSink` writes `{meta, data}` files into the shared
NFS `pending/` dir** (the same one the collectors use), so the
`generic_postgres_writer` DAG loads OBM data with zero changes. One file per
target table per request, written atomically (temp + `os.replace`) so the writer
never reads a partial file. `OUTPUT_SINKS` picks sinks (`staging` for prod;
`jsonl` is a dev-only single-file sink that is **not** replica-safe).

### Source-agnostic layout (add sources, don't fork core)

- `app/core`, `app/services` (ingestion orchestration + storage sinks),
  `app/api` (system routes + shared `handle_metrics`) are **source-agnostic**.
- Each ingest source is a self-contained package under `app/sources/<name>/`
  with its own `normalization.py`, `staging.py` (builds the `{meta,data}`),
  `routes.py`, and `mappings/`. `obm_agent` is the only one today.
- `app/main.py` wires it up: it builds the source's normalizer + identity check +
  staging builder, mounts the source router, and constructs the sinks.
- **To add a new environment/source, add `app/sources/<name>/` and wire it in
  `main.py`. Never put source-specific logic in `core`/`services`.**

### Other architecture notes

- Ingest flow (`services/ingestion_service.py`): **archive raw → normalize →
  persist**, source-agnostic (normalizer + `identity_check` are injected). Raw is
  always written to `$RAW_PAYLOAD_DIR/YYYY/MM/DD/<request_id>.json` *before*
  normalization; weak/failed records go to `$QUARANTINE_DIR`. Both are one file
  per request → safe across replicas.
- **Normalization is intentionally tolerant** (`sources/obm_agent/normalization.py`):
  multiple envelope shapes, prefix-based class inference, unmapped metrics kept
  under `extra_metrics`. Mapping source of truth:
  `app/sources/obm_agent/mappings/collection_policy_summary.json`. The staging
  builder derives a **stable column superset per table** from that mapping so the
  writer's first-record column inference stays consistent; `sql/05_obm_agent.sql`
  is the matching typed DDL (keep them in sync).
- **mTLS is terminated at the proxy, not the app** — Kubernetes Ingress in prod
  (or Nginx in the same-host layout). The proxy forwards `X-SSL-Client-*`
  headers; the app re-checks them when `ENFORCE_PROXY_MTLS_HEADER=true`
  (`core/security.py`). **The app port must never be internet-exposed.**
- **Kubernetes is the deployment target** (`deploy/k8s/`, kustomize): multiple
  replicas spread across the Airflow workers, mTLS Ingress, NFS PVCs (one shared
  with Airflow for `pending/`, one for audit), HPA + PDB. Sized for ~9000 nodes
  pushing every 5–10 min.

### Commands (run from inside `data-collector-webservice/`)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env            # then point storage paths at a writable dir for dev

uvicorn app.main:app --reload --host 127.0.0.1 --port 8000   # run locally
pytest -q                                                    # run all tests
pytest tests/test_staging_output.py -q                       # run one test file
pytest tests/test_staging_output.py::test_no_tmp_files_left_in_pending -q  # one test
```

For local mTLS: `scripts/generate_test_certs.sh` then
`docker compose -f deploy/docker-compose.yml up --build`.

See `data-collector-webservice/docs/` (STAGING_OUTPUT, KUBERNETES, API_CONTRACT,
RUNBOOK, SECURITY) for details.
