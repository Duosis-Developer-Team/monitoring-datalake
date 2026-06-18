# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

A monitoring-to-datalake pipeline. It collects metrics and inventory from
infrastructure monitoring sources (Zabbix 7.x, OpenText OBM agents) and lands
them in a PostgreSQL datalake using Apache Airflow. The repo holds **two
independent subsystems** that do not share code:

1. **`dags/` + `sql/` + `airflow/`** — the Airflow pipeline that pulls from the
   Zabbix API and writes to PostgreSQL.
2. **`kocsistem-coso-webscript/`** — a standalone FastAPI service (the COSO
   webscript) that *receives* OBM-format metric payloads pushed over mTLS HTTPS,
   audits them, normalizes them, and appends to JSON Lines. It is the
   OBM → COSO destination side; the two subsystems are deployed and run
   separately.

Most prose docs are under `docs/` (pipeline) and
`kocsistem-coso-webscript/docs/` (webscript). Note: code comments and DAG
docstrings are in **Turkish**; match that language when editing them.

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

### The two collectors

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
- Connection `zabbix_api_conn` (HTTP) — both collectors. If Login is empty, the
  Password field is treated as a pre-issued API token instead of doing
  `user.login`.
- Postgres connection id is itself a Variable (`pg_writer_conn_id`, default
  `postgres_default` — usually needs overriding).
- Shared Variable `staging_folder_path` is the NFS root holding `pending/`.

There is no build/test tooling for the DAGs in this repo; they are deployed by
copying `dags/*.py` into the Airflow DAGs folder (KubernetesExecutor). See
[docs/SETUP.md](docs/SETUP.md). `airflow/values.example.yaml` is a sanitized Helm
reference.

## Subsystem 2: kocsistem-coso-webscript (FastAPI OBM receiver)

A FastAPI app that accepts OBM-format JSON over HTTPS POST, archives the raw
bytes, normalizes, and persists. Self-contained Python package with its own
`requirements.txt` and pytest suite under `kocsistem-coso-webscript/`.

### Commands (run from inside `kocsistem-coso-webscript/`)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env            # then point storage paths at a writable dir for dev

uvicorn app.main:app --reload --host 127.0.0.1 --port 8000   # run locally
pytest -q                                                    # run all tests
pytest tests/test_normalization.py -q                        # run one test file
pytest tests/test_normalization.py::test_name -q             # run one test
```

For local mTLS: `scripts/generate_test_certs.sh` then
`docker compose -f deploy/docker-compose.yml up --build`.

### Architecture

- Ingest flow (`services/ingestion_service.py`): **archive raw → normalize →
  persist**. The raw payload is always written to
  `$RAW_PAYLOAD_DIR/YYYY/MM/DD/<request_id>.json` *before* normalization, so
  mapping bugs are recoverable by replaying the audit archive after a fix.
  Normalized records append to `$NORMALIZED_JSONL_PATH`; weak/failed records go
  to `$QUARANTINE_DIR`.
- **Normalization is intentionally tolerant** (`normalization_service.py`): it
  accepts multiple envelope shapes, infers metric class by prefix when no
  explicit class is given, and preserves unmapped metrics under `extra_metrics`
  rather than dropping them — so re-processing the archive after adding mappings
  loses nothing. The mapping source of truth is
  `app/mappings/collection_policy_summary.json` (a collection-policy definition,
  not a guaranteed runtime sample).
- **Certificate auth is terminated at Nginx, not the app.** Production path is
  OBM → Nginx (443, mTLS) → FastAPI (127.0.0.1:8000). Nginx passes its verdict
  via `X-SSL-Client-*` headers; the app re-checks them only when
  `ENFORCE_PROXY_MTLS_HEADER=true` (`core/security.py`), optionally against
  subject/fingerprint allowlists. **The app port must never be internet-exposed.**
- Config is pydantic-settings from env / `.env` (`core/config.py`); CSV env vars
  for the allowlists are split by a `field_validator`.

See `kocsistem-coso-webscript/docs/` (API_CONTRACT, RUNBOOK, SECURITY) for
details.
