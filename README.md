# Zabbix → PostgreSQL Datalake Pipeline

An Apache Airflow–based data pipeline that extracts inventory and time-series
data from Zabbix 7.x and loads it into a PostgreSQL datalake using a
file-based, decoupled collector/writer architecture.

## Overview

The pipeline separates **data collection** from **data loading**:

- **Collector DAGs** pull data from the Zabbix API and write self-describing
  JSON files (data + write metadata) to a shared NFS staging directory.
  Collectors know nothing about PostgreSQL.
- **A single generic writer DAG** polls the staging directory, reads each
  file's metadata, and writes to PostgreSQL using the method declared in the
  file (`insert`, `upsert`, or `copy`). The writer knows nothing about Zabbix.

This decoupling means new data sources can be added by writing a new collector
that drops a correctly-formatted file into the staging directory — the writer
requires no changes.

```
┌─────────────────────┐     ┌──────────────────┐     ┌──────────────────────┐
│  Collector DAGs     │     │   NFS staging    │     │  Writer DAG          │
│                     │     │                  │     │                      │
│  zabbix_data_       │────▶│  pending/*.json  │◀────│  generic_postgres_   │
│    collector_v2     │     │  (data + meta)   │     │    writer            │
│  zabbix_history_    │     │                  │     │  (every 5 min)       │
│    collector        │     │                  │     │                      │
└─────────────────────┘     └──────────────────┘     └──────────┬───────────┘
        │                                                        │
        ▼                                                        ▼
   Zabbix 7.x API                                          PostgreSQL
                                                          (datalake)
```

## Components

| DAG | Purpose | Schedule |
|-----|---------|----------|
| `zabbix_data_collector_v2` | Host inventory (interfaces, macros, tags) | `@hourly` |
| `zabbix_history_collector` | Time-series history values | `*/5 * * * *` |
| `zabbix_metadata_collector` | Template + item id↔name lookups | `@hourly` |
| `generic_postgres_writer` | Reads staging files, writes to PostgreSQL | `*/5 * * * *` |

A separate push-side service, [`data-collector-webservice/`](data-collector-webservice/),
receives OBM agent metrics over mTLS and drops the **same** staging-file format
into `pending/`, so the writer loads it with no changes.

## Quick Start

1. Read [`docs/SETUP.md`](docs/SETUP.md) for environment prerequisites.
2. Configure Airflow Connections and Variables (see [`docs/CONFIGURATION.md`](docs/CONFIGURATION.md)).
3. Apply the database setup in [`sql/`](sql/).
4. Deploy the DAGs from [`dags/`](dags/) to your Airflow DAGs folder.

## Architecture

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the detailed design,
including the staging file format, the writer's COPY/staging pattern, and the
overlap-based gap-prevention strategy used by the history collector.

## Repository Layout

```
.
├── README.md
├── dags/
│   ├── zabbix_data_collector_v2.py    # host inventory collector
│   ├── zabbix_history_collector.py    # time-series collector
│   ├── zabbix_metadata_collector.py   # template + item id↔name lookups
│   └── generic_postgres_writer.py     # generic metadata-driven writer
├── docs/
│   ├── ARCHITECTURE.md                # design and data flow
│   ├── SETUP.md                       # environment setup (K8s, Airflow)
│   ├── CONFIGURATION.md               # connections & variables reference
│   └── STAGING_FORMAT.md              # pending-file JSON schema
├── sql/
│   ├── 01_zabbix_inventory.sql        # inventory table + indexes
│   ├── 02_zabbix_history.sql          # history table (time-series)
│   ├── 03_grants.sql                  # user permissions
│   ├── 04_zabbix_metadata.sql         # templates + items lookup tables
│   └── 05_obm_agent.sql               # OBM agent metric tables (webservice)
├── data-collector-webservice/         # push-side FastAPI receiver (own README)
└── airflow/
    └── values.example.yaml            # sanitized Helm values reference
```

## Environment

- Apache Airflow 3.0.x (KubernetesExecutor) — DAGs are also written to be
  Airflow 2.x compatible via fallback imports.
- Zabbix 7.0.x
- PostgreSQL 14+ (TimescaleDB optional; tables start as plain tables and can be
  converted to hypertables after analysis).

## License

Internal project. Adapt as needed.
