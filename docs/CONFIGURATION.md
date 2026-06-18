# Configuration

All credentials and environment-specific values live in Airflow **Connections**
and **Variables** — never in the DAG code. Placeholders below use angle
brackets; replace with your environment's values.

## Connections

### `zabbix_api_conn` (HTTP)

Used by both collectors to reach the Zabbix API.

| Field | Value |
|-------|-------|
| Connection Id | `zabbix_api_conn` |
| Connection Type | `HTTP` |
| Host | `http://<zabbix-host>/api_jsonrpc.php` |
| Login | `<zabbix-username>` |
| Password | `<zabbix-password>` |

> If the Login field is left empty, the collector treats the Password field as
> a Zabbix API token instead of doing a `user.login`.

### `<pg_writer_conn_id>` (Postgres)

Used by the writer. The connection id is configurable via the
`pg_writer_conn_id` Variable (default `postgres_default`).

| Field | Value |
|-------|-------|
| Connection Id | `<pg_writer_conn_id>` (e.g. `zabbix_pg_conn`) |
| Connection Type | `Postgres` |
| Host | `<postgres-host>` |
| Port | `5432` |
| Database | `<database-name>` |
| Login | `<postgres-username>` |
| Password | `<postgres-password>` |

## Variables

### Shared

| Key | Default | Description |
|-----|---------|-------------|
| `staging_folder_path` | `/opt/airflow/dags/data_staging/zabbix` | NFS root for staging files |

### `zabbix_data_collector_v2`

| Key | Default | Description |
|-----|---------|-------------|
| `zabbix_schedule` | `@hourly` | Collector schedule |
| `zabbix_chunk_size` | `500` | `host.get` pagination size |

### `zabbix_history_collector`

| Key | Default | Description |
|-----|---------|-------------|
| `zabbix_history_schedule` | `*/5 * * * *` | Collector schedule |
| `zabbix_history_overlap_sec` | `60` | Window overlap in seconds (gap prevention) |
| `zabbix_history_chunk` | `1000` | `item.get` pagination size |

### `zabbix_metadata_collector`

| Key | Default | Description |
|-----|---------|-------------|
| `zabbix_metadata_schedule` | `@hourly` | Collector schedule |
| `zabbix_metadata_chunk` | `1000` | `template.get` / `item.get` pagination size |

### `generic_postgres_writer`

| Key | Default | Description |
|-----|---------|-------------|
| `pg_writer_conn_id` | `postgres_default` | Postgres connection id to use |
| `pg_writer_batch_size` | `200` | Batch size for insert/upsert |

## Setting variables via CLI

```bash
kubectl exec -n <namespace> deployment/airflow-scheduler -- \
  airflow variables set staging_folder_path "/opt/airflow/dags/data_staging/zabbix"

kubectl exec -n <namespace> deployment/airflow-scheduler -- \
  airflow variables set pg_writer_conn_id "zabbix_pg_conn"
```

## Notes

- Variables have sensible defaults baked into the DAGs; you only need to set the
  ones you want to override. `pg_writer_conn_id` is the one most likely to need
  setting, since the default `postgres_default` rarely matches a real
  connection id.
