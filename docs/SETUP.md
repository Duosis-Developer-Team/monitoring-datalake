# Setup

This guide covers deploying the pipeline on a Kubernetes-based Airflow
installation (KubernetesExecutor).

## Prerequisites

- Apache Airflow 3.0.x deployed on Kubernetes (Helm chart `airflow`).
- KubernetesExecutor.
- A shared NFS volume mounted into the Airflow pods (used for the staging
  directory). The provided setup assumes `storageClassName: nfs-client`.
- Network reachability from Airflow worker pods to:
  - the Zabbix API endpoint,
  - the PostgreSQL server.
- `psycopg2-binary >= 2.9` and `apache-airflow-providers-postgres` available in
  the Airflow image (both ship with recent official images — verify with the
  commands below).

## Verify required packages

```bash
kubectl exec -n <namespace> deployment/airflow-scheduler -- \
  python3 -c "import psycopg2; print('psycopg2', psycopg2.__version__, 'libpq', psycopg2.__libpq_version__)"

kubectl exec -n <namespace> deployment/airflow-scheduler -- \
  python3 -c "from airflow.providers.postgres.hooks.postgres import PostgresHook; print('PostgresHook OK')"
```

`libpq` must be `100000` (PostgreSQL 10) or higher for SCRAM-SHA-256 support.

## PostgreSQL: SCRAM-SHA-256 and pg_hba.conf

If the target PostgreSQL enforces `scram-sha-256`, the connecting client's IP
range must be allowed in `pg_hba.conf`. Under KubernetesExecutor, **each task
runs in its own pod** and connects directly to PostgreSQL — so the **pod CIDR
ranges**, not the node IPs, must be allowed.

Find the pod CIDRs:

```bash
kubectl get nodes -o jsonpath='{.items[*].spec.podCIDR}'
```

Add one `pg_hba.conf` line per CIDR (a single wide range may be disallowed by
security policy — list them individually if required):

```
host    <db>    <user>    10.244.0.0/24    scram-sha-256
host    <db>    <user>    10.244.1.0/24    scram-sha-256
# ... one line per pod CIDR
```

Reload PostgreSQL:

```sql
SELECT pg_reload_conf();
```

When a new node joins the cluster, add its pod CIDR as well.

## Database objects

Apply the SQL in order (see [`../sql/`](../sql/)):

```bash
psql -h <db_host> -U <admin_user> -d <db_name> -f sql/01_zabbix_inventory.sql
psql -h <db_host> -U <admin_user> -d <db_name> -f sql/02_zabbix_history.sql
psql -h <db_host> -U <admin_user> -d <db_name> -f sql/03_grants.sql
```

The writer also creates tables automatically (`CREATE TABLE IF NOT EXISTS`),
but applying the SQL explicitly lets you add indexes and grants up front.

## NFS staging directory

The collectors and writer share a directory tree under
`staging_folder_path`:

```
{staging_folder_path}/
├── data/      # inventory data files (collector writes, writer reads/deletes)
└── pending/   # writer scans this directory
```

Ensure the Airflow pods can read and write this path. It is created
automatically on first run (`os.makedirs(..., exist_ok=True)`).

## Deploy the DAGs

Copy the three DAG files to your Airflow DAGs folder (git-sync, baked image, or
shared volume — whichever your deployment uses):

```
dags/zabbix_data_collector_v2.py
dags/zabbix_history_collector.py
dags/generic_postgres_writer.py
```

Airflow's DAG processor will pick them up within the
`dag_dir_list_interval` (default 30s; may take longer on NFS). If a DAG does
not appear, check for import errors:

```bash
kubectl exec -n <namespace> deployment/airflow-scheduler -- \
  airflow dags list-import-errors
```

## Configure connections and variables

See [`CONFIGURATION.md`](CONFIGURATION.md).

## Unpause

Enable the DAGs in the UI (or via CLI):

```bash
kubectl exec -n <namespace> deployment/airflow-scheduler -- \
  airflow dags unpause zabbix_data_collector_v2
kubectl exec -n <namespace> deployment/airflow-scheduler -- \
  airflow dags unpause zabbix_history_collector
kubectl exec -n <namespace> deployment/airflow-scheduler -- \
  airflow dags unpause generic_postgres_writer
```
