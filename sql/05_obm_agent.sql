-- ============================================================================
-- opsb_agent_* — OBM agent metrics landed by the data-collector-webservice
-- Written by: data-collector-webservice (obm_agent source), method: copy
--             (append-only time-series), via the NFS pending/ dir + the Airflow
--             generic_postgres_writer DAG.
-- ============================================================================
-- The writer auto-creates these as TEXT if absent. Apply this file first to get
-- numeric types (better for Grafana) + query indexes. Column NAMES and the SET
-- of columns must match what the webservice emits (the mapping-derived superset
-- in app/sources/obm_agent/staging.py) — keep them in sync if you add metrics.
--
-- These tables are append-only (one row per node/device per collection cycle);
-- there is no primary key. De-dup/partitioning can be added after analysing
-- volume, mirroring the zabbix_history approach.

-- Shared identity + envelope columns (present on every opsb_agent_* table):
--   cmdb_global_id, cmdb_id, collection_data_flow, collection_type,
--   collection_policy_name, node_fqdn, node_short_name, node_timezone_offset_h,
--   node_ip_type, node_ipv4_address, node_ipv6_address, producer_instance_id,
--   producer_instance_type, tenant_id  → TEXT
--   timestamp_utc_s                    → BIGINT (epoch seconds)
-- Shared metadata columns:
--   class_name, datasource, source, request_id, raw_payload_ref → TEXT
--   received_at  → TIMESTAMPTZ      extra_metrics → JSONB (unmapped metrics)

-- ── opsb_agent_node (GLOBAL + CONFIGURATION) ────────────────────────────────
CREATE TABLE IF NOT EXISTS opsb_agent_node (
    cmdb_global_id          TEXT,
    cmdb_id                 TEXT,
    collection_data_flow    TEXT,
    collection_type         TEXT,
    collection_policy_name  TEXT,
    node_fqdn               TEXT,
    node_short_name         TEXT,
    node_timezone_offset_h  TEXT,
    node_ip_type            TEXT,
    node_ipv4_address       TEXT,
    node_ipv6_address       TEXT,
    producer_instance_id    TEXT,
    producer_instance_type  TEXT,
    tenant_id               TEXT,
    timestamp_utc_s         BIGINT,
    active_cpu_count        DOUBLE PRECISION,
    cpu_util_pct            DOUBLE PRECISION,
    cpu_user_mode_util_pct  DOUBLE PRECISION,
    cpu_sys_mode_util_pct   DOUBLE PRECISION,
    mem_util_pct            DOUBLE PRECISION,
    mem_phys_total          DOUBLE PRECISION,
    disk_phys_byte_rate     DOUBLE PRECISION,
    net_packet_rate         DOUBLE PRECISION,
    -- numeric metrics are DOUBLE PRECISION (not INT/BIGINT) so COPY accepts both
    -- integer and float text (e.g. "8" or "8.0") without failing the whole file.
    uptime_s                DOUBLE PRECISION,
    os_name                 TEXT,
    system_id               TEXT,
    boot_time               TEXT,
    collector               TEXT,
    machine                 TEXT,
    machine_model           TEXT,
    num_cpu                 DOUBLE PRECISION,
    num_disk                DOUBLE PRECISION,
    class_name              TEXT,
    datasource              TEXT,
    source                  TEXT,
    received_at             TIMESTAMPTZ,
    request_id              TEXT,
    raw_payload_ref         TEXT,
    extra_metrics           JSONB
);
CREATE INDEX IF NOT EXISTS idx_opsb_agent_node_node_ts
    ON opsb_agent_node (node_short_name, timestamp_utc_s);
CREATE INDEX IF NOT EXISTS idx_opsb_agent_node_received_at
    ON opsb_agent_node (received_at);

-- ── opsb_agent_disk (DISK) ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS opsb_agent_disk (
    cmdb_global_id          TEXT,
    cmdb_id                 TEXT,
    collection_data_flow    TEXT,
    collection_type         TEXT,
    collection_policy_name  TEXT,
    node_fqdn               TEXT,
    node_short_name         TEXT,
    node_timezone_offset_h  TEXT,
    node_ip_type            TEXT,
    node_ipv4_address       TEXT,
    node_ipv6_address       TEXT,
    producer_instance_id    TEXT,
    producer_instance_type  TEXT,
    tenant_id               TEXT,
    timestamp_utc_s         BIGINT,
    disk_device_name        TEXT,
    disk_device_no          TEXT,
    disk_dir_name           TEXT,
    disk_phys_byte_rate     DOUBLE PRECISION,
    disk_phys_io_rate       DOUBLE PRECISION,
    disk_request_queue      DOUBLE PRECISION,
    disk_util_pct           DOUBLE PRECISION,
    class_name              TEXT,
    datasource              TEXT,
    source                  TEXT,
    received_at             TIMESTAMPTZ,
    request_id              TEXT,
    raw_payload_ref         TEXT,
    extra_metrics           JSONB
);
CREATE INDEX IF NOT EXISTS idx_opsb_agent_disk_node_ts
    ON opsb_agent_disk (node_short_name, timestamp_utc_s);

-- ── opsb_agent_filesys (FILESYSTEM) ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS opsb_agent_filesys (
    cmdb_global_id          TEXT,
    cmdb_id                 TEXT,
    collection_data_flow    TEXT,
    collection_type         TEXT,
    collection_policy_name  TEXT,
    node_fqdn               TEXT,
    node_short_name         TEXT,
    node_timezone_offset_h  TEXT,
    node_ip_type            TEXT,
    node_ipv4_address       TEXT,
    node_ipv6_address       TEXT,
    producer_instance_id    TEXT,
    producer_instance_type  TEXT,
    tenant_id               TEXT,
    timestamp_utc_s         BIGINT,
    fs_block_size           DOUBLE PRECISION,
    fs_device_name          TEXT,
    fs_dir_name             TEXT,
    fs_max_size             DOUBLE PRECISION,
    fs_space_used           DOUBLE PRECISION,
    fs_util_pct             DOUBLE PRECISION,
    fs_type                 TEXT,
    class_name              TEXT,
    datasource              TEXT,
    source                  TEXT,
    received_at             TIMESTAMPTZ,
    request_id              TEXT,
    raw_payload_ref         TEXT,
    extra_metrics           JSONB
);
CREATE INDEX IF NOT EXISTS idx_opsb_agent_filesys_node_ts
    ON opsb_agent_filesys (node_short_name, timestamp_utc_s);

-- ── opsb_agent_cpu (CPU) ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS opsb_agent_cpu (
    cmdb_global_id          TEXT,
    cmdb_id                 TEXT,
    collection_data_flow    TEXT,
    collection_type         TEXT,
    collection_policy_name  TEXT,
    node_fqdn               TEXT,
    node_short_name         TEXT,
    node_timezone_offset_h  TEXT,
    node_ip_type            TEXT,
    node_ipv4_address       TEXT,
    node_ipv6_address       TEXT,
    producer_instance_id    TEXT,
    producer_instance_type  TEXT,
    tenant_id               TEXT,
    timestamp_utc_s         BIGINT,
    cpu_active              TEXT,
    cpu_clock               DOUBLE PRECISION,
    cpu_total_util_pct      DOUBLE PRECISION,
    cpu_id                  TEXT,
    cpu_interrupt_rate      DOUBLE PRECISION,
    cpu_state               TEXT,
    class_name              TEXT,
    datasource              TEXT,
    source                  TEXT,
    received_at             TIMESTAMPTZ,
    request_id              TEXT,
    raw_payload_ref         TEXT,
    extra_metrics           JSONB
);
CREATE INDEX IF NOT EXISTS idx_opsb_agent_cpu_node_ts
    ON opsb_agent_cpu (node_short_name, timestamp_utc_s);

-- ── opsb_agent_netif (NETIF) ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS opsb_agent_netif (
    cmdb_global_id          TEXT,
    cmdb_id                 TEXT,
    collection_data_flow    TEXT,
    collection_type         TEXT,
    collection_policy_name  TEXT,
    node_fqdn               TEXT,
    node_short_name         TEXT,
    node_timezone_offset_h  TEXT,
    node_ip_type            TEXT,
    node_ipv4_address       TEXT,
    node_ipv6_address       TEXT,
    producer_instance_id    TEXT,
    producer_instance_type  TEXT,
    tenant_id               TEXT,
    timestamp_utc_s         BIGINT,
    netif_name              TEXT,
    netif_in_byte_rate      DOUBLE PRECISION,
    netif_out_byte_rate     DOUBLE PRECISION,
    netif_packet_rate       DOUBLE PRECISION,
    netif_net_speed         DOUBLE PRECISION,
    netif_util_pct          DOUBLE PRECISION,
    class_name              TEXT,
    datasource              TEXT,
    source                  TEXT,
    received_at             TIMESTAMPTZ,
    request_id              TEXT,
    raw_payload_ref         TEXT,
    extra_metrics           JSONB
);
CREATE INDEX IF NOT EXISTS idx_opsb_agent_netif_node_ts
    ON opsb_agent_netif (node_short_name, timestamp_utc_s);
