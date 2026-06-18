-- ============================================================================
-- zabbix_inventory — host inventory table
-- Written by: zabbix_data_collector_v2 (method: upsert, key: hostid)
-- ============================================================================
-- The writer creates this table automatically via CREATE TABLE IF NOT EXISTS,
-- but applying it explicitly lets you add indexes and tune types up front.

CREATE TABLE IF NOT EXISTS zabbix_inventory (
    hostid          TEXT        PRIMARY KEY,
    name            TEXT,
    description     TEXT,
    status          TEXT,                       -- 'Enabled' | 'Disabled'
    primary_ip      TEXT,
    secondary_ips   JSONB,
    monitored_by    TEXT,                        -- 'Zabbix Server' | 'Proxy ID: N'
    host_groups     JSONB,
    templates       JSONB,
    interfaces      JSONB,
    macros          JSONB,
    tags            JSONB,
    collected_at    TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for common query patterns ------------------------------------------

-- Lookup by primary IP
CREATE INDEX IF NOT EXISTS idx_zabbix_inventory_primary_ip
    ON zabbix_inventory (primary_ip);

-- Filter by status
CREATE INDEX IF NOT EXISTS idx_zabbix_inventory_status
    ON zabbix_inventory (status);

-- Filter / search within host groups (GIN for JSONB containment)
CREATE INDEX IF NOT EXISTS idx_zabbix_inventory_host_groups
    ON zabbix_inventory USING GIN (host_groups);

-- Filter / search within tags
CREATE INDEX IF NOT EXISTS idx_zabbix_inventory_tags
    ON zabbix_inventory USING GIN (tags);
