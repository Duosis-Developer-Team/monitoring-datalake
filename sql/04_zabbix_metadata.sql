-- ============================================================================
-- zabbix_templates / zabbix_items — Zabbix metadata (id ↔ name lookups)
-- Written by: zabbix_metadata_collector (method: upsert)
-- ============================================================================
-- Purpose: make zabbix_history's raw values human-readable by joining itemid →
-- item name/key/units, hostid → host name, and host templates → category.
-- The writer auto-creates these via CREATE TABLE IF NOT EXISTS, but applying
-- them explicitly lets you set types and indexes up front.

-- ── Templates ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS zabbix_templates (
    templateid        TEXT        PRIMARY KEY,
    name              TEXT,                       -- görünen ad
    host              TEXT,                       -- teknik ad
    description       TEXT,
    template_groups   JSONB,                      -- ait olduğu template grupları
    parent_templates  JSONB,                      -- miras alınan template'ler
    collected_at      TIMESTAMPTZ,
    updated_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_zabbix_templates_name
    ON zabbix_templates (name);

CREATE INDEX IF NOT EXISTS idx_zabbix_templates_groups
    ON zabbix_templates USING GIN (template_groups);

-- ── Items ───────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS zabbix_items (
    itemid            TEXT        PRIMARY KEY,
    hostid            TEXT,                        -- → zabbix_inventory.hostid
    name              TEXT,                        -- okunabilir item adı
    key_              TEXT,                        -- Zabbix item key
    value_type        TEXT,                        -- '0'..'4'
    value_type_name   TEXT,                        -- numeric_float | character | ...
    units             TEXT,
    status            TEXT,                        -- 'Enabled' | 'Disabled'
    templateid        TEXT,                        -- şablon kaynaklı item'ın parent id'si
    collected_at      TIMESTAMPTZ,
    updated_at        TIMESTAMPTZ DEFAULT NOW()
);

-- zabbix_history.itemid → zabbix_items.itemid (PK zaten kapsar)
-- Host bazlı item taraması için
CREATE INDEX IF NOT EXISTS idx_zabbix_items_hostid
    ON zabbix_items (hostid);

CREATE INDEX IF NOT EXISTS idx_zabbix_items_name
    ON zabbix_items (name);

CREATE INDEX IF NOT EXISTS idx_zabbix_items_key
    ON zabbix_items (key_);

-- ============================================================================
-- Örnek: ham history değerlerini okunabilir hale getiren join
-- ============================================================================
--   SELECT h.clock, inv.name AS host, it.name AS item, h.value, it.units
--   FROM   zabbix_history h
--   JOIN   zabbix_items   it  ON it.itemid = h.itemid::text
--   JOIN   zabbix_inventory inv ON inv.hostid = it.hostid
--   WHERE  inv.name = 'web-server-01'
--   ORDER  BY h.clock DESC
--   LIMIT  100;
