-- ============================================================================
-- zabbix_history — time-series values (long / vertical format)
-- Written by: zabbix_history_collector (method: copy + staging, key below)
-- ============================================================================
-- Starts as a plain PostgreSQL table so the data shape can be analysed before
-- committing to a partitioning / hypertable strategy. See the bottom of this
-- file for the TimescaleDB conversion path.

CREATE TABLE IF NOT EXISTS zabbix_history (
    itemid      BIGINT      NOT NULL,
    hostid      BIGINT,
    clock       BIGINT      NOT NULL,            -- unix timestamp (seconds)
    ns          INTEGER     NOT NULL,            -- nanoseconds within the second
    value       TEXT,                             -- all value_types stored as text
    value_type  SMALLINT,                         -- 0=float 1=char 2=log 3=unsigned 4=text
    PRIMARY KEY (itemid, clock, ns)
);

-- The composite primary key (itemid, clock, ns) is the duplicate guard:
-- a single item never emits two values at the same nanosecond, so the writer's
-- ON CONFLICT (itemid, clock, ns) DO NOTHING absorbs the deliberate window
-- overlap used for gap prevention.

-- Indexes for common query patterns ------------------------------------------

-- Time-range scans for a given host
CREATE INDEX IF NOT EXISTS idx_zabbix_history_hostid_clock
    ON zabbix_history (hostid, clock);

-- Time-range scans for a given item
CREATE INDEX IF NOT EXISTS idx_zabbix_history_itemid_clock
    ON zabbix_history (itemid, clock);

-- Global time scans
CREATE INDEX IF NOT EXISTS idx_zabbix_history_clock
    ON zabbix_history (clock);


-- ============================================================================
-- OPTIONAL: TimescaleDB hypertable conversion (run AFTER analysing the data)
-- ============================================================================
-- TimescaleDB requires the partitioning column to be part of any unique index.
-- Our PK already includes `clock`, so conversion is straightforward.
--
-- Convert clock (BIGINT unix seconds) using chunk_time_interval in the same
-- unit (seconds). 604800 = 1 week.
--
--   CREATE EXTENSION IF NOT EXISTS timescaledb;
--
--   SELECT create_hypertable(
--       'zabbix_history',
--       'clock',
--       chunk_time_interval => 604800,
--       migrate_data        => true
--   );
--
-- After conversion you may add compression and retention policies, e.g.:
--
--   ALTER TABLE zabbix_history SET (
--       timescaledb.compress,
--       timescaledb.compress_segmentby = 'itemid'
--   );
--   SELECT add_compression_policy('zabbix_history', BIGINT '2592000');  -- 30d
--   SELECT add_retention_policy('zabbix_history', BIGINT '31536000');   -- 365d
-- ============================================================================
