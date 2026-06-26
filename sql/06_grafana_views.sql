-- ============================================================================
-- Grafana yardımcı VIEW'leri
-- Amaç: Grafana panellerinin sorgularını sade ve dayanıklı tutmak.
-- ----------------------------------------------------------------------------
-- Bu dosya idempotent'tir (CREATE OR REPLACE VIEW): istediğin kadar tekrar
-- çalıştırabilirsin. Önce hedef tabloların var olması gerekir; writer onları
-- otomatik oluşturur, ama temiz tipler için 01/02/04/05 dosyalarını da uygula.
--
-- Çalıştırma:  psql -h <host> -d <db> -U <user> -f sql/06_grafana_views.sql
-- ============================================================================


-- ── v_zabbix_history_readable ───────────────────────────────────────────────
-- Ham zabbix_history değerlerini okunabilir hale getirir: itemid → item adı,
-- hostid → host adı, clock → zaman damgası. Grafana'nın time-series panelleri
-- doğrudan bu view'i kullanır.
--
-- DİKKAT (şema gerçeği): history.itemid/hostid BIGINT, items/inventory ise TEXT.
-- Bu yüzden join'lerde ::text cast'i zorunlu.
--
-- value_type kodları: 0=numeric_float, 1=character, 2=log,
--                     3=numeric_unsigned, 4=text  →  numerik olan 0 ve 3.
-- value_num: yalnızca numerik value_type'larda ve metin gerçekten sayıya
-- benziyorsa cast edilir; aksi halde NULL. Tek bir bozuk satır tüm paneli
-- düşürmesin diye regex guard + NULL kullanılıyor.
CREATE OR REPLACE VIEW v_zabbix_history_readable AS
SELECT
    to_timestamp(h.clock)            AS ts,
    h.clock                          AS clock,
    h.itemid                         AS itemid,
    h.hostid                         AS hostid,
    inv.name                         AS host_name,
    it.name                          AS item_name,
    it.key_                          AS item_key,
    it.units                         AS units,
    h.value_type                     AS value_type,
    it.value_type_name               AS value_type_name,
    h.value                          AS value_text,
    CASE
        WHEN h.value_type IN (0, 3)
         AND h.value ~ '^-?[0-9]+(\.[0-9]+)?$'
        THEN h.value::double precision
        ELSE NULL
    END                              AS value_num
FROM       zabbix_history    h
LEFT JOIN  zabbix_items      it  ON it.itemid  = h.itemid::text
LEFT JOIN  zabbix_inventory  inv ON inv.hostid = h.hostid::text;

COMMENT ON VIEW v_zabbix_history_readable IS
    'Okunabilir zabbix_history: host/item adları + güvenli numerik değer. Grafana time-series kaynağı.';


-- ── v_zabbix_host_item_catalog ──────────────────────────────────────────────
-- Host başına, son 7 günde gerçekten verisi GELEN item'ların listesi.
-- Grafana'daki "item" şablon değişkeninin kaynağıdır; host seçimine zincirlenir
-- (WHERE hostid = '$host'). Sadece numerik item'lar grafiklenebildiği için
-- value_type'ı da taşır — istersen değişken sorgusunda 0,3 ile filtrele.
CREATE OR REPLACE VIEW v_zabbix_host_item_catalog AS
SELECT
    h.hostid::text                   AS hostid,
    inv.name                         AS host_name,
    h.itemid::text                   AS itemid,
    it.name                          AS item_name,
    it.units                         AS units,
    h.value_type                     AS value_type,
    it.value_type_name               AS value_type_name,
    count(*)                         AS sample_count,
    max(to_timestamp(h.clock))       AS last_seen
FROM       zabbix_history    h
LEFT JOIN  zabbix_items      it  ON it.itemid  = h.itemid::text
LEFT JOIN  zabbix_inventory  inv ON inv.hostid = h.hostid::text
WHERE      h.clock >= extract(epoch FROM now() - interval '7 days')
GROUP BY   h.hostid, inv.name, h.itemid, it.name, it.units,
           h.value_type, it.value_type_name;

COMMENT ON VIEW v_zabbix_host_item_catalog IS
    'Host başına son 7 günde veri gelen item katalogu. Grafana item değişkeninin kaynağı.';


-- ── v_host_unified (DENEYSEL — varsayılan olarak DEVRE DIŞI) ─────────────────
-- Zabbix host kimliği (hostid) ile OBM agent node kimliği (node_*) FARKLI
-- kimlik uzaylarıdır. Tek olası köprü IP/isim eşleşmesidir ve ortama bağlıdır.
-- Önce sql/discovery/datalake_profile.sql içindeki kimlik-kesişim bloğunu
-- çalıştırıp primary_ip ↔ node_ipv4_address eşleşmesinin gerçekten var olduğunu
-- doğrula; ancak ondan sonra aşağıdaki view'i aç.
--
-- CREATE OR REPLACE VIEW v_host_unified AS
-- SELECT
--     inv.hostid              AS zabbix_hostid,
--     inv.name                AS zabbix_host_name,
--     inv.primary_ip          AS primary_ip,
--     obm.node_short_name     AS obm_node_short_name,
--     obm.node_fqdn           AS obm_node_fqdn
-- FROM       zabbix_inventory inv
-- LEFT JOIN  (SELECT DISTINCT node_ipv4_address, node_short_name, node_fqdn
--             FROM opsb_agent_node) obm
--        ON obm.node_ipv4_address = inv.primary_ip;
