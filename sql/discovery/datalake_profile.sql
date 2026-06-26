-- ============================================================================
-- Datalake Discovery / Profil
-- ----------------------------------------------------------------------------
-- Amaç: Grafana dashboard'larını somutlaştırmadan ÖNCE datalake'te neyin
-- gerçekten bulunduğunu profillemek — hangi tablolar dolu, veri ne kadar taze,
-- hangi item'lar numerik (grafiklenebilir), her host'ta ne kadar metrik var,
-- ve Zabbix host'ları ile OBM node'ları arasında bir kimlik köprüsü var mı.
--
-- Bu dosyayı BİR BÜTÜN olarak çalıştırma; psql'de blok blok çalıştır ve
-- çıktıyı dashboard sorgularını ayarlamak için kullan:
--   psql -h <host> -d <db> -U <user>
--   \i sql/discovery/datalake_profile.sql        -- ya da blokları tek tek kopyala
--
-- Salt-okunur sorgulardır; hiçbir tabloyu değiştirmez.
-- ============================================================================


-- ── 1) Tablo envanteri + yaklaşık satır sayıları ────────────────────────────
-- Hızlı (reltuples planlayıcı tahmini; tam değer için aşağıdaki count(*) bloğu).
SELECT  c.relname                                    AS table_name,
        to_char(c.reltuples, 'FM999,999,999,990')    AS approx_rows,
        pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size
FROM    pg_class c
JOIN    pg_namespace n ON n.oid = c.relnamespace
WHERE   n.nspname = 'public'
  AND   c.relkind = 'r'
  AND   c.relname IN ('zabbix_inventory','zabbix_history','zabbix_items',
                      'zabbix_templates','opsb_agent_node','opsb_agent_cpu',
                      'opsb_agent_disk','opsb_agent_filesys','opsb_agent_netif')
ORDER BY pg_total_relation_size(c.oid) DESC;


-- ── 2) Veri tazeliği / ingest lag ───────────────────────────────────────────
-- Her zaman-serisi tablosu için en yeni kayıt ve "şimdi - en yeni" gecikmesi.
-- Boru hattının çalışıp çalışmadığını ve hangi kaynağın geride kaldığını gösterir.
SELECT 'zabbix_history' AS source,
       to_timestamp(max(clock))                          AS last_event,
       now() - to_timestamp(max(clock))                  AS lag
FROM   zabbix_history
UNION ALL
SELECT 'opsb_agent_node',
       to_timestamp(max(timestamp_utc_s)),
       now() - to_timestamp(max(timestamp_utc_s))
FROM   opsb_agent_node
UNION ALL
SELECT 'opsb_agent_node.received_at',
       max(received_at),
       now() - max(received_at)
FROM   opsb_agent_node;


-- ── 3) zabbix_history value_type dağılımı ───────────────────────────────────
-- Hangi value_type'lar var ve kaç tanesi NUMERİK (0=float, 3=unsigned →
-- grafiklenebilir). Çoğunluk 1/2/4 (metin) ise zaman-serisi panelleri boş kalır.
SELECT  h.value_type,
        it.value_type_name,
        CASE WHEN h.value_type IN (0,3) THEN 'numeric (grafiklenebilir)'
             ELSE 'text/log (grafiklenemez)' END        AS chartable,
        count(*)                                          AS rows
FROM       zabbix_history h
LEFT JOIN  zabbix_items   it ON it.itemid = h.itemid::text
GROUP BY   h.value_type, it.value_type_name
ORDER BY   rows DESC;


-- ── 4) Host başına item / metrik yoğunluğu ──────────────────────────────────
-- Her host'ta son 24 saatte kaç distinct item ve kaç örnek toplandı. Panel
-- kapasitesini (bir host kaç seri çizecek) ve hangi host'ların aktif olduğunu
-- gösterir.
SELECT  inv.name                                          AS host_name,
        h.hostid,
        count(DISTINCT h.itemid)                          AS distinct_items,
        count(*)                                          AS samples_24h,
        max(to_timestamp(h.clock))                        AS last_sample
FROM       zabbix_history    h
LEFT JOIN  zabbix_inventory  inv ON inv.hostid = h.hostid::text
WHERE      h.clock >= extract(epoch FROM now() - interval '24 hours')
GROUP BY   inv.name, h.hostid
ORDER BY   samples_24h DESC
LIMIT 50;


-- ── 5) En "gürültülü" item'lar (örnek sayısına göre) ────────────────────────
SELECT  it.name                                           AS item_name,
        it.key_                                           AS item_key,
        it.units,
        count(*)                                          AS samples_24h
FROM       zabbix_history h
LEFT JOIN  zabbix_items   it ON it.itemid = h.itemid::text
WHERE      h.clock >= extract(epoch FROM now() - interval '24 hours')
GROUP BY   it.name, it.key_, it.units
ORDER BY   samples_24h DESC
LIMIT 30;


-- ── 6) OBM agent node profili ───────────────────────────────────────────────
-- Kaç distinct node, hangi collection policy'ler, ve node başına son timestamp.
SELECT  node_short_name,
        collection_policy_name,
        os_name,
        count(*)                                          AS rows,
        to_timestamp(max(timestamp_utc_s))                AS last_sample
FROM    opsb_agent_node
GROUP BY node_short_name, collection_policy_name, os_name
ORDER BY last_sample DESC NULLS LAST
LIMIT 50;


-- ── 7) Zabbix ↔ OBM kimlik kesişimi ─────────────────────────────────────────
-- v_host_unified köprü view'ini açmak mantıklı mı? primary_ip ↔
-- node_ipv4_address eşleşmesi var mı? matched > 0 ise IP köprüsü kurulabilir.
SELECT
    (SELECT count(*) FROM zabbix_inventory)                         AS zabbix_hosts,
    (SELECT count(DISTINCT node_ipv4_address) FROM opsb_agent_node) AS obm_nodes,
    (SELECT count(*)
       FROM zabbix_inventory inv
       JOIN (SELECT DISTINCT node_ipv4_address FROM opsb_agent_node) obm
         ON obm.node_ipv4_address = inv.primary_ip)                 AS matched_by_ip;

-- İsim üzerinden olası eşleşmeler (IP tutmazsa alternatif köprü):
SELECT  inv.name           AS zabbix_name,
        inv.primary_ip,
        obm.node_short_name AS obm_name,
        obm.node_ipv4_address
FROM       zabbix_inventory inv
JOIN       (SELECT DISTINCT node_short_name, node_fqdn, node_ipv4_address
            FROM opsb_agent_node) obm
       ON  lower(obm.node_short_name) = lower(inv.name)
       OR  lower(obm.node_fqdn)       = lower(inv.name)
LIMIT 50;
