-- ============================================================================
-- TESHIS: CpuCore / MemoryCapacity neden NULL?
-- ----------------------------------------------------------------------------
-- Sorgudaki core/memtot degerleri su zincirden gecer:
--     zabbix_items (key_ eslesmesi)  →  role='core'/'memtot'
--         →  zabbix_history (itemid + value_type IN (0,3))
--             →  ORDER BY clock DESC LIMIT 1
-- Zincirin HERHANGI bir halkasi bos ise sonuc NULL olur.
-- Asagidaki 3 kontrol hangi halkanin koptugunu tek tek gosterir.
-- Sirayla calistirin; ilk bos donen adim sebeptir.
-- ============================================================================


-- ── KONTROL A: Item'lar var mi, gercek key_ ve value_type nedir? ────────────
-- Gevsek LIKE kullaniliyor: sorgudaki dar desenler tutmasa bile item'i bulur.
-- BAKILACAK: value_type sutunu.
--   0 = numeric_float, 3 = numeric_unsigned  → sorgu bunlari kabul ediyor
--   1 = character, 2 = log, 4 = text         → sorgu bunlari ELIYOR  ← sorun buysa
-- Ayrica key_ sutununu sorgudaki CASE desenleriyle GOZLE karsilastirin.
WITH ah AS (
    SELECT inv.hostid
    FROM   zabbix_inventory inv
    WHERE  inv.host_groups @> '["AGENTLESS_GROUP"]'::jsonb
      AND  inv.status = 'Enabled'
)
SELECT i.key_,
       i.value_type,
       i.value_type_name,
       i.status,
       count(*) AS item_sayisi
FROM   zabbix_items i
JOIN   ah ON ah.hostid = i.hostid
WHERE  i.key_ LIKE '%cpu_core_count%'
   OR  i.key_ LIKE '%mem_total_mb%'
GROUP  BY 1, 2, 3, 4
ORDER  BY item_sayisi DESC;


-- ── KONTROL B: Bu item'larin zabbix_history'de verisi var mi? ───────────────
-- BAKILACAK:
--   satir_sayisi = 0            → collector bu item'i hic toplamamis
--                                 (Keep history=0 / monitored degil / item pasif)
--   satir_sayisi > 0 ama
--   value_type 0/3 DEGIL        → veri var ama sorgunun filtresi eliyor
--   son_kayit cok eski          → item artik veri uretmiyor
WITH ah AS (
    SELECT inv.hostid
    FROM   zabbix_inventory inv
    WHERE  inv.host_groups @> '["AGENTLESS_GROUP"]'::jsonb
      AND  inv.status = 'Enabled'
),
cap_items AS (
    SELECT i.itemid::bigint AS itemid, i.key_, i.value_type
    FROM   zabbix_items i
    JOIN   ah ON ah.hostid = i.hostid
    WHERE  i.key_ LIKE '%cpu_core_count%'
       OR  i.key_ LIKE '%mem_total_mb%'
)
SELECT ci.key_,
       ci.value_type                            AS item_value_type,
       count(*)                                 AS item_sayisi,
       count(*) FILTER (WHERE s.n > 0)          AS verisi_olan_item,
       sum(s.n)                                 AS toplam_satir,
       max(s.son_clock)                         AS son_kayit,
       -- history'de gercekte hangi value_type ile duruyor?
       min(s.hist_vtype)                        AS history_value_type
FROM   cap_items ci
LEFT   JOIN LATERAL (
    SELECT count(*)                    AS n,
           to_timestamp(max(h.clock))  AS son_clock,
           min(h.value_type)           AS hist_vtype
    FROM   zabbix_history h
    WHERE  h.itemid = ci.itemid
) s ON true
GROUP  BY 1, 2
ORDER  BY item_sayisi DESC;


-- ── KONTROL C: Sorgudaki CASE gercekten role atiyor mu? ────────────────────
-- Rapor sorgusundaki desenlerin BIREBIR kopyasi. Sonuc bos donerse ya da
-- 'core'/'memtot' satirlari yoksa, sebep key_ deseni uyusmazligidir
-- (KONTROL A'daki gercek key_ ile burayi karsilastirin).
WITH ah AS (
    SELECT inv.hostid
    FROM   zabbix_inventory inv
    WHERE  inv.host_groups @> '["AGENTLESS_GROUP"]'::jsonb
      AND  inv.status = 'Enabled'
)
SELECT CASE
           WHEN i.key_ IN ('ssh.run[cpu_usage,{$SSH.IP},{$SSH.PORT}]',
                           'winrm_metric.py[{$WINRM.IP},{$WINRM.USER},{$WINRM.PASSWORD},cpu_pct]')
             OR i.key_ LIKE 'ssh.run[cpu_usage_legacy,%'
             OR i.key_ LIKE 'ssh.run[cpu_usage_sunos,%' THEN 'cpu'
           WHEN i.key_ IN ('ssh.run[mem_usage,{$SSH.IP},{$SSH.PORT}]',
                           'winrm_metric.py[{$WINRM.IP},{$WINRM.USER},{$WINRM.PASSWORD},mem_pct]')
             OR i.key_ LIKE 'ssh.run[mem_usage_legacy,%'
             OR i.key_ LIKE 'ssh.run[mem_usage_sunos,%' THEN 'mem'
           WHEN i.key_ IN ('ssh.run[cpu_core_count,{$SSH.IP},{$SSH.PORT}]',
                           'winrm_metric.py[{$WINRM.IP},{$WINRM.USER},{$WINRM.PASSWORD},cpu_core_count]')
             OR i.key_ LIKE 'ssh.run[cpu_core_count_legacy,%'
             OR i.key_ LIKE 'ssh.run[cpu_core_count_sunos,%' THEN 'core'
           WHEN i.key_ IN ('ssh.run[mem_total_mb,{$SSH.IP},{$SSH.PORT}]',
                           'winrm_metric.py[{$WINRM.IP},{$WINRM.USER},{$WINRM.PASSWORD},mem_total_mb]')
             OR i.key_ LIKE 'ssh.run[mem_total_mb_legacy,%'
             OR i.key_ LIKE 'ssh.run[mem_total_mb_sunos,%' THEN 'memtot'
       END AS role,
       count(*) AS item_sayisi
FROM   zabbix_items i
JOIN   ah ON ah.hostid = i.hostid
WHERE  (i.key_ LIKE 'ssh.run[%' OR i.key_ LIKE 'winrm_metric.py[%')
GROUP  BY 1
ORDER  BY role NULLS LAST;


-- ── KONTROL D: Satir sayisi DAGILIMI — item hatasi mi, pipeline kaybi mi? ──
-- AYIRT EDICI TEST:
--   * Cift tepeli (bimodal) dagilim: bircok item 0 satirda, digerleri ~N satirda
--     → item'lar ya hep calisiyor ya hic calismiyor = ZABBIX TARAFINDA ITEM HATASI
--   * Yayvan dagilim: item'lar 3, 5, 8, 11 gibi ARA degerlerde kumeleniyor
--     → herkes bir miktar veri kaybetmis = PIPELINE PENCERE KAYBI
-- Saatlik item'da "beklenen N" = collector'in duzgun calistigi saat sayisi.
WITH ah AS (
    SELECT inv.hostid
    FROM   zabbix_inventory inv
    WHERE  inv.host_groups @> '["AGENTLESS_GROUP"]'::jsonb
      AND  inv.status = 'Enabled'
),
cap AS (
    SELECT i.itemid::bigint AS itemid,
           CASE WHEN i.key_ LIKE '%cpu_core_count%' THEN 'core' ELSE 'memtot' END AS role
    FROM   zabbix_items i
    JOIN   ah ON ah.hostid = i.hostid
    WHERE  i.key_ LIKE '%cpu_core_count%'
       OR  i.key_ LIKE '%mem_total_mb%'
),
cnt AS (
    SELECT c.role, s.n
    FROM   cap c
    CROSS  JOIN LATERAL (
        SELECT count(*) AS n FROM zabbix_history h WHERE h.itemid = c.itemid
    ) s
)
SELECT role,
       n          AS satir_sayisi,
       count(*)   AS item_adedi
FROM   cnt
GROUP  BY role, n
ORDER  BY role, n;


-- ── KONTROL E: Rapor sonucunda kapasite gercekten %100 bos mu? ─────────────
-- KONTROL B'ye gore host'larin ~%31'inde core verisi VAR. Rapor ciktisinda
-- da yaklasik bu oranda dolu satir gormeniz gerekir.
--   * Sonuc "dolu_host > 0" ise  → sorgu dogru; eksik veri Zabbix kaynakli.
--   * Sonuc "dolu_host = 0" ise  → join tarafinda ayrica bir sorun var,
--                                   birlikte bakmamiz gerekir.
WITH ah AS (
    SELECT inv.hostid, inv.name
    FROM   zabbix_inventory inv
    WHERE  inv.host_groups @> '["AGENTLESS_GROUP"]'::jsonb
      AND  inv.status = 'Enabled'
),
core_items AS (
    SELECT i.itemid::bigint AS itemid, i.hostid
    FROM   zabbix_items i
    JOIN   ah ON ah.hostid = i.hostid
    WHERE  i.key_ LIKE '%cpu_core_count%'
),
core_data AS (
    SELECT DISTINCT ON (ci.hostid) ci.hostid, l.value
    FROM   core_items ci
    CROSS  JOIN LATERAL (
        SELECT h.clock, h.value
        FROM   zabbix_history h
        WHERE  h.itemid = ci.itemid
        ORDER  BY h.clock DESC
        LIMIT  1
    ) l
    ORDER  BY ci.hostid, l.clock DESC
)
SELECT count(*)                                   AS toplam_host,
       count(cd.hostid)                           AS dolu_host,
       round(100.0 * count(cd.hostid) / nullif(count(*), 0), 1) AS dolu_yuzde
FROM   ah
LEFT   JOIN core_data cd ON cd.hostid = ah.hostid;


-- ============================================================================
-- SONUCA GORE YAPILACAK
-- ----------------------------------------------------------------------------
-- A/B: value_type 1, 2 veya 4 cikti (item metin tipinde)
--      → Rapor sorgusundaki core_data / memtot_data LATERAL'lerinde
--        `AND h.value_type IN (0, 3)` sartini KALDIRIN. Deger zaten TEXT olarak
--        saklaniyor; ::double precision cast'i "8" / "16384" gibi degerlerde
--        sorunsuz calisir. Bozuk satir riskine karsi guvenli cast:
--            CASE WHEN h.value ~ '^-?[0-9]+(\.[0-9]+)?$'
--                 THEN h.value::double precision ELSE NULL END
--      → 00_reporting_indexes.sql'deki kismi index bu item'lari KAPSAMAZ
--        (WHERE value_type IN (0,3)). Az sayida item oldugu icin PK seek
--        yeterli olur; gerekirse ayri bir kismi index eklenebilir.
--
-- B: satir_sayisi = 0
--      → Zabbix tarafinda bakin: item'in "Keep history" degeri 0 mi?
--        (0 ise yalnizca trends tutulur, history.get HIC veri dondurmez)
--        Item ve host "Enabled" ve monitored mi?
--        Item saatlik ise, collector'in duzeltilmesinden sonra en az 1 saat
--        gecmis olmali.
--
-- C: 'core' / 'memtot' satiri yok
--      → KONTROL A'daki gercek key_ degerini alip rapor sorgusundaki CASE
--        desenine ekleyin (ya da tam esitlik yerine
--        `key_ LIKE 'ssh.run[cpu_core_count,%'` seklinde gevsetin).
-- ============================================================================
