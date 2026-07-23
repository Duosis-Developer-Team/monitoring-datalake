-- ============================================================================
-- DISK RAPORLAMA - v4 (window as-of  →  zaman kovasi hash join)
-- ----------------------------------------------------------------------------
-- NEDEN DISK SORGUSU CPU'DAN COK DAHA AGIR?
--
-- CPU sorgusu:   host basina 1 cpu + 1 mem item  → ~718 + ~718 item
--                1 gun x 5 dk  →  kabaca 0.4M satir
--
-- DISK sorgusu:  host basina HER MOUNT icin ayri item seti
--                (util/used/free/read/write). 718 host x ~5 mount = ~3.6K mount
--                1 gun x 5 dk  →  kabaca 1M surucu satiri + 2M IO satiri
--
-- Yani girdi ~10x buyuk. Asil sorun ise bu satirlarin uzerinde yapilan is:
-- `scan` + `asof` CTE'leri toplam 8 window fonksiyonu calistiriyor ve bunlar
-- IKI FARKLI siralama gerektiriyor (clock ASC ve clock DESC). PostgreSQL bunun
-- icin milyonlarca satiri defalarca SIRALAR. work_mem yetmezse bu siralamalar
-- diske tasar ve sorgu dakikalarca surer — gordugunuz tablo tam olarak budur.
--
-- COZUM: "en yakin clock" eslesmesini window fonksiyonlariyla degil, ZAMAN
-- KOVASI (time bucket) ile yapmak. clock'u 5 dakikalik kovalara bolup kova
-- uzerinden join edince, 8 window + 2 buyuk siralama yerine tek bir HASH
-- AGGREGATE + HASH JOIN kaliyor. Siralama tamamen ortadan kalkiyor.
--
-- SEMANTIK FARK: "mutlak en yakin ornek" yerine "ayni 5 dk kovasindaki ornek"
-- (kova bosca bir onceki kovaya duser). 5 dk'da bir toplanan veride pratikte
-- ayni sonucu verir, cok daha ucuzdur ve daha ongorulebilirdir.
--
-- ── CALISTIRMADAN ONCE ──────────────────────────────────────────────────────
--   SET work_mem = '512MB';   -- oturum bazinda; bu sorgu icin kritik
--   SET max_parallel_workers_per_gather = 4;
-- ============================================================================


-- ############################################################################
-- ADIM 0 — ONCE OLCUN (sorguyu calistirmadan once, saniyeler surer)
-- ############################################################################

-- 0a) HACIM: kac mount, kac item, 1 gunde kac satir isleniyor?
--     Sorgunun ne kadar is yaptigini gosterir.
WITH ah AS (
    SELECT inv.hostid
    FROM   zabbix_inventory inv
    WHERE  inv.host_groups @> '["AGENTLESS_GROUP"]'::jsonb
      AND  inv.status = 'Enabled'
),
di AS (
    SELECT i.itemid::bigint AS itemid,
           CASE WHEN i.key_ LIKE 'fs.usage[%' OR i.key_ LIKE 'winrm.disk.usedpct[%'
                THEN 'util' ELSE 'diger' END AS rol
    FROM   zabbix_items i
    JOIN   ah ON ah.hostid = i.hostid
    WHERE  i.key_ NOT LIKE '%[{#%'
      AND (i.key_ LIKE 'fs.%' OR i.key_ LIKE 'winrm.disk.%')
)
SELECT di.rol,
       count(*)   AS item_sayisi,
       sum(s.n)   AS bir_gunluk_satir
FROM   di
CROSS  JOIN LATERAL (
    SELECT count(*) AS n
    FROM   zabbix_history h
    WHERE  h.itemid = di.itemid
      AND  h.clock >= EXTRACT(EPOCH FROM NOW() - INTERVAL '1 day')::bigint
) s
GROUP  BY di.rol;

-- 0b) mount_key GERCEKTEN ne iceriyor?
--     DIKKAT: CPU item'larinin key'lerinde SSH secenekleri gomulu oldugunu
--     gorduk (ornegin: ssh.run[cpu_core_count_sunos,{$SSH.IP},22,
--     "KexAlgorithms=diffie-hellman-group14-sha1,..."]).
--     Disk item'larinda da boyleyse:
--       * mount_key YUZLERCE KARAKTERLIK bir metin olur → GROUP BY / JOIN /
--         ORDER BY maliyeti patlar (metin karsilastirmasi collation ile yapilir)
--       * dir_name kolonu mount noktasi yerine tum SSH parametrelerini gosterir
--         → RAPOR YANLIS OLUR
--     Cikan degerlere bakin: '[/var]' gibi kisa mi, yoksa uzun bir dizi mi?
SELECT i.key_,
       substring(i.key_ FROM position('[' IN i.key_))        AS mount_key,
       length(substring(i.key_ FROM position('[' IN i.key_))) AS mount_key_uzunluk
FROM   zabbix_items i
WHERE  i.key_ LIKE 'fs.usage[%'
    OR i.key_ LIKE 'winrm.disk.usedpct[%'
ORDER  BY mount_key_uzunluk DESC
LIMIT  20;
-- Uzunluk buyukse mount_key'i sadelestirin, ornegin ilk parametreyi alarak:
--   split_part(substring(i.key_ FROM position('[' IN i.key_) + 1), ',', 1)


-- ############################################################################
-- ADIM 1 — OPTIMIZE EDILMIS DISK SORGUSU
-- ############################################################################
WITH agentless_hosts AS MATERIALIZED (
    SELECT inv.hostid,
           inv.name       AS node_fqdn,
           inv.primary_ip AS node_ipv4_address
    FROM   zabbix_inventory inv
    WHERE  inv.host_groups @> '["AGENTLESS_GROUP"]'::jsonb
      AND  inv.status = 'Enabled'
),

disk_items AS MATERIALIZED (
    SELECT i.itemid::bigint AS itemid,
           ah.hostid,
           ah.node_fqdn,
           ah.node_ipv4_address,
           CASE WHEN i.key_ LIKE 'fs.%' THEN 'unix' ELSE 'win' END AS os_kind,
           substring(i.key_ FROM position('[' IN i.key_)) AS mount_key,
           CASE
               WHEN i.key_ LIKE 'fs.usage[%'             THEN 'util'
               WHEN i.key_ LIKE 'fs.used[%'              THEN 'used'
               WHEN i.key_ LIKE 'fs.free[%'              THEN 'free'
               WHEN i.key_ LIKE 'fs.read_kbps[%'         THEN 'read'
               WHEN i.key_ LIKE 'fs.write_kbps[%'        THEN 'write'
               WHEN i.key_ LIKE 'winrm.disk.usedpct[%'   THEN 'util'
               WHEN i.key_ LIKE 'winrm.disk.usedmb[%'    THEN 'used'
               WHEN i.key_ LIKE 'winrm.disk.sizemb[%'    THEN 'size'
               WHEN i.key_ LIKE 'winrm.disk.readkbps[%'  THEN 'read'
               WHEN i.key_ LIKE 'winrm.disk.writekbps[%' THEN 'write'
           END AS role,
           CASE
               WHEN i.key_ LIKE 'fs.usage[%'            THEN 3
               WHEN i.key_ LIKE 'fs.used[%'             THEN 3
               WHEN i.key_ LIKE 'fs.free[%'             THEN 3
               WHEN i.key_ LIKE 'winrm.disk.usedmb[%'   THEN 3
               ELSE 0
           END AS vtype
    FROM   zabbix_items i
    JOIN   agentless_hosts ah ON ah.hostid = i.hostid
    WHERE  i.key_ NOT LIKE '%[{#%'
      AND (   i.key_ LIKE 'fs.usage[%'
           OR i.key_ LIKE 'fs.used[%'
           OR i.key_ LIKE 'fs.free[%'
           OR i.key_ LIKE 'fs.read_kbps[%'
           OR i.key_ LIKE 'fs.write_kbps[%'
           OR i.key_ LIKE 'winrm.disk.usedpct[%'
           OR i.key_ LIKE 'winrm.disk.usedmb[%'
           OR i.key_ LIKE 'winrm.disk.sizemb[%'
           OR i.key_ LIKE 'winrm.disk.readkbps[%'
           OR i.key_ LIKE 'winrm.disk.writekbps[%')
),

-- Zaman serisi rolleri (util/used/read/write). Kapasite BURADA DEGIL.
-- NRT kullanim icin INTERVAL'i kucultun: '1 day' yerine '15 minutes' →
-- islenen satir sayisi ~100x duser, sorgu saniyeler icinde biter.
hist AS MATERIALIZED (
    SELECT di.os_kind, di.hostid, di.node_fqdn, di.node_ipv4_address,
           di.mount_key, di.role,
           h.clock,
           h.value::double precision AS value
    FROM   disk_items di
    JOIN   zabbix_history h
           ON  h.itemid = di.itemid
           AND h.value_type = di.vtype
    WHERE  h.value_type IN (0, 3)
      AND  di.role IN ('util','used','read','write')
      AND  h.clock >= EXTRACT(EPOCH FROM NOW() - INTERVAL '1 day')::bigint
),

-- KAPASITE: mount basina bilinen SON deger (saatlik toplandigi icin clock
-- esitligi aranmaz). Kapasite item'i basina tek index seek.
capacity_raw AS MATERIALIZED (
    SELECT di.os_kind, di.hostid, di.mount_key, di.role,
           l.value::double precision AS value
    FROM   disk_items di
    CROSS  JOIN LATERAL (
        SELECT h.value
        FROM   zabbix_history h
        WHERE  h.itemid = di.itemid
          AND  h.value_type IN (0, 3)
        ORDER  BY h.clock DESC
        LIMIT  1
    ) l
    WHERE  di.role IN ('free','size')
),
capacity AS (
    SELECT os_kind, hostid, mount_key,
           max(value) FILTER (WHERE role = 'free') AS free_mb,
           max(value) FILTER (WHERE role = 'size') AS size_mb
    FROM   capacity_raw
    GROUP  BY os_kind, hostid, mount_key
),

-- SURUCU SATIRLAR: util zorunlu; used ayni cagridan geldigi icin ayni clock'ta.
-- bucket = 5 dakikalik zaman kovasi (300 sn). Toplama araliginiz farkliysa
-- 300 degerini ona gore degistirin.
driver AS MATERIALIZED (
    SELECT os_kind, hostid, node_fqdn, node_ipv4_address, mount_key, clock,
           (clock / 300)                           AS bucket,
           max(value) FILTER (WHERE role = 'util') AS disk_util_pct,
           max(value) FILTER (WHERE role = 'used') AS disk_used_mb
    FROM   hist
    WHERE  role IN ('util','used')
    GROUP  BY os_kind, hostid, node_fqdn, node_ipv4_address, mount_key, clock
    HAVING count(*) FILTER (WHERE role = 'util') > 0
),

-- IO: ayni kovaya indirgenmis read/write. Window fonksiyonu YOK, siralama YOK.
-- Bu CTE window'lu surumdeki probe/scan/asof zincirinin TAMAMININ yerine gecer.
io_bucket AS MATERIALIZED (
    SELECT os_kind, hostid, mount_key,
           (clock / 300)                            AS bucket,
           max(value) FILTER (WHERE role = 'read')  AS read_kbps,
           max(value) FILTER (WHERE role = 'write') AS write_kbps
    FROM   hist
    WHERE  role IN ('read','write')
    GROUP  BY os_kind, hostid, mount_key, (clock / 300)
)

SELECT
    TO_TIMESTAMP(d.clock)                          AS transaction_datetime,
    d.node_fqdn                                    AS node_fqdn,
    d.node_ipv4_address                            AS node_ipv4_address,
    d.mount_key                                    AS dir_name,
    d.disk_util_pct                                AS disk_util_pct,
    -- Kova bossa bir onceki kovaya dus (kova sinirina denk gelen ornekler icin)
    COALESCE(io.read_kbps,  io_prev.read_kbps)     AS read_kbps,
    COALESCE(io.write_kbps, io_prev.write_kbps)    AS write_kbps,
    d.disk_used_mb                                 AS disk_used_mb,
    CASE WHEN d.os_kind = 'unix'
         THEN d.disk_used_mb + c.free_mb
         ELSE c.size_mb
    END                                            AS disk_capacity_mb
FROM   driver d
LEFT   JOIN capacity c
       ON  c.os_kind = d.os_kind AND c.hostid = d.hostid AND c.mount_key = d.mount_key
LEFT   JOIN io_bucket io
       ON  io.os_kind = d.os_kind AND io.hostid = d.hostid
       AND io.mount_key = d.mount_key AND io.bucket = d.bucket
LEFT   JOIN io_bucket io_prev
       ON  io_prev.os_kind = d.os_kind AND io_prev.hostid = d.hostid
       AND io_prev.mount_key = d.mount_key AND io_prev.bucket = d.bucket - 1
-- ORDER BY milyonlarca satirda TAM BIR SIRALAMA demektir. BI aracina veri
-- aktariyorsaniz siralamayi orada yapin ve asagidaki satiri KALDIRIN.
ORDER BY node_fqdn, dir_name, transaction_datetime DESC;
