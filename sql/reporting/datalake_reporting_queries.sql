-- ============================================================================
-- DATALAKE RAPORLAMA SORGULARI (yalnizca ZABBIX verisi) — v3
-- ----------------------------------------------------------------------------
-- ONCE sql/reporting/00_reporting_indexes.sql calistirilmali (index + ANALYZE).
-- Sorgudan once oturumda:  SET work_mem = '256MB';
--
-- v3 DEGISIKLIKLERI (v2 = "optimize edilmis surum" uzerine):
--   1) KAPASITE NULL SORUNU DUZELTILDI. Ayrintili aciklama asagida.
--   2) zabbix_history'ye giren her join'e ACIK `value_type IN (0,3)` eklendi →
--      00_reporting_indexes.sql'deki kismi covering index kullanilabilsin.
--
-- ── KAPASITE NEDEN BAZEN GELMIYORDU (kok neden) ─────────────────────────────
-- v2'deki `driver` CTE'si kapasite bilesenlerini util ile AYNI `clock` degerine
-- gore topluyordu:
--       GROUP BY ..., clock
--       max(value) FILTER (WHERE role = 'free') AS free_mb
-- Bu, "kapasite ile doluluk AYNI SANIYEDE olculmus olmali" demek.
-- Ama Zabbix'te:
--       fs.usage / winrm.disk.usedpct   → 5 dakikada bir
--       fs.free  / winrm.disk.sizemb    → SAATTE BIR
-- Iki item ayni saniyeye neredeyse hic denk gelmez → `free_mb` / `size_mb`
-- satirlarin cogunda NULL → disk_capacity_mb NULL. Yani evet, tespitin dogru:
-- sebep kapasitenin saatte bir toplanmasi.
--
-- DOGRU SEMANTIK: kapasite YAVAS DEGISEN bir niteliktir; "ayni anda olculmus
-- ornek" degil, "bilinen en son deger" istenir. Asagida kapasite ayri bir
-- CTE'de item basina son degerden okunuyor (CPU sorgusundaki core/memtot ile
-- ayni desen). Bu hem NULL'lari bitirir hem de daha ucuzdur: her kapasite
-- item'i icin tek index seek — util satirlariyla eslestirme yapilmaz.
--
-- NOT: `used` (fs.used / winrm.disk.usedmb) util ile AYNI SSH/WinRM cagrisindan
-- geldigi icin clock'lari zaten esittir; o yuzden `used` tam clock eslesmesinde
-- birakildi (dogru ve en ucuz yol).
-- ============================================================================


-- ############################################################################
-- 1) CPU / MEMORY RAPORLAMA - AGENTLESS_GROUP
-- ############################################################################
WITH agentless_hosts AS MATERIALIZED (
    SELECT inv.hostid,                      -- TEXT
           inv.name AS node_fqdn
    FROM   zabbix_inventory inv
    WHERE  inv.host_groups @> '["AGENTLESS_GROUP"]'::jsonb
      AND  inv.status = 'Enabled'
),

target_items AS MATERIALIZED (
    SELECT i.itemid::bigint AS itemid_num,  -- cast KUCUK tarafta: history index'i korunur
           i.hostid,
           CASE
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
           END AS role
    FROM   zabbix_items i
    JOIN   agentless_hosts ah ON ah.hostid = i.hostid
    -- CASE'in eslesebilecegi tum anahtarlarin ust kumesi (sonucu daraltmaz)
    WHERE  (i.key_ LIKE 'ssh.run[%' OR i.key_ LIKE 'winrm_metric.py[%')
),

items_used AS MATERIALIZED (
    SELECT * FROM target_items WHERE role IS NOT NULL
),

-- ---- cpu + mem yuzdeleri: zabbix_history'ye TEK giris ----
pct_hist AS MATERIALIZED (
    SELECT ti.hostid,
           ti.role,
           h.clock,
           h.value::double precision AS value
    FROM   items_used ti
    JOIN   zabbix_history h
           ON  h.itemid = ti.itemid_num
    -- value_type = 0 (FLOAT) sabiti, kismi index predicate'i IN (0,3)'u
    -- planner tarafindan zaten kapsanir → ayrica IN yazmaya gerek yok.
    WHERE  h.value_type = 0
      AND  ti.role IN ('cpu','mem')
      AND  h.clock >= EXTRACT(EPOCH FROM NOW() - INTERVAL '1 day')::bigint
),

-- ---- CPU ornegine EN YAKIN MEM ornegi (ABS + LIMIT 1 yerine window) ----
probe AS (
    SELECT hostid, clock,
           0 AS is_mem,
           value                  AS cpu_value,
           NULL::double precision AS mem_value,
           NULL::bigint           AS mem_clock
    FROM   pct_hist
    WHERE  role = 'cpu'
    UNION ALL
    SELECT hostid, clock,
           1,
           NULL::double precision,
           value,
           clock
    FROM   pct_hist
    WHERE  role = 'mem'
),

-- Ayni clock'ta MEM satiri once gelsin (is_mem DESC) -> mesafe 0 dogru yakalanir
scan AS (
    SELECT p.*,
           count(mem_value) OVER (PARTITION BY hostid
                                  ORDER BY clock ASC,  is_mem DESC
                                  ROWS UNBOUNDED PRECEDING) AS grp_prev,
           count(mem_value) OVER (PARTITION BY hostid
                                  ORDER BY clock DESC, is_mem DESC
                                  ROWS UNBOUNDED PRECEDING) AS grp_next
    FROM   probe p
),

asof AS (
    SELECT hostid, clock, is_mem, cpu_value,
           first_value(mem_value) OVER w_prev AS prev_value,
           first_value(mem_clock) OVER w_prev AS prev_clock,
           first_value(mem_value) OVER w_next AS next_value,
           first_value(mem_clock) OVER w_next AS next_clock
    FROM   scan
    WINDOW w_prev AS (PARTITION BY hostid, grp_prev ORDER BY clock ASC,  is_mem DESC),
           w_next AS (PARTITION BY hostid, grp_next ORDER BY clock DESC, is_mem DESC)
),

cpu_with_mem AS (
    SELECT hostid, clock, cpu_value,
           CASE
               WHEN prev_clock IS NULL THEN next_value
               WHEN next_clock IS NULL THEN prev_value
               WHEN (clock - prev_clock) <= (next_clock - clock) THEN prev_value
               ELSE next_value
           END AS mem_value
    FROM   asof
    WHERE  is_mem = 0
      -- orijinaldeki INNER LATERAL: hic mem ornegi yoksa CPU satiri dusmeli
      AND  (prev_clock IS NOT NULL OR next_clock IS NOT NULL)
),

-- ============================================================================
-- KAPASITE (CpuCore / MemoryCapacity) — "bilinen en son deger"
-- ----------------------------------------------------------------------------
-- HEDEF: her satirda, o host icin BILINEN EN GUNCEL kapasite degeri gorunsun.
-- Kapasite host basina TEK satira indirgenir ve LEFT JOIN ile o host'un TUM
-- satirlarina yayilir → host'un tek bir kapasite ornegi bile varsa, hicbir
-- satiri bos kalmaz.
--
-- Onceki surumde kapasiteyi sessizce sifirlayabilecek IKI risk vardi;
-- ikisi de burada kaldirildi:
--
--   1) value_type varsayimi. Onceki surum `value_type IN (0,3)` filtreliyordu.
--      Item aslinda character/text tipindeyse (ssh.run script ciktilari sik sik
--      oyledir) satir DONMEZ ve kapasite sessizce NULL kalir. Artik tip
--      filtresi YOK; deger zaten TEXT saklaniyor, sayisal olup olmadigi
--      regex ile guvenli sekilde kontrol ediliyor.
--
--   2) Dar key eslesmesi. Onceki surum role'u tam string esitligine dayanan
--      CASE'den aliyordu; key imzasina bir parametre eklense kapasite komple
--      duserdi. Artik kapasite item'lari GENIS LIKE ile ayrica bulunuyor,
--      yukaridaki CASE'e bagimli degil.
--
-- Zaman filtresi YOK: kapasite yavas degisen bir niteliktir, son deger
-- ne kadar eski olursa olsun gecerlidir. Ne kadar eski oldugunu gormek icin
-- asagida *_olcum_zamani kolonlari da donduruluyor.
-- ============================================================================

-- Kapasite item'lari — GENIS eslesme (key formati surprizlerine dayanikli)
capacity_items AS MATERIALIZED (
    SELECT i.itemid::bigint AS itemid_num,
           i.hostid,
           CASE WHEN i.key_ LIKE '%cpu_core_count%' THEN 'core'
                WHEN i.key_ LIKE '%mem_total_mb%'   THEN 'memtot'
           END AS role
    FROM   zabbix_items i
    JOIN   agentless_hosts ah ON ah.hostid = i.hostid
    WHERE  i.key_ LIKE '%cpu_core_count%'
       OR  i.key_ LIKE '%mem_total_mb%'
),

-- Item basina SON deger — tek index seek (itemid, clock DESC)
capacity_last AS MATERIALIZED (
    SELECT ci.hostid,
           ci.role,
           l.clock,
           -- guvenli sayisal cast: bozuk/metin deger tum sorguyu dusurmesin
           CASE WHEN l.value ~ '^-?[0-9]+(\.[0-9]+)?$'
                THEN l.value::double precision
                ELSE NULL
           END AS value
    FROM   capacity_items ci
    CROSS  JOIN LATERAL (
        SELECT h.clock, h.value
        FROM   zabbix_history h
        WHERE  h.itemid = ci.itemid_num
        ORDER  BY h.clock DESC
        LIMIT  1
    ) l
    WHERE  ci.role IS NOT NULL
),

-- Bir host'ta ayni rolden birden fazla item olabilir (ornegin hem legacy hem
-- standart sablon bagliysa) → en YENI ornegi olan item kazanir.
capacity_pick AS (
    SELECT DISTINCT ON (hostid, role) hostid, role, clock, value
    FROM   capacity_last
    ORDER  BY hostid, role, clock DESC
),

-- Host basina TEK satir
capacity AS (
    SELECT hostid,
           max(value) FILTER (WHERE role = 'core')   AS cpu_core,
           max(clock) FILTER (WHERE role = 'core')   AS cpu_core_clock,
           max(value) FILTER (WHERE role = 'memtot') AS mem_total_mb,
           max(clock) FILTER (WHERE role = 'memtot') AS mem_total_clock
    FROM   capacity_pick
    GROUP  BY hostid
)

SELECT
    TO_TIMESTAMP(c.clock)                AS TransactionDatetime,
    ah.node_fqdn                         AS node_fqdn,
    cap.cpu_core                         AS CpuCore,
    (cap.mem_total_mb / 1024.0)          AS MemoryCapacity,   -- MB -> GB
    c.cpu_value                          AS CpuUtil,
    c.mem_value                          AS MemoryUtil,
    -- Dogrulama kolonlari: kapasite degerinin NE ZAMAN olculdugunu gosterir.
    -- Deger geliyorsa dolu, host'ta hic kapasite ornegi yoksa NULL olur —
    -- boylece "bos mu geliyor, yoksa eski mi" sorusu tahmine kalmaz.
    -- Rapora dahil etmek istemezseniz bu iki satiri silin.
    TO_TIMESTAMP(cap.cpu_core_clock)     AS CpuCore_olcum_zamani,
    TO_TIMESTAMP(cap.mem_total_clock)    AS MemoryCapacity_olcum_zamani
FROM   cpu_with_mem c
JOIN   agentless_hosts ah ON ah.hostid  = c.hostid
LEFT   JOIN capacity cap  ON cap.hostid = c.hostid
ORDER BY node_fqdn, TransactionDatetime DESC;


-- ── Kapasite doluluk kontrolu ───────────────────────────────────────────────
-- Yukaridaki sorgunun sonucunda kapasitenin gercekte kac satirda dolu
-- geldigini olcer. "Bos gorunuyor" izlenimini sayiya cevirir.
-- Sorgunun WITH blogunu buraya da kopyalayip su sekilde bitirebilirsiniz:
--
--   SELECT count(*)                                              AS toplam_satir,
--          count(cap.cpu_core)                                   AS cpucore_dolu,
--          count(cap.mem_total_mb)                               AS memcap_dolu,
--          round(100.0*count(cap.cpu_core)/nullif(count(*),0),1) AS cpucore_yuzde,
--          count(DISTINCT c.hostid)                              AS toplam_host,
--          count(DISTINCT c.hostid) FILTER (WHERE cap.cpu_core IS NOT NULL)
--                                                                AS kapasiteli_host
--   FROM   cpu_with_mem c
--   LEFT   JOIN capacity cap ON cap.hostid = c.hostid;


-- ############################################################################
-- 2) FILESYSTEM / DISK RAPORLAMA - AGENTLESS_GROUP
-- ############################################################################
WITH agentless_hosts AS MATERIALIZED (
    SELECT inv.hostid,
           inv.name       AS node_fqdn,
           inv.primary_ip AS node_ipv4_address
    FROM   zabbix_inventory inv
    WHERE  inv.host_groups @> '["AGENTLESS_GROUP"]'::jsonb
      AND  inv.status = 'Enabled'
),

-- Tum disk item'lari tek CTE'de sinifla: os_kind + role + beklenen value_type
disk_items AS MATERIALIZED (
    SELECT i.itemid::bigint AS itemid,
           ah.hostid,
           ah.node_fqdn,
           ah.node_ipv4_address,
           CASE WHEN i.key_ LIKE 'fs.%' THEN 'unix' ELSE 'win' END AS os_kind,
           -- '[' karakterinden itibaren al: regexp_replace ile birebir ayni sonuc
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
           -- orijinaldeki value_type filtreleri role bazinda korunuyor
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

-- zabbix_history'ye TEK giris. itemid cast'i kucuk tarafta (index korunur).
-- DIKKAT: kapasite rolleri (free/size) artik BURADA DEGIL — asagidaki
-- `capacity` CTE'sinde son degerden okunuyor.
hist AS MATERIALIZED (
    SELECT di.os_kind, di.hostid, di.node_fqdn, di.node_ipv4_address,
           di.mount_key, di.role,
           h.clock,
           h.value::double precision AS value
    FROM   disk_items di
    JOIN   zabbix_history h
           ON  h.itemid = di.itemid
           AND h.value_type = di.vtype
    WHERE  h.value_type IN (0, 3)      -- kismi covering index icin ACIK sart
      AND  di.role IN ('util','used','read','write')
      AND  h.clock >= EXTRACT(EPOCH FROM NOW() - INTERVAL '1 day')::bigint
),

-- ---- KAPASITE: mount basina BILINEN SON deger ----
-- free (unix) ve size (windows) SAATTE BIR toplandigi icin util ile ayni
-- clock'a dusmez → tam clock eslesmesinde NULL kalirdi. Kapasite yavas degisen
-- bir nitelik oldugundan son deger dogru cevaptir. Zaman filtresi YOK: item
-- son 1 gunde ornek uretmemis olsa bile kapasite dolu gelir.
-- Maliyet: kapasite item'i basina TEK index seek.
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

-- SURUCU SATIRLAR: util zorunlu (orijinaldeki INNER JOIN),
-- used ayni clock'ta varsa dolu (util ile AYNI cagridan geldigi icin esittir)
driver AS MATERIALIZED (
    SELECT os_kind, hostid, node_fqdn, node_ipv4_address, mount_key, clock,
           max(value) FILTER (WHERE role = 'util') AS disk_util_pct,
           max(value) FILTER (WHERE role = 'used') AS disk_used_mb
    FROM   hist
    WHERE  role IN ('util','used')
    GROUP  BY os_kind, hostid, node_fqdn, node_ipv4_address, mount_key, clock
    HAVING count(*) FILTER (WHERE role = 'util') > 0
),

-- ---- EN YAKIN CLOCK ESLESMESI (ABS + LIMIT 1 yerine tek siralama) ----
-- read/write ayri bir master item'dan (fs_io.raw) geldigi icin farkli
-- zamanlarda orneklenir → en yakin clock ile eslestirilir.
probe AS (
    SELECT d.os_kind, d.hostid, d.mount_key, r.role, d.clock,
           0 AS is_io,
           NULL::double precision AS io_value,
           NULL::bigint           AS io_clock
    FROM   driver d
    CROSS  JOIN (VALUES ('read'),('write')) AS r(role)
    UNION ALL
    SELECT h.os_kind, h.hostid, h.mount_key, h.role, h.clock,
           1, h.value, h.clock
    FROM   hist h
    WHERE  h.role IN ('read','write')
),

-- Ayni clock'ta IO satiri surucuden once gelsin (is_io DESC) -> mesafe 0 dogru yakalanir
scan AS (
    SELECT p.*,
           count(io_value) OVER (PARTITION BY os_kind, hostid, mount_key, role
                                 ORDER BY clock ASC,  is_io DESC
                                 ROWS UNBOUNDED PRECEDING) AS grp_prev,
           count(io_value) OVER (PARTITION BY os_kind, hostid, mount_key, role
                                 ORDER BY clock DESC, is_io DESC
                                 ROWS UNBOUNDED PRECEDING) AS grp_next
    FROM   probe p
),

asof AS (
    SELECT os_kind, hostid, mount_key, role, clock, is_io,
           first_value(io_value) OVER w_prev AS prev_value,
           first_value(io_clock) OVER w_prev AS prev_clock,
           first_value(io_value) OVER w_next AS next_value,
           first_value(io_clock) OVER w_next AS next_clock
    FROM   scan
    WINDOW w_prev AS (PARTITION BY os_kind, hostid, mount_key, role, grp_prev
                      ORDER BY clock ASC,  is_io DESC),
           w_next AS (PARTITION BY os_kind, hostid, mount_key, role, grp_next
                      ORDER BY clock DESC, is_io DESC)
),

io_match AS (
    SELECT os_kind, hostid, mount_key, clock,
           max(picked) FILTER (WHERE role = 'read')  AS read_kbps,
           max(picked) FILTER (WHERE role = 'write') AS write_kbps
    FROM (
        SELECT os_kind, hostid, mount_key, role, clock,
               CASE
                   WHEN prev_clock IS NULL THEN next_value
                   WHEN next_clock IS NULL THEN prev_value
                   WHEN (clock - prev_clock) <= (next_clock - clock) THEN prev_value
                   ELSE next_value
               END AS picked
        FROM   asof
        WHERE  is_io = 0
    ) x
    GROUP BY os_kind, hostid, mount_key, clock
)

SELECT
    TO_TIMESTAMP(d.clock)     AS transaction_datetime,
    d.node_fqdn               AS node_fqdn,
    d.node_ipv4_address       AS node_ipv4_address,
    d.mount_key               AS dir_name,
    d.disk_util_pct           AS disk_util_pct,
    m.read_kbps               AS read_kbps,
    m.write_kbps              AS write_kbps,
    d.disk_used_mb            AS disk_used_mb,
    CASE WHEN d.os_kind = 'unix'
         THEN d.disk_used_mb + c.free_mb   -- Unix: used + free (free = son deger)
         ELSE c.size_mb                    -- Windows: sizemb (son deger)
    END                       AS disk_capacity_mb
FROM   driver d
LEFT   JOIN capacity c
       ON  c.os_kind   = d.os_kind
       AND c.hostid    = d.hostid
       AND c.mount_key = d.mount_key
LEFT   JOIN io_match m
       ON  m.os_kind   = d.os_kind
       AND m.hostid    = d.hostid
       AND m.mount_key = d.mount_key
       AND m.clock     = d.clock
ORDER BY node_fqdn, dir_name, transaction_datetime DESC;
