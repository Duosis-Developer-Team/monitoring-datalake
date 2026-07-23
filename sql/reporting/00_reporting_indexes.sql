-- ============================================================================
-- RAPORLAMA SORGULARI ICIN INDEX + TUNING
-- ----------------------------------------------------------------------------
-- NEDEN GEREKLI: sayfalama hatasi duzeltilene kadar zabbix_items ~1000 satir,
-- zabbix_inventory ~580 satir, zabbix_history yalnizca 8 host'un verisiydi.
-- Sorgu "17 saniye" surerken aslinda neredeyse bos bir tabloyu tariyordu.
-- Simdi tablolar gercek buyuklugunde; sorgular degismedi, VERI SETI buyudu.
-- Bu noktadan sonra performansi belirleyen sey index'ler ve istatistiklerdir.
--
-- CALISTIRMA: CREATE INDEX CONCURRENTLY transaction blogu ICINDE calismaz.
-- psql'de tek tek calistirin (DBeaver kullaniyorsaniz autocommit'i acin):
--   psql -h <host> -U <user> -d <db> -f sql/reporting/00_reporting_indexes.sql
-- CONCURRENTLY, tabloyu kilitlemeden index kurar; buyuk zabbix_history'de sart.
-- ============================================================================


-- ── 1) zabbix_history: kapsayici (covering) index ───────────────────────────
-- Her iki rapor sorgusunun erisim deseni ayni:
--     JOIN zabbix_history h ON h.itemid = <item> AND h.value_type IN (0,3)
--     WHERE h.clock >= <epoch>
-- PK (itemid, clock, ns) seek icin yeterli, AMA `value` heap'ten okunur.
-- Milyonlarca satirda asil maliyet bu heap erisimidir. INCLUDE (value) ile
-- index-only scan mumkun olur → heap'e hic gidilmez.
--
-- WHERE value_type IN (0,3) kismi index: char/log/text item'lari disarida
-- birakir → index cok daha kucuk. Sorguda da ACIKCA `value_type IN (0,3)`
-- yazmalisiniz, aksi halde planner bu kismi index'i kullanamaz.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_zh_itemid_clock_numeric
    ON zabbix_history (itemid, clock)
    INCLUDE (value, value_type)
    WHERE value_type IN (0, 3);

-- PK (itemid, clock, ns) zaten (itemid, clock) prefix'ini kapsiyor → bu index
-- gereksiz. Dusurmek her INSERT'ten bir index bakimi eksiltir (writer'i da
-- hizlandirir).
DROP INDEX CONCURRENTLY IF EXISTS idx_zabbix_history_itemid_clock;


-- ── 2) zabbix_items: LIKE 'prefix%' icin text_pattern_ops ───────────────────
-- Mevcut idx_zabbix_items_key (varsayilan collation) `key_ LIKE 'fs.usage[%'`
-- gibi prefix aramalarinda KULLANILAMAZ. text_pattern_ops bunu mumkun kilar.
-- hostid ile birlestirmek join + filtre'yi tek index'te toplar.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_zabbix_items_hostid_key_pattern
    ON zabbix_items (hostid, key_ text_pattern_ops);


-- ── 3) Istatistikler ────────────────────────────────────────────────────────
-- Veri hacmi aniden 100x buyudugu icin planner'in istatistikleri BAYAT.
-- Bayat istatistik = yanlis plan (nested loop yerine hash join vs.) = dakikalar.
-- Index'lerden once bunu calistirip sorguyu tekrar olcmek bile buyuk fark
-- yaratabilir.
ANALYZE zabbix_history;
ANALYZE zabbix_items;
ANALYZE zabbix_inventory;

-- zabbix_history cok yuksek insert hizina sahip; varsayilan autovacuum esikleri
-- (0.1 = tablonun %10'u) bu boyutta cok gec tetiklenir → istatistik surekli
-- bayat kalir. Esikleri dusur:
ALTER TABLE zabbix_history SET (
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_vacuum_scale_factor  = 0.02
);


-- ============================================================================
-- 4) SORGU OTURUMU AYARLARI (raporlama sorgusundan ONCE calistirin)
-- ----------------------------------------------------------------------------
-- Rapor sorgulari window fonksiyonlariyla buyuk siralamalar yapiyor
-- (probe/scan/asof). work_mem yetersizse bu siralamalar DISKE tasar ve sorgu
-- dakikalara cikar. EXPLAIN (ANALYZE, BUFFERS) ciktisinda "external merge
-- Disk: ... kB" gorurseniz sebep budur.
--
--   SET work_mem = '256MB';                  -- oturum bazinda, kalici degil
--   SET max_parallel_workers_per_gather = 4; -- paralel tarama
--
-- work_mem BAGLANTI x SIRALAMA basina ayrilir; global postgresql.conf'ta yuksek
-- deger vermeyin, yalnizca raporlama oturumunda SET edin.
-- ============================================================================


-- ============================================================================
-- 5) SONRAKI ADIM: PARTITIONING (bu hacimde artik opsiyonel degil)
-- ----------------------------------------------------------------------------
-- Index'ler tek basina zabbix_history'nin surekli buyumesini cozmez: 1 gunluk
-- sorgu, index uzerinden de olsa giderek buyuyen bir yapiyi geziyor.
-- clock'a gore partitioning ile 1 gunluk sorgu YALNIZCA 1-2 chunk'a dokunur.
-- Ek fayda: retention artik DELETE degil DROP PARTITION (aninda, sisme yok).
--
-- sql/02_zabbix_history.sql icinde TimescaleDB donusum yolu hazir duruyor:
--   SELECT create_hypertable('zabbix_history','clock',
--                            chunk_time_interval => 604800, migrate_data => true);
-- TimescaleDB yoksa native declarative partitioning (RANGE on clock) ayni isi
-- gorur, ama partition'lari elle/otomasyonla olusturmak gerekir.
-- ============================================================================
