# Grafana Görselleştirme Katmanı

Datalake'e (PostgreSQL) landing yapan metrikleri görselleştiren katman. Boru
hattı veriyi toplar ve yazar; bu katman onu **host-merkezli** dashboard'larla
okunabilir kılar. Tüm çıktılar dosyadır (import edilebilir); kurulum
[`grafana/README.md`](../grafana/README.md)'de.

## Mimari

```
collectors / webservice  →  PostgreSQL datalake  →  view'ler  →  Grafana
   (zabbix_*, opsb_*)        (06_grafana_views)      (postgres datasource)
```

Grafana, datalake DB'sine bir PostgreSQL datasource ile bağlanır (datalake'in
kendisiyle aynı bağlantı; salt-okuma yeterli). Zabbix tarafı ham/uzun-format
olduğundan, sorguları sade tutmak için araya `sql/06_grafana_views.sql`
view'leri girer. OBM tarafı zaten tipli olduğundan doğrudan okunur.

## Dosyalar

| Dosya | Rol |
|-------|-----|
| `sql/06_grafana_views.sql` | `v_zabbix_history_readable`, `v_zabbix_host_item_catalog`, (yorumlu) `v_host_unified` |
| `sql/discovery/datalake_profile.sql` | Dashboard öncesi datalake profil/keşif sorguları |
| `grafana/datasource.example.yaml` | PostgreSQL datasource (UID `datalake-postgres`) |
| `grafana/dashboards/host_overview_zabbix.json` | Birincil: host → Zabbix metrikleri |
| `grafana/dashboards/obm_agent_node.json` | OBM node → CPU/Mem/disk/fs/net |

## Tablo → panel haritası

| Kaynak tablo | Üzerinden | Panel |
|--------------|-----------|-------|
| `zabbix_inventory` | `host` değişkeni + bilgi tablosu | Host seçici, Host Bilgisi |
| `zabbix_history` + `zabbix_items` | `v_zabbix_history_readable` | Zaman serisi, Son Değerler, Toplama Hacmi |
| `zabbix_history` (son 7g) | `v_zabbix_host_item_catalog` | `item` çoklu-seçim değişkeni |
| `opsb_agent_node` | doğrudan | Node Bilgisi, CPU & Mem % |
| `opsb_agent_disk` | doğrudan | Disk Util % (cihaz başına) |
| `opsb_agent_filesys` | doğrudan | Filesystem Doluluk % (mount başına) |
| `opsb_agent_netif` | doğrudan | Network Throughput (arayüz başına) |

## Tasarım kararları

- **value_type ve numerik cast.** `zabbix_history.value` TEXT'tir ve tüm
  value_type'lar onun içinde tutulur. Yalnızca `value_type` 0 (float) ve 3
  (unsigned) sayısaldır. `v_zabbix_history_readable.value_num`, bu tipler için ve
  metin gerçekten sayıya benziyorsa (`'^-?[0-9]+(\.[0-9]+)?$'`) cast eder; aksi
  halde NULL — tek bozuk satır paneli düşürmesin diye. Metin/log item'ları (1/2/4)
  grafiklenmez, yalnızca "Son Değerler" tablosunda görünür.
- **Join tip uyumu.** `zabbix_history` id'leri BIGINT, `items`/`inventory` TEXT;
  view'ler `::text` cast'iyle birleştirir.
- **Taşınabilir datasource.** Paneller datasource'a sabit UID yerine
  `${datasource}` değişkeniyle bağlanır; UID `datalake-postgres` varsayılandır.
- **İki ayrı dashboard / kimlik uzayları.** Zabbix `hostid` ile OBM
  `node_short_name` arasında garantili bir bağ yoktur. Birleştirme yalnızca
  IP/isim üzerinden tahminîdir; bu yüzden `v_host_unified` view'i varsayılan
  kapalıdır. Açmadan önce discovery'nin 7. bloğuyla eşleşmeyi doğrula.

## Akış: discovery → somutlaştırma

1. `datasource.example.yaml`'i doldurup datasource'u ekle (UID `datalake-postgres`).
2. `sql/06_grafana_views.sql`'i çalıştır (view'ler).
3. `sql/discovery/datalake_profile.sql`'i çalıştır → tazelik, value_type dağılımı,
   host/node envanteri, kimlik kesişimi.
4. Dashboard'ları import et, `host`/`node` seç.
5. Discovery beklenenden farklıysa (ör. numerik item yok, ya da OBM IP eşleşmesi
   var) ilgili panel sorgusunu / `v_host_unified`'i ona göre ayarla.
