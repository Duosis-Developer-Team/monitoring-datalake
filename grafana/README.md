# Grafana — datalake görselleştirme

Bu klasör, PostgreSQL datalake'teki toplanan metrikleri görselleştirmek için
**import edilebilir** Grafana dosyaları içerir. Deployment yapmaz; kurulumu sen
yaparsın.

```
grafana/
├── datasource.example.yaml          # PostgreSQL datasource (provisioning örneği)
├── dashboards/
│   ├── host_overview_zabbix.json    # Birincil: host seç → Zabbix metrikleri
│   └── obm_agent_node.json          # OBM agent node → CPU/Mem/disk/fs/net
└── README.md
```

## Kurulum (3 adım)

### 1. Datasource ekle
`datasource.example.yaml` içindeki `<...>` placeholder'larını doldur ve Grafana'ya
ekle (provisioning dizini ya da UI > Connections > Data sources > PostgreSQL).

> **UID önemli:** datasource'un UID'i `datalake-postgres` olmalı. Dashboard'lar
> datasource'a bu UID üzerinden referans verir. UI'dan eklerken UID alanını
> elle `datalake-postgres` gir. Farklı bir UID kullanırsan, import sırasında her
> dashboard'ın `${datasource}` değişkeninden doğru datasource'u seçmen yeterli.

Bağlantı bilgileri datalake DB'si ile aynıdır (Airflow `pg_writer_conn_id`
connection'ı ile aynı host/db). `SELECT` yetkisi olan bir kullanıcı yeterli.

### 2. Yardımcı view'leri oluştur
Dashboard'lar (özellikle Zabbix olanı) `sql/06_grafana_views.sql` içindeki
view'lere dayanır. Bir kez çalıştır:

```bash
psql -h <host> -d <db> -U <user> -f ../sql/06_grafana_views.sql
```

> OBM dashboard'ı doğrudan `opsb_agent_*` tablolarını okur, view gerektirmez.

### 3. Dashboard'ları import et
Grafana UI > Dashboards > New > Import > Upload JSON > `dashboards/*.json`.
Sorulursa datasource olarak **Datalake PostgreSQL**'i seç.

## Önce keşif (önerilir)

Canlı veriye göre dashboard'ları ayarlamak için önce profil sorgularını çalıştır:

```bash
psql -h <host> -d <db> -U <user> -f ../sql/discovery/datalake_profile.sql
```

Bu sana şunu söyler: hangi tablolar dolu, veri ne kadar taze, **hangi item'lar
numerik** (yalnızca `value_type` 0/3 grafiklenebilir), her host'ta kaç metrik var,
ve Zabbix host'ları ile OBM node'ları arasında bir kimlik köprüsü olup olmadığı.

## Dashboard'lar

### host_overview_zabbix (birincil)
İş akışı: **`Host` değişkeninden bir host seç** (envanterden gelir) → o host'ta
toplanan item'lar `Item` çoklu-seçim değişkenine dolar → seçtiklerin zaman serisi
çizilir. Ek paneller: host bilgi tablosu, host'taki tüm item'ların son değerleri,
toplama hacmi (veri akıyor mu).

- Zaman: `clock` (unix sn) → `to_timestamp`. Değer: `value` TEXT → güvenli
  `value_num` (yalnızca `value_type` 0=float / 3=unsigned, regex guard'lı).
- Metin/log item'ları (value_type 1/2/4) grafikte çizilmez; "Son Değerler"
  tablosunda ham metin olarak görünür.

### obm_agent_node
`Node` değişkeninden bir OBM node seç → CPU & Mem util %, disk util (cihaz başına),
filesystem doluluk (mount başına), network throughput (arayüz başına in/out).
`opsb_agent_*` tabloları zaten tipli (DOUBLE PRECISION) olduğu için cast gerekmez.

## Önemli: kimlik uzayları ayrıdır

Zabbix host kimliği (`hostid`) ile OBM agent node kimliği (`node_short_name` /
`node_ipv4_address`) **farklı kimlik uzaylarıdır**; otomatik bir bağ yoktur. Bu
yüzden iki ayrı dashboard var. İkisini tek host görünümünde birleştirmek
istiyorsan, önce `discovery` sorgusunun 7. bloğuyla IP/isim eşleşmesi olduğunu
doğrula, sonra `sql/06_grafana_views.sql` içindeki yorumlu `v_host_unified`
view'ini aç.
