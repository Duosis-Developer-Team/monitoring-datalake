"""
DAG: zabbix_history_collector
─────────────────────────────────────────────────────────────────────
Zabbix 7.x history (zaman serisi) verisini toplar.

Akış:
  1. item.get → tüm item'ların value_type haritasını çıkar
  2. Her value_type (0,1,2,3,4) için ayrı history.get çağrısı yap
  3. Pencere: [data_interval_start - overlap, data_interval_end]
     overlap kaçak/sınır kayıplarını önler, duplicate'i writer ON CONFLICT temizler
  4. Tüm değerleri tek dikey tabloya (zabbix_history) yazmak üzere
     pending/ altına COPY + conflict_target metadata'sıyla JSON bırak

Hedef tablo (writer otomatik oluşturur):
  zabbix_history (
      itemid      BIGINT,
      hostid      BIGINT,
      clock       BIGINT,      -- unix timestamp (saniye)
      ns          INTEGER,     -- nanosaniye
      value       TEXT,        -- her value_type tek sütunda
      value_type  SMALLINT,    -- 0=float 1=char 2=log 3=unsigned 4=text
      PRIMARY KEY (itemid, clock, ns)
  )

value_type'ı (itemid, clock, ns) ile birlikte benzersizdir → duplicate olmaz.

Airflow Variables:
  staging_folder_path        → NFS kök dizini
  zabbix_history_schedule    → Zamanlama (default: */5 * * * *)
  zabbix_history_overlap_sec → Pencere örtüşmesi saniye (default: 60)
  zabbix_history_chunk       → item.get sayfalama (default: 1000)

Airflow Connection:
  conn_id : zabbix_api_conn  (collector ile aynı)
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

import requests

# ─── Airflow v2 / v3 uyumlu import'lar ───────────────────────────────────────
try:
    from airflow.sdk import DAG, Variable, task            # Airflow 3.x
    from airflow.sdk.bases.hook import BaseHook
except ImportError:
    from airflow import DAG                                # Airflow 2.x
    from airflow.decorators import task
    from airflow.models import Variable
    from airflow.hooks.base import BaseHook

def _var(key: str, default: str = "") -> str:
    try:
        return Variable.get(key, default=default)
    except TypeError:
        return Variable.get(key, default_var=default)

# ─── Variables ────────────────────────────────────────────────────────────────
BASE_DIR     = _var("staging_folder_path", "/opt/airflow/dags/data_staging/zabbix")
PENDING_DIR  = os.path.join(BASE_DIR, "pending")
SCHEDULE     = _var("zabbix_history_schedule", "*/5 * * * *")
OVERLAP_SEC  = int(_var("zabbix_history_overlap_sec", "60"))
ITEM_CHUNK   = int(_var("zabbix_history_chunk", "1000"))

# Zabbix value_type kodları → history.get "history" parametresi
#   0 = numeric float
#   1 = character
#   2 = log
#   3 = numeric unsigned
#   4 = text
VALUE_TYPES = [0, 1, 2, 3, 4]

# Writer DAG için metadata — zaman serisi, COPY + idempotent
HISTORY_META = {
    "table":  "zabbix_history",
    "method": "copy",                         # staging + ON CONFLICT DO NOTHING
    "conflict_target": ["itemid", "clock", "ns"],
    "column_types": {
        "itemid":     "BIGINT",
        "hostid":     "BIGINT",
        "clock":      "BIGINT",
        "ns":         "INTEGER",
        "value":      "TEXT",
        "value_type": "SMALLINT",
    },
    "add_updated_at": False,                   # append-only, updated_at gereksiz
    "json_columns": [],
    "source": "zabbix_history_collector",
}

default_args = {
    "owner": "data_engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="zabbix_history_collector",
    default_args=default_args,
    schedule=SCHEDULE,
    start_date=datetime(2024, 1, 1),
    catchup=False,                             # Analiz aşaması: geçmişi doldurma
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    tags=["zabbix", "history", "timeseries"],
    doc_md=__doc__,
) as dag:

    # ─── TASK 1: Auth ─────────────────────────────────────────────────────────
    @task
    def get_auth_token() -> dict:
        conn = BaseHook.get_connection("zabbix_api_conn")
        if not conn.login:
            return {"url": conn.host, "token": conn.password}
        r = requests.post(
            conn.host,
            json={"jsonrpc": "2.0", "method": "user.login",
                  "params": {"username": conn.login, "password": conn.password}, "id": 1},
            timeout=(10, 30),
        )
        r.raise_for_status()
        res = r.json()
        if "error" in res:
            raise ValueError(f"Zabbix auth hatası: {res['error']}")
        token = res.get("result")
        if not token:
            raise ValueError("Token alınamadı.")
        return {"url": conn.host, "token": token}

    # ─── TASK 2: Item → value_type haritası ──────────────────────────────────
    @task
    def fetch_item_map(auth: dict) -> dict:
        """
        Tüm aktif item'ları çeker, value_type'a göre gruplar.
        Döndürür: {value_type: [itemid, ...]} ve {itemid: hostid}
        """
        url, token = auth["url"], auth["token"]
        TIMEOUT = (10, 90)

        def zpost(payload):
            r = requests.post(url, json=payload, timeout=TIMEOUT)
            r.raise_for_status()
            res = r.json()
            if "error" in res:
                raise RuntimeError(f"Zabbix API: {res['error']}")
            return res

        # Toplam item sayısı
        # Not: status=0 (aktif) item'lar. webitems=True web senaryosu item'larını da dahil eder.
        # filter yerine düz status kullanımı bazı Zabbix sürümlerinde daha tutarlı.
        _cnt = zpost({
            "jsonrpc": "2.0", "method": "item.get",
            "params": {"output": ["itemid"], "countOutput": True,
                       "webitems": True, "monitored": True},
            "auth": token, "id": 0,
        }).get("result", "0")
        total = len(_cnt) if isinstance(_cnt, list) else int(_cnt)
        print(f"[item.get] Toplam monitored item: {total}")
        if total == 0:
            return {"by_type": {}, "host_of": {}}

        # Sayfalama ile tüm item'ları çek
        by_type: dict[str, list] = {str(vt): [] for vt in VALUE_TYPES}
        host_of: dict[str, str] = {}
        seen_items: set = set()      # aynı itemid'yi iki kez ekleme
        offset = 0
        while offset < total:
            batch = zpost({
                "jsonrpc": "2.0", "method": "item.get",
                "params": {
                    "output": ["itemid", "hostid", "value_type"],
                    "webitems": True,
                    "monitored": True,       # sadece aktif host'lardaki aktif item'lar
                    "limit": ITEM_CHUNK, "offset": offset,
                },
                "auth": token, "id": 1,
            }).get("result", [])
            if not batch:
                break
            for it in batch:
                iid = it["itemid"]
                if iid in seen_items:        # duplicate item koruması
                    continue
                seen_items.add(iid)
                vt = str(it.get("value_type", "0"))
                if vt in by_type:
                    by_type[vt].append(iid)
                    host_of[iid] = it["hostid"]
            offset += len(batch)
            print(f"[item.get] offset={offset}/{total}, benzersiz item={len(seen_items)}")

        for vt, ids in by_type.items():
            print(f"   value_type {vt}: {len(ids)} item")
        return {"by_type": by_type, "host_of": host_of}

    # ─── TASK 3: History çek + pending dosyası yaz ───────────────────────────
    @task
    def fetch_history(auth: dict, item_map: dict, **kwargs) -> None:
        """
        Her value_type için history.get ile pencere içindeki değerleri çeker.
        Pencere: [data_interval_start - OVERLAP, data_interval_end]
        Tüm değerleri tek listede toplar, pending/ altına yazar.
        """
        url, token = auth["url"], auth["token"]
        TIMEOUT = (10, 120)

        by_type = item_map.get("by_type", {})
        host_of = item_map.get("host_of", {})
        if not host_of:
            print("[WARN] İşlenecek item yok.")
            return

        # ── Pencere hesabı ───────────────────────────────────────────────────
        # Airflow data_interval kullan; yoksa (manuel tetikleme) son 5 dk
        di_start = kwargs.get("data_interval_start")
        di_end   = kwargs.get("data_interval_end")
        if di_start and di_end:
            time_from = int(di_start.timestamp()) - OVERLAP_SEC
            time_till = int(di_end.timestamp())
        else:
            now = int(datetime.now(timezone.utc).timestamp())
            time_from = now - 300 - OVERLAP_SEC
            time_till = now

        print(f"[history] Pencere: {time_from} → {time_till} "
              f"({datetime.fromtimestamp(time_from, timezone.utc)} → "
              f"{datetime.fromtimestamp(time_till, timezone.utc)})")
        print(f"[history] Overlap: {OVERLAP_SEC}s")

        def zpost(payload):
            r = requests.post(url, json=payload, timeout=TIMEOUT)
            r.raise_for_status()
            res = r.json()
            if "error" in res:
                raise RuntimeError(f"Zabbix API: {res['error']}")
            return res

        all_records: list = []
        seen_keys: set = set()      # (itemid, clock, ns) — kaynakta dedup

        # ── Her value_type için ayrı history.get ─────────────────────────────
        for vt_str, item_ids in by_type.items():
            if not item_ids:
                continue
            vt = int(vt_str)

            # itemid listesi büyükse parçala (Zabbix payload limiti)
            for i in range(0, len(item_ids), 5000):
                chunk_ids = item_ids[i: i + 5000]
                res = zpost({
                    "jsonrpc": "2.0", "method": "history.get",
                    "params": {
                        "output": "extend",
                        "history": vt,                  # value_type
                        "itemids": chunk_ids,
                        "time_from": time_from,
                        "time_till": time_till,
                        "sortfield": "clock",
                        "sortorder": "ASC",
                    },
                    "auth": token, "id": 2,
                }).get("result", [])

                for row in res:
                    itemid = row.get("itemid")
                    clock  = row.get("clock")
                    ns     = row.get("ns", "0")
                    key    = (itemid, clock, ns)
                    if key in seen_keys:          # aynı (itemid,clock,ns) bir kez
                        continue
                    seen_keys.add(key)
                    all_records.append({
                        "itemid":     itemid,
                        "hostid":     host_of.get(itemid),
                        "clock":      clock,
                        "ns":         ns,
                        "value":      row.get("value"),
                        "value_type": vt,
                    })

            print(f"[history] value_type {vt}: toplam {len(all_records)} kayıt (kümülatif)")

        if not all_records:
            print("[INFO] Pencerede veri yok, pending dosyası yazılmadı.")
            return

        # ── Pending dosyası ──────────────────────────────────────────────────
        os.makedirs(PENDING_DIR, exist_ok=True)
        safe_run = (
            kwargs["run_id"].replace(":", "_").replace("+", "_").replace("/", "_")
        )
        fname = f"zabbix_history_collector_{safe_run}.json"
        fpath = os.path.join(PENDING_DIR, fname)

        payload = {
            "meta": {**HISTORY_META,
                     "window_from": time_from, "window_till": time_till},
            "data": all_records,
        }
        with open(fpath, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False)

        print(f"[OK] {len(all_records)} history kaydı yazıldı.")
        print(f"[OK] Dosya: {fpath}")

    # ─── Pipeline ────────────────────────────────────────────────────────────
    auth     = get_auth_token()
    item_map = fetch_item_map(auth)
    fetch_history(auth, item_map)
