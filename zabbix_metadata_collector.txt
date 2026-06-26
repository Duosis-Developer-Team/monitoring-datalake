"""
DAG: zabbix_metadata_collector
─────────────────────────────────────────────────────────────────────
Zabbix 7.x'teki TÜM template ve item'ların id + isim (ve okunabilirlik için
ek alan) bilgisini toplar. İki ayrı tabloya yazmak üzere pending/ altına iki
self-describing JSON dosyası bırakır. PostgreSQL ile ilişkisi yoktur — generic
writer DAG bağımsız çalışır.

Amaç:
  zabbix_history (itemid, value, ...) tablosundaki ham değerleri insan
  tarafından okunabilir hale getirmek. Join zinciri:

    zabbix_history.itemid  →  zabbix_items.itemid     (item adı, key_, units)
    zabbix_items.hostid    →  zabbix_inventory.hostid (host adı, gruplar)
    host'un template'leri  →  zabbix_templates        (kategori/şablon adı)

  Bir sonraki aşamada host kategorisi bazlı Grafana dashboard'ları için temel
  veri budur.

Hedef tablolar (writer otomatik oluşturur; sql/04_zabbix_metadata.sql açıkça
tanımlar):
  zabbix_templates (templateid PK, name, host, description, ...)
  zabbix_items     (itemid PK, hostid, name, key_, value_type,
                    value_type_name, units, templateid, status, ...)

Airflow Variables:
  staging_folder_path        → NFS kök dizini
  zabbix_metadata_schedule   → Zamanlama (default: @hourly)
  zabbix_metadata_chunk      → item.get sayfalama (default: 1000)

Airflow Connection:
  conn_id : zabbix_api_conn  (diğer collector'larla aynı)
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

import requests

# ─── Airflow v2 / v3 uyumlu import'lar ───────────────────────────────────────
try:
    from airflow.sdk import DAG, Variable, task             # Airflow 3.x
    from airflow.sdk.bases.hook import BaseHook
except ImportError:
    from airflow import DAG                                 # Airflow 2.x
    from airflow.decorators import task
    from airflow.models import Variable
    from airflow.hooks.base import BaseHook


def _var(key: str, default: str = "") -> str:
    """Variable.get wrapper — Airflow 2.x (default_var) ve 3.x (default) uyumlu."""
    try:
        return Variable.get(key, default=default)
    except TypeError:
        return Variable.get(key, default_var=default)


# ─── Variables ────────────────────────────────────────────────────────────────
BASE_DIR     = _var("staging_folder_path", "/opt/airflow/dags/data_staging/zabbix")
PENDING_DIR  = os.path.join(BASE_DIR, "pending")
SCHEDULE     = _var("zabbix_metadata_schedule", "@hourly")
CHUNK_SIZE   = int(_var("zabbix_metadata_chunk", "1000"))

# Zabbix value_type kodları → okunabilir isim
VALUE_TYPE_NAMES = {
    "0": "numeric_float",
    "1": "character",
    "2": "log",
    "3": "numeric_unsigned",
    "4": "text",
}

# ─── Writer metadata — mevcut durum, upsert ──────────────────────────────────
TEMPLATES_META = {
    "table":           "zabbix_templates",
    "method":          "upsert",
    "conflict_target": ["templateid"],
    "json_columns":    ["template_groups", "parent_templates"],
    "source":          "zabbix_metadata_collector",
}

ITEMS_META = {
    "table":           "zabbix_items",
    "method":          "upsert",
    "conflict_target": ["itemid"],
    "json_columns":    [],
    "source":          "zabbix_metadata_collector",
}


# ─── Yardımcı fonksiyonlar — DAG bloğu DIŞINDA tanımlanmalı ───────────────────

def _zabbix_session(timeout=(10, 120)):
    """zabbix_api_conn'dan url + token döndürür. Login boşsa Password = API token."""
    conn = BaseHook.get_connection("zabbix_api_conn")
    if not conn.login:
        return conn.host, conn.password
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
    return conn.host, token


def _make_zpost(url: str, token: str, timeout=(10, 120)):
    def zpost(payload):
        r = requests.post(url, json=payload, timeout=timeout)
        r.raise_for_status()
        res = r.json()
        if "error" in res:
            raise RuntimeError(f"Zabbix API: {res['error']}")
        return res
    return zpost


def _write_pending(records: list, meta: dict, run_id: str, label: str) -> None:
    """Veri + metadata'yı tek JSON dosyası olarak pending/ altına yazar."""
    if not records:
        print(f"[INFO] {label}: kayıt yok, dosya yazılmadı.")
        return
    os.makedirs(PENDING_DIR, exist_ok=True)
    safe_run = run_id.replace(":", "_").replace("+", "_").replace("/", "_")
    fname = f"zabbix_metadata_collector_{label}_{safe_run}.json"
    fpath = os.path.join(PENDING_DIR, fname)
    payload = {
        "meta": {**meta, "collected_at": datetime.utcnow().isoformat(timespec="seconds") + "Z"},
        "data": records,
    }
    # Atomik yazım: tmp + rename (writer yarım dosya okumasın)
    tmp = fpath + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False)
    os.replace(tmp, fpath)
    print(f"[OK] {label}: {len(records)} kayıt → {fpath}")


default_args = {
    "owner": "data_engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="zabbix_metadata_collector",
    default_args=default_args,
    schedule=SCHEDULE,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=20),
    tags=["zabbix", "metadata", "templates", "items"],
    doc_md=__doc__,
) as dag:

    # ─── TASK 1: Auth ─────────────────────────────────────────────────────────
    @task
    def get_auth() -> dict:
        url, token = _zabbix_session()
        return {"url": url, "token": token}

    # ─── TASK 2: Template'leri çek ────────────────────────────────────────────
    @task
    def fetch_templates(auth: dict, **kwargs) -> None:
        url, token = auth["url"], auth["token"]
        zpost = _make_zpost(url, token)

        _cnt = zpost({
            "jsonrpc": "2.0", "method": "template.get",
            "params": {"output": ["templateid"], "countOutput": True},
            "auth": token, "id": 0,
        }).get("result", "0")
        total = len(_cnt) if isinstance(_cnt, list) else int(_cnt)
        print(f"[template.get] Toplam template: {total}")

        records, offset = [], 0
        while offset < total:
            batch = zpost({
                "jsonrpc": "2.0", "method": "template.get",
                "params": {
                    "output": ["templateid", "name", "host", "description"],
                    "selectTemplateGroups": ["name"],
                    "selectParentTemplates": ["templateid", "name"],
                    "limit": CHUNK_SIZE, "offset": offset,
                },
                "auth": token, "id": 1,
            }).get("result", [])
            if not batch:
                break
            for t in batch:
                records.append({
                    "templateid":       t.get("templateid"),
                    "name":             t.get("name"),
                    "host":             t.get("host"),           # teknik ad
                    "description":      t.get("description") or "",
                    "template_groups":  [g.get("name") for g in t.get("templategroups", t.get("templateGroups", []))],
                    "parent_templates": [p.get("name") for p in t.get("parentTemplates", [])],
                })
            offset += len(batch)
            print(f"[template.get] offset={offset}/{total}")

        _write_pending(records, TEMPLATES_META, kwargs["run_id"], "templates")

    # ─── TASK 3: Item'ları çek ────────────────────────────────────────────────
    @task
    def fetch_items(auth: dict, **kwargs) -> None:
        url, token = auth["url"], auth["token"]
        zpost = _make_zpost(url, token)

        # webitems=True → web senaryosu item'larını da dahil eder.
        # monitored/templated filtresi YOK → tüm item'lar (host + şablon kaynaklı).
        _cnt = zpost({
            "jsonrpc": "2.0", "method": "item.get",
            "params": {"output": ["itemid"], "webitems": True, "countOutput": True},
            "auth": token, "id": 0,
        }).get("result", "0")
        total = len(_cnt) if isinstance(_cnt, list) else int(_cnt)
        print(f"[item.get] Toplam item: {total}")

        records, offset, seen = [], 0, set()
        while offset < total:
            batch = zpost({
                "jsonrpc": "2.0", "method": "item.get",
                "params": {
                    "output": ["itemid", "hostid", "name", "key_",
                               "value_type", "units", "status", "templateid"],
                    "webitems": True,
                    "limit": CHUNK_SIZE, "offset": offset,
                },
                "auth": token, "id": 1,
            }).get("result", [])
            if not batch:
                break
            for it in batch:
                iid = it.get("itemid")
                if iid in seen:                 # aynı itemid'yi iki kez ekleme
                    continue
                seen.add(iid)
                vt = str(it.get("value_type", "0"))
                records.append({
                    "itemid":          iid,
                    "hostid":          it.get("hostid"),
                    "name":            it.get("name"),
                    "key_":            it.get("key_"),
                    "value_type":      vt,
                    "value_type_name": VALUE_TYPE_NAMES.get(vt, "unknown"),
                    "units":           it.get("units") or "",
                    "status":          "Enabled" if str(it.get("status")) == "0" else "Disabled",
                    "templateid":      it.get("templateid"),     # şablon kaynaklı item'ın parent id'si
                })
            offset += len(batch)
            print(f"[item.get] offset={offset}/{total}, benzersiz={len(seen)}")

        _write_pending(records, ITEMS_META, kwargs["run_id"], "items")

    # ─── Pipeline ─────────────────────────────────────────────────────────────
    auth = get_auth()
    fetch_templates(auth)
    fetch_items(auth)
