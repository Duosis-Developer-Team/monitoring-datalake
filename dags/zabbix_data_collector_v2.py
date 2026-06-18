"""
DAG: zabbix_data_collector_v2
─────────────────────────────────────────────────────────────────────
Zabbix 7.x API'den host verisi çeker.
Veriyi ve tablo/yazma metodunu içeren metadata'yı tek bir JSON dosyası
olarak pending/ dizinine yazar.
PostgreSQL ile hiçbir ilişkisi yoktur — writer DAG bağımsız çalışır.

Pending dosya formatı:
  {
    "meta": {
      "table":           "zabbix_inventory",
      "method":          "upsert",          # insert | upsert | copy
      "conflict_target": ["hostid"],        # upsert için
      "json_columns":    ["interfaces",...],
      "source":          "zabbix_data_collector_v2",
      "collected_at":    "2026-04-29T11:00:00Z"
    },
    "data": [ { ...host... }, ... ]
  }

Dizin yapısı:
  {staging_folder_path}/pending/  → writer DAG bu klasörü okur

Airflow Variables:
  staging_folder_path  → NFS kök dizini
  zabbix_schedule      → Zamanlama (default: @hourly)
  zabbix_chunk_size    → Sayfalama limit (default: 500)

Airflow Connection:
  conn_id   : zabbix_api_conn
  conn_type : HTTP
  host      : http://<zabbix-server>/api_jsonrpc.php
  login / password
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

import requests

# ─── Airflow v2 / v3 uyumlu import'lar ───────────────────────────────────────
try:
    from airflow.sdk import DAG, Asset, Param, Variable, task   # Airflow 3.x
    from airflow.sdk.bases.hook import BaseHook
except ImportError:
    from airflow import DAG                                      # Airflow 2.x
    from airflow.decorators import task
    from airflow.models import Variable, Param
    from airflow.datasets import Dataset as Asset
    from airflow.hooks.base import BaseHook

try:
    from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator  # noqa: F401
except ImportError:
    pass  # Bu DAG artık trigger kullanmıyor

def _var(key: str, default: str = "") -> str:
    """Variable.get wrapper — Airflow 2.x (default_var) ve 3.x (default) uyumlu."""
    try:
        return Variable.get(key, default=default)
    except TypeError:
        return Variable.get(key, default_var=default)

# ─── Variables ────────────────────────────────────────────────────────────────
BASE_DIR    = _var("staging_folder_path", "/opt/airflow/dags/data_staging/zabbix")
PENDING_DIR = os.path.join(BASE_DIR, "pending")
DAG_SCHEDULE = _var("zabbix_schedule", "@hourly")
CHUNK_SIZE   = int(_var("zabbix_chunk_size", "500"))

# ─── Metadata — writer DAG bu bilgiyle tabloyu ve metodu belirler ─────────────
WRITE_META = {
    "table":           "zabbix_inventory",
    "method":          "upsert",        # insert | upsert | copy
    "conflict_target": ["hostid"],
    "json_columns": [
        "secondary_ips", "host_groups", "templates",
        "interfaces", "macros", "tags",
    ],
    "source": "zabbix_data_collector_v2",
}

ZABBIX_ASSET = Asset("file://zabbix_staged_data")

dag_params = {
    "target_groups": Param(
        [],
        type=["null", "array"],
        description="Filtreli grup listesi. Boş = tümü.",
    )
}

default_args = {
    "owner": "data_engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="zabbix_data_collector_v2",
    default_args=default_args,
    schedule=DAG_SCHEDULE,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    params=dag_params,
    tags=["zabbix", "extract", "modular"],
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
        result = r.json()
        if "error" in result:
            raise ValueError(f"Zabbix auth hatası: {result['error']}")
        token = result.get("result")
        if not token:
            raise ValueError("Token alınamadı.")
        return {"url": conn.host, "token": token}

    # ─── TASK 2: Ham veri ─────────────────────────────────────────────────────
    @task
    def fetch_raw_hosts(auth_data: dict) -> list:
        url, token = auth_data["url"], auth_data["token"]
        TIMEOUT = (10, 90)

        def zpost(payload):
            try:
                r = requests.post(url, json=payload, timeout=TIMEOUT)
                r.raise_for_status()
            except requests.exceptions.ConnectTimeout:
                raise RuntimeError(f"Bağlantı zaman aşımı: {url}")
            except requests.exceptions.ReadTimeout:
                raise RuntimeError("Zabbix yanıt vermedi (90s).")
            except requests.exceptions.ConnectionError as e:
                raise RuntimeError(f"Bağlantı hatası: {e}")
            res = r.json()
            if "error" in res:
                raise RuntimeError(f"Zabbix API: {res['error']}")
            return res

        # Toplam
        _raw = zpost({"jsonrpc": "2.0", "method": "host.get",
                      "params": {"output": ["hostid"], "countOutput": True},
                      "auth": token, "id": 0}).get("result", "0")
        total = len(_raw) if isinstance(_raw, list) else int(_raw)
        print(f"[host.get] Toplam: {total}")
        if total == 0:
            return []

        # Sayfalama
        all_hosts, offset = [], 0
        while offset < total:
            batch = zpost({
                "jsonrpc": "2.0", "method": "host.get",
                "params": {
                    "output": ["hostid", "name", "description", "status", "proxy_hostid"],
                    "selectInterfaces": ["ip", "port", "type", "main", "available", "error", "dns", "useip"],
                    "selectGroups": ["groupid", "name"],
                    "selectParentTemplates": ["templateid", "name"],
                    "limit": CHUNK_SIZE, "offset": offset,
                },
                "auth": token, "id": 1,
            }).get("result", [])
            if not batch:
                break
            all_hosts.extend(batch)
            print(f"[host.get] offset={offset} batch={len(batch)} toplam={len(all_hosts)}/{total}")
            offset += len(batch)

        ids = [h["hostid"] for h in all_hosts]

        macro_map = {h["hostid"]: h.get("macros", []) for h in zpost({
            "jsonrpc": "2.0", "method": "host.get",
            "params": {"output": ["hostid"], "hostids": ids,
                       "selectMacros": ["macro", "value", "type", "description"]},
            "auth": token, "id": 2,
        }).get("result", [])}

        tag_map = {h["hostid"]: h.get("tags", []) for h in zpost({
            "jsonrpc": "2.0", "method": "host.get",
            "params": {"output": ["hostid"], "hostids": ids,
                       "selectTags": ["tag", "value"]},
            "auth": token, "id": 3,
        }).get("result", [])}

        for h in all_hosts:
            h["macros"] = macro_map.get(h["hostid"], [])
            h["tags"]   = tag_map.get(h["hostid"], [])

        print(f"[OK] {len(all_hosts)} host hazır.")
        return all_hosts

    # ─── TASK 3: Temizle ──────────────────────────────────────────────────────
    @task
    def format_and_clean(raw_hosts: list, params: dict | None = None) -> list:
        target_groups = (params or {}).get("target_groups", [])
        now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        IFACE = {"1": "Agent", "2": "SNMP", "3": "IPMI", "4": "JMX"}
        out = []
        for host in raw_hosts:
            groups = [g.get("name", "") for g in host.get("groups", [])]
            if target_groups and not any(tg in groups for tg in target_groups):
                continue
            status = "Enabled" if str(host.get("status")) == "0" else "Disabled"
            proxy  = host.get("proxy_hostid")
            mon_by = "Zabbix Server" if not proxy or proxy == "0" else f"Proxy ID: {proxy}"

            primary, secondary, ifaces = None, [], []
            for iface in host.get("interfaces", []):
                addr = iface.get("ip") if str(iface.get("useip", "1")) == "1" else iface.get("dns")
                ifaces.append({
                    "ip": iface.get("ip"), "dns": iface.get("dns"),
                    "active_address": addr, "port": iface.get("port"),
                    "type": IFACE.get(str(iface.get("type", "")), "Unknown"),
                    "type_id": iface.get("type"), "main": iface.get("main"),
                    "available": iface.get("available"), "error": iface.get("error") or None,
                })
                if str(iface.get("main")) == "1":
                    primary = addr
                elif addr:
                    secondary.append(addr)

            macros = [{
                "macro": m.get("macro"),
                "value": "***MASKED***" if str(m.get("type")) == "1" else m.get("value"),
                "type": m.get("type"), "description": m.get("description") or None,
            } for m in host.get("macros", [])]

            tags = [{"tag": t.get("tag"), "value": t.get("value")} for t in host.get("tags", [])]

            out.append({
                "hostid": host.get("hostid"),
                "name": host.get("name"),
                "description": host.get("description") or "",
                "status": status,
                "primary_ip": primary,
                "secondary_ips": secondary,
                "monitored_by": mon_by,
                "host_groups": groups,
                "templates": [t.get("name") for t in host.get("parentTemplates", [])],
                "interfaces": ifaces,
                "macros": macros,
                "tags": tags,
                "collected_at": now_iso,
            })
        return out

    # ─── TASK 4: Pending dosyası oluştur ──────────────────────────────────────
    @task(outlets=[ZABBIX_ASSET])
    def write_pending(records: list, **kwargs) -> None:
        """
        Veriyi ve metadata'yı tek bir JSON dosyası olarak pending/ dizinine yazar.
        Writer DAG bu dosyayı okuyarak PostgreSQL'e yazar ve dosyayı siler.
        """
        os.makedirs(PENDING_DIR, exist_ok=True)

        safe_run = (
            kwargs["run_id"]
            .replace(":", "_").replace("+", "_").replace("/", "_")
        )
        file_name = f"zabbix_data_collector_v2_{safe_run}.json"
        file_path = os.path.join(PENDING_DIR, file_name)

        payload = {
            "meta": {
                **WRITE_META,
                "collected_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            },
            "data": records,
        }

        with open(file_path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)

        enabled  = sum(1 for r in records if r.get("status") == "Enabled")
        disabled = len(records) - enabled
        print(f"[OK] {len(records)} kayıt yazıldı (Enabled: {enabled}, Disabled: {disabled})")
        print(f"[OK] Dosya: {file_path}")

    # ─── Pipeline ────────────────────────────────────────────────────────────
    auth    = get_auth_token()
    raw     = fetch_raw_hosts(auth)
    clean   = format_and_clean(raw)
    write_pending(clean)
