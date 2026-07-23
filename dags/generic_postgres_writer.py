"""
DAG: generic_postgres_writer
─────────────────────────────────────────────────────────────────────
Her 5 dakikada bir {staging_folder_path}/pending/ klasörünü tarar.
Bulduğu her JSON dosyasını okur, metadata'ya göre PostgreSQL'e yazar,
ardından dosyayı siler.

Desteklenen yazma metodları:
  insert  → INSERT INTO ...
  upsert  → INSERT ... ON CONFLICT (...) DO UPDATE SET ...
  copy    → COPY ile toplu yükleme

Beklenen dosya formatı:
  {
    "meta": {
      "table":           "hedef_tablo",
      "method":          "upsert",
      "conflict_target": ["pk_sutun"],
      "json_columns":    ["sutun1", ...],
      "source":          "kaynak_dag_id"
    },
    "data": [ { ...kayit... }, ... ]
  }

Airflow Variables:
  staging_folder_path       → pending/ klasörünün üst dizini
  pg_writer_conn_id         → PostgreSQL connection id (default: postgres_default)
  pg_writer_batch_size      → execute_values page_size (default: 1000)
  pg_writer_time_budget_sec → Run başına zaman bütçesi (default: 240 = 4 dk)

Performans notu:
  insert/upsert yolu psycopg2'nin `execute_values` fonksiyonunu kullanır — tek
  bir çok satırlı "INSERT ... VALUES (...),(...)" üretir. Eski `executemany`
  SATIR BAŞINA bir round-trip yapıyordu; yüz binlerce kayıtta DAG 10 dakikayı
  aşıp timeout alıyordu. execute_values round-trip sayısını
  satır_sayısı/page_size'a düşürür.

  Ayrıca process_pending bir zaman bütçesiyle çalışır: süre dolunca kalan
  dosyalar silinmeden bırakılır ve bir sonraki run devralır. Böylece bir run
  asla 5 dakikalık schedule aralığını aşıp birikme sarmalı başlatmaz.
"""

from __future__ import annotations

import glob
import io
import json
import os
import time
from datetime import datetime, timedelta

# ─── Airflow v2 / v3 uyumlu import'lar ───────────────────────────────────────
try:
    from airflow.sdk import DAG, Variable, task             # Airflow 3.x
except ImportError:
    from airflow import DAG                                 # Airflow 2.x
    from airflow.decorators import task
    from airflow.models import Variable

try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
except ImportError:
    PostgresHook = None

# execute_values → çok satırlı tek INSERT. Yoksa executemany'ye düşülür.
try:
    from psycopg2.extras import execute_values
except ImportError:
    execute_values = None

def _var(key: str, default: str = "") -> str:
    try:
        return Variable.get(key, default=default)
    except TypeError:
        return Variable.get(key, default_var=default)

# ─── Variables ────────────────────────────────────────────────────────────────
BASE_DIR    = _var("staging_folder_path", "/opt/airflow/dags/data_staging/zabbix")
PENDING_DIR = os.path.join(BASE_DIR, "pending")
PG_CONN_ID  = _var("pg_writer_conn_id", "postgres_default")
# execute_values page_size. 200 çok küçüktü; 1000 tipik olarak en iyi noktada.
BATCH_SIZE  = int(_var("pg_writer_batch_size", "1000"))
# Run başına zaman bütçesi (saniye). Schedule 5 dk olduğu için varsayılan 4 dk:
# kalan süre dosya kapatma/commit için pay bırakır.
TIME_BUDGET_SEC = int(_var("pg_writer_time_budget_sec", "240"))


# ─── Yardımcı fonksiyonlar — DAG bloğu DIŞINDA tanımlanmalı ──────────────────

def _get_conn(conn_id: str):
    """PostgresHook varsa kullan, yoksa psycopg2 ile direkt bağlan."""
    if PostgresHook is not None:
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
    else:
        import psycopg2
        try:
            from airflow.sdk.bases.hook import BaseHook
        except ImportError:
            from airflow.hooks.base import BaseHook
        c = BaseHook.get_connection(conn_id)
        conn = psycopg2.connect(
            host=c.host, port=int(c.port or 5432),
            dbname=c.schema, user=c.login, password=c.password,
            connect_timeout=10,
        )
    conn.autocommit = False
    return conn


def _col_type(col: str, json_cols: list, col_types: dict) -> str:
    # 1. Metadata'da açıkça belirtilmişse onu kullan
    if col in col_types:
        return col_types[col]
    # 2. Otomatik çıkarım
    if col in json_cols:        return "JSONB"
    if col.endswith("_at"):     return "TIMESTAMPTZ"
    return "TEXT"


def _ensure_table(conn, table: str, records: list, meta: dict) -> None:
    """
    Tabloyu oluşturur (yoksa). Sütun tipleri:
      - meta.column_types ile açıkça belirtilebilir (örn. {"clock": "BIGINT"})
      - json_columns → JSONB
      - *_at → TIMESTAMPTZ
      - diğerleri → TEXT

    meta.add_updated_at = False ise updated_at sütunu EKLENMEZ.
    Append-only zaman serisi tablolarında updated_at gereksizdir.
    """
    json_cols    = meta.get("json_columns", [])
    col_types    = meta.get("column_types", {})
    pk_cols      = meta.get("conflict_target", [])
    add_updated  = meta.get("add_updated_at", True)
    sample_cols  = list(records[0].keys())

    all_cols = list(sample_cols)
    if add_updated and "updated_at" not in all_cols:
        all_cols.append("updated_at")

    defs = [f"    {c} {_col_type(c, json_cols, col_types)}" for c in all_cols]
    if pk_cols:
        defs.append(f"    PRIMARY KEY ({', '.join(pk_cols)})")

    ddl = f"CREATE TABLE IF NOT EXISTS {table} (\n" + ",\n".join(defs) + "\n);"
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    print(f"   [OK] Tablo hazır: {table}")


def _serialize_row(record: dict, all_cols: list, json_cols: list) -> tuple:
    row = []
    for col in all_cols:
        val = record.get(col)
        if col in json_cols:
            val = json.dumps(val, ensure_ascii=False)
        elif col == "updated_at":
            val = datetime.utcnow().isoformat()
        row.append(val)
    return tuple(row)


def _run_values(conn, sql_values: str, sql_single: str, rows: list) -> None:
    """
    Çok satırlı INSERT'i çalıştırır.

    sql_values : "... VALUES %s ..."          → execute_values için
    sql_single : "... VALUES (%s, %s, ...) ..." → executemany fallback'i için

    execute_values tek istekte page_size kadar satır gönderir; executemany
    satır başına bir round-trip yapar (çok yavaş, yalnızca fallback).
    """
    with conn.cursor() as cur:
        if execute_values is not None:
            execute_values(cur, sql_values, rows, page_size=BATCH_SIZE)
        else:
            print("   [WARN] psycopg2.extras.execute_values yok → executemany (yavaş).")
            for i in range(0, len(rows), BATCH_SIZE):
                cur.executemany(sql_single, rows[i: i + BATCH_SIZE])


def _insert(conn, table: str, records: list, meta: dict) -> int:
    json_cols = meta.get("json_columns", [])
    all_cols  = list(records[0].keys()) + (["updated_at"] if "updated_at" not in records[0] else [])
    rows      = [_serialize_row(r, all_cols, json_cols) for r in records]
    col_list  = ", ".join(all_cols)

    _run_values(
        conn,
        f"INSERT INTO {table} ({col_list}) VALUES %s;",
        f"INSERT INTO {table} ({col_list}) VALUES ({', '.join(['%s'] * len(all_cols))});",
        rows,
    )
    conn.commit()
    return len(rows)


def _upsert(conn, table: str, records: list, meta: dict) -> int:
    json_cols  = meta.get("json_columns", [])
    pk_cols    = meta.get("conflict_target", [])

    # ── Dosya içi tekilleştirme (execute_values için ZORUNLU) ────────────────
    # execute_values tek bir çok satırlı INSERT ürettiği için, aynı ifade içinde
    # tekrarlanan bir conflict key'i PostgreSQL reddeder:
    #   "ON CONFLICT DO UPDATE command cannot affect row a second time"
    # Eski executemany'de her satır ayrı ifade olduğu için bu sorun görünmüyordu.
    # Son kayıt kazanır (collector'lar zaten güncel durumu en sona yazar).
    if pk_cols:
        deduped: dict = {}
        for r in records:
            deduped[tuple(str(r.get(c)) for c in pk_cols)] = r
        if len(deduped) != len(records):
            print(f"   [DEDUP] {len(records)} → {len(deduped)} kayıt (conflict key tekrarı)")
        records = list(deduped.values())

    all_cols   = list(records[0].keys()) + (["updated_at"] if "updated_at" not in records[0] else [])
    upd_cols   = [c for c in all_cols if c not in pk_cols]
    set_clause = ", ".join(
        f"{c} = EXCLUDED.{c}" if c != "updated_at" else "updated_at = NOW()"
        for c in upd_cols
    )
    rows     = [_serialize_row(r, all_cols, json_cols) for r in records]
    col_list = ", ".join(all_cols)
    tail     = f"ON CONFLICT ({', '.join(pk_cols)}) DO UPDATE SET {set_clause};"

    _run_values(
        conn,
        f"INSERT INTO {table} ({col_list}) VALUES %s {tail}",
        f"INSERT INTO {table} ({col_list}) "
        f"VALUES ({', '.join(['%s'] * len(all_cols))}) {tail}",
        rows,
    )
    conn.commit()
    return len(rows)


def _build_tsv(records: list, columns: list, json_cols: list) -> io.StringIO:
    """Kayıtları PostgreSQL COPY için TSV buffer'ına çevirir."""
    def to_tsv(col, val):
        if val is None:
            return r"\N"
        if col in json_cols:
            return json.dumps(val, ensure_ascii=False).replace("\t", " ").replace("\n", " ")
        return str(val).replace("\t", " ").replace("\n", " ").replace("\r", " ")

    buf = io.StringIO()
    for record in records:
        buf.write("\t".join(to_tsv(c, record.get(c)) for c in columns) + "\n")
    buf.seek(0)
    return buf


def _copy(conn, table: str, records: list, meta: dict) -> int:
    """
    COPY ile yazma. İki mod:

    1. conflict_target YOK → düz COPY (en hızlı, duplicate kontrolü yok).
       Salt-ekleme (append-only) senaryoları için.

    2. conflict_target VAR → staging pattern:
         a. UNLOGGED geçici staging tablosu oluştur (ana tablo şablonundan)
         b. Staging'e ham COPY yap (hızlı)
         c. INSERT INTO ana SELECT FROM staging ON CONFLICT DO NOTHING
         d. Staging'i drop et
       COPY hızını korur + duplicate önler. Büyük zaman serisi için ideal.
    """
    json_cols      = meta.get("json_columns", [])
    conflict_cols  = meta.get("conflict_target", [])
    columns        = list(records[0].keys())
    buf            = _build_tsv(records, columns, json_cols)

    # ── Mod 1: Düz COPY (duplicate kontrolü yok) ─────────────────────────────
    if not conflict_cols:
        with conn.cursor() as cur:
            cur.copy_from(buf, table, columns=columns, null=r"\N")
        conn.commit()
        return len(records)

    # ── Mod 2: Staging + idempotent insert ───────────────────────────────────
    staging = f"_staging_{table}_{int(datetime.utcnow().timestamp() * 1000) % 1000000}"
    col_list = ", ".join(columns)

    with conn.cursor() as cur:
        try:
            # a. Staging tablo — ana tablonun yapısını miras al, constraint/index alma
            #    UNLOGGED = WAL'a yazmaz, geçici veri için çok daha hızlı
            cur.execute(
                f"CREATE UNLOGGED TABLE {staging} "
                f"(LIKE {table} INCLUDING DEFAULTS EXCLUDING CONSTRAINTS EXCLUDING INDEXES);"
            )

            # b. Ham COPY — staging'e (constraint yok, en hızlı)
            cur.copy_from(buf, staging, columns=columns, null=r"\N")

            # c. Ana tabloya idempotent aktarım
            cur.execute(
                f"INSERT INTO {table} ({col_list}) "
                f"SELECT {col_list} FROM {staging} "
                f"ON CONFLICT ({', '.join(conflict_cols)}) DO NOTHING;"
            )
            inserted = cur.rowcount  # gerçekten eklenen satır (duplicate'ler hariç)

            # d. Staging temizle
            cur.execute(f"DROP TABLE IF EXISTS {staging};")
            conn.commit()

            skipped = len(records) - inserted
            print(f"   [COPY] {inserted} yeni satır, {skipped} duplicate atlandı.")
            return inserted

        except Exception:
            conn.rollback()
            try:
                with conn.cursor() as cur2:
                    cur2.execute(f"DROP TABLE IF EXISTS {staging};")
                conn.commit()
            except Exception:
                pass
            raise


# ─── DAG ─────────────────────────────────────────────────────────────────────

default_args = {
    "owner": "data_engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="generic_postgres_writer",
    default_args=default_args,
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    tags=["postgres", "writer", "generic"],
    doc_md=__doc__,
) as dag:

    @task
    def process_pending() -> dict:
        files = sorted(glob.glob(os.path.join(PENDING_DIR, "*.json")))

        if not files:
            print("[INFO] pending/ dizininde dosya yok.")
            return {"processed": 0, "errors": 0, "skipped": 0, "deferred": 0}

        print(f"[INFO] {len(files)} dosya bulundu.")
        processed, errors, skipped, deferred = 0, 0, 0, 0

        # ── Zaman bütçesi ────────────────────────────────────────────────────
        # Schedule 5 dk. Bir run bunu aşarsa max_active_runs=1 nedeniyle sonraki
        # run slot bekler, dosyalar birikir ve her run bir öncekinden daha çok iş
        # devralır (birikme sarmalı → dagrun_timeout). Bütçe dolunca kalan
        # dosyalara DOKUNMADAN çıkıyoruz; sıradaki run en eskiden devam eder.
        deadline = time.monotonic() + TIME_BUDGET_SEC

        conn = _get_conn(PG_CONN_ID)
        try:
            for idx, file_path in enumerate(files):
                if time.monotonic() >= deadline:
                    deferred = len(files) - idx
                    print(f"\n[INFO] Zaman bütçesi doldu ({TIME_BUDGET_SEC}s). "
                          f"Kalan {deferred} dosya sonraki run'a bırakıldı.")
                    break

                fname   = os.path.basename(file_path)
                payload = {}
                try:
                    with open(file_path, "r", encoding="utf-8") as fh:
                        payload = json.load(fh)

                    meta    = payload.get("meta", {})
                    records = payload.get("data", [])

                    table  = meta.get("table")
                    method = meta.get("method", "upsert").lower()

                    if not table:
                        raise ValueError("meta.table eksik.")
                    if method not in ("insert", "upsert", "copy"):
                        raise ValueError(f"Geçersiz method: {method}")
                    if method == "upsert" and not meta.get("conflict_target"):
                        raise ValueError("upsert için meta.conflict_target zorunlu.")
                    if not isinstance(records, list):
                        raise TypeError("data alanı list olmalı.")

                    print(f"\n── {fname}")
                    print(f"   table   : {table}")
                    print(f"   method  : {method}")
                    print(f"   source  : {meta.get('source', '?')}")
                    print(f"   records : {len(records)}")

                    if not records:
                        print("   [SKIP] Boş data.")
                        os.remove(file_path)
                        skipped += 1
                        continue

                    _ensure_table(conn, table, records, meta)

                    t0 = time.monotonic()
                    if method == "insert":
                        written = _insert(conn, table, records, meta)
                    elif method == "upsert":
                        written = _upsert(conn, table, records, meta)
                    elif method == "copy":
                        written = _copy(conn, table, records, meta)
                    elapsed = time.monotonic() - t0

                    rate = written / elapsed if elapsed > 0 else 0
                    print(f"   [OK] {written} kayıt → {table} ({method}) "
                          f"— {elapsed:.1f}s, {rate:,.0f} satır/s")
                    os.remove(file_path)
                    print(f"   [OK] Silindi: {fname}")
                    processed += 1

                except Exception as exc:
                    errors += 1
                    print(f"   [ERROR] {fname}: {exc}")
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    try:
                        payload["_error"]    = str(exc)
                        payload["_error_at"] = datetime.utcnow().isoformat()
                        with open(file_path, "w", encoding="utf-8") as fh:
                            json.dump(payload, fh, ensure_ascii=False, indent=2)
                    except Exception:
                        pass
        finally:
            conn.close()

        summary = {"processed": processed, "errors": errors,
                   "skipped": skipped, "deferred": deferred}
        print(f"\n[SUMMARY] {summary}")
        return summary

    process_pending()
