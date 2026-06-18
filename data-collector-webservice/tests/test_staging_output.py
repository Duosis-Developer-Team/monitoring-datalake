"""Verify the staging sink writes writer-compatible {meta, data} files.

These assert the contract the Airflow generic_postgres_writer consumes: a pending
file with meta.table / meta.method and a flat data array, grouped by target table.
"""

from __future__ import annotations

import json
from pathlib import Path


def _pending_files(pending_dir: Path):
    return sorted(pending_dir.glob("*.json"))


def test_global_payload_writes_pending_file(client, sample_global_payload):
    response = client.post("/api/v1/obm/metrics", json=sample_global_payload)
    assert response.status_code == 200

    settings = client.app.state.test_settings
    files = _pending_files(settings.pending_dir)
    assert len(files) == 1

    payload = json.loads(files[0].read_text(encoding="utf-8"))
    meta, data = payload["meta"], payload["data"]

    assert meta["table"] == "opsb_agent_node"
    assert meta["method"] in ("insert", "upsert", "copy")
    assert meta["source"] == "obm_agent"
    assert "extra_metrics" in meta["json_columns"]
    assert meta["add_updated_at"] is False

    assert isinstance(data, list) and len(data) == 1
    row = data[0]
    # Flattened: common identity + mapped metric columns on the row itself.
    assert row["node_fqdn"] == "server01.example.local"
    assert row["node_short_name"] == "server01"
    assert row["cpu_util_pct"] == 42.3
    assert row["class_name"] == "GLOBAL"
    # Unknown metrics are preserved in the JSONB column, never dropped.
    assert row["extra_metrics"]["GBL_FUTURE_UNKNOWN_METRIC"] == 99.9


def test_no_tmp_files_left_in_pending(client, sample_global_payload):
    response = client.post("/api/v1/obm/metrics", json=sample_global_payload)
    assert response.status_code == 200

    settings = client.app.state.test_settings
    # Atomic write must not leave .tmp files behind.
    leftovers = list(settings.pending_dir.glob("*.tmp")) + list(
        settings.pending_dir.glob(".*")
    )
    assert leftovers == []


def test_multi_class_request_groups_one_file_per_table(client, sample_global_payload):
    disk_record = {
        "MonitoredSystem": "server01.example.local",
        "MonitoredSystemID": "server01",
        "timestamp_utc": 1779638400,
        "class_name": "DISK",
        "metrics": {"BYDSK_DEVNAME": "sda", "BYDSK_UTIL": 80.0},
    }
    response = client.post(
        "/api/v1/obm/metrics",
        json={"records": [sample_global_payload, disk_record]},
    )
    assert response.status_code == 200

    settings = client.app.state.test_settings
    files = _pending_files(settings.pending_dir)
    tables = {json.loads(f.read_text(encoding="utf-8"))["meta"]["table"] for f in files}
    assert tables == {"opsb_agent_node", "opsb_agent_disk"}
