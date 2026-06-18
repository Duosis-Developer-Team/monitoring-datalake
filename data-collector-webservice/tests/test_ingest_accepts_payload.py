"""End-to-end ingestion behaviour: envelope shapes, JSON errors, raw archive."""

from __future__ import annotations

import json
from pathlib import Path


def _last_jsonl_lines(path: Path):
    with path.open("r", encoding="utf-8") as fp:
        return [json.loads(line) for line in fp if line.strip()]


def test_post_single_record_returns_200(client, sample_global_payload):
    response = client.post("/api/v1/obm/metrics", json=sample_global_payload)
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["accepted"] is True
    assert body["record_count"] == 1
    assert body["raw_payload_ref"].startswith("raw/")


def test_post_array_envelope_returns_200(client, sample_global_payload):
    response = client.post(
        "/api/v1/obm/metrics",
        json=[sample_global_payload, sample_global_payload],
    )
    assert response.status_code == 200
    body = response.json()
    assert body["record_count"] == 2


def test_post_records_wrapper_returns_200(client, sample_global_payload):
    response = client.post(
        "/api/v1/obm/metrics",
        json={"records": [sample_global_payload]},
    )
    assert response.status_code == 200
    assert response.json()["record_count"] == 1


def test_post_data_wrapper_returns_200(client, sample_global_payload):
    response = client.post(
        "/api/v1/obm/metrics",
        json={"data": [sample_global_payload]},
    )
    assert response.status_code == 200
    assert response.json()["record_count"] == 1


def test_obm_alias_route_works(client, sample_global_payload):
    response = client.post("/api/v1/obm/metrics", json=sample_global_payload)
    assert response.status_code == 200
    assert response.json()["record_count"] == 1


def test_malformed_json_returns_400(client):
    response = client.post(
        "/api/v1/obm/metrics",
        data="{not valid json",
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 400
    body = response.json()
    assert body["status"] == "error"
    assert body["error"] == "malformed_json"
    assert "request_id" in body


def test_empty_body_returns_400(client):
    response = client.post(
        "/api/v1/obm/metrics",
        data=b"",
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 400


def test_raw_payload_is_persisted(client, sample_global_payload):
    response = client.post("/api/v1/obm/metrics", json=sample_global_payload)
    assert response.status_code == 200

    settings = client.app.state.test_settings
    raw_root = settings.raw_payload_dir
    files = list(raw_root.rglob("*.json"))
    assert len(files) == 1
    stored = json.loads(files[0].read_text(encoding="utf-8"))
    assert stored["MonitoredSystem"] == sample_global_payload["MonitoredSystem"]


def test_normalized_jsonl_is_written(client, sample_global_payload):
    response = client.post("/api/v1/obm/metrics", json=sample_global_payload)
    assert response.status_code == 200

    settings = client.app.state.test_settings
    records = _last_jsonl_lines(settings.normalized_jsonl_path)
    assert len(records) == 1
    rec = records[0]
    assert rec["class_name"] == "GLOBAL"
    assert rec["target_table"] == "opsb_agent_node"
    assert rec["metrics"]["cpu_util_pct"] == 42.3
    assert rec["common"]["node_fqdn"] == "server01.example.local"


def test_unknown_metric_keys_go_to_extra_metrics(client, sample_global_payload):
    response = client.post("/api/v1/obm/metrics", json=sample_global_payload)
    assert response.status_code == 200

    settings = client.app.state.test_settings
    records = _last_jsonl_lines(settings.normalized_jsonl_path)
    rec = records[0]
    assert "GBL_FUTURE_UNKNOWN_METRIC" in rec["extra_metrics"]
    assert rec["extra_metrics"]["GBL_FUTURE_UNKNOWN_METRIC"] == 99.9


def test_strict_validation_missing_identity_returns_422(client_factory):
    client = client_factory(strict_validation=True)
    payload = {
        "datasource": "SCOPE",
        "class_name": "GLOBAL",
        "metrics": {"GBL_CPU_TOTAL_UTIL": 50.0},
    }
    response = client.post("/api/v1/obm/metrics", json=payload)
    assert response.status_code == 422
    body = response.json()
    assert body["error"] == "validation_error"
    assert body["accepted"] is False
    assert isinstance(body["details"], list) and body["details"]


def test_non_strict_missing_identity_returns_200_and_quarantines(client_factory):
    client = client_factory(strict_validation=False)
    payload = {
        "datasource": "SCOPE",
        "class_name": "GLOBAL",
        "metrics": {"GBL_CPU_TOTAL_UTIL": 50.0},
    }
    response = client.post("/api/v1/obm/metrics", json=payload)
    assert response.status_code == 200
    body = response.json()
    assert body["quarantined"] is True

    settings = client.app.state.test_settings
    files = list(settings.quarantine_dir.rglob("*.json"))
    assert len(files) == 1


def test_mtls_enforced_missing_header_returns_401(client_factory):
    client = client_factory(enforce_proxy_mtls_header=True)
    response = client.post(
        "/api/v1/obm/metrics",
        json={"class_name": "GLOBAL", "metrics": {}},
    )
    assert response.status_code == 401
    body = response.json()
    assert body["error"] == "client_certificate_not_verified"


def test_mtls_enforced_success_header_passes(client_factory, sample_global_payload):
    client = client_factory(enforce_proxy_mtls_header=True)
    response = client.post(
        "/api/v1/obm/metrics",
        json=sample_global_payload,
        headers={
            "X-SSL-Client-Verify": "SUCCESS",
            "X-SSL-Client-Subject": "/CN=obm-agent-01",
            "X-SSL-Client-Fingerprint": "AA:BB:CC",
        },
    )
    assert response.status_code == 200


def test_mtls_subject_allowlist_rejects_unknown(client_factory, sample_global_payload):
    client = client_factory(
        enforce_proxy_mtls_header=True,
        allowed_client_cert_subjects="obm-agent-prod",
    )
    response = client.post(
        "/api/v1/obm/metrics",
        json=sample_global_payload,
        headers={
            "X-SSL-Client-Verify": "SUCCESS",
            "X-SSL-Client-Subject": "/CN=intruder",
        },
    )
    assert response.status_code == 401
    assert response.json()["error"] == "client_certificate_subject_not_allowed"


def test_body_size_limit_enforced(client_factory):
    client = client_factory(max_body_bytes=64)
    huge = {"records": [{"metrics": {f"GBL_M_{i}": i for i in range(200)}}]}
    response = client.post("/api/v1/obm/metrics", json=huge)
    assert response.status_code == 413
