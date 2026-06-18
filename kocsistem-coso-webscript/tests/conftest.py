"""Pytest fixtures: per-test isolated storage dirs and configurable settings.

Each test gets its own ``tmp_path``-backed raw/normalized/quarantine layout so that
state can never leak between tests. The fixture rebuilds ``app.state.ingestion_service``
so settings changes (e.g. ``strict_validation``) actually take effect.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterator

import pytest
from fastapi.testclient import TestClient

from app.core import config as config_module
from app.main import create_app
from app.services.ingestion_service import IngestionService
from app.services.normalization_service import Normalizer, NormalizationConfig
from app.services.storage_service import JsonlSink


def _build_settings(tmp_path: Path, **overrides) -> config_module.Settings:
    base = {
        "raw_payload_dir": tmp_path / "raw",
        "normalized_jsonl_path": tmp_path / "normalized" / "metrics.jsonl",
        "quarantine_dir": tmp_path / "quarantine",
        "trust_proxy_cert_headers": True,
        "enforce_proxy_mtls_header": False,
        "strict_validation": False,
        "max_body_bytes": 10 * 1024 * 1024,
    }
    base.update(overrides)
    return config_module.Settings(**base)


@pytest.fixture
def settings_factory(tmp_path: Path):
    def _factory(**overrides) -> config_module.Settings:
        return _build_settings(tmp_path, **overrides)

    return _factory


@pytest.fixture
def settings(settings_factory) -> config_module.Settings:
    return settings_factory()


@pytest.fixture
def normalizer() -> Normalizer:
    mapping_path = Path(__file__).resolve().parent.parent / "app" / "mappings" / "collection_policy_summary.json"
    return Normalizer(NormalizationConfig.from_path(mapping_path))


@pytest.fixture
def client_factory(settings_factory, normalizer):
    """Build a TestClient with custom settings/overrides."""

    def _factory(**overrides) -> TestClient:
        settings = settings_factory(**overrides)
        settings.ensure_runtime_dirs()

        # Override the cached settings getter so route handlers see our test settings.
        app = create_app()
        app.dependency_overrides[config_module.get_settings] = lambda: settings
        sink = JsonlSink(settings)
        app.state.ingestion_service = IngestionService(settings, normalizer, sink)
        app.state.test_settings = settings  # keep a handle for assertions

        client = TestClient(app)
        return client

    return _factory


@pytest.fixture
def client(client_factory) -> TestClient:
    return client_factory()


@pytest.fixture
def sample_global_payload() -> dict:
    return {
        "CollectionConfigName": "OOTB_AgentMetricCollection",
        "collection_data_flow": "OBM_AGENT_TO_CUSTOM_WEBSCRIPT",
        "collection_type": "metric",
        "tenant_id": "kocsistem",
        "MonitoredSystem": "server01.kocsistem.local",
        "MonitoredSystemID": "server01",
        "MonitoredSystemTimezone": "+03:00",
        "node_ip_type": "ipv4",
        "node_ipv4_address": "10.0.0.10",
        "producer_instance_id": "obm-agent-01",
        "producer_instance_type": "OBM_AGENT",
        "timestamp_utc": 1779638400,
        "datasource": "SCOPE",
        "class_name": "GLOBAL",
        "metrics": {
            "GBL_CPU_TOTAL_UTIL": 42.3,
            "GBL_CPU_USER_MODE_UTIL": 12.1,
            "GBL_MEM_UTIL": 68.4,
            "GBL_DISK_PHYS_BYTE_RATE": 1024.5,
            "GBL_SYSTEM_UPTIME_SECONDS": 86400,
            "GBL_FUTURE_UNKNOWN_METRIC": 99.9,
        },
    }
