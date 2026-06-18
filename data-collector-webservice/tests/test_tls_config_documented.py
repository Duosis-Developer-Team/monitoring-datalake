"""Smoke checks on shipped TLS/mTLS deployment assets.

These tests do not exercise a real TLS handshake; they only verify that the
deployment files we ship document the required TLS/mTLS contract.
"""

from __future__ import annotations

from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parent.parent


def test_nginx_config_enforces_tls_1_2_plus_and_mtls():
    nginx_conf = (_REPO_ROOT / "deploy" / "nginx.conf").read_text(encoding="utf-8")
    assert "ssl_protocols TLSv1.2 TLSv1.3" in nginx_conf
    assert "ssl_verify_client on" in nginx_conf
    assert "ssl_client_certificate" in nginx_conf
    assert "X-SSL-Client-Verify" in nginx_conf
    assert "X-SSL-Client-Subject" in nginx_conf
    assert "X-SSL-Client-Fingerprint" in nginx_conf


def test_env_example_documents_security_toggles():
    env_example = (_REPO_ROOT / ".env.example").read_text(encoding="utf-8")
    assert "ENFORCE_PROXY_MTLS_HEADER" in env_example
    assert "TRUST_PROXY_CERT_HEADERS" in env_example
    assert "MAX_BODY_BYTES" in env_example


def test_dockerfile_runs_as_non_root_user():
    docker = (_REPO_ROOT / "deploy" / "Dockerfile").read_text(encoding="utf-8")
    assert "USER appuser" in docker


def test_systemd_unit_uses_environment_file_and_hardening():
    unit = (_REPO_ROOT / "deploy" / "systemd.service").read_text(encoding="utf-8")
    assert "EnvironmentFile=/etc/data-collector.env" in unit
    assert "NoNewPrivileges=true" in unit
    assert "User=appuser" in unit
