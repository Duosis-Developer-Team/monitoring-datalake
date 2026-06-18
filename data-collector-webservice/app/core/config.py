"""Application configuration loaded from environment variables.

The service is source-agnostic. ``obm_agent`` is the first ingest source, but the
core config knows nothing OBM-specific — sources own their own mapping files.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import List

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str = Field(default="data-collector-webservice")
    app_env: str = Field(default="dev")
    log_level: str = Field(default="INFO")

    strict_validation: bool = Field(default=False)

    # ── Audit storage (one file per request → safe across replicas) ──────────
    raw_payload_dir: Path = Field(default=Path("/var/lib/data-collector/raw"))
    quarantine_dir: Path = Field(default=Path("/var/lib/data-collector/quarantine"))

    # ── Output sinks ─────────────────────────────────────────────────────────
    # Comma-separated list of enabled sinks: "staging", "jsonl".
    #   staging → write {meta,data} files into the NFS pending/ dir for the
    #             Airflow generic_postgres_writer to load (multi-replica safe).
    #   jsonl   → append to a single local JSONL file (dev / single-node only;
    #             NOT safe when running multiple replicas against shared storage).
    output_sinks: List[str] = Field(default_factory=lambda: ["staging"])

    # NFS root shared with Airflow. The writer scans "<staging_folder_path>/<pending_dirname>".
    # This MUST resolve to the same physical NFS directory the Airflow
    # generic_postgres_writer DAG scans (its `staging_folder_path` Variable).
    staging_folder_path: Path = Field(default=Path("/nfs/airflow-staging"))
    pending_dirname: str = Field(default="pending")

    # JSONL sink target (only used when "jsonl" is in output_sinks).
    normalized_jsonl_path: Path = Field(
        default=Path("/var/lib/data-collector/normalized/metrics.jsonl")
    )

    max_body_bytes: int = Field(default=10 * 1024 * 1024)

    # ── Proxy / mTLS trust model ─────────────────────────────────────────────
    trust_proxy_cert_headers: bool = Field(default=True)
    enforce_proxy_mtls_header: bool = Field(default=False)

    client_cert_verify_header: str = Field(default="X-SSL-Client-Verify")
    client_cert_subject_header: str = Field(default="X-SSL-Client-Subject")
    client_cert_issuer_header: str = Field(default="X-SSL-Client-Issuer")
    client_cert_fingerprint_header: str = Field(default="X-SSL-Client-Fingerprint")

    allowed_client_cert_subjects: List[str] = Field(default_factory=list)
    allowed_client_cert_fingerprints: List[str] = Field(default_factory=list)

    @field_validator(
        "allowed_client_cert_subjects",
        "allowed_client_cert_fingerprints",
        "output_sinks",
        mode="before",
    )
    @classmethod
    def _split_csv(cls, value):
        if value is None or value == "":
            return []
        if isinstance(value, str):
            return [v.strip() for v in value.split(",") if v.strip()]
        return value

    @property
    def pending_dir(self) -> Path:
        return self.staging_folder_path / self.pending_dirname

    @property
    def staging_enabled(self) -> bool:
        return "staging" in self.output_sinks

    @property
    def jsonl_enabled(self) -> bool:
        return "jsonl" in self.output_sinks

    def ensure_runtime_dirs(self) -> None:
        """Create storage directories on startup. Permission errors are bubbled up."""
        self.raw_payload_dir.mkdir(parents=True, exist_ok=True)
        self.quarantine_dir.mkdir(parents=True, exist_ok=True)
        if self.staging_enabled:
            self.pending_dir.mkdir(parents=True, exist_ok=True)
        if self.jsonl_enabled:
            self.normalized_jsonl_path.parent.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
