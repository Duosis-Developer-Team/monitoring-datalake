"""Application configuration loaded from environment variables."""

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

    app_name: str = Field(default="kocsistem-coso-webscript")
    app_env: str = Field(default="dev")
    log_level: str = Field(default="INFO")

    strict_validation: bool = Field(default=False)

    raw_payload_dir: Path = Field(default=Path("/var/lib/coso-webscript/raw"))
    normalized_jsonl_path: Path = Field(
        default=Path("/var/lib/coso-webscript/normalized/metrics.jsonl")
    )
    quarantine_dir: Path = Field(default=Path("/var/lib/coso-webscript/quarantine"))

    max_body_bytes: int = Field(default=10 * 1024 * 1024)

    trust_proxy_cert_headers: bool = Field(default=True)
    enforce_proxy_mtls_header: bool = Field(default=False)

    client_cert_verify_header: str = Field(default="X-SSL-Client-Verify")
    client_cert_subject_header: str = Field(default="X-SSL-Client-Subject")
    client_cert_issuer_header: str = Field(default="X-SSL-Client-Issuer")
    client_cert_fingerprint_header: str = Field(default="X-SSL-Client-Fingerprint")

    allowed_client_cert_subjects: List[str] = Field(default_factory=list)
    allowed_client_cert_fingerprints: List[str] = Field(default_factory=list)

    mapping_file: Path = Field(
        default=Path(__file__).resolve().parent.parent
        / "mappings"
        / "collection_policy_summary.json"
    )

    @field_validator(
        "allowed_client_cert_subjects",
        "allowed_client_cert_fingerprints",
        mode="before",
    )
    @classmethod
    def _split_csv(cls, value):
        if value is None or value == "":
            return []
        if isinstance(value, str):
            return [v.strip() for v in value.split(",") if v.strip()]
        return value

    def ensure_runtime_dirs(self) -> None:
        """Create storage directories on startup. Permission errors are bubbled up."""
        self.raw_payload_dir.mkdir(parents=True, exist_ok=True)
        self.normalized_jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        self.quarantine_dir.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
