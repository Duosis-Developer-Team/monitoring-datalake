"""Reverse-proxy mTLS metadata extraction and enforcement."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from fastapi import Request

from .config import Settings


@dataclass(frozen=True)
class ClientCertificateInfo:
    """Metadata about the client certificate, as forwarded by the reverse proxy."""

    verify_status: Optional[str]
    subject: Optional[str]
    issuer: Optional[str]
    fingerprint: Optional[str]

    @property
    def is_verified(self) -> bool:
        return (self.verify_status or "").upper() == "SUCCESS"


def extract_client_cert_info(request: Request, settings: Settings) -> ClientCertificateInfo:
    """Read mTLS metadata from configured proxy headers.

    Headers are only consulted when ``trust_proxy_cert_headers`` is true. Otherwise the
    response is an empty record — relevant for direct-from-internet test setups where
    headers must never be trusted.
    """
    if not settings.trust_proxy_cert_headers:
        return ClientCertificateInfo(None, None, None, None)

    headers = request.headers
    return ClientCertificateInfo(
        verify_status=headers.get(settings.client_cert_verify_header),
        subject=headers.get(settings.client_cert_subject_header),
        issuer=headers.get(settings.client_cert_issuer_header),
        fingerprint=headers.get(settings.client_cert_fingerprint_header),
    )


class AuthError(Exception):
    """Raised when mTLS proxy enforcement fails. Mapped to HTTP 401."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def enforce_mtls(cert_info: ClientCertificateInfo, settings: Settings) -> None:
    """Reject the request if proxy mTLS enforcement is on and the cert is missing/invalid.

    Three checks, in order:
      1. Proxy must mark the cert as SUCCESS.
      2. If a subject allowlist is configured, the forwarded subject must match.
      3. If a fingerprint allowlist is configured, the forwarded fingerprint must match.
    """
    if not settings.enforce_proxy_mtls_header:
        return

    if not cert_info.is_verified:
        raise AuthError("client_certificate_not_verified")

    if settings.allowed_client_cert_subjects:
        if not cert_info.subject or not any(
            allowed in cert_info.subject for allowed in settings.allowed_client_cert_subjects
        ):
            raise AuthError("client_certificate_subject_not_allowed")

    if settings.allowed_client_cert_fingerprints:
        if not cert_info.fingerprint or cert_info.fingerprint not in settings.allowed_client_cert_fingerprints:
            raise AuthError("client_certificate_fingerprint_not_allowed")
