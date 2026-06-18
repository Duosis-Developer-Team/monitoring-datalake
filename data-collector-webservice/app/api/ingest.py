"""Source-agnostic metric ingestion handler.

Every ingest source (obm_agent today, others later) mounts its own POST route
but delegates here, so cert enforcement, body limits, JSON parsing, and the
archive→normalize→persist flow stay identical across sources.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

from fastapi import Request, status
from fastapi.responses import JSONResponse

from ..core.config import Settings
from ..core.logging import get_logger
from ..core.security import (
    AuthError,
    ClientCertificateInfo,
    enforce_mtls,
    extract_client_cert_info,
)
from ..schemas.payload import IngestionAcceptedResponse, IngestionErrorResponse
from ..services.ingestion_service import IngestionService


logger = get_logger(__name__)


def new_request_id() -> str:
    return str(uuid.uuid4())


def get_ingestion_service(request: Request) -> IngestionService:
    return request.app.state.ingestion_service


async def handle_metrics(
    request: Request,
    settings: Settings,
    ingestion: IngestionService,
) -> JSONResponse:
    request_id = new_request_id()
    client_ip = request.client.host if request.client else None

    cert_info: ClientCertificateInfo = extract_client_cert_info(request, settings)
    try:
        enforce_mtls(cert_info, settings)
    except AuthError as exc:
        logger.warning(
            "mtls_enforcement_failed",
            extra={
                "request_id": request_id,
                "client_ip": client_ip,
                "reason": exc.reason,
                "cert_subject": cert_info.subject,
            },
        )
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content=IngestionErrorResponse(request_id=request_id, error=exc.reason).model_dump(),
        )

    raw_body = await request.body()

    if settings.max_body_bytes and len(raw_body) > settings.max_body_bytes:
        logger.warning(
            "request_body_too_large",
            extra={
                "request_id": request_id,
                "client_ip": client_ip,
                "body_size": len(raw_body),
                "limit": settings.max_body_bytes,
            },
        )
        return JSONResponse(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            content=IngestionErrorResponse(
                request_id=request_id, error="request_body_too_large"
            ).model_dump(),
        )

    try:
        parsed_payload: Any = json.loads(raw_body.decode("utf-8")) if raw_body else None
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        logger.warning(
            "malformed_json_payload",
            extra={
                "request_id": request_id,
                "client_ip": client_ip,
                "cert_subject": cert_info.subject,
                "error": str(exc),
            },
        )
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content=IngestionErrorResponse(request_id=request_id, error="malformed_json").model_dump(),
        )

    if parsed_payload is None:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content=IngestionErrorResponse(request_id=request_id, error="empty_body").model_dump(),
        )

    result = ingestion.ingest(request_id, raw_body, parsed_payload)

    logger.info(
        "payload_ingested",
        extra={
            "request_id": request_id,
            "client_ip": client_ip,
            "cert_subject": cert_info.subject,
            "cert_fingerprint": cert_info.fingerprint,
            "record_count": result.record_count,
            "raw_payload_ref": result.raw_payload_ref,
            "quarantined": result.quarantined,
            "strict_validation_failed": result.strict_validation_failed,
        },
    )

    if result.strict_validation_failed:
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content=IngestionErrorResponse(
                request_id=request_id,
                error="validation_error",
                details=result.validation_errors or [],
            ).model_dump(),
        )

    response_body = IngestionAcceptedResponse(
        request_id=request_id,
        record_count=result.record_count,
        raw_payload_ref=result.raw_payload_ref,
        quarantined=result.quarantined,
    )
    return JSONResponse(status_code=status.HTTP_200_OK, content=response_body.model_dump())
