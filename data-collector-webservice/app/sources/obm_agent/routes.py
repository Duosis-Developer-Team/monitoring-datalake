"""OBM agent ingest route.

Exposes the OBM metric endpoint and delegates to the shared, source-agnostic
``handle_metrics`` flow. The legacy customer-named path is gone; OBM agents POST
to ``/api/v1/obm/metrics``.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from ...api.ingest import get_ingestion_service, handle_metrics
from ...core.config import Settings, get_settings
from ...services.ingestion_service import IngestionService


router = APIRouter(tags=["obm_agent"])


@router.post("/api/v1/obm/metrics")
async def post_obm_metrics(
    request: Request,
    settings: Settings = Depends(get_settings),
    ingestion: IngestionService = Depends(get_ingestion_service),
) -> JSONResponse:
    return await handle_metrics(request, settings, ingestion)
