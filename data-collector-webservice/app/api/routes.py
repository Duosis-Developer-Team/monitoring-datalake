"""System routes: liveness and readiness probes.

Ingest routes live with their source (e.g. ``app/sources/obm_agent/routes.py``)
and are mounted in ``app/main.py``.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends

from .. import __version__
from ..core.config import Settings, get_settings
from ..schemas.payload import HealthResponse


router = APIRouter()


@router.get("/health", response_model=HealthResponse)
def health(settings: Settings = Depends(get_settings)) -> HealthResponse:
    return HealthResponse(status="ok", service=settings.app_name, version=__version__)


@router.get("/ready", response_model=HealthResponse)
def ready(settings: Settings = Depends(get_settings)) -> HealthResponse:
    """Readiness check: ensures storage directories exist and are writable."""
    settings.ensure_runtime_dirs()
    return HealthResponse(status="ready", service=settings.app_name, version=__version__)
