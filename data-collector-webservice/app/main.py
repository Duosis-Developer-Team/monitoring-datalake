"""FastAPI application entrypoint.

Generic data-collector web service. Core services are source-agnostic; each
ingest source (currently ``obm_agent``) supplies its own normalizer, identity
rule, staging builder, and route.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import List

from fastapi import FastAPI

from . import __version__
from .api.routes import router as system_router
from .core.config import Settings, get_settings
from .core.logging import configure_logging, get_logger
from .services.ingestion_service import IngestionService
from .services.storage_service import (
    CompositeSink,
    JsonlSink,
    NormalizedSink,
    StagingSink,
)

# ── obm_agent source ─────────────────────────────────────────────────────────
from .sources.obm_agent import MAPPING_PATH
from .sources.obm_agent.normalization import (
    NormalizationConfig,
    Normalizer,
    record_has_strong_identity,
)
from .sources.obm_agent.routes import router as obm_router
from .sources.obm_agent.staging import build_staging_payloads


def _build_sinks(settings: Settings) -> NormalizedSink:
    sinks: List[NormalizedSink] = []
    if settings.staging_enabled:
        sinks.append(StagingSink(settings, build_staging_payloads))
    if settings.jsonl_enabled:
        sinks.append(JsonlSink(settings))
    if len(sinks) == 1:
        return sinks[0]
    return CompositeSink(sinks)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    configure_logging(settings.log_level)
    settings.ensure_runtime_dirs()

    normalizer = Normalizer(NormalizationConfig.from_path(MAPPING_PATH))
    sink = _build_sinks(settings)
    app.state.ingestion_service = IngestionService(
        settings,
        normalizer,
        sink,
        identity_check=record_has_strong_identity,
    )

    logger = get_logger(__name__)
    logger.info(
        "service_starting",
        extra={
            "service": settings.app_name,
            "env": settings.app_env,
            "version": __version__,
            "strict_validation": settings.strict_validation,
            "enforce_proxy_mtls_header": settings.enforce_proxy_mtls_header,
            "output_sinks": settings.output_sinks,
            "pending_dir": str(settings.pending_dir) if settings.staging_enabled else None,
        },
    )

    yield

    logger.info("service_stopped", extra={"service": settings.app_name})


def create_app() -> FastAPI:
    app = FastAPI(
        title="Data Collector Web Service",
        version=__version__,
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url=None,
    )
    app.include_router(system_router)
    app.include_router(obm_router)
    return app


app = create_app()
