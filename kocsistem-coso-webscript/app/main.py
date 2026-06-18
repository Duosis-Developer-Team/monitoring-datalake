"""FastAPI application entrypoint."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from . import __version__
from .api.routes import router
from .core.config import get_settings
from .core.logging import configure_logging, get_logger
from .services.ingestion_service import IngestionService
from .services.normalization_service import Normalizer, NormalizationConfig
from .services.storage_service import JsonlSink


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    configure_logging(settings.log_level)
    settings.ensure_runtime_dirs()

    config = NormalizationConfig.from_path(settings.mapping_file)
    normalizer = Normalizer(config)
    sink = JsonlSink(settings)
    app.state.ingestion_service = IngestionService(settings, normalizer, sink)

    logger = get_logger(__name__)
    logger.info(
        "service_starting",
        extra={
            "service": settings.app_name,
            "env": settings.app_env,
            "version": __version__,
            "strict_validation": settings.strict_validation,
            "enforce_proxy_mtls_header": settings.enforce_proxy_mtls_header,
        },
    )

    yield

    logger.info("service_stopped", extra={"service": settings.app_name})


def create_app() -> FastAPI:
    app = FastAPI(
        title="KoçSistem COSO Webscript Receiver",
        version=__version__,
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url=None,
    )
    app.include_router(router)
    return app


app = create_app()
