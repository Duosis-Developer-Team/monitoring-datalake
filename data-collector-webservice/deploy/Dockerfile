FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    # gunicorn honours WEB_CONCURRENCY as the default worker count; override per
    # deployment. With multiple pod replicas, keep this modest (2-4) per pod.
    WEB_CONCURRENCY=4

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Local audit storage (raw + quarantine). The NFS pending/ dir is mounted at
# runtime; it is NOT created here. Storage paths are also created on app startup,
# but pre-creating keeps permissions clean for the non-root user.
RUN mkdir -p /var/lib/data-collector/raw \
             /var/lib/data-collector/quarantine \
             /var/lib/data-collector/normalized \
 && useradd --system --uid 10001 --shell /usr/sbin/nologin appuser \
 && chown -R appuser:appuser /var/lib/data-collector /app

USER appuser

EXPOSE 8000

# Async ASGI workers behind gunicorn. Readiness/liveness are served at /ready
# and /health. The app port is never exposed publicly — an Ingress/proxy
# terminates TLS and mTLS in front of it.
CMD ["gunicorn", "app.main:app", \
     "-k", "uvicorn.workers.UvicornWorker", \
     "--bind", "0.0.0.0:8000", \
     "--timeout", "60", \
     "--graceful-timeout", "30", \
     "--access-logfile", "-", \
     "--error-logfile", "-"]
