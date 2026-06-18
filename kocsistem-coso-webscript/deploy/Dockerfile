FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Storage paths are created on app startup, but pre-creating them keeps permissions clean.
RUN mkdir -p /var/lib/coso-webscript/raw \
             /var/lib/coso-webscript/normalized \
             /var/lib/coso-webscript/quarantine \
 && useradd --system --shell /usr/sbin/nologin cosoweb \
 && chown -R cosoweb:cosoweb /var/lib/coso-webscript /app

USER cosoweb

EXPOSE 8000

CMD ["gunicorn", "app.main:app", \
     "-k", "uvicorn.workers.UvicornWorker", \
     "--bind", "0.0.0.0:8000", \
     "--workers", "2", \
     "--timeout", "60", \
     "--access-logfile", "-", \
     "--error-logfile", "-"]
