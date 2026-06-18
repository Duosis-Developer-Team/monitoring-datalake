# kocsistem-coso-webscript

KoçSistem COSO custom webscript receiver. A FastAPI service that accepts OBM-format
JSON metric payloads over HTTPS POST (port 443, TLS 1.2+, certificate-based auth),
audits the raw body, normalizes known metric classes, and returns `200 OK`.

The service replaces only the **OBM → COSO destination side**. The OBM agent's
collection logic on monitored nodes is untouched.

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET`  | `/health` | Liveness probe. |
| `GET`  | `/ready`  | Readiness probe (ensures storage dirs are writable). |
| `POST` | `/webscript/coso/metrics` | Canonical OBM ingest endpoint. |
| `POST` | `/api/v1/obm/metrics`     | Alias kept for parity with OBM-side naming. |

### Success response

```json
{
  "status": "ok",
  "request_id": "5b0c…",
  "accepted": true,
  "record_count": 1,
  "raw_payload_ref": "raw/2026/05/24/5b0c….json",
  "quarantined": false
}
```

### Error responses

| HTTP | `error` field | When |
|---|---|---|
| 400 | `malformed_json`, `empty_body` | Body is not valid UTF-8 JSON. |
| 401 | `client_certificate_not_verified` (and friends) | `ENFORCE_PROXY_MTLS_HEADER=true` but headers missing or rejected. |
| 413 | `request_body_too_large` | Body exceeded `MAX_BODY_BYTES`. |
| 422 | `validation_error` | `STRICT_VALIDATION=true` and a record is missing identity fields. |

## Run locally

```bash
cd kocsistem-coso-webscript
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# point storage paths at a writable dir for dev:
sed -i.bak 's|/var/lib/coso-webscript|./var|g' .env
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

Smoke test:

```bash
curl -s http://127.0.0.1:8000/health
curl -s -X POST http://127.0.0.1:8000/webscript/coso/metrics \
     -H 'Content-Type: application/json' \
     --data @../kocsistem_coso_webscript_cto_pack/reference/sample_obm_payload_assumed_shape.json
```

## Run tests

```bash
pytest -q
```

## Deploy behind HTTPS 443 (mTLS)

Production architecture: **OBM → Nginx (443, mTLS) → FastAPI (127.0.0.1:8000)**.

1. Provision certificates:
   - `server.crt` + `server.key` for the public-facing host.
   - `client_ca.crt` — the CA that signs the OBM agent's client certificate.
2. Copy [deploy/nginx.conf](deploy/nginx.conf) into the Nginx site directory and
   point its `ssl_*` directives at the certificate files.
3. Validate with `nginx -t` and reload.
4. Install the app (see [docs/RUNBOOK.md](docs/RUNBOOK.md)).
5. Set `ENFORCE_PROXY_MTLS_HEADER=true` and (optionally) populate
   `ALLOWED_CLIENT_CERT_SUBJECTS` or `ALLOWED_CLIENT_CERT_FINGERPRINTS` so the
   app double-checks the proxy's verdict.

For purely local mTLS testing, run `scripts/generate_test_certs.sh` to create a
throw-away CA, server, and client certificate set, then `docker compose -f
deploy/docker-compose.yml up --build`.

## Certificate auth — how it works

- TLS termination and client-certificate verification happen at Nginx.
- Nginx exposes the verification verdict to the app via headers:
  - `X-SSL-Client-Verify` (must be `SUCCESS`),
  - `X-SSL-Client-Subject`,
  - `X-SSL-Client-Issuer`,
  - `X-SSL-Client-Fingerprint`.
- The FastAPI app re-checks these headers when `ENFORCE_PROXY_MTLS_HEADER=true`,
  matching against optional subject / fingerprint allowlists.
- The app port (`8000`) must never be exposed to the internet. Bind it to
  `127.0.0.1` or to a container-internal network only reachable by Nginx.

See [docs/SECURITY.md](docs/SECURITY.md) for the full security model.

## Storage layout

```
$RAW_PAYLOAD_DIR/YYYY/MM/DD/<request_id>.json       # exact bytes received
$NORMALIZED_JSONL_PATH                              # append-only JSON Lines
$QUARANTINE_DIR/YYYY/MM/DD/<request_id>.json        # weak / failed records
```

Raw payloads are always written before normalization runs, so mapping bugs are
recoverable by replaying the audit archive after a fix.

## Project layout

```
app/
  main.py                       FastAPI app + lifespan
  core/config.py                pydantic-settings configuration
  core/logging.py               JSON log formatter
  core/security.py              proxy mTLS header extraction + enforcement
  api/routes.py                 /health, /ready, /webscript/coso/metrics, /api/v1/obm/metrics
  services/storage_service.py   raw archive, JSONL sink, quarantine
  services/normalization_service.py  envelope/class/metric mapping
  services/ingestion_service.py orchestrates archive → normalize → persist
  schemas/payload.py            response models
  mappings/collection_policy_summary.json  source-of-truth mapping
tests/                          pytest suite covering all CTO acceptance criteria
scripts/                        dev cert generation + curl smoke script
deploy/                         nginx.conf, Dockerfile, docker-compose.yml, systemd unit
docs/                           RUNBOOK, SECURITY, API_CONTRACT
```

## Documentation

- [docs/API_CONTRACT.md](docs/API_CONTRACT.md) — request/response contract.
- [docs/RUNBOOK.md](docs/RUNBOOK.md) — production install, restart, replay.
- [docs/SECURITY.md](docs/SECURITY.md) — TLS / mTLS / hardening details.
