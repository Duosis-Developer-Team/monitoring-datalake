# data-collector-webservice

Generic data collector web service. A FastAPI service that accepts pushed metric
payloads over HTTPS POST (TLS 1.2+, mTLS), audits the raw body, normalizes known
metric classes, and writes Airflow-compatible staging files into the shared NFS
`pending/` directory — where the `generic_postgres_writer` DAG picks them up and
loads them into the PostgreSQL datalake.

The service is **source-agnostic**. The first (and currently only) ingest source
is `obm_agent` (OpenText OBM agents). Future sources plug in as sibling packages
under `app/sources/` without touching the core services.

## Data flow

```
OBM agent ──HTTPS/mTLS──▶ Ingress (mTLS termination) ──▶ data-collector pod(s)
                                                              │
                          archive raw ──▶ normalize ──▶ staging sink
                                                              │
                                   NFS  {STAGING_FOLDER_PATH}/pending/*.json
                                                              │
                                   Airflow generic_postgres_writer ──▶ PostgreSQL
```

The staging files use the exact `{meta, data}` contract documented in
[../docs/STAGING_FORMAT.md](../docs/STAGING_FORMAT.md), so the existing writer
loads them with no changes — one file per target table per request.

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET`  | `/health` | Liveness probe. |
| `GET`  | `/ready`  | Readiness probe (ensures storage dirs are writable). |
| `POST` | `/api/v1/obm/metrics` | OBM agent metric ingest. |

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
cd data-collector-webservice
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# point storage paths at a writable dir for dev, and enable the jsonl sink too:
sed -i.bak 's|/var/lib/data-collector|./var|; s|/nfs/airflow-staging|./var/staging|' .env
echo 'OUTPUT_SINKS=staging,jsonl' >> .env
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

Smoke test:

```bash
curl -s http://127.0.0.1:8000/health
curl -s -X POST http://127.0.0.1:8000/api/v1/obm/metrics \
     -H 'Content-Type: application/json' \
     --data @tests/fixtures/sample_obm_payload.json
# → a {meta,data} file appears under ./var/staging/pending/
```

## Run tests

```bash
pytest -q
```

## Deploy on Kubernetes (production target)

Deploys into the same Kubernetes cluster as Airflow, with multiple replicas
spread across the worker nodes, behind an mTLS-terminating Ingress. Manifests are
in [deploy/k8s/](deploy/k8s/); see [docs/KUBERNETES.md](docs/KUBERNETES.md) for
the full procedure (NFS PVCs shared with Airflow, mTLS secrets, HPA/PDB).

```bash
# after filling in image, hostnames, NFS server/path, and secrets:
kubectl apply -k deploy/k8s/
```

## Deploy behind HTTPS 443 (same-host / systemd or docker-compose)

Alternative layout: **OBM → Nginx (443, mTLS) → FastAPI (127.0.0.1:8000)**.
See [deploy/nginx.conf](deploy/nginx.conf), [deploy/systemd.service](deploy/systemd.service),
and [docs/RUNBOOK.md](docs/RUNBOOK.md). For local mTLS testing, run
`scripts/generate_test_certs.sh` then
`docker compose -f deploy/docker-compose.yml up --build`.

## Certificate auth — how it works

- TLS termination and client-certificate verification happen at the proxy
  (Kubernetes Ingress, or Nginx in the same-host layout).
- The proxy forwards its verdict to the app via headers:
  `X-SSL-Client-Verify` (must be `SUCCESS`), `X-SSL-Client-Subject`,
  `X-SSL-Client-Issuer`, `X-SSL-Client-Fingerprint`.
- The app re-checks these when `ENFORCE_PROXY_MTLS_HEADER=true`, matching against
  optional subject / fingerprint allowlists.
- The app port (`8000`) must never be exposed to the internet.

See [docs/SECURITY.md](docs/SECURITY.md) for the full security model.

## Output & storage layout

```
{STAGING_FOLDER_PATH}/pending/<source>_<table>_<request_id>.json  # writer input (primary)
$RAW_PAYLOAD_DIR/YYYY/MM/DD/<request_id>.json                     # exact bytes received (audit)
$QUARANTINE_DIR/YYYY/MM/DD/<request_id>.json                      # weak / failed records
$NORMALIZED_JSONL_PATH                                            # JSONL (dev sink only)
```

`OUTPUT_SINKS` selects which sinks run (`staging`, `jsonl`, or both). Staging is
the production path and is safe under many replicas (one atomically-written file
per request). The shared JSONL file is **not** replica-safe — dev/single-node
only. Raw payloads are always archived before normalization, so mapping bugs are
recoverable by replaying the audit archive after a fix.

## Project layout

```
app/
  main.py                       FastAPI app + lifespan; wires sources → sinks
  core/config.py                pydantic-settings configuration (source-agnostic)
  core/logging.py               JSON log formatter
  core/security.py              proxy mTLS header extraction + enforcement
  api/routes.py                 /health, /ready (system routes)
  api/ingest.py                 shared archive→normalize→persist HTTP handler
  services/ingestion_service.py orchestration (source-agnostic)
  services/storage_service.py   raw archive, quarantine, StagingSink, JsonlSink
  schemas/payload.py            response models
  sources/
    obm_agent/
      normalization.py          OBM envelope/class/metric mapping
      staging.py                normalized records → {meta,data} staging files
      routes.py                 POST /api/v1/obm/metrics
      mappings/collection_policy_summary.json   OBM mapping source of truth
tests/                          pytest suite (+ fixtures/)
scripts/                        dev cert generation + curl smoke script
deploy/                         Dockerfile, docker-compose, nginx.conf, systemd, k8s/
docs/                           API_CONTRACT, RUNBOOK, SECURITY, KUBERNETES, STAGING_OUTPUT
```

## Documentation

- [docs/API_CONTRACT.md](docs/API_CONTRACT.md) — request/response contract.
- [docs/STAGING_OUTPUT.md](docs/STAGING_OUTPUT.md) — how payloads become writer files.
- [docs/KUBERNETES.md](docs/KUBERNETES.md) — multi-replica K8s deployment (design).
- [docs/DEPLOYMENT_WALKTHROUGH.md](docs/DEPLOYMENT_WALKTHROUGH.md) — step-by-step rollout: discovery, image build/registry, placeholder map.
- [docs/RUNBOOK.md](docs/RUNBOOK.md) — same-host install, restart, replay.
- [docs/SECURITY.md](docs/SECURITY.md) — TLS / mTLS / hardening details.
