# Kubernetes deployment

Deploys the service into the same cluster as Airflow (3 masters + 3 workers),
with multiple replicas spread across the workers, behind an mTLS Ingress. Target
load: ~9000 nodes pushing every 5–10 minutes (~20 req/s average, small payloads).

Manifests live in [../deploy/k8s/](../deploy/k8s/) and are applied with kustomize.

## Architecture

```
OBM agents ──mTLS:443──▶ ingress-nginx (verifies client cert, sets X-SSL-* headers)
                              │  ClusterIP Service :80
                              ▼
                    Deployment (6+ replicas, spread across 3 workers)
                       ├── gunicorn + uvicorn workers (WEB_CONCURRENCY)
                       ├── writes pending/*.json ──▶ NFS (shared with Airflow)
                       └── writes raw/quarantine  ──▶ NFS (audit)
                              ▲
                    HPA (6→18 on CPU) · PDB (minAvailable 4)
```

## Why it scales horizontally

- **No shared mutable state.** Each request produces its own raw/quarantine file
  and its own atomically-written `pending/` file (UUID names). No replica ever
  contends with another. The single-file JSONL sink is therefore **disabled** in
  production (`OUTPUT_SINKS=staging`).
- **Stateless pods.** All durable state is on NFS; pods can be killed/rescheduled
  freely. `PodDisruptionBudget` keeps ≥4 serving during drains.
- **Spread.** `topologySpreadConstraints` + pod anti-affinity place replicas
  across the 3 workers; the HPA adds replicas under CPU pressure.

## Prerequisites

1. **Container image** built from [../deploy/Dockerfile](../deploy/Dockerfile)
   and pushed to a registry the cluster can pull. Set it in
   `deploy/k8s/kustomization.yaml` (`images:` newTag) and `30-deployment.yaml`.
2. **NFS** — the `data-collector-staging` PVC MUST be backed by the same NFS
   export + path the Airflow `generic_postgres_writer` scans (its
   `staging_folder_path`). Edit `20-pv-pvc.yaml` `nfs.server` / `nfs.path`. With
   `STAGING_FOLDER_PATH=/nfs/airflow-staging` and `PENDING_DIRNAME=pending`, the
   service writes into the same `…/pending/` the writer reads. A second export
   backs the raw/quarantine audit trail.
3. **mTLS secrets** (namespace `data-collector`):
   ```bash
   kubectl -n data-collector create secret tls data-collector-tls \
     --cert=server.crt --key=server.key
   kubectl -n data-collector create secret generic obm-client-ca \
     --from-file=ca.crt=client_ca.crt
   ```
4. **ingress-nginx** with snippet annotations allowed
   (`allow-snippet-annotations: "true"` in the controller ConfigMap) so the
   `X-SSL-Client-*` headers can be forwarded. If snippets are disallowed, use the
   sidecar nginx layout instead ([../deploy/nginx.conf](../deploy/nginx.conf)).

## Apply

```bash
# 1. create the namespace + secrets (above)
# 2. fill in image, hostnames, and NFS server/path in deploy/k8s/*
# 3. copy 11-secret.example.yaml → 11-secret.yaml, set allowlists, add to kustomization
kubectl apply -k deploy/k8s/

kubectl -n data-collector rollout status deploy/data-collector-webservice
kubectl -n data-collector get pods -o wide        # confirm spread across workers
```

## Configuration

Non-secret env is in `10-configmap.yaml`; allowlists in the Secret. Key values:

| Key | Production value | Notes |
|---|---|---|
| `OUTPUT_SINKS` | `staging` | jsonl is not replica-safe |
| `STAGING_FOLDER_PATH` | `/nfs/airflow-staging` | same NFS as Airflow writer |
| `PENDING_DIRNAME` | `pending` | writer's scan dir |
| `ENFORCE_PROXY_MTLS_HEADER` | `true` | re-check the Ingress verdict |
| `WEB_CONCURRENCY` | `4` | gunicorn workers per pod |

## Capacity notes

6 replicas × 4 workers comfortably absorb ~20 req/s of small JSON. Each request
does a few small file writes; the bottleneck is NFS metadata throughput, not CPU.
If `pending/` backlog grows, scale the **writer** cadence/throughput, not just
this service — files accumulate safely until the writer drains them.

## Health

- Liveness `GET /health`, readiness `GET /ready` (checks storage dirs writable).
- Logs are structured JSON on stdout — scrape with your cluster log stack.
