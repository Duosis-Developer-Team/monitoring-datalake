# Deployment walkthrough (data-collector-webservice → Airflow Kubernetes)

End-to-end, step-by-step deployment onto the existing Airflow Kubernetes cluster
(3 master + 3 worker), with multiple replicas, mTLS, and shared-NFS staging
output. This doc has **no environment-specific values** — it tells you what to
discover and where each discovered value goes. Fill the placeholders locally;
those edits are **not** pushed to git.

Workflow for our meeting:
1. You run the **Discovery** commands (Section 1) and paste the outputs.
2. I fill `10-configmap.yaml`, `20-pv-pvc.yaml` (or PVC reuse), `30-deployment.yaml`,
   `50-ingress.yaml` with your real values.
3. You build & push the image (Section 3) and apply (Section 5).

---

## 0. The one invariant that must hold

> The container path the webservice writes to — `<STAGING_FOLDER_PATH>/<PENDING_DIRNAME>`
> — must be the **same physical NFS directory** the Airflow `generic_postgres_writer`
> scans (`<staging_folder_path Variable>/pending`).

Everything in Section 1–2 exists to satisfy this. The cleanest way is to **reuse
the Airflow staging PVC** rather than define a parallel NFS PV.

---

## 1. Discovery (run these, capture the output)

```bash
# 1.1 Airflow namespace
kubectl get ns | grep -i airflow
NS=airflow                      # ← set to the real namespace

# 1.2 Writer's scan root (the Airflow Variable)
kubectl exec -n $NS deploy/airflow-scheduler -- airflow variables get staging_folder_path
#    e.g. /opt/airflow/dags/data_staging/zabbix   → writer scans .../zabbix/pending

# 1.3 Scheduler volumes + mounts: which volume backs that path?
kubectl get pod -n $NS -l component=scheduler \
  -o jsonpath='{.items[0].spec.volumes}' | python3 -m json.tool
kubectl get pod -n $NS -l component=scheduler \
  -o jsonpath='{range .items[0].spec.containers[*].volumeMounts[*]}{.name}{"  "}{.mountPath}{"  subPath="}{.subPath}{"\n"}{end}'

# 1.4 If it is a PVC, resolve its backing NFS server/path
kubectl get pvc -n $NS
kubectl get pvc <staging-pvc> -n $NS -o jsonpath='{.spec.volumeName}{"\n"}'
kubectl get pv <volume-name> -o jsonpath='nfs.server={.spec.nfs.server}{"\n"}nfs.path={.spec.nfs.path}{"\n"}accessModes={.spec.accessModes}{"\n"}'

# 1.5 Node architecture (decides the image platform) and spread
kubectl get nodes -o wide
kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"  arch="}{.status.nodeInfo.architecture}{"\n"}{end}'

# 1.6 Ingress controller class + whether snippet annotations are allowed
kubectl get ingressclass
kubectl -n ingress-nginx get cm ingress-nginx-controller -o yaml | grep -i 'allow-snippet' || echo "snippet flag not set (default: blocked on newer charts)"

# 1.7 What registry do existing workloads pull from? Is it private?
kubectl get deploy -n $NS -o jsonpath='{range .items[*]}{.spec.template.spec.containers[0].image}{"\n"}{end}' | sort -u
kubectl get pod -n $NS -l component=scheduler -o jsonpath='{.items[0].spec.imagePullSecrets}{"\n"}'

# 1.8 StorageClass for RWX (for the new audit PVC), if dynamic provisioning exists
kubectl get storageclass
```

### Discovery output template (paste filled-in)

```
NS (airflow namespace):                 __________
staging_folder_path Variable:           __________
staging volume name + type:             __________  (PVC name? / nfs / hostPath / git-sync emptyDir?)
  -> if PVC: PVC name:                   __________   accessModes: __________ (need RWX)
  -> backing PV nfs.server : nfs.path:   __________ : __________
  -> mountPath (+subPath) of staging:    __________
node architecture:                       __________ (amd64 / arm64)
ingressClassName:                        __________
snippet annotations allowed?             yes / no
registry in use (host/project):          __________
imagePullSecret name (if private):       __________
RWX StorageClass (for audit PVC):        __________ (or "none")
```

---

## 2. Decide the staging wiring (based on 1.3/1.4)

**Case A — staging is an RWX PVC (recommended): reuse it.**
- Delete `deploy/k8s/20-pv-pvc.yaml`'s staging PV/PVC; point the Deployment's
  `staging` volume at the existing PVC.
- Set `STAGING_FOLDER_PATH` so `<it>/pending` == writer's pending dir.

In `30-deployment.yaml`:
```yaml
      volumes:
        - name: staging
          persistentVolumeClaim:
            claimName: <airflow-staging-pvc>     # from 1.4
        - name: audit
          persistentVolumeClaim:
            claimName: data-collector-audit       # new RWX PVC (keep in 20-pv-pvc.yaml)
        - name: tmp
          emptyDir: {}
```
In `10-configmap.yaml`, if the staging PVC root maps to the export root and the
zabbix dir is a subpath:
```yaml
  STAGING_FOLDER_PATH: "/nfs/airflow-staging/dags/data_staging/zabbix"  # mount + subpath to the zabbix dir
  PENDING_DIRNAME: "pending"
```
> Adjust the subpath so the full container path equals the writer's
> `staging_folder_path` + `/pending`. If you instead mount with `subPath:` in the
> volumeMount, shorten `STAGING_FOLDER_PATH` accordingly. Net rule: container
> pending path == writer pending path.

**Case B — staging is NOT a shared RWX PVC** (e.g. dags are git-sync emptyDir, so
`data_staging` isn't shared) **or you want a separate namespace.**
- Keep `20-pv-pvc.yaml`; set `nfs.server`/`nfs.path` (from 1.4, or your NFS admin)
  to the export that holds the staging dir.
- You may also need to **move Airflow's** `staging_folder_path` onto that same
  shared NFS so both sides agree. Coordinate this change on the Airflow side.

---

## 3. Build the image (CRITICAL: match node architecture)

You are on macOS; cluster nodes are almost certainly `linux/amd64` (confirm with
1.5). A plain `docker build` on a Mac produces the wrong arch → pods crash with
`exec format error`. **Always build for the node platform with buildx.**

```bash
cd data-collector-webservice

REGISTRY=<registry-host>/<project>          # from 1.7, e.g. harbor.corp.local/monitoring
IMAGE=$REGISTRY/data-collector-webservice
TAG=0.1.0

# Log in to the registry (use a robot account / token, not a personal password)
docker login <registry-host>

# Build for amd64 and push in one step (buildx)
docker buildx create --use --name dcw-builder 2>/dev/null || docker buildx use dcw-builder
docker buildx build \
  --platform linux/amd64 \
  -f deploy/Dockerfile \
  -t $IMAGE:$TAG \
  --push .

# Verify the pushed manifest is amd64
docker buildx imagetools inspect $IMAGE:$TAG | grep -i platform
```

Then update `deploy/k8s/kustomization.yaml`:
```yaml
images:
  - name: registry.example.local/data-collector-webservice
    newName: <registry-host>/<project>/data-collector-webservice
    newTag: "0.1.0"
```

### How nodes pull it

- **Public/anonymous registry:** nothing extra.
- **Private registry (typical):** create a pull secret and reference it.
  ```bash
  kubectl -n $NS create secret docker-registry regcred \
    --docker-server=<registry-host> \
    --docker-username=<robot-user> \
    --docker-password=<robot-token>
  ```
  Then uncomment `imagePullSecrets: [{name: regcred}]` in `30-deployment.yaml`
  (already stubbed there), **or** patch the namespace default ServiceAccount:
  ```bash
  kubectl -n $NS patch serviceaccount default \
    -p '{"imagePullSecrets":[{"name":"regcred"}]}'
  ```

### Registry options if you don't have one

| Situation | Approach |
|---|---|
| Have Harbor/Nexus/Artifactory | Push there (above). Best option. |
| Cloud registry (ECR/GCR/ACR) reachable from cluster | Push there; use the cloud's pull-secret mechanism. |
| Air-gapped, no registry | `docker save $IMAGE:$TAG -o dcw.tar`, copy to each node, then `ctr -n k8s.io images import dcw.tar` (containerd). Set `imagePullPolicy: IfNotPresent`. Tedious for 3 nodes; a small in-cluster registry is better long-term. |
| Want an in-cluster registry | Deploy `registry:2` (or use the cluster's built-in), push to its in-cluster Service DNS. |

**Pick the registry in the meeting** based on 1.7 (what Airflow already pulls
from is usually the right answer — same registry, same pull secret).

---

## 4. mTLS secrets

```bash
# server cert/key = this service's public TLS identity
kubectl -n $NS create secret tls data-collector-tls --cert=server.crt --key=server.key
# CA that signed the OBM agents' client certs (key MUST be named ca.crt)
kubectl -n $NS create secret generic obm-client-ca --from-file=ca.crt=client_ca.crt
```

Optional allowlist (defence-in-depth): copy `11-secret.example.yaml` →
`11-secret.yaml`, fill `ALLOWED_CLIENT_CERT_SUBJECTS` / `_FINGERPRINTS`, add it to
`kustomization.yaml`.

If snippet annotations are **blocked** (1.6) you can't forward `X-SSL-*` headers
via the Ingress — either enable `allow-snippet-annotations: "true"` on the
controller, or terminate mTLS with the sidecar nginx layout
([deploy/nginx.conf](../deploy/nginx.conf)) instead of the Ingress.

---

## 5. Placeholder map (file → placeholder → value)

| File | Placeholder | Value from |
|---|---|---|
| all `deploy/k8s/*.yaml` | `namespace: data-collector` | Case A: change to `$NS`. Case B: keep, apply `00-namespace.yaml`. |
| `10-configmap.yaml` | `STAGING_FOLDER_PATH`, `PENDING_DIRNAME` | Section 2 — must equal writer pending path |
| `20-pv-pvc.yaml` | `nfs.server`, `nfs.path` | 1.4 (Case B only; delete staging PV/PVC in Case A) |
| `30-deployment.yaml` | `image:` | Section 3 (`$IMAGE:$TAG`) |
| `30-deployment.yaml` | `imagePullSecrets` | Section 3 (private registry) |
| `30-deployment.yaml` | `staging` volume claim | Section 2 (Case A → airflow PVC) |
| `50-ingress.yaml` | host, `ingressClassName` | 1.6 + your DNS name for OBM agents |
| `kustomization.yaml` | image `newName`/`newTag` | Section 3 |

---

## 6. Apply & verify

```bash
kubectl apply -k data-collector-webservice/deploy/k8s/
kubectl -n $NS rollout status deploy/data-collector-webservice
kubectl -n $NS get pods -o wide          # confirm 6 pods spread across 3 workers

# Staging writability from inside a pod (the make-or-break check)
POD=$(kubectl -n $NS get pod -l app.kubernetes.io/name=data-collector-webservice -o name | head -1)
PEND=/nfs/airflow-staging/dags/data_staging/zabbix/pending   # = STAGING_FOLDER_PATH/PENDING_DIRNAME
kubectl -n $NS exec $POD -- sh -c "ls -ld $PEND && touch $PEND/.wtest && rm -f $PEND/.wtest && echo WRITABLE"
```

- `Permission denied` → NFS `root_squash`. Fix: export `no_root_squash`, **or**
  `chown 10001:10001` the dir (pod runs as uid/gid 10001 with `fsGroup: 10001`).
- Probes failing → `kubectl -n $NS logs $POD` (structured JSON on stdout).

End-to-end: send one mTLS request to `https://<host>/api/v1/obm/metrics`; within
~5 min a `obm_agent_*.json` appears in `pending/`, the writer loads it, and a row
shows up in `opsb_agent_node`.

---

## 7. Database + OBM agent cutover

```bash
psql -h <db_host> -U <admin> -d <db> -f sql/05_obm_agent.sql      # typed OBM tables
psql -h <db_host> -U <admin> -d <db> -f sql/04_zabbix_metadata.sql # for the metadata DAG
```

Repoint the OBM agents' COSO destination to
`https://<host>/api/v1/obm/metrics`, with client certs signed by the CA in
`obm-client-ca`. Roll out to a pilot group first, watch `pending/` fill and the
`opsb_agent_*` tables grow, then ramp to all ~9000 nodes.

---

## 8. Day-2 quick reference

```bash
kubectl -n $NS get hpa data-collector-webservice          # autoscaling state
kubectl -n $NS logs -l app.kubernetes.io/name=data-collector-webservice -f --max-log-requests 6
ls -1 $PEND | wc -l                                       # pending backlog (should drain every 5 min)
find <RAW_PAYLOAD_DIR>/$(date +%Y/%m/%d) -type f | wc -l   # ingest volume today
find <QUARANTINE_DIR> -type f | wc -l                      # quarantined (investigate if growing)
```

Roll a new image: rebuild with a new `TAG`, push, bump `kustomization.yaml`
`newTag`, `kubectl apply -k …`. Rollout is zero-downtime (`maxUnavailable: 0`,
PDB `minAvailable: 4`).
```
