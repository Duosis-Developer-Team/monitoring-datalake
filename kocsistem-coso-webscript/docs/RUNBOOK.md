# Production Runbook

## 1. Prepare the host

```bash
sudo useradd --system --home /opt/coso-webscript --shell /usr/sbin/nologin cosoweb || true
sudo mkdir -p /opt/coso-webscript \
             /var/lib/coso-webscript/raw \
             /var/lib/coso-webscript/normalized \
             /var/lib/coso-webscript/quarantine
sudo chown -R cosoweb:cosoweb /var/lib/coso-webscript
```

## 2. Install the app

```bash
sudo -u cosoweb git clone <repo> /opt/coso-webscript
cd /opt/coso-webscript
sudo -u cosoweb python3 -m venv .venv
sudo -u cosoweb /opt/coso-webscript/.venv/bin/pip install -r requirements.txt
```

## 3. Configure environment

Create `/etc/coso-webscript.env` (owner `root:cosoweb`, mode `0640`):

```env
APP_ENV=prod
LOG_LEVEL=INFO
STRICT_VALIDATION=false
RAW_PAYLOAD_DIR=/var/lib/coso-webscript/raw
NORMALIZED_JSONL_PATH=/var/lib/coso-webscript/normalized/metrics.jsonl
QUARANTINE_DIR=/var/lib/coso-webscript/quarantine
MAX_BODY_BYTES=10485760
TRUST_PROXY_CERT_HEADERS=true
ENFORCE_PROXY_MTLS_HEADER=true
ALLOWED_CLIENT_CERT_SUBJECTS=obm-agent-01,obm-agent-02
```

## 4. Place certificates

```text
/etc/coso-webscript/certs/server.crt        # public-facing host cert
/etc/coso-webscript/certs/server.key        # private key (0600)
/etc/coso-webscript/certs/client_ca.crt     # CA that signs OBM client certs
```

## 5. Configure Nginx

```bash
sudo cp deploy/nginx.conf /etc/nginx/sites-available/coso-webscript
sudo ln -s /etc/nginx/sites-available/coso-webscript /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx
```

## 6. Install systemd unit

```bash
sudo cp deploy/systemd.service /etc/systemd/system/coso-webscript.service
sudo systemctl daemon-reload
sudo systemctl enable --now coso-webscript
sudo systemctl status coso-webscript
```

## 7. Smoke test

Anonymous health (loopback):

```bash
curl -k https://localhost/health
```

mTLS ingest from a workstation with the OBM client cert:

```bash
curl -v --tlsv1.2 \
  --cert client.crt --key client.key --cacert server_ca.crt \
  -H 'Content-Type: application/json' \
  -X POST https://coso-webscript.example.com/webscript/coso/metrics \
  --data @sample.json
```

Verify on the server:

```bash
ls -lah /var/lib/coso-webscript/raw/$(date +%Y/%m/%d)/
tail -n 5 /var/lib/coso-webscript/normalized/metrics.jsonl
journalctl -u coso-webscript -n 50 -f
```

## 8. Rollback

Releases live under `/opt/coso-webscript/releases/<version>` and the
`/opt/coso-webscript/current` symlink points to the active one.

```bash
sudo ln -sfn /opt/coso-webscript/releases/<previous> /opt/coso-webscript/current
sudo systemctl restart coso-webscript
```

Raw payloads are never deleted on rollback — they are replayable.

## 9. Replay

When a normalization bug ships:

```bash
sudo systemctl stop coso-webscript-downstream
# Fix the mapping/parser, redeploy the app.
# Regenerate normalized output from raw archives:
python3 scripts/replay.py /var/lib/coso-webscript/raw/2026/05/24 \
    --jsonl /var/lib/coso-webscript/normalized/metrics.jsonl
sudo systemctl start coso-webscript-downstream
```

(A standalone replay script is left as a follow-up; the receiver itself stores
enough state to drive it.)

## 10. Operational checks

| What | How |
|---|---|
| Service alive | `systemctl status coso-webscript` |
| Logs | `journalctl -u coso-webscript -f` |
| Nginx access | `tail -f /var/log/nginx/coso-webscript.access.log` |
| Recent raw | `ls -lah /var/lib/coso-webscript/raw/$(date +%Y/%m/%d)/` |
| Recent normalized | `tail /var/lib/coso-webscript/normalized/metrics.jsonl` |
| Quarantine count | `find /var/lib/coso-webscript/quarantine -type f \| wc -l` |
