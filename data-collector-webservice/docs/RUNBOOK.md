# Production Runbook

## 1. Prepare the host

```bash
sudo useradd --system --home /opt/data-collector --shell /usr/sbin/nologin appuser || true
sudo mkdir -p /opt/data-collector \
             /var/lib/data-collector/raw \
             /var/lib/data-collector/normalized \
             /var/lib/data-collector/quarantine
sudo chown -R appuser:appuser /var/lib/data-collector
```

## 2. Install the app

```bash
sudo -u appuser git clone <repo> /opt/data-collector
cd /opt/data-collector
sudo -u appuser python3 -m venv .venv
sudo -u appuser /opt/data-collector/.venv/bin/pip install -r requirements.txt
```

## 3. Configure environment

Create `/etc/data-collector.env` (owner `root:appuser`, mode `0640`):

```env
APP_ENV=prod
LOG_LEVEL=INFO
STRICT_VALIDATION=false
RAW_PAYLOAD_DIR=/var/lib/data-collector/raw
NORMALIZED_JSONL_PATH=/var/lib/data-collector/normalized/metrics.jsonl
QUARANTINE_DIR=/var/lib/data-collector/quarantine
MAX_BODY_BYTES=10485760
TRUST_PROXY_CERT_HEADERS=true
ENFORCE_PROXY_MTLS_HEADER=true
ALLOWED_CLIENT_CERT_SUBJECTS=obm-agent-01,obm-agent-02
```

## 4. Place certificates

```text
/etc/data-collector/certs/server.crt        # public-facing host cert
/etc/data-collector/certs/server.key        # private key (0600)
/etc/data-collector/certs/client_ca.crt     # CA that signs OBM client certs
```

## 5. Configure Nginx

```bash
sudo cp deploy/nginx.conf /etc/nginx/sites-available/data-collector
sudo ln -s /etc/nginx/sites-available/data-collector /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx
```

## 6. Install systemd unit

```bash
sudo cp deploy/systemd.service /etc/systemd/system/data-collector.service
sudo systemctl daemon-reload
sudo systemctl enable --now data-collector
sudo systemctl status data-collector
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
  -X POST https://data-collector.example.com/api/v1/obm/metrics \
  --data @sample.json
```

Verify on the server:

```bash
ls -lah /var/lib/data-collector/raw/$(date +%Y/%m/%d)/
tail -n 5 /var/lib/data-collector/normalized/metrics.jsonl
journalctl -u data-collector -n 50 -f
```

## 8. Rollback

Releases live under `/opt/data-collector/releases/<version>` and the
`/opt/data-collector/current` symlink points to the active one.

```bash
sudo ln -sfn /opt/data-collector/releases/<previous> /opt/data-collector/current
sudo systemctl restart data-collector
```

Raw payloads are never deleted on rollback — they are replayable.

## 9. Replay

When a normalization bug ships:

```bash
sudo systemctl stop data-collector-downstream
# Fix the mapping/parser, redeploy the app.
# Regenerate normalized output from raw archives:
python3 scripts/replay.py /var/lib/data-collector/raw/2026/05/24 \
    --jsonl /var/lib/data-collector/normalized/metrics.jsonl
sudo systemctl start data-collector-downstream
```

(A standalone replay script is left as a follow-up; the receiver itself stores
enough state to drive it.)

## 10. Operational checks

| What | How |
|---|---|
| Service alive | `systemctl status data-collector` |
| Logs | `journalctl -u data-collector -f` |
| Nginx access | `tail -f /var/log/nginx/data-collector.access.log` |
| Recent raw | `ls -lah /var/lib/data-collector/raw/$(date +%Y/%m/%d)/` |
| Recent normalized | `tail /var/lib/data-collector/normalized/metrics.jsonl` |
| Quarantine count | `find /var/lib/data-collector/quarantine -type f \| wc -l` |
