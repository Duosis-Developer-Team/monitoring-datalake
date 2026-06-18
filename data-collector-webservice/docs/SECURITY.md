# Security Model

## Network topology

```
OBM Agent  ──HTTPS POST :443, client cert──▶  Nginx  ──HTTP──▶  FastAPI (127.0.0.1:8000)
```

- Only Nginx is bound to `0.0.0.0:443`.
- FastAPI must bind to `127.0.0.1` (or a private container network).
- The OS firewall must reject inbound traffic to `:8000`.

## TLS

- TLS 1.2 and TLS 1.3 only (`ssl_protocols TLSv1.2 TLSv1.3`).
- Strong ciphers (`HIGH:!aNULL:!MD5:!RC4:!3DES`).
- HSTS / OCSP stapling can be added per organisational policy.
- Server cert / key live in `/etc/data-collector/certs/` and are owned `root:root`,
  mode `0600` on the key, `0644` on the cert.

## Client certificate verification (mTLS)

- Nginx is configured with `ssl_verify_client on` and a `client_ca.crt` that must
  sign the OBM client cert.
- On a verified handshake Nginx forwards:
  - `X-SSL-Client-Verify: SUCCESS`
  - `X-SSL-Client-Subject: <DN>`
  - `X-SSL-Client-Issuer: <DN>`
  - `X-SSL-Client-Fingerprint: <hex>`
- The app re-verifies these when `ENFORCE_PROXY_MTLS_HEADER=true`. If the
  verdict is anything other than `SUCCESS`, the request is rejected with
  HTTP 401.
- Optional defence-in-depth allowlists:
  - `ALLOWED_CLIENT_CERT_SUBJECTS` — comma-separated DN substrings.
  - `ALLOWED_CLIENT_CERT_FINGERPRINTS` — comma-separated fingerprints.

## Why headers (and not raw TLS in Python)?

Terminating TLS at Nginx keeps the certificate trust store out of application
code. Rotating client CAs or upgrading TLS policy never requires a deploy of the
Python service. The app only needs to trust that the proxy is on the same host
and that no other path leads to `:8000`.

If you must terminate TLS in Python (single-binary deploy), set
`TRUST_PROXY_CERT_HEADERS=false` and stand up a Uvicorn TLS listener with
`ssl_keyfile`, `ssl_certfile`, and `ssl_ca_certs` — but you lose the rotation
benefit and you must reload the process for every cert change.

## Body size and timeouts

- `MAX_BODY_BYTES` (default 10 MiB) bounded both at the app layer and at Nginx
  (`client_max_body_size`).
- `proxy_read_timeout 60s` / `proxy_send_timeout 60s` / `proxy_connect_timeout
  10s` — keep slow-loris bodies from holding workers.

## Logging hygiene

- Structured JSON logs to stdout (captured by journald in systemd or by the
  container engine in Docker).
- Logged fields include `request_id`, `client_ip`, `cert_subject`,
  `cert_fingerprint`, `record_count`, `raw_payload_ref`.
- The full request body is **not** logged. The raw payload archive on disk is
  the audit source of truth, and lives in a permission-restricted directory.

## File-system permissions

```
/var/lib/data-collector/raw          0700 appuser appuser
/var/lib/data-collector/normalized   0700 appuser appuser
/var/lib/data-collector/quarantine   0700 appuser appuser
/etc/data-collector.env              0640 root appuser
/etc/data-collector/certs/*.key      0600 root root
/etc/data-collector/certs/*.crt      0644 root root
```

The Docker image runs as the unprivileged `appuser` user. The systemd unit sets
`NoNewPrivileges=true`, `PrivateTmp=true`, `ProtectSystem=full`,
`ProtectHome=true`, and `ReadWritePaths=/var/lib/data-collector`.

## Secrets

No secrets are committed. All trust material is loaded from
`/etc/data-collector/certs/` (path is environment-driven). The `.env.example`
file documents every knob but contains no live values.
