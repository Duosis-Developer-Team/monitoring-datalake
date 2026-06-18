#!/usr/bin/env bash
#
# Send a sample OBM payload through the mTLS endpoint using the dev certs produced by
# scripts/generate_test_certs.sh. Override HOST/PAYLOAD/CERT_DIR as environment variables.
#

set -euo pipefail

HOST="${HOST:-https://localhost}"
CERT_DIR="${CERT_DIR:-./deploy/certs}"
PAYLOAD="${PAYLOAD:-./tests/fixtures/sample_obm_payload.json}"

if [[ ! -f "$CERT_DIR/client.crt" ]]; then
  echo "Client cert not found at $CERT_DIR/client.crt — run generate_test_certs.sh first." >&2
  exit 1
fi

curl -v --tlsv1.2 \
  --cert "$CERT_DIR/client.crt" \
  --key "$CERT_DIR/client.key" \
  --cacert "$CERT_DIR/ca.crt" \
  -H "Content-Type: application/json" \
  -X POST "$HOST/api/v1/obm/metrics" \
  --data "@$PAYLOAD"
