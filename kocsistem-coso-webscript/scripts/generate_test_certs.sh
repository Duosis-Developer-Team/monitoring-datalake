#!/usr/bin/env bash
#
# Generate a throw-away mTLS chain (root CA, server cert, client cert) for local dev.
# DO NOT use any output of this script in production. It exists so that
# `scripts/curl_mtls_example.sh` works against a local Nginx without requiring real PKI.
#

set -euo pipefail

OUT_DIR="${1:-./deploy/certs}"
SERVER_CN="${SERVER_CN:-localhost}"
CLIENT_CN="${CLIENT_CN:-obm-agent-dev}"
DAYS="${DAYS:-365}"

mkdir -p "$OUT_DIR"
cd "$OUT_DIR"

echo "Generating local root CA..."
openssl genrsa -out ca.key 4096
openssl req -x509 -new -nodes -key ca.key -sha256 -days "$DAYS" \
  -subj "/CN=COSO-Webscript-Dev-Root-CA" \
  -out ca.crt

echo "Generating server certificate (CN=$SERVER_CN)..."
openssl genrsa -out server.key 4096
openssl req -new -key server.key \
  -subj "/CN=$SERVER_CN" \
  -out server.csr
cat > server.ext <<EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names
[alt_names]
DNS.1 = $SERVER_CN
DNS.2 = localhost
IP.1 = 127.0.0.1
EOF
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out server.crt -days "$DAYS" -sha256 -extfile server.ext

echo "Generating client certificate (CN=$CLIENT_CN)..."
openssl genrsa -out client.key 4096
openssl req -new -key client.key \
  -subj "/CN=$CLIENT_CN" \
  -out client.csr
cat > client.ext <<EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature
extendedKeyUsage = clientAuth
EOF
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out client.crt -days "$DAYS" -sha256 -extfile client.ext

# Nginx expects the CA bundle that signed accepted client certs.
cp ca.crt client_ca.crt

chmod 600 ca.key server.key client.key

echo
echo "Generated files in $OUT_DIR:"
ls -la
echo
echo "Configure nginx with:"
echo "  ssl_certificate     $OUT_DIR/server.crt"
echo "  ssl_certificate_key $OUT_DIR/server.key"
echo "  ssl_client_certificate $OUT_DIR/client_ca.crt"
