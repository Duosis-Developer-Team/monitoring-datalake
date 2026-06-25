# data-collector-webservice — image pull instructions (customer)

The service image is published to GitHub Container Registry (ghcr.io) as a
**private** package. To pull it you need the access token (the "key") we provide.

- **Image:** `ghcr.io/duosis-developer-team/data-collector-webservice`
- **Tags:** `0.1.0` (pinned, recommended) and `latest`
- **Platforms:** `linux/amd64`, `linux/arm64` (Docker picks the right one automatically)
- **Digest (0.1.0):** `sha256:c83a5921b859e34b00a662fb20636daf9ed5a079503fad9b6e2d1f5c889c26c1`

## 1. Log in to ghcr.io

Replace `<TOKEN>` with the access key we gave you. The username is `coskungencay`.

```bash
echo "<TOKEN>" | docker login ghcr.io -u coskungencay --password-stdin
```

## 2. Pull the image

```bash
docker pull ghcr.io/duosis-developer-team/data-collector-webservice:0.1.0
```

## 3. Run

The app listens on `8000` and is meant to sit behind a TLS/mTLS-terminating proxy
(see the repo's `deploy/nginx.conf` / Kubernetes manifests). Quick local smoke run:

```bash
docker run --rm -p 8000:8000 \
  ghcr.io/duosis-developer-team/data-collector-webservice:0.1.0
curl -s http://127.0.0.1:8000/health
```

> Keep the token secret. If it leaks, tell us and we will revoke it.
