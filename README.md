# fast-turbo-cache-server

A self-hosted [Turborepo](https://turborepo.com) remote cache server written in Rust. It stores artifacts on the local filesystem, in AWS S3 or an S3-compatible store such as RustFS or Garage, in Google Cloud Storage or in Azure Blob Storage.

## Features

- Turborepo v8 artifact API, including batch queries, analytics events, artifact signatures and `--preflight`.
- In-memory hot cache (LRU with TTL) in front of every backend.
- Streamed uploads and downloads: memory use does not grow with artifact size.
- Unscoped, namespace-scoped and read-only tokens.
- OIDC token exchange for GitHub Actions, so CI needs no long-lived secret.
- Team isolation by namespace, optional write-once artifacts and retention limits.
- `/healthz`, `/readyz`, Prometheus `/metrics`, JSON logs and a distroless Docker image.

## Quick start

### Docker Compose

```bash
cp .env.example .env              # set TURBO_TOKEN, e.g. openssl rand -hex 32
docker compose up -d --build      # local filesystem backend on 127.0.0.1:3000
```

To use a bundled object store, set `STORAGE_TYPE` and the store credentials in `.env`, then start the matching profile:

```bash
# .env: STORAGE_TYPE=rustfs S3_ACCESS_KEY_ID=... S3_SECRET_ACCESS_KEY=...
docker compose --profile rustfs up -d --build

# .env: STORAGE_TYPE=garage S3_ACCESS_KEY_ID=... S3_SECRET_ACCESS_KEY=...
#       GARAGE_RPC_SECRET=<openssl rand -hex 32> GARAGE_ADMIN_TOKEN=<at least 16 characters>
docker compose --profile garage up -d --build
```

Each profile starts the store and creates the bucket (`S3_BUCKET`, default `turbo-cache`). To run the published image instead of a local build:

```bash
TURBO_CACHE_IMAGE=ghcr.io/aarondpn/fast-turbo-cache-server:latest docker compose up -d --no-build --pull always
```

### Docker

```bash
docker run -d --name fast-turbo-cache-server \
  -p 127.0.0.1:3000:3000 \
  -e TURBO_TOKEN="$(openssl rand -hex 32)" \
  -v turbo-cache-data:/data/turbo-cache \
  --read-only --cap-drop ALL --security-opt no-new-privileges:true \
  --stop-timeout 40 \
  ghcr.io/aarondpn/fast-turbo-cache-server:latest
```

`--stop-timeout 40` lets in-flight uploads finish on `docker stop`. With an object store, also set [`SPOOL_DIR`](docs/deployment.md#read-only-root-filesystem).

### From source

```bash
cargo run --release -- \
  --storage-type rustfs \
  --s3-endpoint http://127.0.0.1:9000 \
  --s3-bucket turbo-cache \
  --s3-access-key-id rustfsadmin \
  --s3-secret-access-key-file ./secrets/s3-secret \
  --token-file ./secrets/turbo-token
```

Pass secrets with the `*_FILE` options, because flags are visible in the process list.

The server speaks plain HTTP. Before you expose it, put a [TLS-terminating reverse proxy](docs/deployment.md#reverse-proxy-and-tls) in front of it.

## Configuring the Turborepo client

```bash
export TURBO_API="https://turbo-cache.example.com"
export TURBO_TOKEN="<token>"
export TURBO_TEAM="my-team"   # the namespace
npx turbo run build
```

turbo ignores the token when `TURBO_TEAM` is not set. For signed artifacts (`"remoteCache": { "signature": true }` in `turbo.json`), set `TURBO_REMOTE_CACHE_SIGNATURE_KEY` on every client.

See also [all client settings](docs/troubleshooting.md#client-configuration) and [Docker builds](docs/troubleshooting.md#docker-builds).

## Documentation

- [Configuration reference](docs/configuration.md): all options, defaults and endpoints.
- [Authentication and access control](docs/authentication.md): tokens, namespaces, write-once artifacts, OIDC for GitHub Actions.
- [Storage backends](docs/storage.md): local, S3, RustFS, Garage, GCS, Azure and retention.
- [Deployment](docs/deployment.md): Docker, reverse proxy and TLS, health checks, replicas, memory, logs and metrics.
- [Troubleshooting](docs/troubleshooting.md): cache misses, client settings and turbo behaviour.
- [Development](docs/development.md): build and tests.

## Not implemented

`/v2/user` and `/v2/teams`, so `turbo login` and `turbo link` do not work. Configure clients with environment variables as shown above.

## License

[MIT](LICENSE)
