# Deployment

## Docker image

`ghcr.io/aarondpn/fast-turbo-cache-server` is built for `linux/amd64` and `linux/arm64`: `latest` and the branch name from the default branch, `1.2.3` and `1.2` from version tags, and `sha-<commit>` for every published commit.

- The image is distroless (no shell) and runs as UID 10001. The local backend stores artifacts in the volume `/data/turbo-cache`.
- The `HEALTHCHECK` runs `fast-turbo-cache-server healthcheck`, which probes `/healthz` using `HOST` and `PORT` from the environment. Set those as environment variables, not flags.
- Docker's default stop timeout (10 s) is shorter than `SHUTDOWN_GRACE_SECS` (30 s). Use `docker run --stop-timeout 40` so in-flight uploads can finish.

### Read-only root filesystem

With an object store, `SPOOL_DIR` defaults to the OS temp directory, which is not writable with `--read-only` or Kubernetes `readOnlyRootFilesystem: true`. Then set `SPOOL_DIR` to a writable volume, such as `/data/turbo-cache/.spool` or an `emptyDir`. The local backend uses `<LOCAL_STORAGE_PATH>/.spool`.

## Docker Compose

See the [quick start](../README.md#docker-compose) for the commands and the [configuration reference](configuration.md#docker-compose) for compose-only variables.

- Every variable in `.env` is passed to the server, except `HOST`, `PORT`, `LOCAL_STORAGE_PATH` and `SPOOL_DIR`, which `docker-compose.yml` fixes.
- Artifacts and spool files are kept on the `turbo-cache-data` volume.
- The port is published on `127.0.0.1`. Set `BIND_ADDRESS=0.0.0.0` only behind a [TLS-terminating reverse proxy](#reverse-proxy-and-tls).
- `docker compose stop` and `down` wait 40 s, so in-flight uploads can finish within `SHUTDOWN_GRACE_SECS`.

### Bundled object stores

The `rustfs` and `garage` profiles create the bucket (`S3_BUCKET`, default `turbo-cache`). The stores are only reachable on the compose network, and `compose/<STORAGE_TYPE>.env` points `S3_ENDPOINT` at them. Keep `S3_ENDPOINT` commented out in `.env`: an empty value overrides that, and the server falls back to its loopback default. For an external S3 store, set `S3_ENDPOINT` and start without a profile.

`S3_ACCESS_KEY_ID` and `S3_SECRET_ACCESS_KEY` are also the store's credentials. Their format rules, and those of `GARAGE_RPC_SECRET` and `GARAGE_ADMIN_TOKEN`, are in `.env.example`; the profiles check them before starting the store. Garage keeps the key once imported: if you later change `S3_SECRET_ACCESS_KEY`, `garage-init` fails, so restore the old secret or remove the `garage-meta` volume.

## Reverse proxy and TLS

The server speaks plain HTTP. Put a TLS-terminating reverse proxy in front of it, bind the server to loopback or a private network, and never expose it directly. Minimal Caddy example:

```caddyfile
turbo-cache.example.com {
    request_body {
        max_size 512MB
    }
    reverse_proxy 127.0.0.1:3000
}
```

The proxy must:

- Allow request bodies of at least `MAX_ARTIFACT_SIZE`, and not buffer them, so transfers stream.
- Limit connections and set request header and keep-alive idle timeouts (Caddy `servers { timeouts { read_header ... idle ... } }`, nginx `client_header_timeout`, `keepalive_timeout` and `limit_conn`, HAProxy `timeout http-request` and `maxconn`). The server has none of these, which leaves it open to slowloris attacks; it only bounds request bodies (`BODY_IDLE_TIMEOUT_SECS`, `BODY_TOTAL_TIMEOUT_SECS`, upload slots).
- Strip the path if `TURBO_API` has one (`https://example.com/turbo`), because the server serves `/v8/...`.
- Pass `OPTIONS` requests through if clients use [`--preflight`](troubleshooting.md#preflight).
- Rate-limit failed token attempts if you need that. The server does not.

## Health checks

`/health` and `/healthz` are liveness checks. `/readyz` answers `200` when the backend is reachable: the local storage directory exists, or listing the object store prefix works. With `S3_HEALTH_CHECK_KEY`, it probes that key instead. `PROTECT_OBSERVABILITY` can require tokens for `/readyz` and `/metrics`.

## Running multiple replicas

Replicas can share one object store bucket; the local backend needs one server per directory.

Each replica has its own hot cache, with no invalidation across replicas. Overwrites, deletes and lifecycle expirations may stay invisible on other replicas for up to `CACHE_TTL_SECS` (default 300 s), and artifacts deleted by retention or a lifecycle rule may be served from the hot cache for that long. Turborepo hashes are content addresses, so this rarely matters. For strict consistency, lower `CACHE_TTL_SECS` or disable the hot cache (`MEMORY_CACHE_MAX_BYTES=0`, `METADATA_CACHE_MAX_ENTRIES=0`).

With OIDC, every replica needs the same `OIDC_SIGNING_KEY`. Every replica runs its own [retention](storage.md#retention) janitor. That is safe, but each one lists the bucket, so for large buckets set the retention limits on one replica only.

## Memory

| Consumer | Limit | Default |
|---|---|---|
| Hot cache | `MEMORY_CACHE_MAX_BYTES` | 128 MiB |
| Uploads in memory | `MAX_CONCURRENT_UPLOADS` × `UPLOAD_MEMORY_THRESHOLD` | 32 × 4 MiB = 128 MiB |
| Streamed uploads to an object store | `MAX_CONCURRENT_UPLOADS` × (`STORAGE_PARTS_IN_FLIGHT` + 1) × 8 MiB | 32 × 5 × 8 MiB = 1.25 GiB |
| Retained analytics events | fixed | about 16 MiB |
| Open connections | the reverse proxy's connection limit | – |

A streamed upload holds the parts being sent plus one being received. The 1.25 GiB worst case is reached only when 32 large uploads arrive faster than the store accepts them; lower either setting to tighten it. The compose file limits the server to 1 GiB (`TURBO_CACHE_MEM_LIMIT`); raise it together with these settings.

## Logging

Logs go to stdout as text or JSON (`LOG_FORMAT`), filtered by `RUST_LOG` (default `info`). Every request is logged once with method, path, status, latency and `x-request-id`, except successful `/health`, `/healthz`, `/readyz` and `/metrics` requests, which are logged at `debug`. The query string, and with it the namespace, is not logged. `RUST_LOG=info,fast_turbo_cache_server::access=off` turns the access log off.

## Metrics

`GET /metrics` serves Prometheus metrics named `turbo_cache_*`: lookups, uploads and rejections, bytes transferred, storage latency and errors, hot cache usage, and client-reported analytics and time saved. [Troubleshooting](troubleshooting.md#seeing-what-happens) explains the most useful ones.
