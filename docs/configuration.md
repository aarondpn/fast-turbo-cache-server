# Configuration reference

Every option is an environment variable and a flag, and flags win. The flag is the lower-case name with `-` for `_` and without `TURBO_`: `MAX_ARTIFACT_SIZE` is `--max-artifact-size`. Exceptions are `--tokens` for `TURBO_TOKEN` and `-p` for `PORT`. `fast-turbo-cache-server --help` lists all flags.

- Booleans accept `true`/`false`, `1`/`0`, `yes`/`no` and `on`/`off`. As flags they need the `=` form (`--allow-delete=false`); a bare flag means `true`.
- Lists are comma-separated. Token files hold tokens separated by commas or newlines, with `#` comment lines.
- `_FILE` options read a secret from a file. Prefer them, because flags are visible in the process list. Set a secret or its `_FILE` option, not both; only token files add to the tokens set directly.
- An empty value (`NAME=`) means unset for text options: tokens, the OIDC issuer, audience, claims and key, `STORAGE_PREFIX`, and bucket, endpoint and credential options. For numbers, booleans, file paths, `HOST` and fixed choices such as `LOG_FORMAT`, it is a startup error.

## Server

| Environment variable | Default | Description |
|---|---|---|
| `HOST` | `0.0.0.0` | Bind address. |
| `PORT` | `3000` | Listen port. |
| `BODY_IDLE_TIMEOUT_SECS` | `30` | `408` when no body data arrives for this long. |
| `BODY_TOTAL_TIMEOUT_SECS` | `900` | `408` when receiving the body takes longer in total, not counting time spent storing it. |
| `SHUTDOWN_GRACE_SECS` | `30` | Wait this long for in-flight requests on SIGTERM or SIGINT. |
| `CORS_ALLOW_ORIGIN` | `*` | Value of `Access-Control-Allow-Origin`: `*` or one origin such as `https://app.example.com` (no path or trailing slash). |
| `LOG_FORMAT` | `text` | `text` or `json`. |

## Authentication and access control

See [Authentication and access control](authentication.md).

| Environment variable | Default | Description |
|---|---|---|
| `TURBO_TOKEN` | – | Unscoped tokens, valid for every namespace. |
| `TURBO_TOKEN_FILE` | – | File with unscoped tokens. |
| `TURBO_SCOPED_TOKENS` | – | `namespace=token` entries. |
| `TURBO_SCOPED_TOKENS_FILE` | – | File with scoped tokens. |
| `TURBO_READ_ONLY_TOKENS` | – | Unscoped tokens that cannot upload or delete. |
| `TURBO_READ_ONLY_TOKEN_FILE` | – | File with read-only tokens. |
| `TURBO_SCOPED_READ_ONLY_TOKENS` | – | Read-only `namespace=token` entries. |
| `TURBO_SCOPED_READ_ONLY_TOKENS_FILE` | – | File with read-only scoped tokens. |
| `OIDC_ISSUER` | – | Enables the [OIDC token exchange](authentication.md#github-actions-oidc). `https` (`http` for loopback); a trailing slash is ignored. |
| `OIDC_AUDIENCE` | – | Required `aud` of ID tokens. Required with `OIDC_ISSUER`. |
| `OIDC_REQUIRED_CLAIMS` | – | [Claim rules](authentication.md#claim-rules) every ID token must match. Required with `OIDC_ISSUER`. |
| `OIDC_WRITE_CLAIMS` | – | Claim rules for read-write cache tokens. Unset: all read-write. |
| `OIDC_NAMESPACE_CLAIM` | – | Claim that names the namespace of a cache token. Unset: all namespaces. |
| `OIDC_TOKEN_TTL_SECS` | `3600` | Lifetime of cache tokens, `1`-`86400`. |
| `OIDC_SIGNING_KEY` | – | Base64 HMAC key of at least 32 bytes for [cache tokens](authentication.md#signing-key). Required with `OIDC_ISSUER`. |
| `OIDC_SIGNING_KEY_FILE` | – | File with the signing key. |
| `ALLOW_UNAUTHENTICATED` | `false` | Accept requests without a token. Cannot be combined with tokens or OIDC. |
| `ISOLATE_TEAMS` | `true` | Separate artifacts by namespace. `false` puts everything in `default`. |
| `IMMUTABLE_ARTIFACTS` | `false` | [Reject overwrites](authentication.md#write-once-artifacts) with `403`. Requires `ALLOW_DELETE=false`. |
| `ALLOW_DELETE` | `false` | Allow `DELETE /v8/artifacts/{hash}`. |
| `PROTECT_OBSERVABILITY` | `false` | Require a token for `/readyz` and an unscoped token for `/metrics`, which covers all namespaces. |
| `CACHING_STATUS` | `enabled` | `enabled`, `disabled`, `over_limit` or `paused` ([turbo's reaction](troubleshooting.md#turbo-behaviour-that-looks-like-a-server-problem)). |

## Uploads and limits

| Environment variable | Default | Description |
|---|---|---|
| `MAX_ARTIFACT_SIZE` | `536870912` (512 MiB) | Maximum upload size in bytes ([object store limits](storage.md#upload-size-limits)). |
| `MAX_CONCURRENT_UPLOADS` | `32` | Maximum concurrent uploads (upload slots). |
| `MAX_CONCURRENT_UPLOADS_PER_NAMESPACE` | `0` (automatic) | Upload slots per namespace. `0`: half of `MAX_CONCURRENT_UPLOADS` with scoped tokens or `OIDC_NAMESPACE_CLAIM`, else unlimited. |
| `BATCH_HEAD_CONCURRENCY` | `64` | Backend lookups in flight for batch queries, server-wide. |
| `UPLOAD_SLOT_WAIT_SECS` | `10` | Wait for an upload slot before `429` with `Retry-After`. Counts towards turbo's upload timeout. |
| `UPLOAD_MEMORY_THRESHOLD` | `4194304` (4 MiB) | Uploads up to this size stay in memory; larger ones are spooled or [streamed](storage.md#uploads). |
| `SPOOL_DIR` | `<LOCAL_STORAGE_PATH>/.spool` (`local`), else the OS temp directory | Spooled uploads and copies of S3 credential tokens. Must be writable. |

## Hot cache

| Environment variable | Default | Description |
|---|---|---|
| `MEMORY_CACHE_MAX_BYTES` | `134217728` (128 MiB) | Payload byte budget. `0` disables the payload cache. |
| `MEMORY_CACHE_MAX_ENTRIES` | `10000` | Maximum cached payloads. |
| `HOT_CACHE_MAX_ITEM_BYTES` | `4194304` (4 MiB) | Largest cached artifact. |
| `METADATA_CACHE_MAX_ENTRIES` | `50000` | Maximum cached metadata entries. `0` disables the metadata cache. |
| `CACHE_TTL_SECS` | `300` | Lifetime of cache entries ([replicas](deployment.md#running-multiple-replicas)). `0`: no expiry. |
| `NEGATIVE_CACHE_TTL_SECS` | `0` | Lifetime of cached misses. `0`: off. |

## Storage

See [Storage backends](storage.md).

| Environment variable | Default | Description |
|---|---|---|
| `STORAGE_TYPE` | `local` | `local`, `s3`, `rustfs`, `garage`, `gcs` or `azure`. |
| `LOCAL_STORAGE_PATH` | `./.turbo-cache` (image: `/data/turbo-cache`) | Directory of the local backend. |
| `LOCAL_TEMP_MAX_AGE_SECS` | `3600` | Age after which orphaned local temp files are deleted. |
| `STORAGE_MAX_BYTES` | `0` | Total size limit in bytes ([retention](storage.md#retention)). `0`: unlimited. |
| `STORAGE_MAX_AGE_SECS` | `0` | Maximum artifact age ([retention](storage.md#retention)). `0`: never expire. |
| `STORAGE_CLEANUP_INTERVAL_SECS` | `600` | Interval of the retention janitor and the spool sweep. |
| `STORAGE_PREFIX` | empty | Key prefix in the bucket or container: `<prefix>/<namespace>/<hash>`. No empty, `.` or `..` segments. Ignored by `local`. Required with `STORAGE_MAX_BYTES` or `STORAGE_MAX_AGE_SECS` ([Retention](storage.md#retention)). |
| `STORAGE_MAX_RETRIES` | `3` | Retries for transient object store errors. |
| `STORAGE_PARTS_IN_FLIGHT` | `4` | Multipart parts (8 MiB) sent concurrently per upload, `1`-`16` ([memory](deployment.md#memory)). |
| `STORAGE_CONNECT_TIMEOUT_SECS` | `5` | Object store connect timeout. |
| `STORAGE_READ_TIMEOUT_SECS` | `30` | Longest wait for data; for response headers plus the connect timeout and 1 s per 256 KiB sent. |

### S3

For `s3`, `rustfs` and `garage`. Values in parentheses are fallback environment variables.

| Environment variable | Default | Description |
|---|---|---|
| `S3_BUCKET` | – (`AWS_BUCKET`) | Bucket name. Required. |
| `S3_REGION` | [per type](storage.md#s3) | Signing region. |
| `S3_ENDPOINT` | [per type](storage.md#s3) | Endpoint URL. |
| `S3_ACCESS_KEY_ID` | – (`AWS_ACCESS_KEY_ID`) | Access key ID. |
| `S3_SECRET_ACCESS_KEY` | – (`AWS_SECRET_ACCESS_KEY`) | Secret access key. |
| `S3_SECRET_ACCESS_KEY_FILE` | – | File with the secret access key. |
| `S3_SESSION_TOKEN` | – (`AWS_SESSION_TOKEN`) | Session token. |
| `S3_FORCE_PATH_STYLE` | [per type](storage.md#s3) | Path-style addressing. |
| `S3_SIGN_PAYLOAD` | `false` | [Sign](storage.md#payload-signing) the SHA-256 of upload payloads. |
| `S3_TREAT_403_AS_MISS` | `false` | Treat `403` on lookups as a miss. See [IAM permissions](storage.md#iam-permissions). |
| `S3_HEALTH_CHECK_KEY` | – | Key that `/readyz` probes instead of listing the prefix. `STORAGE_PREFIX` is not applied. |

### Google Cloud Storage

| Environment variable | Default | Description |
|---|---|---|
| `GCS_BUCKET` | – (`GOOGLE_BUCKET`) | Bucket name. Required. |
| `GCS_SERVICE_ACCOUNT_KEY_FILE` | – (`GOOGLE_SERVICE_ACCOUNT`) | Service account key file (JSON). Unset: Application Default Credentials. |

### Azure Blob Storage

| Environment variable | Default | Description |
|---|---|---|
| `AZURE_STORAGE_ACCOUNT` | – (`AZURE_STORAGE_ACCOUNT_NAME`) | Storage account name. Required. |
| `AZURE_CONTAINER` | – (`AZURE_CONTAINER_NAME`) | Container name. Required. |
| `AZURE_STORAGE_ACCESS_KEY` | – (`AZURE_STORAGE_ACCOUNT_KEY`) | Storage account access key. |
| `AZURE_STORAGE_ACCESS_KEY_FILE` | – | File with the access key. |
| `AZURE_STORAGE_SAS_TOKEN` | – (`AZURE_STORAGE_SAS_KEY`) | SAS token, instead of an access key. |
| `AZURE_STORAGE_ENDPOINT` | `https://<account>.blob.core.windows.net` | Blob service endpoint URL, for sovereign clouds or Azurite. |

## Docker Compose

Read by `docker-compose.yml`, not by the server.

| Variable | Default | Description |
|---|---|---|
| `BIND_ADDRESS` | `127.0.0.1` | Host address the port is published on. |
| `PORT` | `3000` | Host port. |
| `TURBO_CACHE_IMAGE` | `fast-turbo-cache-server:local` | Server image. |
| `TURBO_CACHE_MEM_LIMIT`, `RUSTFS_MEM_LIMIT`, `GARAGE_MEM_LIMIT` | `1g` | Memory limits. |
| `GARAGE_RPC_SECRET` | – | `garage` profile: 64 hex characters. |
| `GARAGE_ADMIN_TOKEN` | – | `garage` profile: admin token, at least 16 characters. |

## Endpoints

| Path | Description |
|---|---|
| `/v8/...` | Turborepo API. Requires a token. |
| `/health`, `/healthz` | Liveness. |
| `/readyz` | Readiness: the backend is reachable. |
| `/metrics` | Prometheus metrics. |
| `/oidc/token` | OIDC token exchange (`POST`). Exists only with `OIDC_ISSUER`. |
