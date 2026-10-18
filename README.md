# fast-turbo-cache-server

A self-hosted [Turborepo](https://turborepo.com) remote cache server written in Rust. It stores artifacts in **RustFS**, **Garage**, **AWS S3** (or another S3-compatible store), **Google Cloud Storage**, **Azure Blob Storage** or on the **local filesystem**.

## Features

- Turborepo v8 artifact API: `GET`/`PUT`/`HEAD`/`DELETE /v8/artifacts/{hash}`, batch queries (`POST /v8/artifacts`), status, analytics events and CORS preflight (`turbo --preflight`). Artifact signatures (`x-artifact-tag`) are stored unchanged.
- Bounded in-memory hot cache (LRU, TTL) in front of every backend; concurrent misses for the same artifact share one backend request.
- Optional retention limits (max age, max total size) for every backend.
- Streaming uploads and downloads. Large uploads are streamed to the object store as multipart uploads (or spooled to disk), so memory use does not grow with artifact size.
- Bearer tokens, namespace-scoped and read-only tokens, OIDC token exchange for GitHub Actions (no long-lived CI secrets), team isolation, write-once artifacts.
- Hardened HTTP server: body timeouts, bounded upload slots, graceful shutdown.
- `/healthz`, `/readyz`, Prometheus `/metrics`, JSON logs, distroless Docker image.

Not implemented: `/v2/user` and `/v2/teams` (so `turbo login` and `turbo link` do not work; configure clients with environment variables).

## Quick start

### Docker Compose

```bash
cp .env.example .env              # set TURBO_TOKEN (e.g. openssl rand -hex 32)
docker compose up -d --build      # local filesystem backend on 127.0.0.1:3000
```

With a bundled object store, set `STORAGE_TYPE` and the store credentials in `.env` and start the matching profile:

```bash
# .env: STORAGE_TYPE=rustfs S3_ACCESS_KEY_ID=... S3_SECRET_ACCESS_KEY=...
docker compose --profile rustfs up -d --build

# .env: STORAGE_TYPE=garage S3_ACCESS_KEY_ID=... S3_SECRET_ACCESS_KEY=...
#       GARAGE_RPC_SECRET=<openssl rand -hex 32> GARAGE_ADMIN_TOKEN=<at least 16 characters>
docker compose --profile garage up -d --build
```

Each profile starts the store, creates the bucket (`S3_BUCKET`, default `turbo-cache`) and points the server at it. To use the published image instead of building locally:

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
  ghcr.io/aarondpn/fast-turbo-cache-server:latest
```

The image runs as UID 10001 and has no shell. Its `HEALTHCHECK` runs `fast-turbo-cache-server healthcheck`, which reads `HOST`/`PORT` from the environment, so set those as environment variables rather than flags. With `--read-only` (or Kubernetes `readOnlyRootFilesystem: true`) and an S3 backend, set `SPOOL_DIR` to a writable volume (e.g. `/data/turbo-cache/.spool` or an `emptyDir`); it also holds the credential token copies for web identity and container credentials.

Images are published for `linux/amd64` and `linux/arm64`: `latest` and the branch name from the default branch, `1.2.3` and `1.2` from version tags, and `sha-<commit>` for every published commit.

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

Prefer the `*_FILE` options for secrets; flags are visible in the process list.

## Configuring the Turborepo client

```bash
export TURBO_API="https://turbo-cache.example.com"
export TURBO_TOKEN="<token>"
export TURBO_TEAM="my-team"   # the namespace
npx turbo run build
```

For signed artifacts (`"remoteCache": { "signature": true }` in `turbo.json`), set `TURBO_REMOTE_CACHE_SIGNATURE_KEY` on every client.

## GitHub Actions (OIDC)

CI jobs can authenticate without a stored secret: a workflow step sends the job's GitHub OIDC ID token to `POST /oidc/token` and receives a cache token, which it uses as `TURBO_TOKEN`. ID tokens expire within minutes, but turbo reads `TURBO_TOKEN` once per run, so the cache token is minted by the server and valid for `OIDC_TOKEN_TTL_SECS` (default one hour).

Server configuration:

```bash
OIDC_ISSUER=https://token.actions.githubusercontent.com
# Any string; the workflow requests its ID token for this audience. The cache URL is a good choice.
OIDC_AUDIENCE=https://turbo-cache.example.com
# Required: which workflows may get a cache token (here: every repository of one organization).
OIDC_REQUIRED_CLAIMS=repository_owner_id=123456
# Optional: only pushes to main of one repository get read-write tokens; every other run (pull
# requests, other branches, events and repositories) gets read-only tokens.
OIDC_WRITE_CLAIMS=repository_id=654321,ref=refs/heads/main,event_name=push
# Optional: scope the cache token to the namespace named by this claim (TURBO_TEAM=<owner id>).
OIDC_NAMESPACE_CLAIM=repository_owner_id
# Base64 HMAC key for minted cache tokens (`openssl rand -base64 32`); same on every replica, but
# one key per deployment.
OIDC_SIGNING_KEY_FILE=/run/secrets/oidc_signing_key
```

Workflow:

```yaml
jobs:
  build:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      id-token: write # allows requesting the OIDC ID token
    steps:
      - uses: actions/checkout@v4
      - name: Get a remote cache token
        env:
          CACHE_URL: https://turbo-cache.example.com # must match OIDC_AUDIENCE above
        run: |
          set -euo pipefail # a failed exchange fails the step instead of exporting an empty token
          id_token=$(curl -sSf -H "Authorization: bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN" \
            "$ACTIONS_ID_TOKEN_REQUEST_URL&audience=$CACHE_URL" | jq -er .value)
          echo "::add-mask::$id_token"
          token=$(curl -sSf -X POST -H "Authorization: Bearer $id_token" "$CACHE_URL/oidc/token" | jq -er .token)
          echo "::add-mask::$token"
          echo "TURBO_API=$CACHE_URL" >> "$GITHUB_ENV"
          echo "TURBO_TEAM=${{ github.repository_owner_id }}" >> "$GITHUB_ENV"
          echo "TURBO_TOKEN=$token" >> "$GITHUB_ENV"
      - run: npx turbo run build
```

`$GITHUB_ENV` makes the cache token available to every later step of the job, including third-party actions. If that matters, get the token in the step that runs turbo instead.

The response is `{"token": "...", "expiresAt": <unix seconds>, "readOnly": false}`, plus `"namespace"` with `OIDC_NAMESPACE_CLAIM`. Invalid, expired or unverifiable ID tokens get `401`; valid ID tokens rejected by the claim rules get `403`. Without `OIDC_ISSUER` the endpoint does not exist (`404`).

- **`OIDC_REQUIRED_CLAIMS` is what restricts access.** Any workflow in any GitHub repository can request an ID token for any audience, so issuer and audience alone authenticate nobody. Rules are `claim=value` pairs: rules for the same claim are alternatives, rules for different claims must all match (`repository_id=1,repository_id=2,ref=refs/heads/main`: either repository, and only `main`). Prefer the numeric IDs `repository_owner_id` and `repository_id` over the names `repository_owner` and `repository`: a deleted or renamed organization or repository name can be registered by someone else. Other useful claims: `ref`, `ref_type`, `environment`, `event_name`, `workflow_ref`, `job_workflow_ref`, `runner_environment` (see GitHub's [OIDC reference](https://docs.github.com/en/actions/reference/security/oidc)).
- **`OIDC_WRITE_CLAIMS`** (same syntax) decides which runs may upload; the other cache tokens are read-only (see read-only tokens under [Security](#security) for turbo versions that do not skip uploads on their own). Name the repository (`repository_id`) and the event (`event_name=push`), not only the branch. `ref=refs/heads/main` alone matches a push to `main` of *any* repository admitted by `OIDC_REQUIRED_CLAIMS` (organization members can usually create repositories), and that repository could upload artifacts under the hashes of your production builds in the shared namespace (cache poisoning). It also matches `pull_request_target`, `issue_comment`, `workflow_run` and `schedule` runs, whose `ref` is the default branch while some of them run code from pull requests. To allow only one workflow file, add `workflow_ref=my-org/app/.github/workflows/build.yml@refs/heads/main` (`job_workflow_ref` for reusable workflows).
- **`OIDC_NAMESPACE_CLAIM`** scopes each cache token to the namespace given by that claim, so `TURBO_TEAM` must equal the claim's value (`${{ github.repository_owner_id }}` for `repository_owner_id`, `${{ github.repository_id }}` for `repository_id`). The value must be a valid namespace (`repository` is not: it contains `/`), otherwise the exchange gets `403`. Without it, cache tokens are valid for every namespace (and `/metrics`). All repositories that may write to the same namespace can overwrite each other's artifacts; give repositories that do not trust each other their own namespace (`repository_id`), and consider `IMMUTABLE_ARTIFACTS=true` (existing artifacts can then not be replaced) for shared ones.
- Pull requests from forks get no ID token with the `pull_request` event, so the step fails there; skip it for forks or let those runs work without the remote cache. `pull_request_target` workflows and private repositories that send write tokens to fork pull requests do get ID tokens, possibly while running fork code: `event_name=push` in `OIDC_WRITE_CLAIMS` keeps those runs read-only.
- Cache tokens are self-contained and signed with a key derived from `OIDC_SIGNING_KEY`, `OIDC_ISSUER` and `OIDC_AUDIENCE`: they stay valid until they expire and cannot be revoked one by one. Changing the key invalidates all of them. Every replica needs the same key; use a separate key for each deployment.
- The server fetches the issuer's signing keys (`<OIDC_ISSUER>/.well-known/openid-configuration`, whose `issuer` must equal `OIDC_ISSUER`, then `jwks_uri`) on the first exchange, so it needs outbound HTTPS to the issuer. Keys are cached; an unknown key id triggers a refetch at most once per minute, a failed fetch is retried after 10 seconds at the earliest (exchanges get `500` meanwhile).

## Security

- **TLS**: the server speaks plain HTTP. Put a TLS-terminating reverse proxy in front of it and bind the server to loopback or a private network. Allow request bodies of at least `MAX_ARTIFACT_SIZE` and disable proxy buffering so transfers stream. Minimal Caddy example:

  ```caddyfile
  turbo-cache.example.com {
      request_body {
          max_size 512MB
      }
      reverse_proxy 127.0.0.1:3000
  }
  ```

- **Connection limits, header timeouts and slowloris protection are the reverse proxy's job** (the proxy is required for TLS anyway). The server only bounds request bodies (`BODY_IDLE_TIMEOUT_SECS`, `BODY_TOTAL_TIMEOUT_SECS`, upload slots); it has no connection limit, request header timeout or keep-alive idle timeout, so never expose it directly. Configure them at the proxy, e.g. Caddy `servers { timeouts { read_header ... idle ... } }`, nginx `client_header_timeout`, `keepalive_timeout` and `limit_conn`, HAProxy `timeout http-request` and `maxconn`.
- **Tokens**: use long random tokens (`openssl rand -hex 32`). There is no rate limiting of failed attempts; add it at the proxy if needed.
  - Unscoped tokens (`TURBO_TOKEN`) can access every namespace.
  - Scoped tokens (`TURBO_SCOPED_TOKENS=team-a=secret-a,team-b=secret-b`) are limited to their namespaces and require `ISOLATE_TEAMS=true`.
  - Read-only tokens (`TURBO_READ_ONLY_TOKENS`, `TURBO_SCOPED_READ_ONLY_TOKENS`; same scoping) can download and query artifacts and send analytics, but uploads and deletes get `403` (`forbidden_read_only`): e.g. CI gets a read-write token and developer machines a read-only one, so a laptop cannot poison the cache. A token cannot be both read-only and read-write. `GET /v8/artifacts/status` returns `"readOnly": true` for them, which turbo will use to skip uploads once [vercel/turborepo#14797](https://github.com/vercel/turborepo/pull/14797) (not merged yet) is released. Until then turbo tries every upload and prints a warning when it is rejected, so run turbo with remote writes disabled on those machines: `turbo run build --cache=local:rw,remote:r` or `TURBO_CACHE=local:rw,remote:r`.
  - Cache tokens from the OIDC token exchange ([GitHub Actions (OIDC)](#github-actions-oidc)) follow the same scoping and read-only rules.
  - `ALLOW_UNAUTHENTICATED=true` disables authentication and cannot be combined with tokens or OIDC.
- **Namespaces** come from the `slug`, `teamId` or `team` query parameter (`default` if none): 1-128 characters from `[A-Za-z0-9._-]`, not starting with `.`.
- **Observability**: with `PROTECT_OBSERVABILITY=true`, `/readyz` requires a token and `/metrics` an unscoped token.
- **Multi-tenant limits**: upload slots are shared. `MAX_CONCURRENT_UPLOADS_PER_NAMESPACE` stops one tenant from taking every upload slot, but tenants that do not trust each other are better served by separate instances.

## S3 backends

### IAM permissions

For bucket `turbo-cache` with `STORAGE_PREFIX=ci`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"],
      "Resource": "arn:aws:s3:::turbo-cache/ci/*"
    },
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::turbo-cache"
    }
  ]
}
```

`s3:ListBucket` (without an `s3:prefix` condition) makes S3 answer 404 instead of 403 for missing objects, and `/readyz` needs it: it lists the prefix (`ListObjectsV2` of `ci/` with `max-keys=1`). If you cannot grant it, set `S3_TREAT_403_AS_MISS=true` and `S3_HEALTH_CHECK_KEY=ci/.readyz` (a key inside the granted prefix; it does not need to exist). Credential errors are still reported as errors. `s3:AbortMultipartUpload` lets failed uploads clean up their parts.

### Credentials

Static credentials come from `S3_ACCESS_KEY_ID` + `S3_SECRET_ACCESS_KEY` (or `S3_SECRET_ACCESS_KEY_FILE`), falling back to `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`. Without them, the AWS credential chain is used, in this order:

1. Web identity / STS (EKS IRSA): `AWS_WEB_IDENTITY_TOKEN_FILE`, `AWS_ROLE_ARN`, optional `AWS_ROLE_SESSION_NAME`, `AWS_ENDPOINT_URL_STS`.
2. ECS / container credentials (also EKS Pod Identity): `AWS_CONTAINER_CREDENTIALS_FULL_URI` or `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, optional `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE` or `AWS_CONTAINER_AUTHORIZATION_TOKEN`.
3. EC2 instance metadata (IMDSv2): automatic only for `*.amazonaws.com` endpoints; elsewhere enable it with `AWS_EC2_METADATA_DISABLED=false` or `AWS_EC2_METADATA_SERVICE_ENDPOINT`.

Token files (`AWS_WEB_IDENTITY_TOKEN_FILE`, `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`) are read at startup (an unreadable file or a token with control or non-ASCII characters is a startup error; surrounding whitespace such as a trailing newline is removed) and re-read at most once per second when credentials are requested, so rotated tokens are picked up; a rotated file that cannot be read or is invalid keeps the previous token (with a warning). The token is kept in a private copy in `SPOOL_DIR` (default: the OS temp directory, `TMPDIR`), which must be writable; with a read-only root filesystem (e.g. Kubernetes `readOnlyRootFilesystem: true`) point `SPOOL_DIR` to a writable volume such as an `emptyDir`. A copy removed by a temp directory cleaner is recreated.

Temporary credentials are cached and refreshed 5 minutes before they expire.

### Limits and caveats

- **Uploads** up to 8 MiB (or `UPLOAD_MEMORY_THRESHOLD`, if larger) are one `PUT`. Larger uploads with a `Content-Length` are streamed into a multipart upload (8 MiB parts, `STORAGE_PARTS_IN_FLIGHT` sent concurrently) while they arrive, without touching the disk; uploads without a length are spooled to `SPOOL_DIR` first. The object becomes visible only when the whole body has arrived; on errors, timeouts and disconnects the multipart upload is aborted. Artifacts can have up to 10000 parts (about 78 GiB).
- **Abandoned multipart uploads**: if an abort cannot be sent (crash, network failure), the parts stay in the bucket (and are billed) but never become an object. Remove them with a [lifecycle rule](#lifecycle-rules).
- **Write-once** (`IMMUTABLE_ARTIFACTS=true`) relies on `If-None-Match: *` and therefore uses one `PUT` per artifact (at most 5 GiB, spooled to `SPOOL_DIR` first). AWS S3 supports it; some S3-compatible stores ignore it (older Ceph, the bundled Garage), where immutability is best-effort only.
- **Payload signing**: uploads use `UNSIGNED-PAYLOAD` unless `S3_SIGN_PAYLOAD=true`. For plain `http://` endpoints that are not loopback (including the compose hostnames), payload signing is always on.
- **Retries**: 5xx, 408 and 429 answers and transport errors are retried (`STORAGE_MAX_RETRIES`); redirects are never followed.
- **Metadata** is stored in `x-amz-meta-*` headers; a proxy that strips them loses it.
- **Deletes** send a `HEAD` first, so a missing artifact answers `404` as with the other backends, then a `DeleteObjects` request (`POST ?delete`).
- **Addressing**: path-style by default for `rustfs`/`garage`, custom endpoints and bucket names containing `.`; virtual-host style otherwise (`S3_FORCE_PATH_STYLE` overrides).

### Lifecycle rules

A lifecycle rule expires old artifacts and removes the parts of abandoned multipart uploads; it is the cheapest option when expiry by age is enough (the server-side [retention limits](#retention) also cap the total size). `lifecycle.json` for `STORAGE_PREFIX=ci` (without a prefix use `"Filter": {}`, which covers the whole bucket, so only do that for a bucket used only by the cache):

```json
{
  "Rules": [
    {
      "ID": "turbo-cache",
      "Status": "Enabled",
      "Filter": { "Prefix": "ci/" },
      "Expiration": { "Days": 14 },
      "AbortIncompleteMultipartUpload": { "DaysAfterInitiation": 1 }
    }
  ]
}
```

```bash
aws s3api put-bucket-lifecycle-configuration --bucket turbo-cache --lifecycle-configuration file://lifecycle.json
aws s3api get-bucket-lifecycle-configuration --bucket turbo-cache
```

- The command replaces all lifecycle rules of the bucket; merge existing rules into the file first. It needs `s3:PutLifecycleConfiguration`, which the server's policy does not include, so run it with admin credentials.
- Expiry counts from the upload, not the last access: an artifact used every day still expires after N days and is uploaded again by the next run that misses it. (`STORAGE_MAX_AGE_SECS` of the local backend counts from the last read instead.) The hot cache may serve an expired artifact for up to `CACHE_TTL_SECS`.
- On a versioned bucket `Expiration` only adds delete markers; also add `NoncurrentVersionExpiration` or keep versioning off.
- **Garage** supports `Expiration` and `AbortIncompleteMultipartUpload` (the prefix must be inside `Filter`) and applies the rules once a day at midnight (UTC by default). Setting them needs a key with the `owner` permission on the bucket; the compose profile grants its key only read and write: `docker compose exec garage /garage bucket allow --owner turbo-cache --key <key-id>`.
- **RustFS** supports both actions: its background scanner (on by default, `RUSTFS_SCANNER_ENABLED`) expires objects, and its stale multipart cleanup aborts uploads. The compose profile's key is the RustFS root user and may set the rule.
- The compose profiles do not publish the store ports; run the AWS CLI inside the compose network, with `AWS_REGION=garage` and `http://garage:3900` for Garage or `AWS_REGION=us-east-1` and `http://rustfs:9000` for RustFS:

  ```bash
  docker run --rm --network fast-turbo-cache-server_default -v "$PWD:/w:ro" -w /w \
    -e AWS_ACCESS_KEY_ID=<key-id> -e AWS_SECRET_ACCESS_KEY=<secret> -e AWS_REGION=garage \
    amazon/aws-cli --endpoint-url http://garage:3900 \
    s3api put-bucket-lifecycle-configuration --bucket turbo-cache --lifecycle-configuration file://lifecycle.json
  ```

## Google Cloud Storage and Azure Blob Storage

`STORAGE_TYPE=gcs` and `STORAGE_TYPE=azure` behave like the S3 backends: the same upload strategy (single `PUT` up to 8 MiB, streamed multipart uploads above, aborted on failure), metadata stored as object metadata, write-once via conditional creates, and the same timeouts, retries and readiness probe (a list of the prefix). Only a missing object is a miss; a missing bucket or container and any authorization failure (also a 403) is a storage error (`502`).

Without a configured key both fall back to the instance metadata service (GCE metadata server, Azure IMDS). On a host without one, every request waits about `(STORAGE_MAX_RETRIES + 1) × STORAGE_CONNECT_TIMEOUT_SECS` for it before failing with `502`: configure credentials explicitly outside the cloud.

**GCS** (`GCS_BUCKET`):

- Credentials: `GCS_SERVICE_ACCOUNT_KEY_FILE` (a service account JSON key), otherwise Application Default Credentials: `GOOGLE_APPLICATION_CREDENTIALS` (service account or `gcloud auth application-default login` file), the gcloud default file, then the metadata server (GCE, GKE Workload Identity). Workload identity federation configuration files (`"type": "external_account"`) are not supported.
- Permissions: the Storage Object User role (`roles/storage.objectUser`) on the bucket. Without `storage.objects.list`, GCS answers 403 for missing objects, which is reported as an error.
- Write-once uses `x-goog-if-generation-match: 0` (atomic). Metadata is stored in `x-goog-meta-*` headers. Abandoned multipart uploads: add a lifecycle rule with the `AbortIncompleteMultipartUpload` action.
- Emulator: `fsouza/fake-gcs-server` lacks the XML API this backend uses; the `tustvold/fake-gcs-server` fork (used by the `object_store` tests) works with a key file containing `"gcs_base_url": "http://<host>:4443"` and `"disable_oauth": true`, but ignores metadata and preconditions and has no multipart uploads.

**Azure** (`AZURE_STORAGE_ACCOUNT`, `AZURE_CONTAINER`, optional `AZURE_STORAGE_ENDPOINT`):

- Credentials, in this order: `AZURE_STORAGE_ACCESS_KEY` (or `AZURE_STORAGE_ACCESS_KEY_FILE`), `AZURE_STORAGE_SAS_TOKEN`, workload identity (`AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_FEDERATED_TOKEN_FILE`, optional `AZURE_AUTHORITY_HOST`, as injected by the AKS workload identity webhook), then managed identity (instance metadata, or `IDENTITY_ENDPOINT`; `AZURE_CLIENT_ID` selects a user-assigned identity).
- Permissions: the Storage Blob Data Contributor role on the container, or a SAS token with read, add, create, write, delete and list permissions.
- Write-once uses `If-None-Match: *` (atomic) and one `Put Blob` request, so `MAX_ARTIFACT_SIZE` is limited to 5000 MiB with `IMMUTABLE_ARTIFACTS=true`.
- Metadata is stored in `x-ms-meta-*` headers; Azure only allows identifier names, so `dirty-hash` and `client-ci` are stored as `dirty_hash` and `client_ci`.
- Large uploads are block uploads committed at the end. Blocks of a failed upload cannot be deleted; Azure discards uncommitted blocks after 7 days. Concurrent large uploads of the same artifact all succeed (the first commit stores it).
- Azurite: `AZURE_STORAGE_ENDPOINT=http://127.0.0.1:10000/devstoreaccount1`, `AZURE_STORAGE_ACCOUNT=devstoreaccount1` and Azurite's well-known account key.

## Local filesystem backend

Each artifact is one file, `<LOCAL_STORAGE_PATH>/<namespace>/<shard>/<hash>.artifact`, written to a temp file, fsynced and published atomically. Uppercase letters in path components are escaped, so case-insensitive filesystems are safe. A background janitor enforces the [retention](#retention) limits and removes orphaned temp files (`LOCAL_TEMP_MAX_AGE_SECS`). Use one server per directory.

## Retention

`STORAGE_MAX_AGE_SECS` and `STORAGE_MAX_BYTES` (both off by default) limit the cache for every backend. A background janitor runs at startup and then every `STORAGE_CLEANUP_INTERVAL_SECS`: it deletes the artifacts older than the max age, then the oldest ones until the rest fits the budget.

- **Local**: age and order follow the file modification time, which reads refresh (at most hourly), so artifacts in use stay.
- **S3, GCS, Azure**: objects have no access time, so the age counts from the upload (`last_modified`): an artifact used every day still expires and is uploaded again by the next run that misses it. Each run lists the prefix twice (one request per 1000 objects, Azure 5000) and deletes in batches (`DeleteObjects` with up to 1000 keys on S3, batch requests on Azure, one request per object on GCS); memory stays small for millions of objects. Retention requires `STORAGE_PREFIX` here: every key of the form `<prefix>/<namespace>/<hash>` counts and may be deleted, so without a prefix unrelated objects of that shape (e.g. `backups/db-2024_10`) would be too. Other keys and `S3_HEALTH_CHECK_KEY` are left alone. Deletes are not conditional: an artifact uploaded again between the listing and the delete can be removed, which only costs a cache miss. The server needs list and delete permissions (`s3:ListBucket`, `s3:DeleteObject`).
- For age-only expiry on S3, a [lifecycle rule](#lifecycle-rules) is cheaper (no listing); only `STORAGE_MAX_BYTES` caps the total size.
- With several replicas, each one runs the janitor. That is safe, but every replica lists the bucket; for large buckets set the limits on one replica only.
- The hot cache may serve a deleted artifact for up to `CACHE_TTL_SECS`.

## Running multiple replicas

Replicas can share one S3 bucket. Each replica has its own hot cache without cross-replica invalidation, so overwrites, deletes and lifecycle expirations may stay invisible on other replicas for up to `CACHE_TTL_SECS` (default 300 s). Turborepo hashes are content addresses, so this rarely matters; for strict consistency lower the TTL or disable the hot cache (`MEMORY_CACHE_MAX_BYTES=0`, `METADATA_CACHE_MAX_ENTRIES=0`).

## Memory usage

Besides the hot cache (`MEMORY_CACHE_MAX_BYTES`, default 128 MiB), memory is used by in-flight uploads, retained analytics events (at most about 16 MiB) and open connections (bounded by the reverse proxy's connection limit). An upload holds at most `UPLOAD_MEMORY_THRESHOLD` (larger ones are spooled to disk), or with an object store at most `STORAGE_PARTS_IN_FLIGHT` + 1 multipart parts of 8 MiB (the parts being sent plus one being received). In-flight uploads therefore use up to `MAX_CONCURRENT_UPLOADS` x `UPLOAD_MEMORY_THRESHOLD` (default 128 MiB), with an object store up to `MAX_CONCURRENT_UPLOADS` x (`STORAGE_PARTS_IN_FLIGHT` + 1) x 8 MiB (default 32 x 5 x 8 MiB = 1.25 GiB, reached only when 32 large uploads arrive faster than the store accepts them; lower either setting to tighten the bound). Immutable S3 uploads larger than 8 MiB are sent from a memory mapping of the spool file (page cache, not heap). The compose file sets a 1 GiB limit.

## Logging and metrics

Logs go to stdout as text or JSON (`LOG_FORMAT`), filtered by `RUST_LOG` (default `info`). Every request is logged once with its status, latency and `x-request-id`; `RUST_LOG=info,fast_turbo_cache_server::access=off` turns the access log off.

`GET /metrics` serves Prometheus metrics (`turbo_cache_*`): hits and misses, uploads, bytes transferred, upload rejections, storage latency and errors per operation, hot cache usage, and the analytics events and time saved reported by clients.

## Troubleshooting

When the remote cache is never used or always misses, the cause is almost always the client configuration or task hashes that change between runs, not the server.

### Client configuration

turbo prints the remote cache state at the start of every run: `Remote caching enabled`, `Remote caching disabled (...)` or `Remote caching unavailable (...)`. `Remote caching disabled (TURBO_TOKEN set without TURBO_TEAM)` means the token is ignored until a team is set.

| Setting | Environment variable | Flag | `turbo.json` `remoteCache` |
|---|---|---|---|
| Server URL | `TURBO_API` | `--api` | `apiUrl` |
| Token | `TURBO_TOKEN` | `--token` | – |
| Team slug (namespace) | `TURBO_TEAM` | `--team` | `teamSlug` |
| Team ID | `TURBO_TEAMID` | – | `teamId` |
| Timeouts (s) | `TURBO_REMOTE_CACHE_TIMEOUT`, `TURBO_REMOTE_CACHE_UPLOAD_TIMEOUT` | `--remote-cache-timeout` | `timeout` (30), `uploadTimeout` (60) |

- **Namespace**: turbo sends the team slug as `?slug=` and the team ID as `?teamId=`, the latter only if it starts with `team_`. The server uses `slug`, then `teamId`, then `team`, else `default`. `TURBO_TEAMID=ci` alone is not sent, so the namespace is `default`. Clients with different `TURBO_TEAM` values (e.g. developers and CI) use separate caches.
- **URL**: a trailing slash (`TURBO_API=https://turbo-cache.example.com/`) makes turbo request `//v8/...`, which the server accepts. If `TURBO_API` contains a path (`https://example.com/turbo`), the reverse proxy must strip it; the server serves `/v8/...`.
- **Check by hand** (`{"status":"enabled"}` expected; `401` is a wrong token, `403` a scoped token not valid for the namespace):

  ```bash
  curl -sS -H "Authorization: Bearer $TURBO_TOKEN" "$TURBO_API/v8/artifacts/status?slug=$TURBO_TEAM"
  ```

- **Signatures** (`"remoteCache": { "signature": true }`): turbo signs uploads with `TURBO_REMOTE_CACHE_SIGNATURE_KEY` and silently treats downloads with a missing or invalid `x-artifact-tag` as misses. The signed data includes `TURBO_TEAMID`, so all clients of a namespace need the same key and the same `TURBO_TEAMID` (or none). The server stores the tag unchanged and does not check it. Artifacts uploaded without signing stay misses for signing clients.
- **`--preflight`** (`TURBO_PREFLIGHT`, `remoteCache.preflight`) is not needed for this server. With it, turbo sends the token only if the `OPTIONS` answer lists `Authorization` in `Access-Control-Allow-Headers`, and sends the request to its `Location` header if present; a proxy that answers `OPTIONS` itself can cause `401`s.

### Seeing what happens

- `turbo run build --dry=json` shows each task's `hash` and `cache` (`status` `HIT`/`MISS`, `source` `LOCAL`/`REMOTE`) without running anything; the remote lookup is one batch request. To check the remote cache only, skip the local one:

  ```bash
  turbo run build --dry=json --cache=local:,remote:r | jq '.tasks[] | {taskId, hash, cache}'
  ```

- `--summarize` (`TURBO_RUN_SUMMARY`) writes the same per-task data for a real run to `.turbo/runs/`.
- `-vv` / `-vvv` (`--verbosity=2`/`3`) prints debug and trace logs, including remote cache errors.
- turbo warnings worth searching CI logs for: `Remote artifact cache is temporarily unavailable; retrying after a 30-second cooldown`, `the cache artifact for <hash> was too large to upload within the timeout`, `failed to contact remote cache: ...`.
- The server logs every request (`finished processing request` with method, path, status, latency and request id; the query string, and therefore the namespace, is not logged). No requests at all means the client is not configured or cannot reach the server.
- Metrics (`/metrics`):
  - `turbo_cache_get_hits_total`, `turbo_cache_get_misses_total`, `turbo_cache_head_hits_total`, `turbo_cache_head_misses_total`: artifact lookups (batch queries are not counted).
  - `turbo_cache_puts_total`: misses without puts mean clients do not upload (`--cache=remote:r`, `TURBO_REMOTE_CACHE_READ_ONLY`, failing uploads).
  - `turbo_cache_upload_rejections_total{reason}`: `timeout` (`408`), `slot_unavailable` (`429`), `too_large` (`413`).
  - `turbo_cache_storage_errors_total{operation}`, `turbo_cache_storage_operation_duration_seconds`: backend failures (answered with `5xx`) and latency.
  - `turbo_cache_analytics_events_total{source,event}`: local and remote hits and misses as reported by clients.

### turbo behaviour that looks like a server problem

- **Outage backoff**: after 3 consecutive failed remote cache requests (connection errors, timeouts, `5xx`), turbo skips the remote cache, reads included, for 30 s, then tries one request and backs off again if it fails. A short storage outage or a slow link therefore shows up as a series of misses.
- **403**: turbo disables the remote cache for the rest of the run only for a `403` with the error code `forbidden` (as sent by Vercel), which this server never sends. Its `forbidden_namespace` and `artifact_immutable` answers fail just that request (a read counts as a miss). The `remote_caching_*` answers of `CACHING_STATUS` other than `enabled` make turbo stop using the remote cache for the run after the first rejected upload.
- **Timeouts**: uploads must finish within `remoteCache.uploadTimeout` (60 s; waiting for an upload slot counts), every other request, downloads included, within `remoteCache.timeout` (30 s); `0` disables them. Raise them for large artifacts on slow links.
- **No upload retries**: an upload that fails, times out or is rejected (`408`, `413`, `429`) is not retried; the next run that misses the artifact uploads it. Downloads and lookups are retried once on `429`, `5xx` and timeouts.

### Unstable hashes

If the server sees lookups for hashes that were never uploaded, the hashes change between runs:

- Variables in `globalEnv` or a task's `env` that differ per run (`GITHUB_RUN_ID`, `GITHUB_SHA`, `CI_JOB_ID`, timestamps, paths to per-job credential files) change the hash; a change in `globalEnv` misses every task. Variables a task needs but whose value does not affect its output belong in `passThroughEnv` / `globalPassThroughEnv` (available to the task, not hashed).
- Untracked files that are not gitignored are inputs, e.g. files written by an earlier CI step. Gitignore them or narrow `inputs`. Conversely, files a task reads outside its inputs (root config files, other packages) do not change the hash and cause stale hits; add them with `$TURBO_ROOT$/...` in `inputs` or `globalDependencies`.
- Find the differing input by comparing two dry runs (or two `--summarize` files); environment variable values appear only as hashes:

  ```bash
  turbo run build --dry=json > a.json   # run A (e.g. CI), then run B (e.g. local) > b.json
  diff <(jq -S .globalCacheInputs a.json) <(jq -S .globalCacheInputs b.json)
  diff <(jq -S '.tasks[] | {taskId, hash, inputs, environmentVariables}' a.json) \
       <(jq -S '.tasks[] | {taskId, hash, inputs, environmentVariables}' b.json)
  ```

### Docker builds

Pass the token as a BuildKit secret, not as a build argument (as the Turborepo Docker guide does): build arguments stay in the image and invalidate the layer cache when they change, secret values do neither.

```dockerfile
# syntax=docker/dockerfile:1
ARG TURBO_API TURBO_TEAM
ENV TURBO_API=$TURBO_API TURBO_TEAM=$TURBO_TEAM
RUN --mount=type=secret,id=turbo_token,env=TURBO_TOKEN npx turbo run build
```

```bash
docker build --secret id=turbo_token,env=TURBO_TOKEN \
  --build-arg TURBO_API=https://turbo-cache.example.com --build-arg TURBO_TEAM=my-team .
```

turbo trusts its bundled Mozilla CA list as well as the system store, so a publicly trusted certificate works in slim images without `ca-certificates`. A certificate from a private CA must be added to the image's system store (e.g. Debian: install `ca-certificates`, copy the CA to `/usr/local/share/ca-certificates/`, run `update-ca-certificates`).

## Configuration reference

Every option can be set as a flag or an environment variable; flags win. `fast-turbo-cache-server --help` prints the same list. Booleans need the `=` form for flags (`--allow-delete=false`). Lists are comma-separated. An empty value is an error for numbers, booleans, choices (e.g. `STORAGE_TYPE`), paths and `HOST`; for other text options (tokens, `STORAGE_PREFIX`, `S3_*`, `GCS_*`, `AZURE_*`, `OIDC_*`) it counts as unset.

### Server

| Environment variable | Flag | Default | Description |
|---|---|---|---|
| `HOST` | `--host` | `0.0.0.0` | Bind address. |
| `PORT` | `-p`, `--port` | `3000` | Listen port. |
| `BODY_IDLE_TIMEOUT_SECS` | `--body-idle-timeout-secs` | `30` | Abort a request body with `408` when no data arrives for this long. |
| `BODY_TOTAL_TIMEOUT_SECS` | `--body-total-timeout-secs` | `900` | Abort a request body with `408` when waiting for it took this long in total. Time the server spends storing received data (e.g. S3 accepting a multipart part of a streamed upload) does not count. |
| `SHUTDOWN_GRACE_SECS` | `--shutdown-grace-secs` | `30` | On SIGTERM/SIGINT, wait this long for in-flight requests. |
| `CORS_ALLOW_ORIGIN` | `--cors-allow-origin` | `*` | Value of `Access-Control-Allow-Origin`: `*` or one origin such as `https://app.example.com` (no path or trailing slash). |
| `LOG_FORMAT` | `--log-format` | `text` | `text` or `json`. |

### Authentication and access control

| Environment variable | Flag | Default | Description |
|---|---|---|---|
| `TURBO_TOKEN` | `--tokens` | – | Unscoped tokens (all namespaces). A token is required unless `ALLOW_UNAUTHENTICATED=true`. |
| `TURBO_TOKEN_FILE` | `--token-file` | – | File with tokens (comma- or newline-separated, `#` comments). |
| `TURBO_SCOPED_TOKENS` | `--scoped-tokens` | – | `namespace=token` entries. A token listed for several namespaces is valid for each. |
| `TURBO_SCOPED_TOKENS_FILE` | `--scoped-tokens-file` | – | File with scoped tokens. |
| `TURBO_READ_ONLY_TOKENS` | `--read-only-tokens` | – | Read-only unscoped tokens: no uploads or deletes. |
| `TURBO_READ_ONLY_TOKEN_FILE` | `--read-only-token-file` | – | File with read-only tokens. |
| `TURBO_SCOPED_READ_ONLY_TOKENS` | `--scoped-read-only-tokens` | – | Read-only `namespace=token` entries. |
| `TURBO_SCOPED_READ_ONLY_TOKENS_FILE` | `--scoped-read-only-tokens-file` | – | File with read-only scoped tokens. |
| `OIDC_ISSUER` | `--oidc-issuer` | – | Enables the OIDC token exchange `POST /oidc/token`, e.g. `https://token.actions.githubusercontent.com` (https only, http for loopback; a trailing slash is ignored). See [GitHub Actions (OIDC)](#github-actions-oidc). |
| `OIDC_AUDIENCE` | `--oidc-audience` | – | Required `aud` of ID tokens. Required with `OIDC_ISSUER`. |
| `OIDC_REQUIRED_CLAIMS` | `--oidc-required-claims` | – | `claim=value` rules every ID token must match (same claim: any of; different claims: all). Required with `OIDC_ISSUER`. |
| `OIDC_WRITE_CLAIMS` | `--oidc-write-claims` | – | `claim=value` rules for read-write cache tokens, e.g. `repository_id=…,ref=refs/heads/main,event_name=push`; others are read-only. Unset: all read-write. |
| `OIDC_NAMESPACE_CLAIM` | `--oidc-namespace-claim` | – | Claim naming the namespace a cache token is scoped to. Unset: all namespaces. |
| `OIDC_TOKEN_TTL_SECS` | `--oidc-token-ttl-secs` | `3600` | Lifetime of minted cache tokens, `1`-`86400`. |
| `OIDC_SIGNING_KEY` | `--oidc-signing-key` | – | Base64 HMAC key of at least 32 bytes for minted cache tokens (`openssl rand -base64 32`), same on every replica, one per deployment. Required with `OIDC_ISSUER`. |
| `OIDC_SIGNING_KEY_FILE` | `--oidc-signing-key-file` | – | File containing the signing key. |
| `ALLOW_UNAUTHENTICATED` | `--allow-unauthenticated` | `false` | Accept requests without a token. |
| `ISOLATE_TEAMS` | `--isolate-teams` | `true` | Separate artifacts by namespace; `false` puts everything in `default`. |
| `IMMUTABLE_ARTIFACTS` | `--immutable-artifacts` | `false` | Reject overwrites (`403`). Requires `ALLOW_DELETE=false`. |
| `ALLOW_DELETE` | `--allow-delete` | `false` | Allow `DELETE /v8/artifacts/{hash}`. |
| `PROTECT_OBSERVABILITY` | `--protect-observability` | `false` | Require a token for `/readyz` and an unscoped token for `/metrics`. |
| `CACHING_STATUS` | `--caching-status` | `enabled` | `enabled`, `disabled`, `over_limit` or `paused`. |

### Uploads and limits

| Environment variable | Flag | Default | Description |
|---|---|---|---|
| `MAX_ARTIFACT_SIZE` | `--max-artifact-size` | `536870912` (512 MiB) | Maximum upload size in bytes (object stores: at most 10000 x 8 MiB; with `IMMUTABLE_ARTIFACTS=true` at most 5 GiB for S3 and 5000 MiB for Azure). |
| `MAX_CONCURRENT_UPLOADS` | `--max-concurrent-uploads` | `32` | Maximum concurrent uploads. |
| `MAX_CONCURRENT_UPLOADS_PER_NAMESPACE` | `--max-concurrent-uploads-per-namespace` | `0` (automatic) | Per-namespace upload limit. `0` = half of `MAX_CONCURRENT_UPLOADS` with scoped tokens or `OIDC_NAMESPACE_CLAIM`, otherwise unlimited. |
| `BATCH_HEAD_CONCURRENCY` | `--batch-head-concurrency` | `64` | Backend lookups in flight for batch queries, server-wide. |
| `UPLOAD_SLOT_WAIT_SECS` | `--upload-slot-wait-secs` | `10` | Wait for an upload slot before `429` + `Retry-After`. Counts towards turbo's upload timeout (60 s by default). |
| `UPLOAD_MEMORY_THRESHOLD` | `--upload-memory-threshold` | `4194304` (4 MiB) | Uploads up to this size stay in RAM; larger ones are spooled to `SPOOL_DIR`, or streamed to S3 (see [Limits and caveats](#limits-and-caveats)). |
| `SPOOL_DIR` | `--spool-dir` | `<LOCAL_STORAGE_PATH>/.spool` (`local`), else the OS temp dir | Directory for spooled uploads and the private copies of S3 credential tokens. Must be writable. |

### Hot cache

| Environment variable | Flag | Default | Description |
|---|---|---|---|
| `MEMORY_CACHE_MAX_BYTES` | `--memory-cache-max-bytes` | `134217728` (128 MiB) | Payload byte budget (`0` disables the payload cache). |
| `MEMORY_CACHE_MAX_ENTRIES` | `--memory-cache-max-entries` | `10000` | Maximum cached payloads. |
| `HOT_CACHE_MAX_ITEM_BYTES` | `--hot-cache-max-item-bytes` | `4194304` (4 MiB) | Largest cached artifact. |
| `METADATA_CACHE_MAX_ENTRIES` | `--metadata-cache-max-entries` | `50000` | Maximum cached metadata entries. |
| `CACHE_TTL_SECS` | `--cache-ttl-secs` | `300` | Lifetime of cache entries (`0` = no expiry). |
| `NEGATIVE_CACHE_TTL_SECS` | `--negative-cache-ttl-secs` | `0` | Lifetime of cached misses (`0` = off). |

### Storage

| Environment variable | Flag | Default | Description |
|---|---|---|---|
| `STORAGE_TYPE` | `--storage-type` | `local` | `local`, `s3`, `rustfs`, `garage`, `gcs` or `azure`. |
| `LOCAL_STORAGE_PATH` | `--local-storage-path` | `./.turbo-cache` (image: `/data/turbo-cache`) | Directory of the local backend. |
| `STORAGE_MAX_BYTES` | `--storage-max-bytes` | `0` | Storage budget in bytes; the oldest artifacts are evicted beyond it (`0` = unlimited). See [Retention](#retention). |
| `STORAGE_MAX_AGE_SECS` | `--storage-max-age-secs` | `0` | Delete artifacts older than this (`0` = never); local: since the last read or write, object stores: since the upload. |
| `STORAGE_CLEANUP_INTERVAL_SECS` | `--storage-cleanup-interval-secs` | `600` | Interval of the retention janitor and the spool sweep. |
| `LOCAL_TEMP_MAX_AGE_SECS` | `--local-temp-max-age-secs` | `3600` | Age after which orphaned temp files are deleted. |
| `STORAGE_PREFIX` | `--storage-prefix` | empty | Key prefix in the bucket or container (`<prefix>/<namespace>/<hash>`; no empty, `.` or `..` segments). Object stores only; ignored by `local`. Required with `STORAGE_MAX_BYTES` or `STORAGE_MAX_AGE_SECS` ([Retention](#retention)). |
| `STORAGE_MAX_RETRIES` | `--storage-max-retries` | `3` | Retries for transient object store errors. |
| `STORAGE_PARTS_IN_FLIGHT` | `--storage-parts-in-flight` | `4` | Multipart parts (8 MiB) uploaded concurrently per upload to an object store, `1`-`16`. More parts in flight speed up large uploads at (value + 1) x 8 MiB of memory per upload. |
| `STORAGE_CONNECT_TIMEOUT_SECS` | `--storage-connect-timeout-secs` | `5` | Object store connect timeout. |
| `STORAGE_READ_TIMEOUT_SECS` | `--storage-read-timeout-secs` | `30` | Object store read timeout: longest wait for data, and (plus the connect timeout and 1 s per 256 KiB sent) for response headers. |
| `S3_BUCKET` | `--s3-bucket` | – (`AWS_BUCKET`) | Bucket name, required for S3 backends. |
| `S3_REGION` | `--s3-region` | `AWS_REGION`, `AWS_DEFAULT_REGION`, then `garage` (`garage`) or `us-east-1` | Signing region. |
| `S3_ENDPOINT` | `--s3-endpoint` | `AWS_ENDPOINT_URL`, then `http://127.0.0.1:9000` (`rustfs`), `http://127.0.0.1:3900` (`garage`), AWS (`s3`) | Endpoint URL. |
| `S3_ACCESS_KEY_ID` | `--s3-access-key-id` | – (`AWS_ACCESS_KEY_ID`) | Access key ID. |
| `S3_SECRET_ACCESS_KEY` | `--s3-secret-access-key` | – (`AWS_SECRET_ACCESS_KEY`) | Secret access key. |
| `S3_SECRET_ACCESS_KEY_FILE` | `--s3-secret-access-key-file` | – | File containing the secret. |
| `S3_SESSION_TOKEN` | `--s3-session-token` | – (`AWS_SESSION_TOKEN`) | Session token. |
| `S3_FORCE_PATH_STYLE` | `--s3-force-path-style` | see [Limits and caveats](#limits-and-caveats) | Path-style addressing. |
| `S3_SIGN_PAYLOAD` | `--s3-sign-payload` | `false` | Sign the payload SHA-256 of uploads and parts. |
| `S3_TREAT_403_AS_MISS` | `--s3-treat-403-as-miss` | `false` | Treat 403 on lookups as a miss (see [IAM permissions](#iam-permissions)). |
| `S3_HEALTH_CHECK_KEY` | `--s3-health-check-key` | – | Object key probed by `/readyz` instead of listing `STORAGE_PREFIX` (`STORAGE_PREFIX` is not applied). |
| `GCS_BUCKET` | `--gcs-bucket` | – (`GOOGLE_BUCKET`) | Bucket name, required for `gcs`. |
| `GCS_SERVICE_ACCOUNT_KEY_FILE` | `--gcs-service-account-key-file` | – (`GOOGLE_SERVICE_ACCOUNT`) | Service account key file (JSON); Application Default Credentials without it. |
| `AZURE_STORAGE_ACCOUNT` | `--azure-storage-account` | – (`AZURE_STORAGE_ACCOUNT_NAME`) | Storage account name, required for `azure`. |
| `AZURE_CONTAINER` | `--azure-container` | – (`AZURE_CONTAINER_NAME`) | Container name, required for `azure`. |
| `AZURE_STORAGE_ACCESS_KEY` | `--azure-storage-access-key` | – (`AZURE_STORAGE_ACCOUNT_KEY`) | Storage account access key. |
| `AZURE_STORAGE_ACCESS_KEY_FILE` | `--azure-storage-access-key-file` | – | File containing the access key. |
| `AZURE_STORAGE_SAS_TOKEN` | `--azure-storage-sas-token` | – (`AZURE_STORAGE_SAS_KEY`) | SAS token (instead of an access key). |
| `AZURE_STORAGE_ENDPOINT` | `--azure-storage-endpoint` | `https://<account>.blob.core.windows.net` | Blob service endpoint URL (sovereign clouds, Azurite). |

### Docker Compose

| Variable | Default | Description |
|---|---|---|
| `BIND_ADDRESS` | `127.0.0.1` | Host address the port is published on. |
| `PORT` | `3000` | Host port. |
| `TURBO_CACHE_IMAGE` | `fast-turbo-cache-server:local` | Server image. |
| `TURBO_CACHE_MEM_LIMIT`, `RUSTFS_MEM_LIMIT`, `GARAGE_MEM_LIMIT` | `1g` | Memory limits. |
| `GARAGE_RPC_SECRET` | – | `garage` profile: 64 hex characters. |
| `GARAGE_ADMIN_TOKEN` | – | `garage` profile: admin token, at least 16 characters. |

### Endpoints

| Path | Description |
|---|---|
| `/v8/...` | Turborepo API (token required). |
| `/health`, `/healthz` | Liveness. |
| `/readyz` | Readiness (backend reachable). |
| `/metrics` | Prometheus metrics. |
| `/oidc/token` | OIDC token exchange (`POST`, only with `OIDC_ISSUER`). |

## Development

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo deny check
```

Minimum supported Rust version: 1.88.

The live S3 test runs against RustFS from the compose profile, like the CI `s3-live` job:

```bash
export COMPOSE_FILE=docker-compose.yml:.github/compose-live.yml COMPOSE_PROFILES=rustfs
S3_ACCESS_KEY_ID=ci-access-key S3_SECRET_ACCESS_KEY=ci-secret-key-0123456789 \
S3_BUCKET=turbo-cache-ci S3_REGION=us-east-1 \
  docker compose -p turbo-s3-live run --rm rustfs-init
TURBO_S3_LIVE_TEST=1 TURBO_S3_ENDPOINT="http://127.0.0.1:${S3_LIVE_PORT:-9000}" \
TURBO_S3_ACCESS_KEY=ci-access-key TURBO_S3_SECRET_KEY=ci-secret-key-0123456789 \
TURBO_S3_BUCKET=turbo-cache-ci TURBO_S3_REGION=us-east-1 \
  cargo test --locked --test it s3::live_s3_roundtrip -- --exact --nocapture
docker compose -p turbo-s3-live down -v
```

Set `TURBO_S3_CONDITIONAL=false` for stores that ignore `If-None-Match`.

The live GCS and Azure tests (`tests/it/cloud.rs`, CI jobs `gcs-live` and `azure-live`) run against fake-gcs-server and Azurite; the bucket and container must exist (see the tests for the variables and the CI jobs for the setup):

```bash
TURBO_GCS_LIVE_TEST=1 TURBO_GCS_EMULATOR=http://127.0.0.1:4443 \
  cargo test --locked --test it cloud::live_gcs_roundtrip -- --exact --nocapture
TURBO_AZURE_LIVE_TEST=1 cargo test --locked --test it cloud::live_azure_roundtrip -- --exact --nocapture
```

## License

[MIT](LICENSE)
