# Storage backends

`STORAGE_TYPE` selects the backend: `local` (default), `s3`, `rustfs`, `garage`, `gcs` or `azure`. An in-memory hot cache sits in front of every backend. To run several replicas, use an object store; replicas can share a bucket. Options: [configuration reference](configuration.md#storage).

## Local filesystem

- Each artifact is one file: `<LOCAL_STORAGE_PATH>/<namespace>/<shard>/<hash>.artifact`.
- Uploads are written to a temp file, synced to disk and then published atomically.
- Uppercase letters in path components are escaped, so case-insensitive filesystems are safe.
- A background janitor enforces the [retention](#retention) limits and removes orphaned temp files older than `LOCAL_TEMP_MAX_AGE_SECS`.
- Use one server per directory.

## Object stores

This section applies to S3, GCS and Azure.

### Uploads

- Uploads up to 8 MiB (or `UPLOAD_MEMORY_THRESHOLD`, if larger) are sent in one request.
- Larger uploads with a `Content-Length` are streamed into a multipart upload as they arrive: 8 MiB parts, `STORAGE_PARTS_IN_FLIGHT` sent concurrently. Uploads without a length are spooled to `SPOOL_DIR` first.
- The object becomes visible only after the whole body has arrived. On errors, timeouts and disconnects, the multipart upload is aborted.
- If the abort cannot be sent (crash, network failure), the parts stay in the bucket and are billed but never become an object. On S3 and GCS, remove them with a [lifecycle rule](#lifecycle-rules); Azure discards them after 7 days.

### Errors

Only a missing object is a miss; a missing bucket or container and any authorization failure (also `403`) is a storage error (`502`). Transient errors (`5xx`, `408`, `429` and transport errors) are retried up to `STORAGE_MAX_RETRIES` times. Redirects are never followed.

Configure credentials explicitly outside the cloud. Without a configured key, GCS and Azure fall back to the instance metadata service (GCE metadata server, Azure IMDS). On a host without one, every request waits about `(STORAGE_MAX_RETRIES + 1) × STORAGE_CONNECT_TIMEOUT_SECS` and then fails with `502`.

### Write-once on object stores

With `IMMUTABLE_ARTIFACTS=true`, overwrites are rejected by a conditional write: `If-None-Match: *` on S3 and Azure, `x-goog-if-generation-match: 0` on GCS. The server receives the whole body first, in memory up to `UPLOAD_MEMORY_THRESHOLD` and otherwise in `SPOOL_DIR`, and sends it in one request. `SPOOL_DIR` needs disk space for all write-once uploads in flight.

AWS S3, RustFS (asserted in CI), GCS and Azure honour the condition. Some S3-compatible stores ignore it, for example the bundled Garage and older Ceph; there, immutability is best-effort only.

### Upload size limits

On object stores, `MAX_ARTIFACT_SIZE` can be at most 10000 parts of 8 MiB (about 78 GiB). With `IMMUTABLE_ARTIFACTS=true`, the limit is 5 GiB on S3 and 5000 MiB on Azure. The server refuses to start with a larger value.

## S3

`STORAGE_TYPE` `s3`, `rustfs` and `garage` use the same backend with different defaults:

| `STORAGE_TYPE` | Default endpoint | Default region | Default addressing |
|---|---|---|---|
| `s3` | AWS | `us-east-1` | virtual-host style |
| `rustfs` | `http://127.0.0.1:9000` | `us-east-1` | path style |
| `garage` | `http://127.0.0.1:3900` | `garage` | path style |

- `AWS_ENDPOINT_URL`, `AWS_REGION` and `AWS_DEFAULT_REGION` take precedence over these defaults. `S3_BUCKET` is required.
- Path style is also the default for custom endpoints and bucket names containing `.`. `S3_FORCE_PATH_STYLE` overrides it.
- Artifact metadata is stored in `x-amz-meta-*` headers; a proxy that strips them loses it.
- Deletes use `DeleteObjects` (`POST ?delete`).

### IAM permissions

Policy for bucket `turbo-cache` with `STORAGE_PREFIX=ci`:

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

- Grant `s3:ListBucket` without an `s3:prefix` condition. Without it, S3 answers `403` instead of `404` for missing objects, and the server reports every miss as an error. `/readyz` (`ListObjectsV2` of `ci/` with `max-keys=1`) and [retention](#retention) also need it.
- `s3:AbortMultipartUpload` lets failed uploads clean up their parts.

If you cannot grant `s3:ListBucket`, set `S3_TREAT_403_AS_MISS=true` and `S3_HEALTH_CHECK_KEY=ci/.readyz`, a key inside the granted prefix that does not need to exist. Credential errors are still reported as errors.

### Credentials

Static credentials come from `S3_ACCESS_KEY_ID` and `S3_SECRET_ACCESS_KEY` (or `S3_SECRET_ACCESS_KEY_FILE`), with `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN` as fallbacks. Without them, the server uses the AWS credential chain in this order:

1. Web identity / STS (EKS IRSA): `AWS_WEB_IDENTITY_TOKEN_FILE`, `AWS_ROLE_ARN`, optional `AWS_ROLE_SESSION_NAME` and `AWS_ENDPOINT_URL_STS`.
2. ECS / container credentials, also used by EKS Pod Identity: `AWS_CONTAINER_CREDENTIALS_FULL_URI` or `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, optional `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE` or `AWS_CONTAINER_AUTHORIZATION_TOKEN`.
3. EC2 instance metadata (IMDSv2), automatic only for `*.amazonaws.com` endpoints. Elsewhere, enable it with `AWS_EC2_METADATA_DISABLED=false` or `AWS_EC2_METADATA_SERVICE_ENDPOINT`.

The token files of 1 and 2 are read at startup; an unreadable file or a token with control or non-ASCII characters is a startup error, and surrounding whitespace is removed. They are re-read at most once per second, so rotated tokens are picked up. If a rotated file is unreadable or invalid, the previous token is kept with a warning. A private copy of the token is kept in `SPOOL_DIR`, which must be writable.

### Payload signing

Uploads use `UNSIGNED-PAYLOAD` unless `S3_SIGN_PAYLOAD=true`. For plain `http://` endpoints that are not loopback, including the compose hostnames, payload signing is always on.

### Lifecycle rules

A lifecycle rule expires old artifacts and removes the parts of abandoned multipart uploads. It is cheaper than [retention](#retention) because it needs no listing, but cannot cap the total size. `lifecycle.json` for `STORAGE_PREFIX=ci` (without a prefix, `"Filter": {}` covers the whole bucket; use it only for a bucket used only by the cache):

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

- The command replaces all lifecycle rules of the bucket, so merge existing rules into the file first. It needs `s3:PutLifecycleConfiguration`, which the server's policy lacks, so run it with admin credentials.
- Expiry counts from the upload, not the last access (see [Retention](#retention)).
- On a versioned bucket, `Expiration` only adds delete markers. Also add `NoncurrentVersionExpiration`, or keep versioning off.

Garage supports `Expiration` and `AbortIncompleteMultipartUpload` (prefix inside `Filter`) and applies them daily at midnight, UTC by default. Setting rules needs the `owner` permission on the bucket, which the compose profile's key lacks:

```bash
docker compose exec garage /garage bucket allow --owner turbo-cache --key <key-id>
```

RustFS supports both actions: its background scanner (on by default, `RUSTFS_SCANNER_ENABLED`) expires objects, and its stale multipart cleanup aborts uploads. The compose profile's key is the root user and can set the rule.

#### Setting lifecycle rules on the bundled stores

The store ports are not published, so run the AWS CLI inside the compose network, with `AWS_REGION=garage` and `http://garage:3900`, or `AWS_REGION=us-east-1` and `http://rustfs:9000`:

```bash
docker run --rm --network fast-turbo-cache-server_default -v "$PWD:/w:ro" -w /w \
  -e AWS_ACCESS_KEY_ID=<key-id> -e AWS_SECRET_ACCESS_KEY=<secret> -e AWS_REGION=garage \
  amazon/aws-cli --endpoint-url http://garage:3900 \
  s3api put-bucket-lifecycle-configuration --bucket turbo-cache --lifecycle-configuration file://lifecycle.json
```

## Google Cloud Storage

Set `STORAGE_TYPE=gcs` and `GCS_BUCKET`. Credentials are tried in this order:

1. `GCS_SERVICE_ACCOUNT_KEY_FILE`: a service account JSON key.
2. `GOOGLE_APPLICATION_CREDENTIALS`: a service account key or a `gcloud auth application-default login` file.
3. The gcloud default credentials file.
4. The metadata server (GCE, GKE Workload Identity).

Workload identity federation files (`"type": "external_account"`) are not supported.

- Grant the Storage Object User role (`roles/storage.objectUser`) on the bucket. Without `storage.objects.list`, GCS answers `403` for missing objects, which the server reports as an error.
- Artifact metadata is stored in `x-goog-meta-*` headers.
- Remove abandoned multipart uploads with a lifecycle rule with the `AbortIncompleteMultipartUpload` action.
- Emulator: see [Emulators](development.md#emulators).

## Azure Blob Storage

Set `STORAGE_TYPE=azure`, `AZURE_STORAGE_ACCOUNT` and `AZURE_CONTAINER`. Credentials are tried in this order:

1. `AZURE_STORAGE_ACCESS_KEY` or `AZURE_STORAGE_ACCESS_KEY_FILE`.
2. `AZURE_STORAGE_SAS_TOKEN`.
3. Workload identity, as injected by the AKS workload identity webhook: `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_FEDERATED_TOKEN_FILE` and optional `AZURE_AUTHORITY_HOST`.
4. Managed identity, from instance metadata or `IDENTITY_ENDPOINT`. `AZURE_CLIENT_ID` selects a user-assigned identity.

- Grant the Storage Blob Data Contributor role on the container, or use a SAS token with read, add, create, write, delete and list permissions.
- Artifact metadata is stored in `x-ms-meta-*` headers. Azure only allows identifier names, so `dirty-hash` and `client-ci` are stored as `dirty_hash` and `client_ci`.
- Large uploads are block uploads, committed at the end. Concurrent large uploads of the same artifact all succeed; the first commit stores it.
- Azurite: see [Emulators](development.md#emulators).

## Retention

`STORAGE_MAX_AGE_SECS` and `STORAGE_MAX_BYTES` (both off by default) limit the cache for every backend. A background janitor runs at startup and every `STORAGE_CLEANUP_INTERVAL_SECS`. It deletes artifacts older than the maximum age, then the oldest ones until the rest fits the size limit.

On `local`, age counts from the last read or write: reads refresh the file modification time (at most hourly), so artifacts in use stay. Objects have no access time, so on object stores age counts from the upload (`last_modified`). An artifact used every day still expires there, and the next run that misses it uploads it again. On object stores:

- Each janitor run lists the prefix twice, with one request per 1000 objects (Azure: 5000).
- Deletes are batched: `DeleteObjects` with up to 1000 keys on S3 and batch requests on Azure. GCS needs one request per object.
- The server needs list and delete permissions (`s3:ListBucket` and `s3:DeleteObject` on S3).
- Retention requires `STORAGE_PREFIX`. Every key of the form `<prefix>/<namespace>/<hash>` counts and may be deleted, so without a prefix unrelated objects of that shape (e.g. `backups/db-2024_10`) would be too. Other keys and `S3_HEALTH_CHECK_KEY` are left alone.
- Deletes are not conditional: an artifact uploaded again between the listing and the delete can be removed, which only costs a cache miss.

For replicas, see [Running multiple replicas](deployment.md#running-multiple-replicas).
