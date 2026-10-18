# Development

Minimum supported Rust version: 1.88. Development and CI use the toolchain pinned in `rust-toolchain.toml`; `rustup toolchain install` installs it.

## Checks

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo deny check
```

## Live S3 test

The live S3 test runs against RustFS from the compose profile, like the CI `s3-live` job.

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

## Live GCS and Azure tests

The live GCS and Azure tests (`tests/it/cloud.rs`, CI jobs `gcs-live` and `azure-live`) run against fake-gcs-server and Azurite. The bucket and container must exist; the tests list their variables and the CI jobs show the setup.

```bash
TURBO_GCS_LIVE_TEST=1 TURBO_GCS_EMULATOR=http://127.0.0.1:4443 \
  cargo test --locked --test it cloud::live_gcs_roundtrip -- --exact --nocapture
TURBO_AZURE_LIVE_TEST=1 cargo test --locked --test it cloud::live_azure_roundtrip -- --exact --nocapture
```

## Emulators

### fake-gcs-server

`fsouza/fake-gcs-server` lacks the XML API the GCS backend uses, but the `tustvold/fake-gcs-server` fork works. Point the server at it with a key file (`GCS_SERVICE_ACCOUNT_KEY_FILE`) that contains `"gcs_base_url": "http://<host>:4443"` and `"disable_oauth": true`. The fork ignores metadata and preconditions and has no multipart uploads.

### Azurite

```bash
STORAGE_TYPE=azure
AZURE_STORAGE_ENDPOINT=http://127.0.0.1:10000/devstoreaccount1
AZURE_STORAGE_ACCOUNT=devstoreaccount1
# The container must exist.
AZURE_CONTAINER=turbo-cache
# Azurite's well-known account key
AZURE_STORAGE_ACCESS_KEY=<key>
```
