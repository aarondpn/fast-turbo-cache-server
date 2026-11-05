# Development

Minimum supported Rust version: 1.88. Development and CI use the toolchain pinned in `rust-toolchain.toml`; `rustup toolchain install` installs it.

## Checks

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo deny check
zizmor .github/workflows
```

CI runs [zizmor](https://docs.zizmor.sh/) on the GitHub Actions workflows for every pull request and push to `master` or a version tag. Findings fail CI and block release and image publishing. Actions are pinned to commit SHAs and updated by Renovate.

## Versioning and releases

[release-please](https://github.com/googleapis/release-please-action) manages SemVer releases on `master`. Use [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) for commits, or for PR titles when squash merging:

| Commit | Version change |
| --- | --- |
| `fix: correct cache expiry` or `deps: update dependencies` | Patch (`0.1.0` → `0.1.1`) |
| `feat: add a storage backend` | Minor (`0.1.0` → `0.2.0`) |
| `feat!: change the token format` or a `BREAKING CHANGE:` footer | Minor before 1.0; major from 1.0 (`1.2.3` → `2.0.0`) |
| `docs:`, `chore:`, `ci:`, `test:`, `refactor:` | No release by themselves |

After all CI checks pass on `master`, the workflow opens or updates a release PR with the next version in `Cargo.toml`, `Cargo.lock` and `.release-please-manifest.json`, plus generated `CHANGELOG.md` entries. The baseline is the existing `0.1.0`; the first release PR includes the repository's existing commit history. Review and merge that PR to publish its `vMAJOR.MINOR.PATCH` tag and GitHub Release with release notes. The project is distributed through GHCR; nothing is published to crates.io.

The same workflow publishes the multi-architecture image with the full version (`1.2.3`), minor (`1.2`) and, from 1.0 onward, major (`1`) tags. It also keeps the existing `master`, `latest` and `sha-<commit>` tags. Version tags must match the Cargo and release manifest versions; CI rejects malformed or mismatched tags. If Docker publishing fails after a release is created, use **Re-run failed jobs** so the successful release job's tag output is retained.

### GitHub setup

In **Settings → Actions → General → Workflow permissions**, enable **Allow GitHub Actions to create and approve pull requests**. The workflow uses the built-in `GITHUB_TOKEN`, with write permissions limited to the release and image publishing jobs; no additional secret is needed.

GitHub does not start new workflows for PRs or tags created with `GITHUB_TOKEN`. After the bot creates or updates a release PR, close and reopen it as a maintainer to run PR checks against the latest commit before merging. Image publishing uses the release job's tag output in the same workflow run, so it does not depend on a tag-triggered run.

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
