# Authentication and access control

Clients send a token in the `Authorization: Bearer <token>` header. The server refuses to start without tokens, OIDC or `ALLOW_UNAUTHENTICATED=true`. All options are in the [configuration reference](configuration.md#authentication-and-access-control).

## Tokens

Use long random tokens (`openssl rand -hex 32`). Unscoped tokens (`TURBO_TOKEN`) are valid for every namespace, [scoped tokens](#scoped-tokens) (`TURBO_SCOPED_TOKENS`) only for theirs. [Read-only](#read-only-tokens) variants of both (`TURBO_READ_ONLY_TOKENS`, `TURBO_SCOPED_READ_ONLY_TOKENS`) cannot upload or delete. [OIDC cache tokens](#github-actions-oidc) follow the same scoping and read-only rules.

Each option has a `_FILE` variant. Startup fails if a token is configured as both read-only and read-write, or as both unscoped and scoped.

### Scoped tokens

Scoped tokens are `namespace=token` entries, for example `TURBO_SCOPED_TOKENS=team-a=secret-a,team-b=secret-b`. Other namespaces get `403`. A token listed for several namespaces is valid for each. Scoped tokens require `ISOLATE_TEAMS=true`, the default.

### Read-only tokens

Read-only tokens can download and query artifacts and send analytics events; uploads and deletes get `403` (`forbidden_read_only`). Give CI a read-write token and developer machines a read-only one, so a laptop cannot poison the cache.

For read-only tokens, `GET /v8/artifacts/status` returns `"readOnly": true`, which turbo will use to skip uploads once [vercel/turborepo#14797](https://github.com/vercel/turborepo/pull/14797) (not merged yet) is released. Until then turbo tries every upload and prints a warning when it is rejected. On those machines, disable remote writes with `turbo run build --cache=local:rw,remote:r` or `TURBO_CACHE=local:rw,remote:r`.

## Namespaces

The server takes the namespace from the `slug`, `teamId` or `team` query parameter, in that order, and uses `default` if none is set. turbo sets it from `TURBO_TEAM` (see [Client configuration](troubleshooting.md#namespace)). Valid names have 1-128 characters from `[A-Za-z0-9._-]` and do not start with `.`.

`ISOLATE_TEAMS=false` puts every artifact in `default`. It cannot be combined with scoped tokens or `OIDC_NAMESPACE_CLAIM`.

Upload slots are shared by all namespaces. `MAX_CONCURRENT_UPLOADS_PER_NAMESPACE` keeps one namespace from taking every slot. For teams that do not trust each other, run separate instances.

## Write-once artifacts

With `IMMUTABLE_ARTIFACTS=true`, uploads of an existing artifact get `403` with the code `artifact_immutable`. It requires `ALLOW_DELETE=false`, the default. On object stores, it relies on conditional writes, which not every S3-compatible store honours, and lowers the size limit (see [Write-once on object stores](storage.md#write-once-on-object-stores)).

## GitHub Actions (OIDC)

A CI job can exchange its GitHub OIDC ID token at `POST /oidc/token` for a cache token and use it as `TURBO_TOKEN`, so no secret is stored. ID tokens expire within minutes, but turbo reads `TURBO_TOKEN` once per run, so the server issues its own token, valid for `OIDC_TOKEN_TTL_SECS` (one hour by default).

### Server configuration

```bash
OIDC_ISSUER=https://token.actions.githubusercontent.com
# Any string; the workflow requests its ID token for it. The cache URL is a good choice.
OIDC_AUDIENCE=https://turbo-cache.example.com
# Required: which workflows get a cache token. Here: every repository of one organization.
OIDC_REQUIRED_CLAIMS=repository_owner_id=123456
# Optional: only pushes to main of one repository get read-write tokens, other runs read-only.
OIDC_WRITE_CLAIMS=repository_id=654321,ref=refs/heads/main,event_name=push
# Optional: scope each cache token to the namespace named by this claim (TURBO_TEAM=<owner id>).
OIDC_NAMESPACE_CLAIM=repository_owner_id
# Base64 HMAC key for cache tokens: openssl rand -base64 32
OIDC_SIGNING_KEY_FILE=/run/secrets/oidc_signing_key
```

### Workflow

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

`$GITHUB_ENV` makes the cache token available to every later step of the job, including third-party actions. If that matters, get the token in the step that runs turbo.

### Responses

The response is `{"token": "...", "expiresAt": <unix seconds>, "readOnly": false}`, plus `"namespace"` with `OIDC_NAMESPACE_CLAIM`. Invalid, expired or unverifiable ID tokens get `401`, and valid ID tokens rejected by the claim rules get `403`. Without `OIDC_ISSUER`, the endpoint does not exist (`404`).

### Claim rules

`OIDC_REQUIRED_CLAIMS` and `OIDC_WRITE_CLAIMS` are lists of `claim=value` rules. Rules for the same claim are alternatives; rules for different claims must all match. For example, `repository_id=1,repository_id=2,ref=refs/heads/main` admits either repository, but only on `main`.

Useful claims are `repository_owner_id`, `repository_id`, `ref`, `ref_type`, `environment`, `event_name`, `workflow_ref`, `job_workflow_ref` and `runner_environment` (see GitHub's [OIDC reference](https://docs.github.com/en/actions/reference/security/oidc)).

#### Who gets a token: `OIDC_REQUIRED_CLAIMS`

`OIDC_REQUIRED_CLAIMS` is what restricts access. Any workflow in any GitHub repository can request an ID token for any audience, so issuer and audience alone admit every GitHub workflow.

Use the numeric IDs `repository_owner_id` and `repository_id`, not the names `repository_owner` and `repository`. After an organization or repository is deleted or renamed, someone else can register its old name.

#### Who may upload: `OIDC_WRITE_CLAIMS`

Runs that match `OIDC_WRITE_CLAIMS` get read-write cache tokens; all other runs get [read-only tokens](#read-only-tokens). If it is unset, every cache token is read-write.

Name the repository (`repository_id`) and the event (`event_name=push`), not only the branch. `ref=refs/heads/main` alone is not enough:

- It matches a push to `main` of any repository that `OIDC_REQUIRED_CLAIMS` admits, and organization members can usually create repositories. Such a repository could upload artifacts under the hashes of your production builds in the shared namespace (cache poisoning).
- It also matches `pull_request_target`, `issue_comment`, `workflow_run` and `schedule` runs. Their `ref` is the default branch, and some of them run code from pull requests.

To allow only one workflow file, add `workflow_ref=my-org/app/.github/workflows/build.yml@refs/heads/main` (`job_workflow_ref` for reusable workflows).

#### Which namespace: `OIDC_NAMESPACE_CLAIM`

`OIDC_NAMESPACE_CLAIM` scopes each cache token to the namespace named by that claim, so `TURBO_TEAM` must equal the claim's value: `${{ github.repository_owner_id }}` for `repository_owner_id`, `${{ github.repository_id }}` for `repository_id`. The value must be a valid [namespace](#namespaces), otherwise the exchange gets `403`; `repository` is not, because it contains `/`. Without `OIDC_NAMESPACE_CLAIM`, cache tokens are valid for every namespace and for `/metrics`.

All repositories that can write to the same namespace can overwrite each other's artifacts. Give repositories that do not trust each other their own namespace with `repository_id`. For shared namespaces, consider [write-once artifacts](#write-once-artifacts).

### Pull requests from forks

With the `pull_request` event, runs for fork pull requests get no ID token, so the token step fails. Skip the step for forks, or let those runs work without the remote cache.

`pull_request_target` workflows, and private repositories that send write tokens to fork pull requests, do get ID tokens, possibly while running fork code. `event_name=push` in `OIDC_WRITE_CLAIMS` keeps those runs read-only.

### Signing key

Cache tokens are self-contained and signed with a key derived from `OIDC_SIGNING_KEY`, `OIDC_ISSUER` and `OIDC_AUDIENCE`. They stay valid until they expire and cannot be revoked one by one; changing the key invalidates all of them. Every replica needs the same key, and each deployment its own.

### Issuer keys

On the first exchange, the server reads `<OIDC_ISSUER>/.well-known/openid-configuration`, whose `issuer` must equal `OIDC_ISSUER`, and fetches the keys from `jwks_uri`. It therefore needs outbound HTTPS to the issuer. Keys are cached, and an unknown key ID triggers a refetch at most once per minute. A failed fetch is retried after 10 seconds at the earliest; exchanges get `500` meanwhile.
