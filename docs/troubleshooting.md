# Troubleshooting

When the remote cache is never used or always misses, the cause is almost always the client configuration or unstable task hashes, not the server.

## Client configuration

turbo prints the remote cache state at the start of every run: `Remote caching enabled`, `Remote caching disabled (...)` or `Remote caching unavailable (...)`. `Remote caching disabled (TURBO_TOKEN set without TURBO_TEAM)` means turbo ignores the token until a team is set.

| Setting | Environment variable | Flag | `turbo.json` `remoteCache` |
|---|---|---|---|
| Server URL | `TURBO_API` | `--api` | `apiUrl` |
| Token | `TURBO_TOKEN` | `--token` | – |
| Team slug (namespace) | `TURBO_TEAM` | `--team` | `teamSlug` |
| Team ID | `TURBO_TEAMID` | – | `teamId` |
| Timeouts (s) | `TURBO_REMOTE_CACHE_TIMEOUT`, `TURBO_REMOTE_CACHE_UPLOAD_TIMEOUT` | `--remote-cache-timeout` | `timeout` (30), `uploadTimeout` (60) |

A trailing slash in `TURBO_API` makes turbo request `//v8/...`, which the server accepts. If `TURBO_API` contains a path, the [reverse proxy must strip it](deployment.md#reverse-proxy-and-tls).

### Namespace

turbo sends the team slug as `?slug=` and the team ID as `?teamId=`, but the team ID only if it starts with `team_`. With only `TURBO_TEAMID=ci`, the namespace is therefore `default`. Clients with different `TURBO_TEAM` values, such as developers and CI, use separate caches. See also [Namespaces](authentication.md#namespaces).

### Check by hand

```bash
curl -sS -H "Authorization: Bearer $TURBO_TOKEN" "$TURBO_API/v8/artifacts/status?slug=$TURBO_TEAM"
```

`{"status":"enabled"}` means the token and namespace work. `401` means a wrong token, `403` a scoped token that is not valid for this namespace.

### Signatures

With `"remoteCache": { "signature": true }`, turbo signs uploads with `TURBO_REMOTE_CACHE_SIGNATURE_KEY` and silently treats downloads with a missing or invalid `x-artifact-tag` as misses. The signed data includes `TURBO_TEAMID`, so all clients of a namespace need the same key and the same `TURBO_TEAMID` (or none). The server stores the tag unchanged and does not check it. Artifacts uploaded without signing stay misses for signing clients.

### Preflight

This server does not need `--preflight` (`TURBO_PREFLIGHT`, `remoteCache.preflight`). With it, turbo sends the token only if the `OPTIONS` answer lists `Authorization` in `Access-Control-Allow-Headers`, and sends the request to that answer's `Location` header, if any. A proxy that answers `OPTIONS` itself can therefore cause `401`s.

## Seeing what happens

`turbo run build --dry=json` shows each task's `hash` and `cache` (`status` `HIT`/`MISS`, `source` `LOCAL`/`REMOTE`) without running anything. To check only the remote cache:

```bash
turbo run build --dry=json --cache=local:,remote:r | jq '.tasks[] | {taskId, hash, cache}'
```

- `--summarize` (`TURBO_RUN_SUMMARY`) writes the same data for a real run to `.turbo/runs/`.
- `-vv` or `-vvv` prints debug and trace logs, including remote cache errors.
- turbo warnings to search CI logs for: `Remote artifact cache is temporarily unavailable; retrying after a 30-second cooldown`, `the cache artifact for <hash> was too large to upload within the timeout` and `failed to contact remote cache: ...`.

The server logs each request as `finished processing request` (see [Logging](deployment.md#logging)). No requests at all means the client is not configured or cannot reach the server.

| Metric | Shows |
|---|---|
| `turbo_cache_get_hits_total`, `turbo_cache_get_misses_total`, `turbo_cache_head_hits_total`, `turbo_cache_head_misses_total` | Artifact lookups, without batch queries. |
| `turbo_cache_puts_total` | Uploads. Misses without uploads mean clients do not upload (`--cache=remote:r`, `TURBO_REMOTE_CACHE_READ_ONLY` or failing uploads). |
| `turbo_cache_upload_rejections_total{reason}` | `timeout` (`408`), `slot_unavailable` (`429`), `too_large` (`413`). |
| `turbo_cache_storage_errors_total{operation}`, `turbo_cache_storage_operation_duration_seconds` | Backend failures (answered with `5xx`) and latency. |
| `turbo_cache_analytics_events_total{source,event}` | Local and remote hits and misses as reported by clients. |

## turbo behaviour that looks like a server problem

- **Outage backoff.** After 3 consecutive failed requests (connection errors, timeouts, `5xx`), turbo skips the remote cache, reads included, for 30 s, then tries one request and backs off again if it fails. A short storage outage or a slow link therefore shows up as a series of misses.
- **403 answers.** turbo disables the remote cache for the rest of the run only on a `403` with the code `forbidden`, which this server never sends. Its `forbidden_namespace` and `artifact_immutable` answers fail only that request; a failed read counts as a miss.
- **`CACHING_STATUS`.** With any value other than `enabled`, turbo stops using the remote cache for the run after the first rejected upload.
- **Timeouts.** Uploads must finish within `remoteCache.uploadTimeout` (60 s), including the wait for an upload slot. All other requests must finish within `remoteCache.timeout` (30 s). `0` disables a timeout. Raise them for large artifacts on slow links.
- **No upload retries.** A failed, timed-out or rejected upload (`408`, `413`, `429`) is not retried; the next run that misses the artifact uploads it. Downloads and lookups are retried once on `429`, `5xx` and timeouts.

## Unstable hashes

Lookups for hashes that were never uploaded mean the hashes change between runs:

- **Per-run variables** in `globalEnv` or a task's `env` change the hash, for example `GITHUB_RUN_ID`, `GITHUB_SHA`, `CI_JOB_ID`, timestamps or paths to per-job credential files. A change in `globalEnv` misses every task. Put variables whose value does not affect the output in `passThroughEnv` or `globalPassThroughEnv`; they are available to the task but not hashed.
- **Untracked files** that are not gitignored are inputs, for example files written by an earlier CI step. Gitignore them or narrow `inputs`.
- **Missing inputs** cause stale hits: files a task reads outside its inputs, such as root config files or other packages, do not change the hash. Add them with `$TURBO_ROOT$/...` in `inputs` or in `globalDependencies`.

To find the differing input, compare two dry runs or `--summarize` files (environment variable values appear only as hashes):

```bash
turbo run build --dry=json > a.json   # run A (e.g. CI), then run B (e.g. local) > b.json
diff <(jq -S .globalCacheInputs a.json) <(jq -S .globalCacheInputs b.json)
diff <(jq -S '.tasks[] | {taskId, hash, inputs, environmentVariables}' a.json) \
     <(jq -S '.tasks[] | {taskId, hash, inputs, environmentVariables}' b.json)
```

## Docker builds

Pass the token as a BuildKit secret, not as a build argument as in the Turborepo Docker guide. Build arguments stay in the image and invalidate the layer cache when they change.

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

turbo trusts its bundled Mozilla CA list and the system store, so public certificates work in slim images. A private CA must be added to the image's system store; on Debian, install `ca-certificates`, copy the CA to `/usr/local/share/ca-certificates/` and run `update-ca-certificates`.
