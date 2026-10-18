# syntax=docker/dockerfile:1.28@sha256:bb22d9815c728170f72750f4e5b0d672e06176142e1d602c7e66c050100b7e5b

# Base images are pinned by multi-arch index digest, written inline in the FROM lines so
# automated dependency updates can bump them. Builder and runtime are both Debian 12
# (bookworm), so the glibc the binary links against matches the runtime.
ARG CARGO_CHEF_VERSION=0.1.78

# =============================================================================
# Stage 1: cargo-chef (dependency caching that survives source changes)
# =============================================================================
FROM rust:1.99.1-bookworm@sha256:e12566d5abc17a0da741628782cdb504b68b8961f5bfcf677936bf3e8b32de29 AS chef
ARG CARGO_CHEF_VERSION
WORKDIR /app
RUN cargo install --locked cargo-chef --version "${CARGO_CHEF_VERSION}"

# Compute a dependency-only recipe from the manifests. The recipe only changes when
# Cargo.toml/Cargo.lock (or the target layout) change, so the `cook` layer below stays cached.
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# =============================================================================
# Stage 2: build the release binary
# =============================================================================
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --locked --recipe-path recipe.json --bin fast-turbo-cache-server
COPY . .
RUN cargo build --release --locked --bin fast-turbo-cache-server && \
    install -D -m 0755 target/release/fast-turbo-cache-server /out/fast-turbo-cache-server && \
    mkdir -p /out/data/turbo-cache

# =============================================================================
# Stage 3: minimal runtime (distroless: no shell, no package manager, no curl)
# =============================================================================
FROM gcr.io/distroless/cc-debian12:nonroot@sha256:9dac0a79194e45a7da0158a9c6da57b217585af0786db3845d1f0ec1a0dd182f AS runtime

# UID/GID 10001 (not distroless' 65532) keeps existing volumes created by earlier images writable.
COPY --from=builder --chown=10001:10001 /out/data /data
COPY --from=builder /out/fast-turbo-cache-server /usr/local/bin/fast-turbo-cache-server

USER 10001:10001

# Only container-specific settings live here; every other option uses the binary's built-in
# default (see `fast-turbo-cache-server --help` / docs/configuration.md).
# NO_COLOR=1: container logs go to a log driver, not a terminal, so the text log format is
# written without ANSI colour escapes (https://no-color.org/; `-e NO_COLOR=` re-enables them).
ENV LOCAL_STORAGE_PATH=/data/turbo-cache \
    NO_COLOR=1

EXPOSE 3000

VOLUME ["/data/turbo-cache"]

# The `healthcheck` sub-command reads HOST/PORT from the container environment, so the check
# follows `-e PORT=...` and needs no curl/shell in the image.
HEALTHCHECK --interval=15s --timeout=10s --start-period=10s --retries=3 \
    CMD ["/usr/local/bin/fast-turbo-cache-server", "healthcheck"]

ENTRYPOINT ["/usr/local/bin/fast-turbo-cache-server"]
