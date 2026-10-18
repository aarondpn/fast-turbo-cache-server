use std::{
    io::IsTerminal,
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

use anyhow::Context;
use clap::Parser;
use fast_turbo_cache_server::{
    AppState, AuthConfig, CliConfig, LogFormat, build_router, build_storage,
    config::Command,
    oidc::OidcExchange,
    server::{run_healthcheck, serve, shutdown_signal},
    storage::purge_stale_spool_files,
};
use tokio::net::TcpListener;
use tracing::{info, warn};
use tracing_subscriber::{EnvFilter, fmt};

/// Default log filter when `RUST_LOG` is unset: INFO for everything, including one access log
/// line per request (target `fast_turbo_cache_server::access`).
const DEFAULT_LOG_FILTER: &str = "info";

fn init_logging(format: LogFormat) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_LOG_FILTER));
    match format {
        LogFormat::Text => fmt()
            .with_ansi(use_ansi_colors(
                std::io::stdout().is_terminal(),
                std::env::var_os("NO_COLOR").as_deref(),
            ))
            .with_env_filter(filter)
            .init(),
        LogFormat::Json => fmt()
            .json()
            .flatten_event(true)
            .with_current_span(true)
            .with_env_filter(filter)
            .init(),
    }
}

/// ANSI colours only for an interactive terminal, and never when `NO_COLOR` is set to a
/// non-empty value (<https://no-color.org>): log files, `journald` and container log drivers
/// would otherwise store raw escape sequences.
fn use_ansi_colors(stdout_is_terminal: bool, no_color: Option<&std::ffi::OsStr>) -> bool {
    stdout_is_terminal && no_color.is_none_or(|value| value.is_empty())
}

/// Time granted on top of `SHUTDOWN_GRACE_SECS` for tearing down the runtime after the server
/// loop has returned.
const RUNTIME_SHUTDOWN_SLACK: Duration = Duration::from_millis(500);

fn main() -> anyhow::Result<()> {
    let config = CliConfig::parse();
    let grace = Duration::from_secs(config.shutdown_grace_secs);
    let shutdown_started = Arc::new(OnceLock::new());

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("Failed to start the Tokio runtime")?;
    let result = runtime.block_on(run(config, shutdown_started.clone()));

    // Dropping a runtime waits indefinitely for `spawn_blocking` jobs (local fsync/copy, S3
    // payload hashing, spool sweeps) of requests that were cut off, so the process could outlive
    // SHUTDOWN_GRACE_SECS by minutes. Give them what is left of the grace period (measured from
    // the shutdown signal), then abandon them: async tasks, including detached storage writes,
    // are dropped, and blocking threads end with the process.
    let remaining = shutdown_started
        .get()
        .map_or(Duration::ZERO, |started: &Instant| {
            grace.saturating_sub(started.elapsed())
        });
    runtime.shutdown_timeout(remaining + RUNTIME_SHUTDOWN_SLACK);
    result
}

async fn run(
    mut config: CliConfig,
    shutdown_started: Arc<OnceLock<Instant>>,
) -> anyhow::Result<()> {
    if config.command == Some(Command::Healthcheck) {
        let addr = config
            .healthcheck_addr()
            .context("Failed to parse HOST/PORT")?;
        match run_healthcheck(addr).await {
            Ok(()) => std::process::exit(0),
            Err(err) => {
                eprintln!("healthcheck failed: {err:#}");
                std::process::exit(1);
            }
        }
    }

    init_logging(config.log_format);

    config
        .load_secret_files()
        .context("Failed to load secret files")?;
    config.validate().context("Invalid configuration")?;

    let tokens = config.effective_tokens();
    let scoped_tokens = config
        .effective_scoped_tokens()
        .context("Failed to parse scoped tokens")?;
    let scoped_read_only_tokens = config
        .effective_scoped_read_only_tokens()
        .context("Failed to parse scoped read-only tokens")?;
    let oidc = config.oidc_config().context("Invalid OIDC configuration")?;
    let has_scoped = !scoped_tokens.is_empty()
        || oidc
            .as_ref()
            .is_some_and(|oidc| oidc.namespace_claim.is_some());
    if oidc
        .as_ref()
        .is_some_and(|oidc| oidc.namespace_claim.is_none())
        && !(scoped_tokens.is_empty() && scoped_read_only_tokens.is_empty())
    {
        warn!(
            "OIDC_NAMESPACE_CLAIM is not set: cache tokens from the OIDC token exchange are valid for every namespace (and /metrics), unlike the scoped tokens"
        );
    }
    let uploads_per_namespace = config.uploads_per_namespace(has_scoped);
    let auth = AuthConfig::new(tokens, config.allow_unauthenticated)
        .with_scoped_tokens(scoped_tokens)
        .with_read_only_tokens(config.effective_read_only_tokens())
        .with_scoped_read_only_tokens(scoped_read_only_tokens);

    let addr = config
        .socket_addr()
        .context("Failed to parse bind socket address")?;
    let server_options = config.server_options();

    let storage = build_storage(&config)
        .await
        .context("Failed to initialize storage backend")?;

    create_private_dir(&server_options.spool_dir)
        .await
        .with_context(|| {
            format!(
                "Failed to create spool directory '{}'",
                server_options.spool_dir.display()
            )
        })?;
    spawn_spool_sweeper(
        server_options.spool_dir.clone(),
        config.spool_max_age(),
        config.spool_cleanup_interval(),
    );

    let mut state = AppState::new(storage.clone(), auth)
        .with_caching_status(config.caching_status)
        .with_isolate_teams(config.isolate_teams)
        .with_max_artifact_size(config.max_artifact_size)
        .with_max_concurrent_uploads(config.max_concurrent_uploads)
        .with_max_uploads_per_namespace(uploads_per_namespace)
        .with_batch_head_concurrency(config.batch_head_concurrency)
        .with_immutable_artifacts(config.immutable_artifacts)
        .with_allow_delete(config.allow_delete)
        .with_protect_observability(config.protect_observability)
        .with_cors_allow_origin(&config.cors_allow_origin)
        .with_server_options(server_options.clone());
    if let Some(oidc) = oidc {
        let exchange =
            OidcExchange::new(oidc).context("Failed to initialize the OIDC token exchange")?;
        state = state.with_oidc(exchange);
    }

    let oidc_enabled = state.oidc.is_some();
    let app = build_router(state);

    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("Failed to bind TCP listener on {addr}"))?;
    // The bound address (differs from `addr` with `PORT=0`).
    let bound = listener.local_addr().unwrap_or(addr);

    info!(
        address = %bound,
        backend = storage.backend_name(),
        isolate_teams = config.isolate_teams,
        allow_unauthenticated = config.allow_unauthenticated,
        immutable_artifacts = config.immutable_artifacts,
        allow_delete = config.allow_delete,
        max_concurrent_uploads = config.max_concurrent_uploads,
        max_uploads_per_namespace = uploads_per_namespace,
        oidc_token_exchange = oidc_enabled,
        "Starting fast-turbo-cache-server"
    );

    let shutdown = async move {
        shutdown_signal().await;
        let _ = shutdown_started.set(Instant::now());
    };
    serve(listener, app, &server_options, shutdown)
        .await
        .context("Server exited with error")?;

    info!("Server stopped");
    Ok(())
}

/// Creates `dir` (and parents) if missing; on unix the leaf is restricted to the owner (0700).
async fn create_private_dir(dir: &std::path::Path) -> std::io::Result<()> {
    let mut builder = tokio::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    builder.mode(0o700);
    builder.create(dir).await
}

/// Removes orphaned upload spool files (left behind by a crash or `SIGKILL` mid-upload) at
/// startup and then every `interval`. Only `.turbo-spool-*.tmp` files older than `max_age` are
/// touched, so a spool directory shared with other processes (e.g. the OS temp dir) is safe.
fn spawn_spool_sweeper(
    dir: std::path::PathBuf,
    max_age: std::time::Duration,
    interval: std::time::Duration,
) {
    let interval = interval.max(std::time::Duration::from_secs(1));
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            // The first tick completes immediately: sweep once at startup.
            ticker.tick().await;
            match purge_stale_spool_files(&dir, max_age).await {
                Ok(0) => {}
                Ok(removed) => info!(
                    removed,
                    spool_dir = %dir.display(),
                    "Removed orphaned upload spool files"
                ),
                Err(err) => warn!(
                    spool_dir = %dir.display(),
                    error = %err,
                    "Failed to sweep orphaned upload spool files"
                ),
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;

    use super::use_ansi_colors;

    #[test]
    fn ansi_colors_only_on_terminals_without_no_color() {
        assert!(use_ansi_colors(true, None));
        assert!(use_ansi_colors(true, Some(OsStr::new(""))));
        assert!(!use_ansi_colors(true, Some(OsStr::new("1"))));
        assert!(!use_ansi_colors(false, None));
        assert!(!use_ansi_colors(false, Some(OsStr::new(""))));
    }
}
