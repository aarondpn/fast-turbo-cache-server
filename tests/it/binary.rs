//! Tests of the `fast-turbo-cache-server` binary and its documentation: environment parsing,
//! startup failures, secret handling in output, logging, the `healthcheck` sub-command, bounded
//! shutdown (also with a stuck blocking job) and the sweep of orphaned spool files;
//! docs/configuration.md and .env.example must cover every option.

use std::{
    io::Write,
    path::Path,
    time::{Duration, Instant},
};

use clap::CommandFactory;
use fast_turbo_cache_server::CliConfig;

use crate::support::{
    Api, Fetch,
    process::{NO_SERVER_PORT, Server, combined, command, run},
    set_mtime,
};

/// Environment variables read by the server.
fn env_names() -> Vec<String> {
    CliConfig::command()
        .get_arguments()
        .filter_map(|arg| arg.get_env())
        .map(|env| env.to_string_lossy().into_owned())
        .collect()
}

#[test]
fn bool_env_vars_accept_boolish_values() {
    // `healthcheck` against a closed port exits 1; a clap parse error would exit 2.
    let mut cmd = command();
    cmd.env("PORT", NO_SERVER_PORT)
        .env("ALLOW_DELETE", "false")
        .env("ISOLATE_TEAMS", "false")
        .env("ALLOW_UNAUTHENTICATED", "true")
        .env("IMMUTABLE_ARTIFACTS", "0")
        .env("PROTECT_OBSERVABILITY", "yes")
        .env("S3_SIGN_PAYLOAD", "false")
        .env("S3_FORCE_PATH_STYLE", "false")
        .env("S3_TREAT_403_AS_MISS", "true")
        .arg("healthcheck");
    let out = run(cmd);
    assert_eq!(out.status.code(), Some(1), "{}", combined(&out));

    let mut bad = command();
    bad.env("ALLOW_DELETE", "maybe").arg("healthcheck");
    let out = run(bad);
    assert_eq!(out.status.code(), Some(2), "{}", combined(&out));
}

/// Starts the server with `STORAGE_TYPE=local` (in a temporary directory) plus `envs`, expects
/// it to fail before binding and returns its output.
fn startup_error(envs: &[(&str, &str)]) -> String {
    let data_dir = tempfile::tempdir().unwrap();
    let mut cmd = command();
    cmd.env("HOST", "127.0.0.1")
        .env("PORT", NO_SERVER_PORT)
        .env("STORAGE_TYPE", "local")
        .env("LOCAL_STORAGE_PATH", data_dir.path())
        .envs(envs.iter().copied());
    let out = run(cmd);
    assert!(
        !out.status.success(),
        "expected startup failure for {envs:?}"
    );
    combined(&out)
}

#[test]
fn invalid_configuration_fails_startup_without_printing_secrets() {
    let text = startup_error(&[]);
    assert!(
        text.contains("No authentication tokens configured"),
        "{text}"
    );

    let text = startup_error(&[
        ("TURBO_TOKEN", "t"),
        ("STORAGE_TYPE", "s3"),
        ("S3_BUCKET", "b"),
        ("S3_ACCESS_KEY_ID", "AK"),
        ("S3_SECRET_ACCESS_KEY", "SK"),
        ("S3_ENDPOINT", "https://admin:supersecretpw@s3.example.com"),
    ]);
    assert!(text.contains("must not contain credentials"), "{text}");
    assert!(!text.contains("supersecretpw"), "{text}");

    let oidc = [
        ("OIDC_ISSUER", "https://token.actions.githubusercontent.com"),
        ("OIDC_AUDIENCE", "https://turbo-cache.example.com"),
        (
            "OIDC_SIGNING_KEY",
            "supersecret-not-base64-but-longer-than-32",
        ),
    ];
    let text = startup_error(&oidc);
    assert!(text.contains("OIDC_REQUIRED_CLAIMS is required"), "{text}");
    let text = startup_error(
        &[
            &oidc[..],
            &[("OIDC_REQUIRED_CLAIMS", "repository_owner=acme")],
        ]
        .concat(),
    );
    assert!(
        text.contains("OIDC_SIGNING_KEY must be base64 encoding at least 32 random bytes"),
        "{text}"
    );
    assert!(!text.contains("supersecret"), "{text}");
}

#[test]
fn server_with_token_file_json_logs_and_bounded_shutdown() {
    let mut token_file = tempfile::NamedTempFile::new().unwrap();
    writeln!(token_file, "# tokens\nfile-token-1\nfile-token-2").unwrap();
    let token_path = token_file.path().to_str().unwrap().to_string();

    // `Server::start` waits until the `healthcheck` sub-command succeeds.
    let mut server = Server::start(&[
        ("TURBO_TOKEN_FILE", &token_path),
        ("LOG_FORMAT", "json"),
        ("SHUTDOWN_GRACE_SECS", "1"),
    ]);
    let port = server.port;

    let rt = tokio::runtime::Runtime::new().unwrap();
    let (ok_status, bad_status) = rt.block_on(async {
        let api = Api::new(server.url(""));
        let path = "/v8/artifacts/status";
        let ok = api.with_token("file-token-2").get(path).fetch().await;
        let bad = api.with_token("wrong").get(path).fetch().await;
        (ok.status(), bad.status())
    });
    assert_eq!(ok_status, reqwest::StatusCode::OK);
    assert_eq!(bad_status, reqwest::StatusCode::UNAUTHORIZED);

    let first_line = server.first_line();
    let parsed: serde_json::Value = serde_json::from_str(first_line.trim())
        .unwrap_or_else(|e| panic!("log line is not JSON ({e}): {first_line}"));
    assert!(parsed.get("level").is_some(), "{first_line}");

    // Hold an idle connection open: shutdown must still finish within the grace period.
    let mut idle = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();

    #[cfg(unix)]
    {
        // Panics if the server does not stop within 10 s of SIGTERM.
        let (_, status, out) = server.terminate(Duration::from_secs(10));
        assert!(status.success(), "server exited with {status}\n{out}");
        // The idle connection was closed by the server (EOF or reset). Probing the old port
        // instead would race with other tests: the OS may hand it to another `PORT=0` server.
        use std::io::Read;
        idle.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut buf = [0u8; 1];
        match idle.read(&mut buf) {
            Ok(0) => {}
            Ok(n) => panic!("unexpected {n} byte(s) on the idle connection after shutdown"),
            Err(e) => assert!(
                !matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ),
                "idle connection still open after shutdown: {e}"
            ),
        }
    }
    #[cfg(not(unix))]
    drop(idle);
}

/// Every commented-out `# NAME=value` line of `.env.example` must be accepted by the server when
/// uncommented as written.
#[test]
fn env_example_lines_are_valid_when_uncommented() {
    let example = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/.env.example"))
        .expect("read .env.example");
    let server_vars = env_names();
    let mut checked = 0;
    for line in example.lines().filter_map(|l| l.strip_prefix("# ")) {
        let Some((name, value)) = line.split_once('=') else {
            continue;
        };
        if !server_vars.iter().any(|v| v == name) {
            continue;
        }
        // `healthcheck` parses the full configuration: a parse error exits 2, the unreachable
        // server exits 1.
        let mut cmd = command();
        cmd.env("PORT", NO_SERVER_PORT)
            .env(name, value)
            .arg("healthcheck");
        let out = run(cmd);
        assert_ne!(
            out.status.code(),
            Some(2),
            ".env.example line `{name}={value}` is rejected:\n{}",
            combined(&out)
        );
        checked += 1;
    }
    assert!(checked > 40, "only {checked} lines checked");
}

/// `--help` shows which environment variables are set, but never the values of secrets.
#[test]
fn help_does_not_print_secret_values() {
    let mut cmd = command();
    for (name, value) in [
        ("TURBO_TOKEN", "supersecret-token"),
        ("TURBO_SCOPED_TOKENS", "team=supersecret-scoped"),
        ("TURBO_READ_ONLY_TOKENS", "supersecret-read-only"),
        (
            "TURBO_SCOPED_READ_ONLY_TOKENS",
            "team=supersecret-scoped-ro",
        ),
        ("S3_ACCESS_KEY_ID", "supersecret-key-id"),
        ("S3_SECRET_ACCESS_KEY", "supersecret-s3"),
        ("S3_SESSION_TOKEN", "supersecret-session"),
        ("S3_ENDPOINT", "http://user:supersecret-pw@example.com"),
        ("OIDC_SIGNING_KEY", "supersecret-oidc-signing-key"),
    ] {
        cmd.env(name, value);
    }
    cmd.arg("--help");
    let out = run(cmd);
    assert!(out.status.success(), "{}", combined(&out));
    let help = combined(&out);
    assert!(help.contains("[env: TURBO_TOKEN"), "{help}");
    assert!(
        !help.contains("supersecret"),
        "secret value in --help:\n{help}"
    );
}

/// Every environment variable must appear as `| \`NAME\` |` in the configuration reference
/// (docs/configuration.md), so new options cannot ship undocumented.
#[test]
fn configuration_reference_documents_every_env_option() {
    let reference = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/docs/configuration.md"
    ))
    .expect("read docs/configuration.md");
    let names = env_names();
    assert!(names.len() > 40, "only {} env options found", names.len());
    let missing: Vec<&String> = names
        .iter()
        .filter(|name| !reference.contains(&format!("| `{name}` |")))
        .collect();
    assert!(
        missing.is_empty(),
        "not documented in docs/configuration.md: {missing:?}"
    );
}

/// Requests `path` and returns all log lines up to (and including) the access log line of a
/// following `GET /v8/artifacts/status` marker request.
fn log_lines_through_marker(server: &Server, path: &str) -> Vec<String> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let api = Api::new(server.url(""));
        api.get(path).fetch().await;
        api.with_token("tok")
            .get("/v8/artifacts/status")
            .fetch()
            .await;
    });
    let mut seen = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while let Some(left) = deadline.checked_duration_since(Instant::now()) {
        match server.next_line(left) {
            Ok(line) => {
                let marker = line.contains("path=/v8/artifacts/status")
                    && line.contains("finished processing request");
                seen.push(line);
                if marker {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    seen
}

/// One INFO access log line per request (target `fast_turbo_cache_server::access`), except that
/// successful probes (`/healthz`, `/readyz`, `/metrics`, ...) are logged at DEBUG; the access log
/// can be switched off with `RUST_LOG=info,fast_turbo_cache_server::access=off`. Text logs carry
/// no ANSI colour codes when stdout is not a terminal.
#[test]
fn access_log_skips_probes_and_can_be_disabled() {
    let server = Server::start(&[("TURBO_TOKEN", "tok")]);
    let seen = log_lines_through_marker(&server, "/healthz");
    assert!(
        seen.iter().all(|l| !l.contains('\u{1b}')),
        "ANSI escape codes in piped log output: {seen:#?}"
    );
    let access: Vec<&String> = seen
        .iter()
        .filter(|l| l.contains("finished processing request"))
        .collect();
    assert_eq!(
        access.len(),
        1,
        "expected only the marker request: {seen:#?}"
    );
    assert!(
        access[0].contains("fast_turbo_cache_server::access"),
        "{}",
        access[0]
    );
    assert!(access[0].contains("status=200"), "{}", access[0]);
    assert!(access[0].contains("request_id="), "{}", access[0]);
    // Failed requests are logged at INFO.
    let seen = log_lines_through_marker(&server, "/v8/nope");
    assert!(
        seen.iter()
            .any(|l| l.contains("path=/v8/nope") && l.contains("status=404")),
        "{seen:#?}"
    );
    let seen = log_lines_through_marker(&server, "/metrics?x=1");
    assert!(
        seen.iter()
            .all(|l| !(l.contains("path=/metrics") && l.contains("finished processing request"))),
        "successful probe logged at INFO: {seen:#?}"
    );
    drop(server);

    let quiet = Server::start(&[
        ("TURBO_TOKEN", "tok"),
        ("RUST_LOG", "info,fast_turbo_cache_server::access=off"),
    ]);
    let seen = log_lines_through_marker(&quiet, "/v8/artifacts/status");
    assert!(
        seen.iter()
            .all(|l| !l.contains("finished processing request")),
        "access log not disabled: {seen:#?}"
    );
    assert!(
        seen.iter()
            .any(|l| l.contains("Starting fast-turbo-cache-server")),
        "other INFO logs must stay: {seen:#?}"
    );
}

const TOKEN: &str = "integration-token";

/// [`Server::start`] with `TURBO_TOKEN` set to [`TOKEN`].
fn start_server(envs: &[(&str, &str)]) -> Server {
    Server::start(&[&[("TURBO_TOKEN", TOKEN)], envs].concat())
}

/// SIGTERM while a `spawn_blocking` storage job never finishes: the local backend opening an
/// artifact file that is a FIFO blocks until a writer appears. The default `#[tokio::main]`
/// runtime teardown would wait for that job forever; the process must exit within
/// `SHUTDOWN_GRACE_SECS` plus a small margin.
#[cfg(unix)]
#[test]
fn process_exits_within_grace_despite_stuck_blocking_job() {
    use std::{io::Write, path::PathBuf, process::Command};

    use reqwest::StatusCode;

    use crate::support::{Api, Fetch, all_files};

    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let mut server = start_server(&[
        ("STORAGE_TYPE", "local"),
        ("LOCAL_STORAGE_PATH", data_dir.to_str().unwrap()),
        // No hot cache, so the GET reaches the local backend's blocking pool.
        ("MEMORY_CACHE_MAX_BYTES", "0"),
        ("METADATA_CACHE_MAX_ENTRIES", "0"),
        ("SHUTDOWN_GRACE_SECS", "1"),
    ]);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let status = rt.block_on(async {
        let api = Api::new(server.url("")).with_token(TOKEN);
        let resp = api.put("/v8/artifacts/stuckhash").body("payload").fetch();
        resp.await.status()
    });
    assert_eq!(status, StatusCode::ACCEPTED);

    let artifacts: Vec<PathBuf> = all_files(&data_dir)
        .into_iter()
        .filter(|path| path.to_string_lossy().ends_with(".artifact"))
        .collect();
    assert_eq!(artifacts.len(), 1, "{artifacts:?}");
    std::fs::remove_file(&artifacts[0]).unwrap();
    assert!(
        Command::new("mkfifo")
            .arg(&artifacts[0])
            .status()
            .unwrap()
            .success()
    );

    let mut stuck = std::net::TcpStream::connect(("127.0.0.1", server.port)).unwrap();
    write!(
        stuck,
        "GET /v8/artifacts/stuckhash HTTP/1.1\r\nHost: localhost\r\n\
         Authorization: Bearer {TOKEN}\r\n\r\n"
    )
    .unwrap();
    // Give the request time to reach the blocking `open()`.
    std::thread::sleep(Duration::from_millis(500));

    let (elapsed, _, out) = server.terminate(Duration::from_secs(15));
    assert!(
        elapsed < Duration::from_secs(4),
        "shutdown took {elapsed:?} with SHUTDOWN_GRACE_SECS=1:\n{out}"
    );
    drop(stuck);
}

/// Spool files left behind by a crash mid-upload are removed at startup and by the periodic
/// sweep once older than `max(LOCAL_TEMP_MAX_AGE_SECS, 2 x BODY_TOTAL_TIMEOUT_SECS)` (2 s here);
/// files without the spool prefix are kept.
#[test]
fn orphaned_spool_files_are_swept_at_startup_and_periodically() {
    let dir = tempfile::tempdir().unwrap();
    let spool = dir.path().join("spool");
    std::fs::create_dir(&spool).unwrap();
    let stale = spool.join(".turbo-spool-crashed.tmp");
    std::fs::write(&stale, b"partial upload").unwrap();
    set_mtime(&stale, Duration::from_secs(3600));
    let unrelated = spool.join("keep-me.tmp");
    std::fs::write(&unrelated, b"x").unwrap();
    set_mtime(&unrelated, Duration::from_secs(3600));

    let _server = start_server(&[
        ("SPOOL_DIR", spool.to_str().unwrap()),
        ("LOCAL_TEMP_MAX_AGE_SECS", "2"),
        ("BODY_TOTAL_TIMEOUT_SECS", "1"),
        ("STORAGE_CLEANUP_INTERVAL_SECS", "1"),
    ]);

    let wait_until_gone = |path: &Path, limit: Duration| {
        let started = Instant::now();
        while path.exists() {
            assert!(
                started.elapsed() < limit,
                "{} was not swept within {limit:?}",
                path.display()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    };
    wait_until_gone(&stale, Duration::from_secs(5));

    // A file created while the server runs survives sweeps while young, then is removed.
    let fresh = spool.join(".turbo-spool-later.tmp");
    std::fs::write(&fresh, b"partial").unwrap();
    std::thread::sleep(Duration::from_millis(1200));
    assert!(fresh.exists(), "young spool file removed too early");
    wait_until_gone(&fresh, Duration::from_secs(6));
    assert!(unrelated.exists(), "unrelated file must never be removed");
}
