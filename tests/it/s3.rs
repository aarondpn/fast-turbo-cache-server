//! S3 backend tests: signing, metadata format, conditional and multipart uploads, miss/error
//! classification, readiness, HTTP client policy and credential providers.
//!
//! Most tests run against the in-process mock S3 server of [`crate::support::mock_s3`], which
//! verifies AWS SigV4 signatures with an independent implementation. `live_s3_roundtrip` runs
//! against a real S3-compatible server when `TURBO_S3_LIVE_TEST=1` (see the test for the
//! variables).

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    Router,
    body::Body,
    http::{HeaderMap, Method, StatusCode, header},
    response::IntoResponse,
};
use bytes::Bytes;
use fast_turbo_cache_server::{
    AppError, PutMode, PutOutcome, S3StorageConfig, ServerOptions, SpooledFile, Storage,
    UploadBody,
    models::ArtifactMetadata,
    storage::{RetentionOptions, object::MULTIPART_PART_SIZE, s3::S3Storage},
    upload::{UploadLimits, UploadStream},
};
use futures_util::StreamExt;
use sha2::{Digest, Sha256};

use crate::support::{
    Api, Fetch, body_bytes,
    mock_s3::{
        ACCESS_KEY, BUCKET, LoggedRequest, Mock, REGION, SECRET_KEY, Scripted, StoredObject,
        spawn_mock,
    },
    spawn_server, tok_state,
};

const PART: usize = MULTIPART_PART_SIZE;

// ---------------------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------------------

fn config(endpoint: &str) -> S3StorageConfig {
    S3StorageConfig {
        provider_name: "s3",
        bucket: BUCKET.to_string(),
        region: REGION.to_string(),
        endpoint: endpoint.to_string(),
        access_key_id: ACCESS_KEY.to_string(),
        secret_access_key: SECRET_KEY.to_string(),
        session_token: None,
        prefix: String::new(),
        force_path_style: true,
        sign_payload: false,
        max_retries: 3,
        parts_in_flight: 4,
        connect_timeout: Duration::from_secs(2),
        read_timeout: Duration::from_secs(5),
        treat_403_as_miss: false,
        health_check_key: None,
        token_dir: std::env::temp_dir(),
    }
}

fn storage(cfg: S3StorageConfig) -> S3Storage {
    S3Storage::with_env(cfg, &|_| None).unwrap()
}

fn meta(duration: u64, tag: Option<&str>) -> ArtifactMetadata {
    ArtifactMetadata {
        duration,
        tag: tag.map(Arc::from),
        ..Default::default()
    }
}

fn bytes_body(data: &[u8]) -> UploadBody {
    UploadBody::Bytes(Bytes::copy_from_slice(data))
}

/// Overwrite-mode PUT of `data`; panics on errors.
async fn put(
    s3: &dyn Storage,
    namespace: &str,
    hash: &str,
    data: &[u8],
    metadata: ArtifactMetadata,
) {
    let body = bytes_body(data);
    s3.put(namespace, hash, body, metadata, PutMode::Overwrite)
        .await
        .unwrap();
}

fn count(log: &[LoggedRequest], method: &Method, path_suffix: &str) -> usize {
    log.iter()
        .filter(|r| r.method == *method && r.raw_path.ends_with(path_suffix))
        .count()
}

/// `DeleteObjects` requests (`POST /bucket?delete`).
fn deletes(log: &[LoggedRequest]) -> usize {
    log.iter()
        .filter(|r| r.method == Method::POST && r.has_param("delete"))
        .count()
}

/// Deterministic test payload of `len` bytes (a repeated 251-byte pattern, cheap to build for
/// multi-MiB payloads in debug builds).
fn payload(len: usize, seed: u32) -> Vec<u8> {
    let pattern: Vec<u8> = (0..251u32).map(|i| ((i * 7 + seed) % 251) as u8).collect();
    let mut data = pattern.repeat(len / pattern.len() + 1);
    data.truncate(len);
    data
}

fn limits(spool_dir: &std::path::Path) -> UploadLimits {
    UploadLimits {
        max_size: 1 << 30,
        memory_threshold: 4 * 1024 * 1024,
        idle_timeout: Duration::from_secs(5),
        total_timeout: Duration::from_secs(60),
        spool_dir: spool_dir.to_path_buf(),
    }
}

/// A request body yielding `data` in 64 KiB chunks, then `tail`.
fn body_then(
    data: Vec<u8>,
    tail: impl futures_util::Stream<Item = std::io::Result<Bytes>> + Send + 'static,
) -> Body {
    let chunks: Vec<std::io::Result<Bytes>> = Bytes::from(data)
        .chunks(64 * 1024)
        .map(|c| Ok(Bytes::copy_from_slice(c)))
        .collect();
    Body::from_stream(futures_util::stream::iter(chunks).chain(tail))
}

/// Waits until `condition` holds (5s at most).
async fn eventually(condition: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !condition() {
        assert!(Instant::now() < deadline, "condition not reached in time");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The `x-amz-meta-*` headers (and content type) stored for an artifact.
const STORED_FORMAT: [(&str, &str); 7] = [
    ("content-type", "application/octet-stream"),
    ("x-amz-meta-duration", "1234"),
    ("x-amz-meta-size", "7"),
    ("x-amz-meta-tag", "tag+/="),
    ("x-amz-meta-sha", "abc  def"),
    ("x-amz-meta-dirty-hash", "dirty"),
    ("x-amz-meta-client-ci", "github  actions   runner"),
];

fn sorted_meta(object: &StoredObject) -> Vec<(String, String)> {
    let mut meta = object.meta.clone();
    meta.sort();
    meta
}

fn stored_format(size: usize) -> Vec<(String, String)> {
    let mut meta: Vec<(String, String)> = STORED_FORMAT
        .iter()
        .map(|(n, v)| (n.to_string(), v.to_string()))
        .collect();
    meta[2].1 = size.to_string();
    meta.sort();
    meta
}

fn stored_format_metadata() -> ArtifactMetadata {
    ArtifactMetadata {
        duration: 1234,
        size: 0,
        tag: Some(Arc::from("tag+/=")),
        sha: Some(Arc::from("abc  def")),
        dirty_hash: Some(Arc::from("dirty")),
        client_ci: Some(Arc::from("github  actions   runner")),
    }
}

// ---------------------------------------------------------------------------------------------
// Signing, metadata format, conditional writes, retries
// ---------------------------------------------------------------------------------------------

/// Prefixes with spaces/unicode and metadata with repeated spaces produce signatures that an
/// S3-style verifier accepts (the canonical URI must not double-encode `%20` to `%2520`).
#[tokio::test]
async fn sigv4_verified_for_unicode_prefix_and_spaced_metadata() {
    let mock = Mock::new();
    let endpoint = spawn_mock(&mock).await;
    let mut cfg = config(&endpoint);
    cfg.prefix = "my prefix/ünï €".to_string();
    cfg.sign_payload = true;
    let s3 = storage(cfg);

    let mut metadata = meta(1234, Some("tag+/="));
    metadata.client_ci = Some(Arc::from("github  actions   runner"));
    metadata.sha = Some(Arc::from("abc  def"));
    let outcome = s3
        .put(
            "team.a",
            "hash_1",
            bytes_body(b"payload"),
            metadata,
            PutMode::Overwrite,
        )
        .await
        .unwrap();
    assert_eq!(outcome, PutOutcome::Stored);
    assert!(mock.object("my prefix/ünï €/team.a/hash_1").is_some());

    let head = s3.head("team.a", "hash_1").await.unwrap().unwrap();
    assert_eq!(head.duration, 1234);
    assert_eq!(head.size, 7);
    assert_eq!(head.client_ci.as_deref(), Some("github  actions   runner"));

    let got = s3.get("team.a", "hash_1").await.unwrap().unwrap();
    assert_eq!(got.metadata.tag.as_deref(), Some("tag+/="));
    assert_eq!(body_bytes(got).await, b"payload");
    assert!(s3.delete("team.a", "hash_1").await.unwrap());
    assert!(mock.object("my prefix/ünï €/team.a/hash_1").is_none());
    assert_eq!(mock.signature_failures(), 0);
    let log = mock.log();
    assert_eq!(deletes(&log), 1);
    assert!(log.iter().filter(|r| !r.has_param("delete")).all(|r| {
        r.raw_path
            .starts_with("/turbo-bucket/my%20prefix/%C3%BCn%C3%AF%20%E2%82%AC/")
    }));
}

/// The key layout and `x-amz-meta-*` header set are a storage format: an object seeded with
/// exactly these headers is read back with all its metadata, and uploads (single `PUT` and
/// multipart) write exactly that header set.
#[tokio::test]
async fn stored_metadata_format_is_stable() {
    let mock = Mock::new();
    let endpoint = spawn_mock(&mock).await;
    let mut cfg = config(&endpoint);
    cfg.prefix = "ci".to_string();
    let s3 = storage(cfg);

    let stored: Vec<(&str, &str)> = STORED_FORMAT.to_vec();
    mock.with(|m| {
        m.objects.insert(
            "ci/team/oldhash".into(),
            StoredObject::new(b"payload", &stored),
        )
    });
    let expected = ArtifactMetadata {
        size: 7,
        ..stored_format_metadata()
    };
    assert_eq!(s3.head("team", "oldhash").await.unwrap().unwrap(), expected);
    let got = s3.get("team", "oldhash").await.unwrap().unwrap();
    assert_eq!(got.metadata, expected);
    assert_eq!(got.content_length, Some(7));
    assert_eq!(body_bytes(got).await, b"payload");

    // Single PUT.
    s3.put(
        "team",
        "newhash",
        bytes_body(b"payload"),
        stored_format_metadata(),
        PutMode::Overwrite,
    )
    .await
    .unwrap();
    let object = mock.object("ci/team/newhash").unwrap();
    assert_eq!(sorted_meta(&object), stored_format(7));

    // Multipart upload (spooled body larger than one part).
    let data = payload(PART + 1000, 1);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("spool");
    std::fs::write(&path, &data).unwrap();
    s3.put(
        "team",
        "bighash",
        UploadBody::File(SpooledFile::new(path, data.len() as u64)),
        stored_format_metadata(),
        PutMode::Overwrite,
    )
    .await
    .unwrap();
    let object = mock.object("ci/team/bighash").unwrap();
    assert_eq!(sorted_meta(&object), stored_format(data.len()));
    assert_eq!(object.body.as_ref(), data.as_slice());
    assert_eq!(mock.multipart_ops(), ["create", "part", "part", "complete"]);
    assert_eq!(mock.signature_failures(), 0);
}

/// IfAbsent is a conditional PUT (`If-None-Match: *`); 412 → AlreadyExists, nothing written.
#[tokio::test]
async fn conditional_put_if_absent_is_atomic() {
    let mock = Mock::new();
    let s3 = storage(config(&spawn_mock(&mock).await));

    let first = s3
        .put(
            "t",
            "h",
            bytes_body(b"one"),
            meta(1, Some("A")),
            PutMode::IfAbsent,
        )
        .await
        .unwrap();
    assert_eq!(first, PutOutcome::Stored);
    let second = s3
        .put(
            "t",
            "h",
            bytes_body(b"two"),
            meta(2, Some("B")),
            PutMode::IfAbsent,
        )
        .await
        .unwrap();
    assert_eq!(second, PutOutcome::AlreadyExists);

    let puts: Vec<_> = mock
        .log()
        .into_iter()
        .filter(|r| r.method == Method::PUT)
        .collect();
    // No HEAD pre-check: exactly two conditional PUTs.
    assert_eq!(puts.len(), 2);
    assert_eq!(count(&mock.log(), &Method::HEAD, "/t/h"), 0);
    for put in &puts {
        assert_eq!(put.headers.get(header::IF_NONE_MATCH).unwrap(), "*");
    }
    assert_eq!(mock.object("t/h").unwrap().body.as_ref(), b"one");

    // Overwrite mode never sends If-None-Match.
    put(&s3, "t", "h", b"three", meta(3, None)).await;
    assert!(
        mock.log()
            .last()
            .unwrap()
            .headers
            .get(header::IF_NONE_MATCH)
            .is_none()
    );
    assert_eq!(mock.object("t/h").unwrap().body.as_ref(), b"three");
}

/// An IfAbsent PUT whose first attempt was applied but answered with 5xx is retried; the
/// retry's 412 is reported as AlreadyExists (the object is stored), without further requests.
#[tokio::test]
async fn retried_conditional_put_412_is_already_exists() {
    let mock = Mock::new();
    let s3 = storage(config(&spawn_mock(&mock).await));
    mock.script([Scripted::ApplyThen(503)]);
    let outcome = s3
        .put(
            "t",
            "own",
            bytes_body(b"data"),
            meta(5, Some("T")),
            PutMode::IfAbsent,
        )
        .await
        .unwrap();
    assert_eq!(outcome, PutOutcome::AlreadyExists);
    assert_eq!(mock.object("t/own").unwrap().body.as_ref(), b"data");
    assert_eq!(count(&mock.log(), &Method::PUT, "/t/own"), 2);
    assert_eq!(count(&mock.log(), &Method::HEAD, "/t/own"), 0);

    // A genuinely different existing artifact is still AlreadyExists after an ambiguous retry.
    mock.script([Scripted::Status(503, Some("SlowDown"))]);
    let outcome = s3
        .put(
            "t",
            "own",
            bytes_body(b"other"),
            meta(6, Some("U")),
            PutMode::IfAbsent,
        )
        .await
        .unwrap();
    assert_eq!(outcome, PutOutcome::AlreadyExists);
    assert_eq!(mock.object("t/own").unwrap().body.as_ref(), b"data");
}

/// 5xx responses are retried for every operation; with retries disabled they fail.
#[tokio::test]
async fn transient_errors_are_retried() {
    let mock = Mock::new();
    let endpoint = spawn_mock(&mock).await;
    let s3 = storage(config(&endpoint));

    mock.script([
        Scripted::Status(503, Some("SlowDown")),
        Scripted::Status(500, Some("InternalError")),
    ]);
    put(&s3, "t", "r", b"retry-me", meta(9, None)).await;
    assert_eq!(count(&mock.log(), &Method::PUT, "/t/r"), 3);

    mock.script([Scripted::Status(503, None)]);
    let got = s3.get("t", "r").await.unwrap().unwrap();
    assert_eq!(body_bytes(got).await, b"retry-me");
    mock.script([Scripted::Status(502, None)]);
    assert!(s3.head("t", "r").await.unwrap().is_some());
    // DELETE is preceded by a HEAD (S3 answers the delete of a missing key with success, too).
    mock.script([Scripted::Pass, Scripted::Status(503, Some("SlowDown"))]);
    assert!(s3.delete("t", "r").await.unwrap());
    assert_eq!(count(&mock.log(), &Method::GET, "/t/r"), 2);
    assert_eq!(count(&mock.log(), &Method::HEAD, "/t/r"), 3);
    assert_eq!(deletes(&mock.log()), 2);

    // Retries exhausted / disabled → storage error (not a miss).
    let mut cfg = config(&endpoint);
    cfg.max_retries = 0;
    let no_retry = storage(cfg);
    mock.clear_log();
    mock.script([Scripted::Status(503, Some("SlowDown"))]);
    assert!(no_retry.get("t", "r").await.is_err());
    assert_eq!(mock.log().len(), 1);
}

/// Spooled uploads up to one part are one PUT, retried as a whole, sent with Content-Length (S3
/// rejects chunked uploads), and their signed payload hash verifies.
#[tokio::test]
async fn spooled_upload_is_retried_and_signed() {
    let mock = Mock::new();
    let mut cfg = config(&spawn_mock(&mock).await);
    cfg.sign_payload = true;
    let s3 = storage(cfg);

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("upload.tmp");
    let data = payload(700_000, 0);
    std::fs::write(&path, &data).unwrap();
    let body = UploadBody::File(SpooledFile::new(path.clone(), data.len() as u64));

    mock.script([Scripted::Status(500, None)]);
    s3.put("t", "file", body, meta(1, None), PutMode::Overwrite)
        .await
        .unwrap();
    let puts: Vec<_> = mock
        .log()
        .into_iter()
        .filter(|r| r.method == Method::PUT)
        .collect();
    assert_eq!(puts.len(), 2);
    for put in &puts {
        assert_eq!(put.body_len, data.len());
        assert_eq!(
            put.headers.get(header::CONTENT_LENGTH).unwrap(),
            &data.len().to_string()
        );
        assert!(put.headers.get(header::TRANSFER_ENCODING).is_none());
        assert_eq!(
            put.headers.get("x-amz-content-sha256").unwrap(),
            &hex::encode(Sha256::digest(&data))
        );
    }
    assert_eq!(mock.signature_failures(), 0);
    assert_eq!(
        mock.object("t/file").unwrap().body.as_ref(),
        data.as_slice()
    );
    assert!(!path.exists(), "spooled file is removed after the upload");
}

/// The spool sweep removes `.turbo-spool-*.tmp` files by mtime, which stops changing once the
/// body is received. A slow upload still being sent after the cutoff must not fail: multipart
/// uploads stream the parts from a descriptor opened before the upload started, single
/// conditional PUTs (immutable mode) send a mapping of the file that survives the unlink.
#[cfg(unix)]
#[tokio::test]
async fn spooled_upload_survives_spool_sweep_between_retries() {
    let mock = Mock::new();
    let s3 = storage(config(&spawn_mock(&mock).await));
    let dir = tempfile::tempdir().unwrap();

    for (hash, mode) in [
        ("swept", PutMode::Overwrite),
        ("swept_once", PutMode::IfAbsent),
    ] {
        let (mut spool, mut file) = SpooledFile::create_in(dir.path()).await.unwrap();
        let data = payload(PART + 900_000, hash.len() as u32);
        tokio::io::AsyncWriteExt::write_all(&mut file, &data)
            .await
            .unwrap();
        drop(file);
        spool.set_len(data.len() as u64);
        let path = spool.path().to_path_buf();

        // The first request fails after the sweep removed the file; the upload must still
        // send the data.
        mock.clear_log();
        mock.script([Scripted::SweepSpoolThen(dir.path().to_path_buf(), 500)]);
        let outcome = s3
            .put("t", hash, UploadBody::File(spool), meta(1, None), mode)
            .await
            .unwrap();
        assert_eq!(outcome, PutOutcome::Stored, "{hash}");
        assert!(!path.exists());
        assert_eq!(mock.signature_failures(), 0);
        assert_eq!(
            mock.object(&format!("t/{hash}")).unwrap().body.as_ref(),
            data.as_slice()
        );
        let log = mock.log();
        match mode {
            PutMode::Overwrite => {
                let ops: Vec<_> = log.iter().filter_map(LoggedRequest::multipart_op).collect();
                assert_eq!(ops, ["create", "create", "part", "part", "complete"]);
            }
            PutMode::IfAbsent => {
                let puts: Vec<_> = log.iter().filter(|r| r.method == Method::PUT).collect();
                assert_eq!(puts.len(), 2);
                assert!(puts.iter().all(|put| put.body_len == data.len()));
                assert!(
                    puts.iter()
                        .all(|put| put.headers.contains_key(header::IF_NONE_MATCH))
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Streamed (multipart) uploads
// ---------------------------------------------------------------------------------------------

/// Large uploads with a `Content-Length` go to S3 while they arrive: a multipart upload of 8 MiB
/// parts with at most `STORAGE_PARTS_IN_FLIGHT` parts in flight, nothing spooled to disk, completed
/// when the body is complete. The metadata (incl. `x-amz-meta-size`) is set when it starts.
#[tokio::test]
async fn large_uploads_stream_into_a_multipart_upload() {
    let mock = Mock::new();
    mock.with(|m| m.part_delay = Duration::from_millis(100));
    let s3: Arc<dyn Storage> = Arc::new(storage(S3StorageConfig {
        parts_in_flight: 2,
        ..config(&spawn_mock(&mock).await)
    }));
    let spool = tempfile::tempdir().unwrap();
    let state = tok_state(s3).with_server_options(ServerOptions {
        spool_dir: spool.path().to_path_buf(),
        ..ServerOptions::default()
    });
    let api = Api::serve(state).await.with_token("tok");

    let data = payload(2 * PART + 12_345, 7);
    let upload = {
        let request = api
            .put("/v8/artifacts/bigstream?slug=acme")
            .header("x-artifact-duration", "77")
            .header("x-artifact-tag", "streamed")
            .body(data.clone());
        tokio::spawn(async move { request.fetch().await.status() })
    };
    eventually(|| mock.with(|m| m.uploads.values().any(|u| !u.parts.is_empty()))).await;
    assert_eq!(
        std::fs::read_dir(spool.path()).unwrap().count(),
        0,
        "nothing is spooled while streaming"
    );
    assert_eq!(upload.await.unwrap(), reqwest::StatusCode::ACCEPTED);

    assert_eq!(
        mock.multipart_ops(),
        ["create", "part", "part", "part", "complete"]
    );
    let parts: Vec<usize> = mock
        .log()
        .iter()
        .filter(|r| r.multipart_op() == Some("part"))
        .map(|r| r.body_len)
        .collect();
    assert_eq!(parts, [PART, PART, 12_345]);
    assert_eq!(mock.with(|m| m.max_parts_in_flight), 2);
    let object = mock.object("acme/bigstream").unwrap();
    assert_eq!(object.body.as_ref(), data.as_slice());
    assert_eq!(
        object.meta("x-amz-meta-size"),
        Some(data.len().to_string().as_str())
    );
    assert_eq!(object.meta("x-amz-meta-duration"), Some("77"));
    assert_eq!(object.meta("x-amz-meta-tag"), Some("streamed"));
    assert_eq!(mock.signature_failures(), 0);

    let resp = api.get("/v8/artifacts/bigstream?slug=acme").fetch().await;
    assert_eq!(resp.headers()["x-artifact-tag"], "streamed");
    assert_eq!(resp.bytes().await.unwrap().as_ref(), data.as_slice());
}

/// Uploads above `UPLOAD_MEMORY_THRESHOLD` that fit into one part are one `PUT` from memory;
/// chunked uploads (no `Content-Length`) larger than one part are spooled first, then sent as a
/// multipart upload from the spool file; immutable uploads are always one conditional `PUT`.
#[tokio::test]
async fn upload_strategy_by_size_length_and_mode() {
    let mock = Mock::new();
    let s3: Arc<dyn Storage> = Arc::new(storage(config(&spawn_mock(&mock).await)));
    let spool = tempfile::tempdir().unwrap();
    let options = ServerOptions {
        spool_dir: spool.path().to_path_buf(),
        ..ServerOptions::default()
    };
    let api = Api::serve(tok_state(s3.clone()).with_server_options(options.clone()))
        .await
        .with_token("tok");

    // One part (6 MiB > 4 MiB threshold).
    let one_part = payload(6 * 1024 * 1024, 1);
    let resp = api
        .put("/v8/artifacts/onepart")
        .body(one_part.clone())
        .fetch()
        .await;
    assert_eq!(resp.status(), reqwest::StatusCode::ACCEPTED);
    assert!(mock.multipart_ops().is_empty());
    assert_eq!(count(&mock.log(), &Method::PUT, "/default/onepart"), 1);
    assert_eq!(
        mock.object("default/onepart").unwrap().body.len(),
        one_part.len()
    );

    // Unknown length, larger than a part.
    mock.clear_log();
    let chunked = payload(PART + 4096, 2);
    let chunks: Vec<std::io::Result<Vec<u8>>> =
        chunked.chunks(64 * 1024).map(|c| Ok(c.to_vec())).collect();
    let resp = api
        .put("/v8/artifacts/chunked")
        .body(reqwest::Body::wrap_stream(futures_util::stream::iter(
            chunks,
        )))
        .fetch()
        .await;
    assert_eq!(resp.status(), reqwest::StatusCode::ACCEPTED);
    assert_eq!(mock.multipart_ops(), ["create", "part", "part", "complete"]);
    let object = mock.object("default/chunked").unwrap();
    assert_eq!(object.body.as_ref(), chunked.as_slice());
    assert_eq!(
        object.meta("x-amz-meta-size"),
        Some(chunked.len().to_string().as_str())
    );
    assert_eq!(std::fs::read_dir(spool.path()).unwrap().count(), 0);

    // Immutable: one conditional PUT, also above one part (spooled, then mapped).
    mock.clear_log();
    let immutable = Api::serve(
        tok_state(s3)
            .with_server_options(options)
            .with_immutable_artifacts(true),
    )
    .await
    .with_token("tok");
    let big = payload(PART + 1, 3);
    let resp = immutable
        .put("/v8/artifacts/once")
        .body(big.clone())
        .fetch()
        .await;
    assert_eq!(resp.status(), reqwest::StatusCode::ACCEPTED);
    let resp = immutable
        .put("/v8/artifacts/once")
        .body(big.clone())
        .fetch()
        .await;
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);
    assert!(mock.multipart_ops().is_empty());
    let puts: Vec<_> = mock
        .log()
        .into_iter()
        .filter(|r| r.method == Method::PUT)
        .collect();
    assert_eq!(puts.len(), 2);
    assert!(
        puts.iter()
            .all(|p| p.headers[header::IF_NONE_MATCH] == "*" && p.body_len == big.len())
    );
    assert_eq!(
        mock.object("default/once").unwrap().body.as_ref(),
        big.as_slice()
    );
    assert_eq!(std::fs::read_dir(spool.path()).unwrap().count(), 0);
}

/// Body errors, idle timeouts, short bodies and size violations while streaming abort the
/// multipart upload: no parts stay behind and no object becomes visible.
#[tokio::test]
async fn failed_streamed_uploads_are_aborted() {
    let mock = Mock::new();
    let s3 = storage(config(&spawn_mock(&mock).await));
    let dir = tempfile::tempdir().unwrap();
    let data = payload(PART + 100_000, 4);
    let len = data.len() as u64 + 1000;

    type Case = (&'static str, Body, UploadLimits, fn(&AppError) -> bool);
    let cases: Vec<Case> = vec![
        (
            "client error",
            body_then(
                data.clone(),
                futures_util::stream::iter([Err(std::io::Error::other("client went away"))]),
            ),
            limits(dir.path()),
            |e| matches!(e, AppError::BadRequest(_)),
        ),
        (
            "idle timeout",
            body_then(data.clone(), futures_util::stream::pending()),
            UploadLimits {
                idle_timeout: Duration::from_millis(300),
                ..limits(dir.path())
            },
            |e| matches!(e, AppError::RequestTimeout(m) if m.contains("idle")),
        ),
        (
            "total timeout",
            body_then(data.clone(), futures_util::stream::pending()),
            UploadLimits {
                total_timeout: Duration::from_millis(500),
                ..limits(dir.path())
            },
            |e| matches!(e, AppError::RequestTimeout(m) if m.contains("total")),
        ),
        (
            "short body",
            body_then(data.clone(), futures_util::stream::empty()),
            limits(dir.path()),
            |e| matches!(e, AppError::BadRequest(m) if m.contains("Content-Length")),
        ),
    ];
    for (case, body, limits, expected) in cases {
        mock.clear_log();
        let upload = UploadStream::new(body, Some(len), &limits).unwrap();
        let err = s3
            .put_stream("t", "failed", upload, meta(1, None))
            .await
            .unwrap_err();
        assert!(expected(&err), "{case}: {err:?}");
        let ops = mock.multipart_ops();
        assert_eq!(ops.first(), Some(&"create"), "{case}");
        assert_eq!(ops.last(), Some(&"abort"), "{case}: {ops:?}");
        assert!(!ops.contains(&"complete"), "{case}");
        assert!(mock.with(|m| m.uploads.is_empty()), "{case}");
        assert!(mock.object("t/failed").is_none(), "{case}");
    }
    assert_eq!(mock.with(|m| m.aborted_uploads), 4);
}

/// A client that stops sending a streamed upload gets 408 after the body idle timeout (counted as
/// an upload timeout, not as a storage error), and the multipart upload is aborted.
#[tokio::test]
async fn stalled_streamed_upload_times_out_through_the_routes() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mock = Mock::new();
    let s3: Arc<dyn Storage> = Arc::new(storage(config(&spawn_mock(&mock).await)));
    let state = tok_state(s3).with_server_options(ServerOptions {
        body_idle_timeout: Duration::from_millis(300),
        ..ServerOptions::default()
    });
    let api = Api::serve(state).await.with_token("tok");

    let mut client = tokio::net::TcpStream::connect(api.addr()).await.unwrap();
    let head = format!(
        "PUT /v8/artifacts/stalled HTTP/1.1\r\nHost: cache\r\nAuthorization: Bearer tok\r\n\
         Content-Length: {}\r\n\r\n",
        2 * PART
    );
    client.write_all(head.as_bytes()).await.unwrap();
    client.write_all(&payload(PART + 1000, 8)).await.unwrap();
    let mut response = vec![0u8; 1024];
    let n = tokio::time::timeout(Duration::from_secs(5), client.read(&mut response))
        .await
        .unwrap()
        .unwrap();
    let response = String::from_utf8_lossy(&response[..n]);
    assert!(response.starts_with("HTTP/1.1 408"), "{response}");

    eventually(|| mock.with(|m| m.aborted_uploads == 1)).await;
    assert!(mock.object("default/stalled").is_none());
    let metrics = api.get("/metrics").fetch().await.text().await.unwrap();
    for line in [
        "turbo_cache_upload_rejections_total{backend=\"s3\",reason=\"timeout\"} 1",
        "turbo_cache_storage_errors_total{backend=\"s3\",operation=\"put\"} 0",
    ] {
        assert!(metrics.contains(line), "{line} missing:\n{metrics}");
    }
}

/// The total body timeout counts only the time spent waiting for the client: an S3 that needs
/// longer than the timeout to accept the parts of a streamed upload causes no 408 (which would be
/// blamed on the client as an upload timeout) while the client sends at a steady pace.
#[tokio::test]
async fn slow_s3_parts_do_not_count_against_the_total_body_timeout() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mock = Mock::new();
    mock.with(|m| m.part_delay = Duration::from_millis(600));
    // One part in flight: the part uploads (not the client) set the pace.
    let s3: Arc<dyn Storage> = Arc::new(storage(S3StorageConfig {
        parts_in_flight: 1,
        ..config(&spawn_mock(&mock).await)
    }));
    let state = tok_state(s3).with_server_options(ServerOptions {
        body_total_timeout: Duration::from_millis(900),
        ..ServerOptions::default()
    });
    let api = Api::serve(state).await.with_token("tok");

    let data = payload(4 * PART + 1000, 9);
    let mut client = tokio::net::TcpStream::connect(api.addr()).await.unwrap();
    let head = format!(
        "PUT /v8/artifacts/slows3 HTTP/1.1\r\nHost: cache\r\nAuthorization: Bearer tok\r\n\
         Content-Length: {}\r\n\r\n",
        data.len()
    );
    client.write_all(head.as_bytes()).await.unwrap();
    let started = Instant::now();
    // About 100ms of sending per part: the server waits for body data in every part, also
    // after the timeout has passed since the request started (from the 4th part on).
    for chunk in data.chunks(256 * 1024) {
        client.write_all(chunk).await.unwrap();
        tokio::time::sleep(Duration::from_millis(3)).await;
    }
    let mut response = vec![0u8; 1024];
    let n = tokio::time::timeout(Duration::from_secs(10), client.read(&mut response))
        .await
        .unwrap()
        .unwrap();
    let response = String::from_utf8_lossy(&response[..n]);
    assert!(response.starts_with("HTTP/1.1 202"), "{response}");
    // Four parts were delayed by 600ms each: longer than the total body timeout.
    assert!(started.elapsed() > Duration::from_millis(2400));
    assert_eq!(mock.object("default/slows3").unwrap().body, data);
    let metrics = api.get("/metrics").fetch().await.text().await.unwrap();
    assert!(!metrics.contains("reason=\"timeout\"} 1"), "{metrics}");
}

/// A streamed upload whose request is cancelled (client disconnect, shutdown) is aborted in the
/// background.
#[tokio::test]
async fn cancelled_streamed_upload_is_aborted() {
    let mock = Mock::new();
    let s3 = storage(config(&spawn_mock(&mock).await));
    let dir = tempfile::tempdir().unwrap();
    let data = payload(2 * PART, 5);
    let upload = UploadStream::new(
        body_then(data, futures_util::stream::pending()),
        Some(3 * PART as u64),
        &limits(dir.path()),
    )
    .unwrap();
    let put = s3.put_stream("t", "cancelled", upload, meta(1, None));
    let started = tokio::time::timeout(Duration::from_secs(2), put).await;
    assert!(started.is_err(), "the upload waits for more body data");
    eventually(|| mock.with(|m| m.aborted_uploads == 1)).await;
    assert!(mock.with(|m| m.uploads.is_empty()));
    assert!(mock.object("t/cancelled").is_none());
    assert!(!mock.multipart_ops().contains(&"complete"));
}

/// Memory per streamed upload is bounded: the body is read at most `STORAGE_PARTS_IN_FLIGHT + 1`
/// parts ahead of what S3 has stored, however slow the part uploads are.
#[tokio::test]
async fn streamed_upload_memory_is_bounded() {
    const IN_FLIGHT: usize = 3;
    let mock = Mock::new();
    mock.with(|m| m.part_delay = Duration::from_millis(150));
    let s3 = storage(S3StorageConfig {
        parts_in_flight: IN_FLIGHT,
        ..config(&spawn_mock(&mock).await)
    });
    let dir = tempfile::tempdir().unwrap();

    const CHUNK: usize = 256 * 1024;
    let total = 6 * PART;
    let pulled = Arc::new(AtomicU64::new(0));
    let max_ahead = Arc::new(AtomicU64::new(0));
    let body = {
        let (mock, pulled, max_ahead) = (mock.clone(), pulled.clone(), max_ahead.clone());
        futures_util::stream::iter(0..total / CHUNK).map(move |i| {
            let stored: usize = mock.with(|m| {
                m.uploads
                    .values()
                    .map(|u| u.parts.values().map(Bytes::len).sum::<usize>())
                    .sum()
            });
            let before = pulled.fetch_add(CHUNK as u64, Ordering::SeqCst);
            max_ahead.fetch_max(before.saturating_sub(stored as u64), Ordering::SeqCst);
            Ok::<_, std::io::Error>(Bytes::from(vec![i as u8; CHUNK]))
        })
    };
    let upload = UploadStream::new(
        Body::from_stream(body),
        Some(total as u64),
        &limits(dir.path()),
    )
    .unwrap();
    s3.put_stream("t", "bounded", upload, meta(1, None))
        .await
        .unwrap();
    assert_eq!(mock.object("t/bounded").unwrap().body.len(), total);
    let ahead = max_ahead.load(Ordering::SeqCst);
    assert!(
        ahead <= ((IN_FLIGHT + 1) * PART) as u64,
        "read {ahead} bytes ahead of S3"
    );
    assert!(ahead > PART as u64, "receiving overlaps the part uploads");
    assert_eq!(mock.with(|m| m.max_parts_in_flight), IN_FLIGHT);
}

// ---------------------------------------------------------------------------------------------
// Misses, missing buckets, deletes
// ---------------------------------------------------------------------------------------------

/// A 403 for a missing object (no `s3:ListBucket`) is an error by default and a miss with
/// `treat_403_as_miss`; signature problems are never misses. `HEAD` errors carry no body (no S3
/// error code), so a bare `HEAD` 403 is re-checked with a ranged `GET`.
#[tokio::test]
async fn forbidden_missing_objects_are_misses_only_with_treat_403_as_miss() {
    let mock = Mock::new();
    mock.with(|m| m.missing_as_403 = true);
    let endpoint = spawn_mock(&mock).await;

    let strict = storage(config(&endpoint));
    assert!(strict.get("t", "missing").await.is_err());
    assert!(strict.head("t", "missing").await.is_err());

    let mut cfg = config(&endpoint);
    cfg.treat_403_as_miss = true;
    let lenient = storage(cfg.clone());
    assert!(lenient.get("t", "missing").await.unwrap().is_none());
    mock.clear_log();
    assert!(lenient.head("t", "missing").await.unwrap().is_none());
    let methods: Vec<Method> = mock.log().into_iter().map(|r| r.method).collect();
    assert_eq!(methods, vec![Method::HEAD, Method::GET]);

    // Existing objects still need a single HEAD.
    put(&lenient, "t", "present", b"x", meta(1, None)).await;
    mock.clear_log();
    assert!(lenient.head("t", "present").await.unwrap().is_some());
    assert_eq!(mock.log().len(), 1);

    cfg.secret_access_key = "wrong-secret".to_string();
    let wrong = storage(cfg);
    assert!(wrong.get("t", "missing").await.is_err());
    let err = wrong.head("t", "missing").await.unwrap_err().to_string();
    assert!(err.contains("SignatureDoesNotMatch"), "{err}");
}

/// A missing bucket (`NoSuchBucket`) is a storage error, never a miss. A bare `HEAD` 404 (no
/// body, no error-code header, as sent by AWS and RustFS) is re-checked with a ranged GET unless
/// the bucket was seen recently, at the cost of at most one extra request per minute.
#[tokio::test]
async fn missing_bucket_is_an_error_not_a_miss() {
    let mock = Mock::new();
    mock.with(|m| m.no_such_bucket = true);
    let endpoint = spawn_mock(&mock).await;
    let s3 = storage(config(&endpoint));
    let err = s3.get("t", "missing").await.err().unwrap().to_string();
    assert!(err.contains("NoSuchBucket"), "{err}");
    let err = s3.head("t", "missing").await.err().unwrap().to_string();
    assert!(err.contains("NoSuchBucket"), "{err}");
    let err = s3.head("t", "missing").await.err().unwrap().to_string();
    assert!(
        err.contains("NoSuchBucket"),
        "missing bucket is never cached as seen: {err}"
    );

    mock.with(|m| m.no_such_bucket = false);
    mock.clear_log();
    assert!(s3.head("t", "missing").await.unwrap().is_none());
    let log = mock.log();
    assert_eq!(count(&log, &Method::HEAD, "/t/missing"), 1);
    assert_eq!(
        count(&log, &Method::GET, "/t/missing"),
        1,
        "one verification GET"
    );

    // The NoSuchKey answer confirmed the bucket: further misses cost one HEAD each.
    mock.clear_log();
    for _ in 0..3 {
        assert!(s3.head("t", "missing").await.unwrap().is_none());
    }
    let log = mock.log();
    assert_eq!(count(&log, &Method::HEAD, "/t/missing"), 3);
    assert_eq!(count(&log, &Method::GET, "/t/missing"), 0);

    // Any successful request also confirms the bucket.
    let s3 = storage(config(&endpoint));
    put(&s3, "t", "x", b"x", meta(1, None)).await;
    mock.clear_log();
    assert!(s3.head("t", "missing").await.unwrap().is_none());
    assert_eq!(count(&mock.log(), &Method::GET, "/t/missing"), 0);
}

/// S3 GETs report the exact `Content-Length`; a `HEAD` without `Content-Length` (stripped
/// by a proxy) is an error instead of a bogus size of 0, also through the routes.
#[tokio::test]
async fn sizes_come_from_content_length() {
    let mock = Mock::new();
    let s3 = storage(config(&spawn_mock(&mock).await));
    put(&s3, "acme", "len", b"0123456789abcdef", meta(1, None)).await;

    let known = s3.get("acme", "len").await.unwrap().unwrap();
    assert_eq!(known.content_length, Some(16));
    assert_eq!(known.metadata.size, 16);
    assert_eq!(body_bytes(known).await.len(), 16);

    mock.with(|m| m.strip_head_length = true);
    assert!(s3.head("acme", "len").await.is_err());
    let api = Api::serve(tok_state(Arc::new(s3))).await.with_token("tok");
    let resp = api.head("/v8/artifacts/len?slug=acme").fetch().await;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_GATEWAY);
}

// ---------------------------------------------------------------------------------------------
// Retention
// ---------------------------------------------------------------------------------------------

/// The janitor deletes artifacts older than the max age, then the oldest (by `last_modified`)
/// while the rest exceeds the budget, with `DeleteObjects`. Foreign objects and the health check
/// key are never touched, however old.
#[tokio::test]
async fn cleanup_expires_and_evicts_oldest_artifacts_only() {
    let mock = Mock::new();
    let s3 = storage(S3StorageConfig {
        prefix: "ci".into(),
        health_check_key: Some("ci/health/probe".into()),
        ..config(&spawn_mock(&mock).await)
    });
    // The start of the minute `minutes` ago, so offsets below stay within one minute.
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let minute = |minutes: u64| UNIX_EPOCH + Duration::from_secs((now - minutes * 60) / 60 * 60);
    let seed = |key: &str, last_modified: SystemTime| {
        let mut object = StoredObject::new(&[0; 100], &[]);
        object.last_modified = last_modified;
        mock.with(|m| m.objects.insert(key.to_string(), object));
    };
    seed("ci/team/expired", minute(180));
    seed("ci/team/oldest", minute(90));
    seed("ci/team/older", minute(80) + Duration::from_secs(10));
    seed("ci/team/newer", minute(80) + Duration::from_secs(20));
    seed("ci/other/kept", minute(60));
    put(&s3, "team", "fresh", &[0; 100], meta(1, None)).await;
    let foreign = [
        "ci/health/probe",
        "ci/team/hash/extra",
        "ci/.hidden/hash",
        "ci/team/not.a.hash",
        "ci/readme",
        "other/team/hash",
    ];
    for key in foreign {
        seed(key, minute(600));
    }
    mock.clear_log();

    // 500 bytes are not expired; a budget of 300 evicts the oldest minute and, of the two in
    // the next one, the older.
    let options = RetentionOptions {
        max_bytes: Some(300),
        max_age: Some(Duration::from_secs(2 * 3600)),
        interval: Duration::from_secs(600),
    };
    let report = s3.object_storage().run_cleanup(&options).await.unwrap();
    assert_eq!(
        (
            report.listed,
            report.removed,
            report.removed_bytes,
            report.failed
        ),
        (6, 3, 300, 0)
    );
    assert_eq!(deletes(&mock.log()), 1);
    let mut keys: Vec<String> = mock.with(|m| m.objects.keys().cloned().collect());
    keys.sort();
    let mut expected: Vec<String> = ["ci/team/newer", "ci/other/kept", "ci/team/fresh"]
        .into_iter()
        .chain(foreign)
        .map(String::from)
        .collect();
    expected.sort();
    assert_eq!(keys, expected);

    let report = s3.object_storage().run_cleanup(&options).await.unwrap();
    assert_eq!((report.listed, report.removed), (3, 0));
}

// ---------------------------------------------------------------------------------------------
// Readiness
// ---------------------------------------------------------------------------------------------

/// The readiness probe cap also covers credential loading (hanging IMDS/STS/ECS endpoint).
#[tokio::test]
async fn health_check_bound_covers_credential_loading() {
    let (creds, _) = spawn_server(Router::new().fallback(|| async {
        tokio::time::sleep(Duration::from_secs(30)).await;
        "never"
    }))
    .await;
    let mock = Mock::new();
    let mut cfg = config(&spawn_mock(&mock).await);
    cfg.access_key_id.clear();
    cfg.secret_access_key.clear();
    cfg.connect_timeout = Duration::from_millis(500);
    cfg.read_timeout = Duration::from_millis(500);
    let full_uri = format!("{creds}/v2/credentials");
    let env =
        move |name: &str| (name == "AWS_CONTAINER_CREDENTIALS_FULL_URI").then(|| full_uri.clone());
    let s3 = S3Storage::with_env(cfg, &env).unwrap();

    let started = Instant::now();
    let err = s3.health_check().await.err().unwrap().to_string();
    let elapsed = started.elapsed();
    assert!(err.contains("timed out"), "{err}");
    assert!(elapsed < Duration::from_secs(3), "{elapsed:?}");
    assert!(mock.log().is_empty());
}

/// A credential endpoint answering 404 (e.g. an instance without a role) is a storage error, not
/// a miss: only the object request's own 404 is.
#[tokio::test]
async fn credential_endpoint_404_is_an_error_not_a_miss() {
    let (creds, _) = spawn_server(Router::new().fallback(|| async { StatusCode::NOT_FOUND })).await;
    let mock = Mock::new();
    let mut cfg = config(&spawn_mock(&mock).await);
    cfg.access_key_id.clear();
    cfg.secret_access_key.clear();
    let full_uri = format!("{creds}/v2/credentials");
    let env =
        move |name: &str| (name == "AWS_CONTAINER_CREDENTIALS_FULL_URI").then(|| full_uri.clone());
    let s3 = S3Storage::with_env(cfg, &env).unwrap();

    for err in [
        s3.get("t", "h").await.err().unwrap(),
        s3.head("t", "h").await.unwrap_err(),
        s3.delete("t", "h").await.unwrap_err(),
    ] {
        assert!(matches!(err, AppError::Storage(_)), "{err}");
    }
    assert!(mock.log().is_empty());
}

/// Readiness lists `STORAGE_PREFIX` (`max-keys=1`); with `health_check_key` it probes an object
/// (404 = healthy) instead, for policies that do not allow listing; a missing bucket stays
/// unhealthy.
#[tokio::test]
async fn health_check_key_supports_prefix_scoped_policies() {
    let mock = Mock::new();
    let endpoint = spawn_mock(&mock).await;

    let mut cfg = config(&endpoint);
    cfg.prefix = "ci".to_string();
    assert!(storage(cfg.clone()).health_check().await.is_ok());
    let probe = mock.log().pop().unwrap();
    assert_eq!(probe.method, Method::GET);
    assert_eq!(probe.raw_path, "/turbo-bucket");
    assert_eq!(probe.query, "list-type=2&prefix=ci%2F&max-keys=1");

    mock.with(|m| m.list_status = Some(403));
    assert!(storage(cfg.clone()).health_check().await.is_err());

    cfg.health_check_key = Some("ci/healthz".to_string());
    assert!(storage(cfg.clone()).health_check().await.is_ok());
    let probe = mock.log().pop().unwrap();
    assert_eq!(probe.method, Method::GET);
    assert_eq!(probe.raw_path, "/turbo-bucket/ci/healthz");
    assert_eq!(probe.headers[header::RANGE], "bytes=0-0");

    // Existing health object → 206 healthy; an empty one (416) too.
    for body in [&b"ok"[..], b""] {
        mock.with(|m| {
            m.objects
                .insert("ci/healthz".into(), StoredObject::new(body, &[]))
        });
        assert!(storage(cfg.clone()).health_check().await.is_ok());
    }

    mock.with(|m| m.no_such_bucket = true);
    assert!(storage(cfg.clone()).health_check().await.is_err());
    cfg.health_check_key = None;
    assert!(storage(cfg).health_check().await.is_err());
}

/// Failed readiness checks are cached for the same window as successes (and, with
/// `max_retries: 3`, not retried).
#[tokio::test]
async fn health_check_failures_are_cached() {
    let mock = Mock::new();
    mock.with(|m| m.list_status = Some(500));
    let s3 = storage(config(&spawn_mock(&mock).await));
    for _ in 0..5 {
        assert!(s3.health_check().await.is_err());
    }
    assert_eq!(mock.log().len(), 1, "one backend request per cache window");

    // Concurrent probes are also coalesced.
    let mock2 = Mock::new();
    mock2.with(|m| m.list_status = Some(503));
    let s3 = Arc::new(storage(config(&spawn_mock(&mock2).await)));
    let tasks: Vec<_> = (0..10)
        .map(|_| {
            let s3 = s3.clone();
            tokio::spawn(async move { s3.health_check().await.is_err() })
        })
        .collect();
    for t in tasks {
        assert!(t.await.unwrap());
    }
    assert_eq!(mock2.log().len(), 1);
}

/// Readiness probes are bounded (5s) even with a long read timeout, and callers arriving while a
/// probe is in flight get the previous result instead of queueing behind the probe.
#[tokio::test]
async fn health_check_is_bounded_and_does_not_queue() {
    let mock = Mock::new();
    let mut cfg = config(&spawn_mock(&mock).await);
    cfg.read_timeout = Duration::from_secs(60);
    let s3 = Arc::new(storage(cfg));

    // A hanging backend: the first-ever probe fails at the 5s probe cap, not after 62s.
    mock.with(|m| m.list_delay = Duration::from_secs(8));
    let started = Instant::now();
    assert!(s3.health_check().await.is_err());
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(4500) && elapsed < Duration::from_secs(7),
        "{elapsed:?}"
    );

    // Let the cached failure expire, then start a slow (successful) probe.
    tokio::time::sleep(Duration::from_millis(5100)).await;
    mock.with(|m| m.list_delay = Duration::from_secs(2));
    let probe = {
        let s3 = s3.clone();
        tokio::spawn(async move { s3.health_check().await.is_ok() })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    let started = Instant::now();
    assert!(
        s3.health_check().await.is_err(),
        "previous result while probing"
    );
    assert!(started.elapsed() < Duration::from_millis(200));
    assert!(probe.await.unwrap());
    assert!(s3.health_check().await.is_ok(), "fresh result cached");
}

// ---------------------------------------------------------------------------------------------
// HTTP client policy and credentials
// ---------------------------------------------------------------------------------------------

/// Redirects are not followed (they could leak signed headers / session tokens).
#[tokio::test]
async fn redirects_are_not_followed() {
    let target_hits = Arc::new(AtomicUsize::new(0));
    let hits = target_hits.clone();
    let (target, _) = spawn_server(Router::new().fallback(move || {
        let hits = hits.clone();
        async move {
            hits.fetch_add(1, Ordering::SeqCst);
            StatusCode::OK
        }
    }))
    .await;

    let mock = Mock::new();
    let s3 = storage(config(&spawn_mock(&mock).await));
    mock.script([Scripted::Redirect(307, format!("{target}/elsewhere"))]);
    assert!(s3.get("t", "redir").await.is_err());
    mock.script([Scripted::Redirect(301, format!("{target}/elsewhere"))]);
    assert!(
        s3.put(
            "t",
            "redir",
            bytes_body(b"x"),
            meta(1, None),
            PutMode::Overwrite
        )
        .await
        .is_err()
    );
    assert_eq!(target_hits.load(Ordering::SeqCst), 0);
}

/// JSON credentials as served by the ECS/EKS container endpoint and IMDS.
fn json_credentials(n: usize, expires_in: chrono::Duration) -> String {
    let exp = (chrono::Utc::now() + expires_in).to_rfc3339();
    format!(
        r#"{{"Code":"Success","AccessKeyId":"ASIATEMP{n}","SecretAccessKey":"temp-secret-{n}","Token":"temp-token","Expiration":"{exp}"}}"#
    )
}

/// A mock S3 that accepts the temporary credentials `ASIATEMP1..=8` with session token
/// `temp-token`.
async fn mock_for_temporary_credentials() -> (Mock, S3StorageConfig) {
    let mock = Mock::new();
    mock.with(|m| {
        m.secrets.clear();
        for n in 1..=8 {
            m.secrets
                .insert(format!("ASIATEMP{n}"), format!("temp-secret-{n}"));
        }
        m.required_token = Some("temp-token".into());
    });
    let mut cfg = config(&spawn_mock(&mock).await);
    cfg.access_key_id.clear();
    cfg.secret_access_key.clear();
    (mock, cfg)
}

/// The access key that signed `request`.
fn signing_key(request: &LoggedRequest) -> String {
    let auth = request.headers[header::AUTHORIZATION].to_str().unwrap();
    let credential = auth.split("Credential=").nth(1).unwrap();
    credential.split('/').next().unwrap().to_string()
}

fn env_from(pairs: Vec<(&'static str, String)>) -> impl Fn(&str) -> Option<String> {
    move |name| {
        pairs
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, v)| v.clone())
    }
}

/// Without static keys the container credential provider (ECS / EKS Pod Identity) is used:
/// requests carry the temporary access key and a signed session token, the authorization token
/// (from `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE` or `AWS_CONTAINER_AUTHORIZATION_TOKEN`) is
/// sent to the endpoint, credentials are cached and refreshed before they expire. Token files
/// may end with a newline and are re-read when rotated; a rotated token that cannot be sent keeps
/// the previous one (`object_store` itself would panic building the request).
#[tokio::test]
async fn container_credentials_are_used_and_refreshed() {
    #[derive(Clone, Default)]
    struct Endpoint {
        hits: Arc<AtomicUsize>,
        auth: Arc<std::sync::Mutex<Vec<Option<String>>>>,
        expires_in_mins: Arc<AtomicU64>,
    }
    let endpoint = Endpoint::default();
    endpoint.expires_in_mins.store(60, Ordering::SeqCst);
    let (creds, _) = spawn_server(Router::new().fallback({
        let endpoint = endpoint.clone();
        move |headers: HeaderMap| {
            let endpoint = endpoint.clone();
            async move {
                let n = endpoint.hits.fetch_add(1, Ordering::SeqCst) + 1;
                endpoint.auth.lock().unwrap().push(
                    headers
                        .get(header::AUTHORIZATION)
                        .map(|v| v.to_str().unwrap().to_string()),
                );
                let mins = endpoint.expires_in_mins.load(Ordering::SeqCst) as i64;
                json_credentials(n, chrono::Duration::minutes(mins))
            }
        }
    }))
    .await;
    let full_uri = format!("{creds}/v2/credentials");

    let (mock, cfg) = mock_for_temporary_credentials().await;
    let token_dir = tempfile::tempdir().unwrap();
    let token_file = token_dir.path().join("token");
    std::fs::write(&token_file, "pod-identity-token\n").unwrap();
    let env = env_from(vec![
        ("AWS_CONTAINER_CREDENTIALS_FULL_URI", full_uri.clone()),
        (
            "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
            token_file.to_str().unwrap().to_string(),
        ),
    ]);
    let s3 = S3Storage::with_env(cfg.clone(), &env).unwrap();
    assert_eq!(s3.credential_source(), "ecs-container");

    put(&s3, "t", "tmp", b"temp", meta(1, None)).await;
    assert!(s3.get("t", "tmp").await.unwrap().is_some());
    assert!(s3.head("t", "tmp").await.unwrap().is_some());
    assert_eq!(
        endpoint.hits.load(Ordering::SeqCst),
        1,
        "credentials are cached"
    );
    assert_eq!(mock.signature_failures(), 0);
    let first = &mock.log()[0];
    assert_eq!(signing_key(first), "ASIATEMP1");
    let auth = first.headers[header::AUTHORIZATION].to_str().unwrap();
    assert!(auth.contains("x-amz-security-token"), "{auth}");

    // Credentials that expire within 5 minutes are refreshed before the next request.
    endpoint.expires_in_mins.store(3, Ordering::SeqCst);
    let s3 = S3Storage::with_env(cfg.clone(), &env).unwrap();
    mock.clear_log();
    for expected in ["ASIATEMP2", "ASIATEMP3"] {
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(s3.get("t", "tmp").await.unwrap().is_some());
        assert_eq!(signing_key(&mock.log().pop().unwrap()), expected);
    }
    assert_eq!(
        endpoint.auth.lock().unwrap().as_slice(),
        vec![Some("pod-identity-token".to_string()); 3]
    );

    // Rotation: the new token is used once the file is re-read (at most every second).
    let last_auth = || endpoint.auth.lock().unwrap().last().cloned().flatten();
    std::fs::write(&token_file, "rotated-token\r\n").unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(s3.get("t", "tmp").await.unwrap().is_some());
    assert_eq!(last_auth().as_deref(), Some("rotated-token"));
    // An invalid or missing token file keeps the previous token.
    std::fs::write(&token_file, "two\nlines").unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(s3.get("t", "tmp").await.unwrap().is_some());
    std::fs::remove_file(&token_file).unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(s3.get("t", "tmp").await.unwrap().is_some());
    let auth = endpoint.auth.lock().unwrap().clone();
    assert_eq!(
        auth[auth.len() - 3..],
        vec![Some("rotated-token".to_string()); 3]
    );

    // An inline token is sent too; without any token no `Authorization` header is sent.
    endpoint.expires_in_mins.store(60, Ordering::SeqCst);
    for token in [Some("inline-token"), None] {
        let mut pairs = vec![("AWS_CONTAINER_CREDENTIALS_FULL_URI", full_uri.clone())];
        if let Some(token) = token {
            pairs.push(("AWS_CONTAINER_AUTHORIZATION_TOKEN", token.to_string()));
        }
        let s3 = S3Storage::with_env(cfg.clone(), &env_from(pairs)).unwrap();
        assert!(s3.get("t", "tmp").await.unwrap().is_some());
        assert_eq!(
            endpoint.auth.lock().unwrap().last().unwrap().as_deref(),
            token
        );
    }
    assert_eq!(mock.signature_failures(), 0);
}

/// Web identity (EKS IRSA): the token file is exchanged at STS (`AssumeRoleWithWebIdentity`,
/// `AWS_ENDPOINT_URL_STS`) and the session credentials sign the S3 requests.
#[tokio::test]
async fn web_identity_credentials_come_from_sts() {
    let calls: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
    let (sts, _) = spawn_server(Router::new().fallback({
        let calls = calls.clone();
        move |method: Method, uri: axum::http::Uri| {
            let calls = calls.clone();
            async move {
                calls
                    .lock()
                    .unwrap()
                    .push(format!("{method} {}", uri.query().unwrap_or("")));
                let exp = (chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc3339();
                format!(
                    "<AssumeRoleWithWebIdentityResponse><AssumeRoleWithWebIdentityResult><Credentials>\
                     <SessionToken>temp-token</SessionToken><SecretAccessKey>temp-secret-5</SecretAccessKey>\
                     <Expiration>{exp}</Expiration><AccessKeyId>ASIATEMP5</AccessKeyId>\
                     </Credentials></AssumeRoleWithWebIdentityResult></AssumeRoleWithWebIdentityResponse>"
                )
            }
        }
    }))
    .await;
    let (mock, cfg) = mock_for_temporary_credentials().await;
    let dir = tempfile::tempdir().unwrap();
    let token_file = dir.path().join("token");
    std::fs::write(&token_file, "eyJhbGciOi.jwt.token\n").unwrap();
    let env = env_from(vec![
        (
            "AWS_WEB_IDENTITY_TOKEN_FILE",
            token_file.to_str().unwrap().to_string(),
        ),
        (
            "AWS_ROLE_ARN",
            "arn:aws:iam::123456789012:role/turbo".into(),
        ),
        ("AWS_ROLE_SESSION_NAME", "turbo-session".into()),
        ("AWS_ENDPOINT_URL_STS", format!("{sts}/")),
    ]);
    let s3 = S3Storage::with_env(cfg, &env).unwrap();
    assert_eq!(s3.credential_source(), "web-identity");
    put(&s3, "t", "irsa", b"x", meta(1, None)).await;
    assert!(s3.get("t", "irsa").await.unwrap().is_some());
    assert_eq!(signing_key(&mock.log()[0]), "ASIATEMP5");
    assert_eq!(mock.signature_failures(), 0);
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 1, "{calls:?}");
    for part in [
        "POST ",
        "Action=AssumeRoleWithWebIdentity",
        "RoleArn=arn%3Aaws%3Aiam%3A%3A123456789012%3Arole%2Fturbo",
        "RoleSessionName=turbo-session",
    ] {
        assert!(calls[0].contains(part), "{part}: {}", calls[0]);
    }
    // Exactly the token: the trailing newline of the file is not sent.
    let token = calls[0]
        .split('&')
        .find(|p| p.starts_with("WebIdentityToken="));
    assert_eq!(
        token,
        Some("WebIdentityToken=eyJhbGciOi.jwt.token"),
        "{}",
        calls[0]
    );
}

/// EC2 instance metadata (IMDSv2: session token, role, credentials) via
/// `AWS_EC2_METADATA_SERVICE_ENDPOINT`, also for a non-AWS endpoint.
#[tokio::test]
async fn instance_metadata_credentials_use_imds_v2() {
    let calls: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
    let token_ok = |headers: &HeaderMap| {
        headers
            .get("x-aws-ec2-metadata-token")
            .is_some_and(|v| v == "imds-session-token")
    };
    let (imds, _) = spawn_server(Router::new().fallback({
        let calls = calls.clone();
        move |method: Method, uri: axum::http::Uri, headers: HeaderMap| {
            let calls = calls.clone();
            async move {
                let path = uri.path().to_string();
                calls.lock().unwrap().push(format!("{method} {path}"));
                match (method, path.as_str()) {
                    (Method::PUT, "/latest/api/token")
                        if headers.contains_key("x-aws-ec2-metadata-token-ttl-seconds") =>
                    {
                        "imds-session-token".into_response()
                    }
                    (Method::GET, "/latest/meta-data/iam/security-credentials/")
                        if token_ok(&headers) =>
                    {
                        "my-instance-role".into_response()
                    }
                    (
                        Method::GET,
                        "/latest/meta-data/iam/security-credentials/my-instance-role",
                    ) if token_ok(&headers) => {
                        json_credentials(6, chrono::Duration::hours(6)).into_response()
                    }
                    _ => StatusCode::UNAUTHORIZED.into_response(),
                }
            }
        }
    }))
    .await;
    let (mock, cfg) = mock_for_temporary_credentials().await;
    let env = env_from(vec![("AWS_EC2_METADATA_SERVICE_ENDPOINT", imds)]);
    let s3 = S3Storage::with_env(cfg, &env).unwrap();
    assert_eq!(s3.credential_source(), "ec2-imds");
    put(&s3, "t", "imds", b"x", meta(1, None)).await;
    assert_eq!(signing_key(&mock.log()[0]), "ASIATEMP6");
    assert_eq!(mock.signature_failures(), 0);
    assert_eq!(
        calls.lock().unwrap().as_slice(),
        [
            "PUT /latest/api/token",
            "GET /latest/meta-data/iam/security-credentials/",
            "GET /latest/meta-data/iam/security-credentials/my-instance-role"
        ]
    );
}

// ---------------------------------------------------------------------------------------------
// Live test against a real S3-compatible server (RustFS in CI; Garage, AWS, ...)
// ---------------------------------------------------------------------------------------------

/// Creates `bucket` (signed `PUT Bucket`); an existing bucket is fine.
async fn create_bucket(cfg: &S3StorageConfig) {
    use object_store::{
        aws::{AwsAuthorizer, AwsCredential},
        client::HttpRequestBody,
    };
    let url = format!("{}/{}", cfg.endpoint, cfg.bucket);
    let credential = AwsCredential {
        key_id: cfg.access_key_id.clone(),
        secret_key: cfg.secret_access_key.clone(),
        token: None,
    };
    let mut request = axum::http::Request::builder()
        .method(Method::PUT)
        .uri(&url)
        .body(HttpRequestBody::empty())
        .unwrap();
    AwsAuthorizer::new(&credential, "s3", &cfg.region).authorize(&mut request, None);
    let resp = reqwest::Client::new()
        .put(&url)
        .headers(request.headers().clone())
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success() || resp.status() == StatusCode::CONFLICT,
        "create bucket: {} {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );
}

/// Enabled with `TURBO_S3_LIVE_TEST=1`; otherwise it prints `skipping live S3 test` and passes.
/// Variables (defaults in brackets), the same names `.github/workflows/ci.yml` (`s3-live` job)
/// sets: `TURBO_S3_ENDPOINT` [http://127.0.0.1:9000], `TURBO_S3_BUCKET` [turbo-cache-live],
/// `TURBO_S3_ACCESS_KEY` [rustfsadmin], `TURBO_S3_SECRET_KEY` [rustfsadmin] (RustFS defaults),
/// `TURBO_S3_REGION` [us-east-1], `TURBO_S3_PATH_STYLE` [true], `TURBO_S3_CONDITIONAL` [true]
/// (false for stores that ignore `If-None-Match`). The bucket is created when missing (path
/// style). When it runs it prints `live S3 test against <endpoint>`, which CI checks for.
///
/// Run: `TURBO_S3_LIVE_TEST=1 cargo test --test it s3::live_s3_roundtrip -- --exact --nocapture`
#[tokio::test]
async fn live_s3_roundtrip() {
    if std::env::var("TURBO_S3_LIVE_TEST").ok().as_deref() != Some("1") {
        eprintln!("skipping live S3 test (set TURBO_S3_LIVE_TEST=1)");
        return;
    }
    let var = |name: &str, default: &str| {
        std::env::var(name)
            .ok()
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| default.into())
    };
    let bucket = var("TURBO_S3_BUCKET", "turbo-cache-live");
    let endpoint = var("TURBO_S3_ENDPOINT", "http://127.0.0.1:9000");
    eprintln!("live S3 test against {endpoint} bucket {bucket}");
    let base = S3StorageConfig {
        provider_name: "s3",
        bucket: bucket.clone(),
        region: var("TURBO_S3_REGION", "us-east-1"),
        endpoint: endpoint.trim_end_matches('/').to_string(),
        access_key_id: var("TURBO_S3_ACCESS_KEY", "rustfsadmin"),
        secret_access_key: var("TURBO_S3_SECRET_KEY", "rustfsadmin"),
        force_path_style: var("TURBO_S3_PATH_STYLE", "true") == "true",
        prefix: format!("live test/ünï €/{}", fastrand::u64(..)),
        max_retries: 2,
        ..S3StorageConfig::default()
    };
    let conditional = var("TURBO_S3_CONDITIONAL", "true") == "true";
    create_bucket(&base).await;

    for sign_payload in [false, true] {
        let mut cfg = base.clone();
        cfg.sign_payload = sign_payload;
        let s3 = storage(cfg);
        let hash = format!("hash_{sign_payload}");

        let mut metadata = meta(777, Some("live-tag+/=="));
        // Repeated spaces survive (SigV4 trimall collapses them only in the canonical form).
        metadata.client_ci = Some(Arc::from("github  actions   runner"));
        metadata.sha = Some(Arc::from("deadbeef"));
        let outcome = s3
            .put(
                "team-live",
                &hash,
                bytes_body(b"live payload"),
                metadata.clone(),
                PutMode::IfAbsent,
            )
            .await
            .unwrap();
        assert_eq!(outcome, PutOutcome::Stored);

        if conditional {
            let again = s3
                .put(
                    "team-live",
                    &hash,
                    bytes_body(b"other"),
                    meta(1, None),
                    PutMode::IfAbsent,
                )
                .await
                .unwrap();
            assert_eq!(again, PutOutcome::AlreadyExists);
        }

        let head = s3.head("team-live", &hash).await.unwrap().unwrap();
        assert_eq!(head.duration, 777);
        assert_eq!(head.size, 12);
        assert_eq!(head.tag.as_deref(), Some("live-tag+/=="));
        // Internal repeated spaces are signed in collapsed form but stored verbatim (some
        // stores normalize them; accept either form).
        let ci = head.client_ci.as_deref().unwrap();
        assert!(
            ci == "github  actions   runner" || ci == "github actions runner",
            "{ci:?}"
        );

        let got = s3.get("team-live", &hash).await.unwrap().unwrap();
        assert_eq!(got.content_length, Some(12));
        assert_eq!(body_bytes(got).await, b"live payload");

        // Spooled upload larger than one part: multipart upload from the file, overwrite mode.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spool");
        let big = payload(PART + 300_000, 9);
        std::fs::write(&path, &big).unwrap();
        s3.put(
            "team-live",
            &hash,
            UploadBody::File(SpooledFile::new(path, big.len() as u64)),
            meta(5, Some("t2")),
            PutMode::Overwrite,
        )
        .await
        .unwrap();
        let got = s3.get("team-live", &hash).await.unwrap().unwrap();
        assert_eq!(got.metadata.tag.as_deref(), Some("t2"));
        assert_eq!(got.metadata.size, big.len() as u64);
        assert_eq!(body_bytes(got).await, big);

        // Streamed upload (multipart while the body arrives).
        let streamed = payload(PART + 4321, 10);
        let upload = UploadStream::new(
            body_then(streamed.clone(), futures_util::stream::empty()),
            Some(streamed.len() as u64),
            &limits(dir.path()),
        )
        .unwrap();
        s3.put_stream("team-live", "streamed", upload, meta(6, Some("t3")))
            .await
            .unwrap();
        let got = s3.get("team-live", "streamed").await.unwrap().unwrap();
        assert_eq!(got.metadata.tag.as_deref(), Some("t3"));
        assert_eq!(body_bytes(got).await, streamed);
        assert!(s3.delete("team-live", "streamed").await.unwrap());

        // 404 → miss.
        assert!(
            s3.get("team-live", "does_not_exist")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            s3.head("team-live", "does_not_exist")
                .await
                .unwrap()
                .is_none()
        );

        assert!(s3.health_check().await.is_ok());
        assert!(s3.delete("team-live", &hash).await.unwrap());
        assert!(s3.get("team-live", &hash).await.unwrap().is_none());
        // Deleting a missing artifact reports "not found" (S3 answers 204 here too).
        assert!(!s3.delete("team-live", &hash).await.unwrap());
    }

    // Health check against an object key (404 = healthy).
    let mut cfg = base.clone();
    cfg.health_check_key = Some("healthz/missing-object".into());
    assert!(storage(cfg).health_check().await.is_ok());

    // 403 handling: bad credentials are errors even with treat_403_as_miss.
    let mut cfg = base.clone();
    cfg.secret_access_key = "definitely-wrong".into();
    cfg.treat_403_as_miss = true;
    cfg.max_retries = 0;
    let wrong = storage(cfg.clone());
    assert!(wrong.get("team-live", "x").await.is_err());
    assert!(wrong.head("team-live", "x").await.is_err());
    assert!(wrong.health_check().await.is_err());
    // Also with a health-check key, where a 403 could otherwise count as a missing object
    // (Garage answers a wrong secret with `403 AccessDenied`).
    cfg.health_check_key = Some("healthz/missing-object".into());
    assert!(storage(cfg).health_check().await.is_err());

    // Missing bucket → storage error, not a miss.
    let mut cfg = base.clone();
    cfg.bucket = format!("{bucket}-missing-{}", fastrand::u32(..));
    let missing = storage(cfg.clone());
    let err = missing
        .get("team-live", "x")
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(err.contains("NoSuchBucket"), "{err}");
    // HEAD too, although stores like RustFS/AWS send no error code on HEAD.
    let err = storage(cfg.clone())
        .head("team-live", "x")
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(err.contains("NoSuchBucket"), "{err}");
    assert!(missing.health_check().await.is_err());
    cfg.health_check_key = Some("healthz".into());
    assert!(storage(cfg).health_check().await.is_err());
}
