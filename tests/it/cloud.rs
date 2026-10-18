//! Live tests of the Google Cloud Storage and Azure Blob Storage backends against the services
//! or their emulators (fake-gcs-server, Azurite), enabled with `TURBO_GCS_LIVE_TEST=1` and
//! `TURBO_AZURE_LIVE_TEST=1` (see the tests for the variables); otherwise they pass without doing
//! anything. The logic these backends share with S3 (`storage::object`) is tested in `s3`;
//! `gcs_conditional_create_and_error_codes` checks the GCS error mapping against a mock.

use std::{sync::Arc, time::Duration};

use axum::{
    Router,
    body::Body,
    http::{HeaderMap, Method, StatusCode, Uri},
    response::IntoResponse,
};
use fast_turbo_cache_server::{
    AzureStorageConfig, GcsStorageConfig, PutMode, PutOutcome, SpooledFile, Storage, UploadBody,
    config::{RedactedUrl, Secret},
    models::ArtifactMetadata,
    storage::{
        azure, gcs,
        object::{MULTIPART_PART_SIZE, ObjectStorage},
    },
    upload::{UploadLimits, UploadStream},
};

use crate::support::{body_bytes, spawn_server};

const NS: &str = "team-live";

/// What a store (or its emulator) supports besides plain reads and writes.
struct Supports {
    metadata: bool,
    conditional_create: bool,
    multipart: bool,
}

fn var(name: &str, default: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.into())
}

fn live_prefix() -> String {
    format!("live test/ünï €/{}", fastrand::u64(..))
}

fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// Writes, reads, overwrites (multipart) and deletes artifacts; checks metadata, conditional
/// creates, misses and readiness.
async fn roundtrip(store: &ObjectStorage, supports: &Supports) {
    let metadata = ArtifactMetadata {
        duration: 777,
        tag: Some(Arc::from("live-tag+/==")),
        sha: Some(Arc::from("deadbeef")),
        dirty_hash: Some(Arc::from("dirty")),
        client_ci: Some(Arc::from("github actions")),
        ..Default::default()
    };
    let small = UploadBody::from(&b"live payload"[..]);
    let outcome = store
        .put(NS, "hash", small, metadata.clone(), PutMode::IfAbsent)
        .await
        .unwrap();
    assert_eq!(outcome, PutOutcome::Stored);
    if supports.conditional_create {
        let other = UploadBody::from(&b"other"[..]);
        let again = store
            .put(
                NS,
                "hash",
                other,
                ArtifactMetadata::default(),
                PutMode::IfAbsent,
            )
            .await
            .unwrap();
        assert_eq!(again, PutOutcome::AlreadyExists);
    }

    let head = store.head(NS, "hash").await.unwrap().unwrap();
    assert_eq!(head.size, 12);
    if supports.metadata {
        assert_eq!(
            head,
            ArtifactMetadata {
                size: 12,
                ..metadata
            }
        );
    }
    let got = store.get(NS, "hash").await.unwrap().unwrap();
    assert_eq!(got.content_length, Some(12));
    assert_eq!(body_bytes(got).await, b"live payload");

    if supports.multipart {
        let dir = tempfile::tempdir().unwrap();
        let tagged = |tag: &str| ArtifactMetadata {
            tag: Some(Arc::from(tag)),
            ..Default::default()
        };
        // Spooled upload larger than one part: multipart upload from the file.
        let spooled = payload(MULTIPART_PART_SIZE + 300_000);
        let path = dir.path().join("spool");
        std::fs::write(&path, &spooled).unwrap();
        let file = SpooledFile::new(path, spooled.len() as u64);
        store
            .put(NS, "hash", file.into(), tagged("t2"), PutMode::Overwrite)
            .await
            .unwrap();
        let got = store.get(NS, "hash").await.unwrap().unwrap();
        assert_eq!(got.metadata.tag.as_deref(), Some("t2"));
        assert_eq!(body_bytes(got).await, spooled);

        // Streamed upload (multipart while the body arrives).
        let streamed = payload(MULTIPART_PART_SIZE + 4321);
        let limits = UploadLimits {
            max_size: 1 << 30,
            memory_threshold: 4 * 1024 * 1024,
            idle_timeout: Duration::from_secs(5),
            total_timeout: Duration::from_secs(60),
            spool_dir: dir.path().to_path_buf(),
        };
        let len = Some(streamed.len() as u64);
        let body = UploadStream::new(Body::from(streamed.clone()), len, &limits).unwrap();
        store
            .put_stream(NS, "streamed", body, tagged("t3"))
            .await
            .unwrap();
        let got = store.get(NS, "streamed").await.unwrap().unwrap();
        assert_eq!(got.metadata.tag.as_deref(), Some("t3"));
        assert_eq!(body_bytes(got).await, streamed);

        // Concurrent uploads of the same artifact all succeed (Azure drops the uncommitted
        // blocks of the others when the first one commits).
        let uploads = (0..3).map(|_| {
            let body = UploadStream::new(Body::from(streamed.clone()), len, &limits).unwrap();
            store.put_stream(NS, "streamed", body, tagged("t3"))
        });
        for result in futures_util::future::join_all(uploads).await {
            assert_eq!(result.unwrap(), PutOutcome::Stored);
        }
        let got = store.get(NS, "streamed").await.unwrap().unwrap();
        assert_eq!(body_bytes(got).await, streamed);
        assert!(store.delete(NS, "streamed").await.unwrap());
    }

    assert!(store.get(NS, "does_not_exist").await.unwrap().is_none());
    assert!(store.head(NS, "does_not_exist").await.unwrap().is_none());
    store.health_check().await.unwrap();
    assert!(store.delete(NS, "hash").await.unwrap());
    assert!(store.get(NS, "hash").await.unwrap().is_none());
    assert!(!store.delete(NS, "hash").await.unwrap());
}

/// A missing bucket or container is a storage error (never a miss) for lookups, deletes and
/// readiness.
async fn assert_unavailable(store: &ObjectStorage, code: &str) {
    let err = store.delete(NS, "x").await.unwrap_err().to_string();
    assert!(err.contains(code), "{err}");
    let err = store.get(NS, "x").await.err().unwrap().to_string();
    assert!(err.contains(code), "{err}");
    let err = store.head(NS, "x").await.err().unwrap().to_string();
    assert!(err.contains(code), "{err}");
    assert!(store.health_check().await.is_err());
}

/// A service account key file that points the GCS client at `url` without OAuth (emulators).
fn emulator_key_file(url: &str) -> tempfile::NamedTempFile {
    let file = tempfile::NamedTempFile::new().unwrap();
    let key = serde_json::json!({
        "gcs_base_url": url.trim_end_matches('/'),
        "disable_oauth": true,
        "client_email": "",
        "private_key": "",
        "private_key_id": "",
    });
    std::fs::write(file.path(), key.to_string()).unwrap();
    file
}

/// GCS error handling against a mock of the XML API (always runs): a conditional create answered
/// with 412 is `AlreadyExists`, `NoSuchKey` is a miss and `NoSuchBucket` an error, also for the
/// body-less `HEAD`.
#[tokio::test]
async fn gcs_conditional_create_and_error_codes() {
    let (url, _) = spawn_server(Router::new().fallback(
        |method: Method, uri: Uri, headers: HeaderMap| async move {
            let error = |status: StatusCode, code: &str| {
                let body = format!("<Error><Code>{code}</Code><Message>m</Message></Error>");
                (status, body).into_response()
            };
            if method == Method::PUT {
                assert_eq!(headers["x-goog-if-generation-match"], "0");
                error(StatusCode::PRECONDITION_FAILED, "PreconditionFailed")
            } else if uri.path().starts_with("/gone/") {
                error(StatusCode::NOT_FOUND, "NoSuchBucket")
            } else {
                error(StatusCode::NOT_FOUND, "NoSuchKey")
            }
        },
    ))
    .await;
    let key = emulator_key_file(&url);
    let store = |bucket: &str| {
        gcs::new(GcsStorageConfig {
            bucket: bucket.into(),
            service_account_key_file: Some(key.path().to_path_buf()),
            max_retries: 0,
            ..GcsStorageConfig::default()
        })
        .unwrap()
    };

    let live = store("live");
    let body = UploadBody::from(&b"x"[..]);
    let outcome = live
        .put(
            NS,
            "h",
            body,
            ArtifactMetadata::default(),
            PutMode::IfAbsent,
        )
        .await
        .unwrap();
    assert_eq!(outcome, PutOutcome::AlreadyExists);
    assert!(live.head(NS, "h").await.unwrap().is_none());
    assert!(live.get(NS, "h").await.unwrap().is_none());
    assert!(!live.delete(NS, "h").await.unwrap());

    let gone = store("gone");
    for err in [
        gone.head(NS, "h").await.unwrap_err(),
        gone.get(NS, "h").await.err().unwrap(),
    ] {
        assert!(err.to_string().contains("NoSuchBucket"), "{err}");
    }
}

/// Enabled with `TURBO_GCS_LIVE_TEST=1`; variables (defaults in brackets): `TURBO_GCS_BUCKET`
/// [turbo-cache-live], `TURBO_GCS_KEY_FILE` (service account key file; Application Default
/// Credentials without it). With `TURBO_GCS_EMULATOR=<url>` (fake-gcs-server) the test uses a
/// key file pointing there and skips what the emulator does not implement: metadata,
/// preconditions, XML multipart uploads and XML error codes. The bucket must exist. When it runs
/// it prints `live GCS test against <bucket>`, which CI checks for.
///
/// Run: `TURBO_GCS_LIVE_TEST=1 cargo test --test it cloud::live_gcs_roundtrip -- --exact --nocapture`
#[tokio::test]
async fn live_gcs_roundtrip() {
    if std::env::var("TURBO_GCS_LIVE_TEST").ok().as_deref() != Some("1") {
        eprintln!("skipping live GCS test (set TURBO_GCS_LIVE_TEST=1)");
        return;
    }
    let bucket = var("TURBO_GCS_BUCKET", "turbo-cache-live");
    let emulator = std::env::var("TURBO_GCS_EMULATOR").ok();
    let emulator_key = emulator.as_deref().map(emulator_key_file);
    let key_file = match &emulator_key {
        Some(file) => Some(file.path().to_path_buf()),
        None => std::env::var_os("TURBO_GCS_KEY_FILE").map(Into::into),
    };
    eprintln!("live GCS test against {bucket}");
    let config = GcsStorageConfig {
        bucket: bucket.clone(),
        prefix: live_prefix(),
        service_account_key_file: key_file,
        application_credentials: std::env::var_os("GOOGLE_APPLICATION_CREDENTIALS").map(Into::into),
        max_retries: 2,
        ..GcsStorageConfig::default()
    };
    let real = emulator.is_none();
    let supports = Supports {
        metadata: real,
        conditional_create: real,
        multipart: real,
    };
    roundtrip(&gcs::new(config.clone()).unwrap(), &supports).await;

    if real {
        let missing = GcsStorageConfig {
            bucket: format!("{bucket}-missing-{}", fastrand::u32(..)),
            ..config
        };
        assert_unavailable(&gcs::new(missing).unwrap(), "NoSuchBucket").await;
    }
}

/// Enabled with `TURBO_AZURE_LIVE_TEST=1`; variables (defaults in brackets, Azurite's well-known
/// account): `TURBO_AZURE_ENDPOINT` [http://127.0.0.1:10000/devstoreaccount1],
/// `TURBO_AZURE_ACCOUNT` [devstoreaccount1], `TURBO_AZURE_ACCESS_KEY` [Azurite's key],
/// `TURBO_AZURE_CONTAINER` [turbo-cache-live]. The container must exist. When it runs it prints
/// `live Azure test against <endpoint> container <container>`, which CI checks for.
///
/// Run: `TURBO_AZURE_LIVE_TEST=1 cargo test --test it cloud::live_azure_roundtrip -- --exact --nocapture`
#[tokio::test]
async fn live_azure_roundtrip() {
    if std::env::var("TURBO_AZURE_LIVE_TEST").ok().as_deref() != Some("1") {
        eprintln!("skipping live Azure test (set TURBO_AZURE_LIVE_TEST=1)");
        return;
    }
    let endpoint = var(
        "TURBO_AZURE_ENDPOINT",
        "http://127.0.0.1:10000/devstoreaccount1",
    );
    let container = var("TURBO_AZURE_CONTAINER", "turbo-cache-live");
    eprintln!("live Azure test against {endpoint} container {container}");
    let config = AzureStorageConfig {
        account: var("TURBO_AZURE_ACCOUNT", "devstoreaccount1"),
        container: container.clone(),
        prefix: live_prefix(),
        endpoint: Some(RedactedUrl(endpoint)),
        access_key: Some(Secret(var(
            "TURBO_AZURE_ACCESS_KEY",
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
        ))),
        max_retries: 2,
        ..AzureStorageConfig::default()
    };
    let supports = Supports {
        metadata: true,
        conditional_create: true,
        multipart: true,
    };
    roundtrip(&azure::new(config.clone()).unwrap(), &supports).await;

    let missing = AzureStorageConfig {
        container: format!("{container}-missing-{}", fastrand::u32(..)),
        ..config.clone()
    };
    assert_unavailable(&azure::new(missing).unwrap(), "ContainerNotFound").await;

    // A wrong key is a storage error, never a miss.
    let wrong = azure::new(AzureStorageConfig {
        access_key: Some(Secret("d3Jvbmcta2V5".into())),
        max_retries: 0,
        ..config
    })
    .unwrap();
    assert!(wrong.get(NS, "x").await.is_err());
    assert!(wrong.head(NS, "x").await.is_err());
    assert!(wrong.health_check().await.is_err());
}
