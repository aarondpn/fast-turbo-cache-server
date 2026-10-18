//! Local filesystem backend: atomic publishing, corruption handling, janitor and on-disk layout.

use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use fast_turbo_cache_server::{
    AppError, LocalOptions, PutMode, PutOutcome, SpooledFile, Storage, UploadBody,
    models::ArtifactMetadata,
    storage::{ArtifactPayload, StoredArtifact, local::LocalStorage},
};
use futures_util::StreamExt;
use tokio::io::AsyncWriteExt;

use crate::support::{all_files, set_mtime};

fn meta(tag: &str) -> ArtifactMetadata {
    ArtifactMetadata {
        duration: 42,
        tag: Some(Arc::from(tag)),
        ..ArtifactMetadata::default()
    }
}

async fn read_body(artifact: StoredArtifact) -> Vec<u8> {
    match artifact.payload {
        ArtifactPayload::Bytes(b) => b.to_vec(),
        ArtifactPayload::Stream(mut s) => {
            let mut out = Vec::new();
            while let Some(chunk) = s.next().await {
                out.extend_from_slice(&chunk.unwrap());
            }
            out
        }
    }
}

async fn put_bytes(store: &LocalStorage, ns: &str, hash: &str, body: &[u8], tag: &str) {
    let outcome = store
        .put(
            ns,
            hash,
            UploadBody::from(body.to_vec()),
            meta(tag),
            PutMode::Overwrite,
        )
        .await
        .unwrap();
    assert_eq!(outcome, PutOutcome::Stored);
}

async fn spooled(dir: &Path, content: &[u8]) -> SpooledFile {
    tokio::fs::create_dir_all(dir).await.unwrap();
    let (mut spool, mut file) = SpooledFile::create_in(dir).await.unwrap();
    file.write_all(content).await.unwrap();
    file.flush().await.unwrap();
    spool.set_len(content.len() as u64);
    spool
}

fn temp_files(dir: &Path) -> Vec<PathBuf> {
    all_files(dir)
        .into_iter()
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with(".tcrs-"))
        })
        .collect()
}

/// Concurrent overwrites of one hash with different (body, tag) pairs are never observed as a
/// mismatched pair.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_overwrites_never_mix_body_and_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalStorage::new(dir.path()).await.unwrap());
    let writers = 8u64;
    let puts_per_writer = 25u64;
    let done = Arc::new(AtomicBool::new(false));
    let checked = Arc::new(AtomicU64::new(0));

    put_bytes(&store, "team", "racehash", b"w0-i0|", "w0-i0").await;

    let mut readers = Vec::new();
    for _ in 0..8 {
        let store = store.clone();
        let done = done.clone();
        let checked = checked.clone();
        readers.push(tokio::spawn(async move {
            while !done.load(Ordering::Relaxed) {
                if let Some(meta) = store.head("team", "racehash").await.unwrap() {
                    assert!(meta.tag.is_some(), "HEAD returned metadata without tag");
                }
                let artifact = store.get("team", "racehash").await.unwrap().unwrap();
                let tag = artifact.metadata.tag.clone().expect("tag must be present");
                let size = artifact.metadata.size;
                let body = read_body(artifact).await;
                assert_eq!(body.len() as u64, size, "size metadata matches body");
                let id = std::str::from_utf8(&body)
                    .unwrap()
                    .split('|')
                    .next()
                    .unwrap();
                assert_eq!(id, &*tag, "body/tag pair mismatch");
                checked.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    let mut handles = Vec::new();
    for w in 0..writers {
        let store = store.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..puts_per_writer {
                let id = format!("w{w}-i{i}");
                // Different body lengths so a mixed pair would also break the size check.
                let body = format!(
                    "{id}|{}",
                    "x".repeat(((w * 7919 + i * 104_729) % 20_000) as usize)
                );
                put_bytes(&store, "team", "racehash", body.as_bytes(), &id).await;
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    done.store(true, Ordering::Relaxed);
    for r in readers {
        r.await.unwrap();
    }
    assert!(checked.load(Ordering::Relaxed) > 0);
    assert!(temp_files(dir.path()).is_empty(), "no temp files leak");
}

/// Exactly one of many concurrent `IfAbsent` writers wins and the stored artifact is that
/// writer's (body, tag) pair, for in-memory and spooled bodies.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn if_absent_has_exactly_one_winner() {
    for spool in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalStorage::new(dir.path()).await.unwrap());
        let spool_dir = dir.path().join(".spool");
        for round in 0..10 {
            let hash = format!("immutable{round}");
            let mut handles = Vec::new();
            for w in 0..12 {
                let store = store.clone();
                let hash = hash.clone();
                let spool_dir = spool_dir.clone();
                handles.push(tokio::spawn(async move {
                    let id = format!("writer{w}");
                    let content = format!("{id}|payload");
                    let body = if spool {
                        UploadBody::File(spooled(&spool_dir, content.as_bytes()).await)
                    } else {
                        UploadBody::from(content.into_bytes())
                    };
                    let outcome = store
                        .put("team", &hash, body, meta(&id), PutMode::IfAbsent)
                        .await
                        .unwrap();
                    (outcome, id)
                }));
            }
            let mut winners = Vec::new();
            for h in handles {
                let (outcome, id) = h.await.unwrap();
                if outcome == PutOutcome::Stored {
                    winners.push(id);
                }
            }
            assert_eq!(
                winners.len(),
                1,
                "exactly one IfAbsent writer wins: {winners:?}"
            );
            let artifact = store.get("team", &hash).await.unwrap().unwrap();
            assert_eq!(artifact.metadata.tag.as_deref(), Some(winners[0].as_str()));
            let body = read_body(artifact).await;
            assert_eq!(body, format!("{}|payload", winners[0]).as_bytes());
        }
        assert!(temp_files(dir.path()).is_empty(), "no temp files leak");
        if spool {
            let left = std::fs::read_dir(&spool_dir).unwrap().count();
            assert_eq!(left, 0, "spool files are consumed or removed");
        }
    }
}

/// A truncated or corrupt artifact (e.g. after a crash) is never served; it is a miss and removed.
#[tokio::test]
async fn corrupt_artifacts_are_misses_and_removed() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalStorage::new(dir.path()).await.unwrap();

    for (hash, damage) in [("truncated", 0u8), ("zeroed", 1), ("badjson", 2)] {
        put_bytes(&store, "team", hash, &vec![7u8; 1000], "tag").await;
        let path = store.artifact_path("team", hash).unwrap();
        let mut raw = std::fs::read(&path).unwrap();
        match damage {
            0 => raw.truncate(500),
            1 => raw = vec![0u8; raw.len()],
            // Break the metadata JSON in place (keeps the trailer valid).
            _ => raw[1000] = b'#',
        }
        std::fs::write(&path, &raw).unwrap();
        assert!(store.head("team", hash).await.unwrap().is_none());
        assert!(store.get("team", hash).await.unwrap().is_none());
        assert!(!path.exists(), "corrupt artifact {hash} removed");
    }
}

/// A corrupt artifact does not count as existing for `IfAbsent`, so it never blocks a re-upload.
#[tokio::test]
async fn if_absent_replaces_corrupt_artifacts() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalStorage::new(dir.path()).await.unwrap();
    for (hash, spool) in [("corruptbytes", false), ("corruptspool", true)] {
        put_bytes(&store, "team", hash, &[1u8; 100], "old").await;
        let path = store.artifact_path("team", hash).unwrap();
        let raw = std::fs::read(&path).unwrap();
        std::fs::write(&path, &raw[..50]).unwrap();

        let body = if spool {
            UploadBody::File(spooled(&dir.path().join(".spool"), b"fresh").await)
        } else {
            UploadBody::from(b"fresh".to_vec())
        };
        let outcome = store
            .put("team", hash, body, meta("fresh"), PutMode::IfAbsent)
            .await
            .unwrap();
        assert_eq!(outcome, PutOutcome::Stored, "{hash}");
        let a = store.get("team", hash).await.unwrap().unwrap();
        assert_eq!(a.metadata.tag.as_deref(), Some("fresh"));
        assert_eq!(read_body(a).await, b"fresh");
    }
    assert!(temp_files(dir.path()).is_empty());
}

/// Spooled bodies are published by rename (no payload copy), and temp/spool files never leak,
/// also when publishing fails.
#[tokio::test]
async fn spooled_publish_renames_and_never_leaks_files() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalStorage::new(dir.path()).await.unwrap();
    let spool_dir = dir.path().join(".spool");

    let payload = vec![9u8; 300_000];
    let spool = spooled(&spool_dir, &payload).await;
    let spool_path = spool.path().to_path_buf();
    #[cfg(unix)]
    let spool_ino = {
        use std::os::unix::fs::MetadataExt;
        std::fs::metadata(&spool_path).unwrap().ino()
    };
    store
        .put(
            "team",
            "spooled",
            UploadBody::File(spool),
            meta("spool-tag"),
            PutMode::Overwrite,
        )
        .await
        .unwrap();
    assert!(!spool_path.exists());
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let ino = std::fs::metadata(store.artifact_path("team", "spooled").unwrap())
            .unwrap()
            .ino();
        assert_eq!(
            ino, spool_ino,
            "same-filesystem spool file is renamed, not copied"
        );
    }
    let a = store.get("team", "spooled").await.unwrap().unwrap();
    assert_eq!(a.metadata.tag.as_deref(), Some("spool-tag"));
    assert_eq!(read_body(a).await, payload);

    // Publishing fails (target path is a non-empty directory): a local I/O error is a 500, and
    // temp and spool files are removed.
    let target = store.artifact_path("team", "blocked").unwrap();
    std::fs::create_dir_all(target.join("occupied")).unwrap();
    let err = store
        .put(
            "team",
            "blocked",
            UploadBody::from(b"data".to_vec()),
            meta("t"),
            PutMode::Overwrite,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, AppError::Internal(_)), "{err:?}");
    let spool = spooled(&spool_dir, b"data").await;
    let spool_path = spool.path().to_path_buf();
    store
        .put(
            "team",
            "blocked",
            UploadBody::File(spool),
            meta("t"),
            PutMode::Overwrite,
        )
        .await
        .unwrap_err();
    assert!(!spool_path.exists(), "spool file removed on failure");

    // Length mismatch between SpooledFile and file content is rejected without leaks.
    let mut spool = spooled(&spool_dir, b"12345").await;
    spool.set_len(3);
    let spool_path = spool.path().to_path_buf();
    store
        .put(
            "team",
            "mismatch",
            UploadBody::File(spool),
            meta("t"),
            PutMode::Overwrite,
        )
        .await
        .unwrap_err();
    assert!(!spool_path.exists());
    assert!(store.head("team", "mismatch").await.unwrap().is_none());
    assert!(temp_files(dir.path()).is_empty(), "no temp files leak");
}

#[tokio::test]
async fn empty_artifact_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalStorage::new(dir.path()).await.unwrap();
    assert!(store.get("team", "rt").await.unwrap().is_none());
    assert!(store.head("team", "rt").await.unwrap().is_none());
    put_bytes(&store, "team", "rt", b"", "empty").await;
    let h = store.head("team", "rt").await.unwrap().unwrap();
    assert_eq!(h.size, 0);
    assert_eq!(h.tag.as_deref(), Some("empty"));
    let a = store.get("team", "rt").await.unwrap().unwrap();
    assert_eq!(read_body(a).await, Bytes::new());
    assert!(store.delete("team", "rt").await.unwrap());
    assert!(!store.delete("team", "rt").await.unwrap());
}

/// Identifiers differing only in case never share a file, even on case-insensitive filesystems
/// (macOS default).
#[tokio::test]
async fn case_variants_are_distinct() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalStorage::new(dir.path()).await.unwrap();
    let cases = [
        ("team", "abcdef", "lower"),
        ("team", "ABCDEF", "upper"),
        ("Team", "abcdef", "ns-upper"),
    ];
    for (ns, hash, tag) in cases {
        put_bytes(&store, ns, hash, tag.as_bytes(), tag).await;
    }
    for (ns, hash, tag) in cases {
        let a = store.get(ns, hash).await.unwrap().unwrap();
        assert_eq!(a.metadata.tag.as_deref(), Some(tag));
        assert_eq!(read_body(a).await, tag.as_bytes());
    }
    let upper = store.artifact_path("Team", "ABCDEF").unwrap();
    assert!(upper.starts_with(dir.path().join("team^1")), "{upper:?}");
    assert!(upper.ends_with("ab^3/abcdef^f3.artifact"), "{upper:?}");
    assert!(store.delete("team", "ABCDEF").await.unwrap());
    assert!(store.head("team", "abcdef").await.unwrap().is_some());
}

/// Case escaping keeps the file names of maximal identifiers within `NAME_MAX` (255 bytes).
#[tokio::test]
async fn maximal_uppercase_identifiers_are_stored() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalStorage::new(dir.path()).await.unwrap();
    let upper_hash = "A".repeat(128);
    let upper_ns = "N".repeat(128);
    let mixed_hash = "aB".repeat(64);
    let cases = [
        ("t", upper_hash.as_str(), "upper-hash"),
        (upper_ns.as_str(), "abcdef", "upper-ns"),
        (upper_ns.as_str(), upper_hash.as_str(), "both"),
        ("t", mixed_hash.as_str(), "mixed"),
        ("t", &upper_hash.to_ascii_lowercase(), "lower-hash"),
    ];
    for (ns, hash, tag) in cases {
        put_bytes(&store, ns, hash, tag.as_bytes(), tag).await;
        let name_len = store
            .artifact_path(ns, hash)
            .unwrap()
            .file_name()
            .unwrap()
            .len();
        assert!(name_len <= 255, "{tag}: file name has {name_len} bytes");
    }
    for (ns, hash, tag) in cases {
        let a = store.get(ns, hash).await.unwrap().unwrap();
        assert_eq!(a.metadata.tag.as_deref(), Some(tag));
        assert_eq!(read_body(a).await, tag.as_bytes());
    }
}

#[cfg(unix)]
#[tokio::test]
async fn artifact_files_and_dirs_are_private() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("store");
    let store = LocalStorage::new(&base).await.unwrap();
    put_bytes(&store, "team", "privatehash", b"secret", "t").await;
    let spool = spooled(&base.join(".spool"), b"spooled").await;
    store
        .put(
            "team",
            "spooledhash",
            UploadBody::File(spool),
            meta("t"),
            PutMode::Overwrite,
        )
        .await
        .unwrap();
    for hash in ["privatehash", "spooledhash"] {
        let path = store.artifact_path("team", hash).unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600, "{hash} file mode {mode:o}");
        let shard = path.parent().unwrap();
        for d in [shard, shard.parent().unwrap(), base.as_path()] {
            let mode = std::fs::metadata(d).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o700, "{} dir mode {mode:o}", d.display());
        }
    }
}

/// Entries older than `max_age` are deleted, and with `max_bytes` the oldest entries are evicted
/// first until the store fits.
#[tokio::test]
async fn janitor_expires_and_evicts_oldest_first() {
    let dir = tempfile::tempdir().unwrap();
    let opts = LocalOptions {
        max_bytes: None,
        max_age: Some(Duration::from_secs(3600)),
        cleanup_interval: Duration::from_secs(3600),
        temp_max_age: Duration::from_secs(3600),
    };
    let store = LocalStorage::with_options(dir.path(), opts).await.unwrap();
    put_bytes(&store, "team", "oldentry", b"old", "t").await;
    put_bytes(&store, "team", "newentry", b"new", "t").await;
    set_mtime(
        &store.artifact_path("team", "oldentry").unwrap(),
        Duration::from_secs(7200),
    );
    // The constructor's immediate background pass may race with this one, so assert on the
    // resulting state rather than on per-pass counters.
    store.run_cleanup().await.unwrap();
    assert!(store.head("team", "oldentry").await.unwrap().is_none());
    assert!(store.head("team", "newentry").await.unwrap().is_some());

    // Size-based eviction, oldest mtime first (including entries within the same minute).
    let dir = tempfile::tempdir().unwrap();
    let mut opts = LocalOptions::default();
    let store = LocalStorage::with_options(dir.path(), opts.clone())
        .await
        .unwrap();
    for i in 0..10u64 {
        let hash = format!("entry{i:02}");
        put_bytes(&store, "team", &hash, &vec![1u8; 1000], "t").await;
        // entry00 is the oldest ... entry09 the newest; 00-04 are spread over several minutes,
        // 05-09 lie within the same minute.
        let ago = if i < 5 { 600 - i * 60 } else { 30 - i };
        set_mtime(
            &store.artifact_path("team", &hash).unwrap(),
            Duration::from_secs(ago),
        );
    }
    let entry_size = std::fs::metadata(store.artifact_path("team", "entry00").unwrap())
        .unwrap()
        .len();
    drop(store);
    opts.max_bytes = Some(entry_size * 3 + entry_size / 2);
    let store = LocalStorage::with_options(dir.path(), opts).await.unwrap();
    let report = store.run_cleanup().await.unwrap();
    assert_eq!(report.remaining_entries, 3, "{report:?}");
    assert!(report.remaining_bytes <= entry_size * 3 + entry_size / 2);
    for i in 0..10u64 {
        let present = store
            .head("team", &format!("entry{i:02}"))
            .await
            .unwrap()
            .is_some();
        assert_eq!(present, i >= 7, "entry{i:02}");
    }
}

/// The janitor runs in the background on the configured interval and skips the `.spool`
/// directory; GET hits refresh the mtime (LRU approximation) so read entries survive.
#[tokio::test]
async fn background_janitor_spares_recently_read_entries() {
    let dir = tempfile::tempdir().unwrap();
    let spool_dir = dir.path().join(".spool");
    std::fs::create_dir_all(&spool_dir).unwrap();
    let foreign_spool = spool_dir.join("upload-in-progress.bin");
    std::fs::write(&foreign_spool, b"x").unwrap();
    set_mtime(&foreign_spool, Duration::from_secs(86_400));

    let opts = LocalOptions {
        max_bytes: None,
        max_age: Some(Duration::from_secs(3600)),
        cleanup_interval: Duration::from_millis(50),
        temp_max_age: Duration::from_secs(3600),
    };
    let store = LocalStorage::with_options(dir.path(), opts).await.unwrap();
    put_bytes(&store, "team", "readentry", b"keep", "t").await;
    put_bytes(&store, "team", "idleentry", b"drop", "t").await;
    let read_path = store.artifact_path("team", "readentry").unwrap();
    let idle_path = store.artifact_path("team", "idleentry").unwrap();
    // Both are 50 minutes old: below max_age, but older than the touch interval (15 min).
    set_mtime(&read_path, Duration::from_secs(3000));
    set_mtime(&idle_path, Duration::from_secs(3000));

    let a = store.get("team", "readentry").await.unwrap().unwrap();
    drop(a);
    let touched = std::fs::metadata(&read_path).unwrap().modified().unwrap();
    assert!(
        SystemTime::now().duration_since(touched).unwrap() < Duration::from_secs(60),
        "GET hit refreshes mtime"
    );
    // A second GET right after does not touch again (at most once per interval).
    let before = std::fs::metadata(&read_path).unwrap().modified().unwrap();
    drop(store.get("team", "readentry").await.unwrap().unwrap());
    assert_eq!(
        std::fs::metadata(&read_path).unwrap().modified().unwrap(),
        before
    );

    // Age the idle entry past max_age; the read entry was refreshed and stays.
    set_mtime(&idle_path, Duration::from_secs(4000));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while idle_path.exists() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "janitor did not run"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(read_path.exists());
    assert!(
        foreign_spool.exists(),
        "janitor never touches the spool dir"
    );
}

/// The janitor removes only its own temp files older than `temp_max_age` (never another
/// replica's in-flight upload) and never counts, expires or evicts foreign hidden files such as
/// macOS AppleDouble `._*`, even when they end in `.artifact`.
#[tokio::test]
async fn janitor_only_touches_its_own_files() {
    let dir = tempfile::tempdir().unwrap();
    let shard = dir.path().join("team").join("ab");
    std::fs::create_dir_all(&shard).unwrap();
    let fresh_temp = shard.join(".tcrs-inflight.tmp");
    let stale_temp = shard.join(".tcrs-crashed.tmp");
    for p in [&fresh_temp, &stale_temp] {
        std::fs::write(p, b"partial").unwrap();
    }
    set_mtime(&stale_temp, Duration::from_secs(7200));
    let apple_double = shard.join("._abcdef.artifact");
    let dot_file = shard.join(".foo.artifact");
    for p in [&apple_double, &dot_file] {
        std::fs::write(p, vec![0u8; 4096]).unwrap();
        set_mtime(p, Duration::from_secs(7200));
    }

    let opts = LocalOptions {
        max_bytes: Some(1),
        max_age: Some(Duration::from_secs(60)),
        ..LocalOptions::default()
    };
    let store = LocalStorage::with_options(dir.path(), opts).await.unwrap();
    let report = store.run_cleanup().await.unwrap();
    assert!(fresh_temp.exists(), "in-flight temp file must survive");
    assert!(!stale_temp.exists());
    assert!(apple_double.exists());
    assert!(dot_file.exists());
    assert_eq!(report.remaining_entries, 0, "{report:?}");
    assert_eq!(report.remaining_bytes, 0, "{report:?}");
    assert_eq!(
        report.expired_removed + report.evicted_removed,
        0,
        "{report:?}"
    );
}

fn set_dir_mtime(path: &Path, ago: Duration) {
    std::fs::File::open(path)
        .unwrap()
        .set_modified(SystemTime::now() - ago)
        .unwrap();
}

/// Directories emptied by expiry/eviction are removed after a grace period, a spool directory
/// inside the storage tree is never removed, and puts into a removed directory recreate it.
#[tokio::test]
async fn janitor_removes_empty_directories() {
    let dir = tempfile::tempdir().unwrap();
    let opts = LocalOptions {
        max_bytes: None,
        max_age: Some(Duration::from_secs(3600)),
        cleanup_interval: Duration::from_secs(3600),
        temp_max_age: Duration::from_secs(3600),
    };
    // A user-configured spool directory inside the tree (looks like a namespace).
    let spool_dir = dir.path().join("spool");
    std::fs::create_dir_all(&spool_dir).unwrap();
    set_dir_mtime(&spool_dir, Duration::from_secs(7200));

    let store = LocalStorage::with_options(dir.path(), opts).await.unwrap();
    put_bytes(&store, "gone", "aaaaaa", b"x", "t").await;
    put_bytes(&store, "kept", "bbbbbb", b"x", "t").await;
    put_bytes(&store, "kept", "cccccc", b"x", "t").await;
    let gone = store.artifact_path("gone", "aaaaaa").unwrap();
    let expired_in_kept = store.artifact_path("kept", "bbbbbb").unwrap();
    set_mtime(&gone, Duration::from_secs(7200));
    set_mtime(&expired_in_kept, Duration::from_secs(7200));

    // First pass: the artifacts expire; their directories were just modified, so they stay.
    store.run_cleanup().await.unwrap();
    assert!(!gone.exists() && !expired_in_kept.exists());
    let gone_shard = gone.parent().unwrap().to_path_buf();
    let kept_shard = expired_in_kept.parent().unwrap().to_path_buf();
    assert!(gone_shard.is_dir() && kept_shard.is_dir());

    // Once idle, empty shard dirs and their then-empty namespace dir are removed; a namespace
    // with remaining shards and the spool dir are kept.
    set_dir_mtime(&gone_shard, Duration::from_secs(120));
    set_dir_mtime(&kept_shard, Duration::from_secs(120));
    let report = store.run_cleanup().await.unwrap();
    assert!(!gone_shard.exists());
    assert!(!dir.path().join("gone").exists());
    assert!(!kept_shard.exists());
    assert!(dir.path().join("kept").is_dir());
    assert!(store.head("kept", "cccccc").await.unwrap().is_some());
    assert!(spool_dir.is_dir(), "spool directory must never be removed");
    assert!(report.dirs_removed >= 3, "{report:?}");

    // Puts recreate removed directories (Bytes and spooled bodies).
    put_bytes(&store, "gone", "aaaaaa", b"again", "t2").await;
    let a = store.get("gone", "aaaaaa").await.unwrap().unwrap();
    assert_eq!(a.metadata.tag.as_deref(), Some("t2"));
    assert_eq!(read_body(a).await, b"again");
    let spool = spooled(&dir.path().join(".spool"), b"spooled").await;
    let outcome = store
        .put(
            "kept",
            "bbbbbb",
            UploadBody::File(spool),
            meta("t3"),
            PutMode::IfAbsent,
        )
        .await
        .unwrap();
    assert_eq!(outcome, PutOutcome::Stored);
    let a = store.get("kept", "bbbbbb").await.unwrap().unwrap();
    assert_eq!(read_body(a).await, b"spooled");
}
