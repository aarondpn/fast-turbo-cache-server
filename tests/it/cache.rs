//! Tests of the hot cache (`TieredStorage`) over a fake backend.
//!
//! Races are reproduced deterministically with the shared [`FakeStorage`], whose operations can
//! be paused at well-defined points by gates.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use fast_turbo_cache_server::{
    models::ArtifactMetadata,
    storage::{
        ArtifactPayload, CacheOptions, PutMode, PutOutcome, Storage, UploadBody,
        cache::TieredStorage,
    },
};
use futures_util::StreamExt;

use crate::support::{
    body_bytes,
    fake_storage::{FakeStorage, metadata},
};

const NS: &str = "team";
const KEY: &str = "artifact";
const WAIT: Duration = Duration::from_secs(10);

fn options() -> CacheOptions {
    CacheOptions {
        max_bytes: 1 << 20,
        max_entries: 1000,
        max_item_bytes: 64 << 10,
        metadata_max_entries: 1000,
        ttl: None,
        negative_ttl: None,
    }
}

fn tiered(inner: &Arc<FakeStorage>, opts: CacheOptions) -> Arc<TieredStorage> {
    Arc::new(TieredStorage::with_options(inner.clone(), opts))
}

async fn put(storage: &dyn Storage, data: &[u8], tag: &str) -> PutOutcome {
    storage
        .put(
            NS,
            KEY,
            UploadBody::Bytes(Bytes::copy_from_slice(data)),
            metadata(data.len(), tag),
            PutMode::Overwrite,
        )
        .await
        .unwrap()
}

async fn put_if_absent(storage: &dyn Storage, hash: &str, data: &'static [u8]) -> PutOutcome {
    storage
        .put(
            NS,
            hash,
            UploadBody::Bytes(Bytes::from_static(data)),
            metadata(data.len(), "if-absent"),
            PutMode::IfAbsent,
        )
        .await
        .unwrap()
}

async fn read(storage: &dyn Storage, hash: &str) -> Option<(Vec<u8>, ArtifactMetadata)> {
    let artifact = storage.get(NS, hash).await.unwrap()?;
    let metadata = artifact.metadata.clone();
    Some((body_bytes(artifact).await, metadata))
}

async fn read_data(storage: &dyn Storage) -> Option<Vec<u8>> {
    read(storage, KEY).await.map(|(data, _)| data)
}

async fn head_tag(storage: &dyn Storage) -> Option<String> {
    storage
        .head(NS, KEY)
        .await
        .unwrap()
        .map(|m| m.tag.as_deref().unwrap_or_default().to_string())
}

async fn yield_many() {
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
}

// ---------------------------------------------------------------------------------------------
// Coherence with the backend under races
// ---------------------------------------------------------------------------------------------

/// A GET that read the artifact before a DELETE must not re-insert it into the cache afterwards.
#[tokio::test]
async fn get_racing_delete_does_not_resurrect_artifact() {
    let inner = FakeStorage::new();
    inner.insert_direct(KEY, b"old", "tag-old");
    let cache = tiered(&inner, options());

    let gate = inner.arm(|g| &mut g.get);
    let reader = tokio::spawn({
        let cache = cache.clone();
        async move { read_data(cache.as_ref()).await }
    });
    gate.wait_entered().await;

    assert!(cache.delete(NS, KEY).await.unwrap());
    gate.release();
    // The racing read may legitimately observe the pre-delete state.
    assert_eq!(reader.await.unwrap().as_deref(), Some(&b"old"[..]));

    assert_eq!(
        read_data(cache.as_ref()).await,
        None,
        "deleted artifact resurrected"
    );
    assert_eq!(head_tag(cache.as_ref()).await, None);
}

/// A GET that read the old payload must not overwrite the cache after a newer PUT completed.
#[tokio::test]
async fn get_racing_put_does_not_cache_stale_payload() {
    let inner = FakeStorage::new();
    inner.insert_direct(KEY, b"old", "tag-old");
    let cache = tiered(&inner, options());

    let gate = inner.arm(|g| &mut g.get);
    let reader = tokio::spawn({
        let cache = cache.clone();
        async move { read_data(cache.as_ref()).await }
    });
    gate.wait_entered().await;

    assert_eq!(
        put(cache.as_ref(), b"new", "tag-new").await,
        PutOutcome::Stored
    );
    gate.release();
    reader.await.unwrap();

    assert_eq!(
        read_data(cache.as_ref()).await.as_deref(),
        Some(&b"new"[..])
    );
    assert_eq!(head_tag(cache.as_ref()).await.as_deref(), Some("tag-new"));
}

#[tokio::test]
async fn head_racing_put_does_not_cache_stale_metadata() {
    let inner = FakeStorage::new();
    inner.insert_direct(KEY, b"old", "tag-old");
    let cache = tiered(&inner, options());

    let gate = inner.arm(|g| &mut g.head);
    let reader = tokio::spawn({
        let cache = cache.clone();
        async move { head_tag(cache.as_ref()).await }
    });
    gate.wait_entered().await;

    put(cache.as_ref(), b"new!", "tag-new").await;
    gate.release();
    assert_eq!(reader.await.unwrap().as_deref(), Some("tag-old"));

    assert_eq!(head_tag(cache.as_ref()).await.as_deref(), Some("tag-new"));
    assert_eq!(
        cache.head_hot(NS, KEY).and_then(|m| m.tag).as_deref(),
        Some("tag-new")
    );
}

/// Two concurrent PUTs whose cache updates would complete in the reverse order of their backend
/// writes must not leave the cache pointing at the value the backend no longer holds.
#[tokio::test]
async fn concurrent_puts_never_leave_cache_behind_backend() {
    let inner = FakeStorage::new();
    let cache = tiered(&inner, options());

    // PUT A writes to the backend first, then stalls before updating the cache.
    let gate = inner.arm(|g| &mut g.put_after);
    let writer_a = tokio::spawn({
        let cache = cache.clone();
        async move { put(cache.as_ref(), b"AAAA", "tag-a").await }
    });
    gate.wait_entered().await;

    // PUT B writes later (backend now holds B) and completes fully.
    put(cache.as_ref(), b"BBBB", "tag-b").await;
    gate.release();
    writer_a.await.unwrap();

    assert_eq!(inner.snapshot(KEY).unwrap().0, Bytes::from_static(b"BBBB"));
    assert_eq!(
        read_data(cache.as_ref()).await.as_deref(),
        Some(&b"BBBB"[..])
    );
    assert_eq!(head_tag(cache.as_ref()).await.as_deref(), Some("tag-b"));
}

/// A GET that started after a PUT began but read the old value before the PUT wrote must not
/// survive the PUT.
#[tokio::test]
async fn put_invalidates_reads_that_overlapped_its_backend_write() {
    let inner = FakeStorage::new();
    inner.insert_direct(KEY, b"old", "tag-old");
    let cache = tiered(&inner, options());

    let before = inner.arm(|g| &mut g.put_before);
    let after = inner.arm(|g| &mut g.put_after);
    let writer = tokio::spawn({
        let cache = cache.clone();
        async move { put(cache.as_ref(), b"new", "tag-new").await }
    });
    before.wait_entered().await;

    // The read starts after the PUT started and observes the old backend state.
    let read_gate = inner.arm(|g| &mut g.get);
    let reader = tokio::spawn({
        let cache = cache.clone();
        async move { read_data(cache.as_ref()).await }
    });
    read_gate.wait_entered().await;

    before.release();
    after.wait_entered().await; // backend now holds "new"
    read_gate.release();
    assert_eq!(reader.await.unwrap().as_deref(), Some(&b"old"[..]));
    after.release();
    writer.await.unwrap();

    assert_eq!(
        read_data(cache.as_ref()).await.as_deref(),
        Some(&b"new"[..])
    );
    assert_eq!(head_tag(cache.as_ref()).await.as_deref(), Some("tag-new"));
}

/// A cancelled PUT/DELETE whose backend work continues in the background must still invalidate
/// the cache after that work finished; reads in between may only cache the old state briefly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_write_invalidates_after_backend_finishes() {
    for delete in [false, true] {
        let inner = Arc::new(FakeStorage {
            detached_writes: Some(Duration::from_millis(150)),
            ..FakeStorage::default()
        });
        inner.insert_direct(KEY, b"old", "t");
        let cache = tiered(&inner, options());

        let writer = tokio::spawn({
            let cache = cache.clone();
            async move {
                if delete {
                    cache.delete(NS, KEY).await.map(|_| ())
                } else {
                    cache
                        .put(
                            NS,
                            KEY,
                            UploadBody::Bytes(Bytes::from_static(b"new")),
                            metadata(3, "t"),
                            PutMode::Overwrite,
                        )
                        .await
                        .map(|_| ())
                }
            }
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        writer.abort();
        let _ = writer.await;

        // Reads while the backend is still working observe (and may cache) the old value.
        assert_eq!(
            read_data(cache.as_ref()).await.as_deref(),
            Some(&b"old"[..])
        );
        assert_eq!(head_tag(cache.as_ref()).await.as_deref(), Some("t"));

        let expected: Option<&[u8]> = if delete { None } else { Some(b"new") };
        let deadline = tokio::time::Instant::now() + WAIT;
        loop {
            let backend = inner.snapshot(KEY).map(|(data, _)| data);
            if backend.as_deref() == expected {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "backend never finished"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // The end step runs right after the backend call returns.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            read_data(cache.as_ref()).await.as_deref(),
            expected,
            "cache serves the pre-write value (delete = {delete})"
        );
        assert_eq!(cache.head(NS, KEY).await.unwrap().is_some(), !delete);
        assert_eq!(cache.head_hot(NS, KEY).is_some(), !delete);
    }
}

/// Overwriting a cached artifact with a payload above the item limit drops the old payload.
#[tokio::test]
async fn overwrite_with_uncacheable_payload_drops_cached_payload() {
    let inner = FakeStorage::new();
    let opts = CacheOptions {
        max_item_bytes: 1024,
        ..options()
    };
    let cache = tiered(&inner, opts);
    put(cache.as_ref(), b"abc", "tag-small").await;
    assert!(cache.get_hot(NS, KEY).is_some());

    put(cache.as_ref(), &[1u8; 2048], "tag-big").await;
    assert!(cache.get_hot(NS, KEY).is_none());
    assert_eq!(read_data(cache.as_ref()).await.unwrap().len(), 2048);
    assert_eq!(head_tag(cache.as_ref()).await.as_deref(), Some("tag-big"));
}

/// Concurrent overwrites, conditional writes, deletes and reads on a few keys; once all work has
/// stopped every cache view must agree with the backend.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn randomised_operations_leave_cache_consistent_with_backend() {
    const KEYS: [&str; 3] = ["k0", "k1", "k2"];
    let counter = Arc::new(AtomicUsize::new(0));
    for round in 0..200 {
        let inner = Arc::new(FakeStorage {
            random_yields: true,
            ..FakeStorage::default()
        });
        let cache = tiered(
            &inner,
            CacheOptions {
                negative_ttl: (round % 2 == 0).then_some(Duration::from_secs(600)),
                ..options()
            },
        );
        let workers: Vec<_> = (0..12)
            .map(|_| {
                let cache = cache.clone();
                let counter = counter.clone();
                tokio::spawn(async move {
                    for _ in 0..20 {
                        let key = KEYS[fastrand::usize(..KEYS.len())];
                        match fastrand::u8(0..7) {
                            op @ (0 | 1) => {
                                let n = counter.fetch_add(1, Ordering::Relaxed);
                                let tag = format!("v{n}");
                                let mode = if op == 0 {
                                    PutMode::Overwrite
                                } else {
                                    PutMode::IfAbsent
                                };
                                cache
                                    .put(
                                        NS,
                                        key,
                                        UploadBody::Bytes(Bytes::from(tag.clone().into_bytes())),
                                        metadata(tag.len(), &tag),
                                        mode,
                                    )
                                    .await
                                    .unwrap();
                            }
                            2 => {
                                cache.delete(NS, key).await.unwrap();
                            }
                            3 => {
                                let _ = read(cache.as_ref(), key).await;
                            }
                            4 => {
                                cache.head(NS, key).await.unwrap();
                            }
                            5 => {
                                let _ = cache.get_hot(NS, key);
                            }
                            _ => {
                                let _ = cache.head_hot(NS, key);
                            }
                        }
                    }
                })
            })
            .collect();
        for worker in workers {
            worker.await.unwrap();
        }

        for key in KEYS {
            let expected = inner.snapshot(key);
            let expected_data = expected.as_ref().map(|(d, _)| d.to_vec());
            let expected_tag = expected.as_ref().and_then(|(_, m)| m.tag.clone());
            if let Some((data, _)) = cache.get_hot(NS, key) {
                assert_eq!(
                    Some(data.to_vec()),
                    expected_data,
                    "round {round}: get_hot {key}"
                );
            }
            if let Some(meta) = cache.head_hot(NS, key) {
                assert_eq!(meta.tag, expected_tag, "round {round}: head_hot {key}");
            }
            let head = cache.head(NS, key).await.unwrap().and_then(|m| m.tag);
            assert_eq!(head, expected_tag, "round {round}: head {key}");
            let data = read(cache.as_ref(), key).await.map(|(d, _)| d);
            assert_eq!(data, expected_data, "round {round}: get {key}");
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Conditional writes (IfAbsent)
// ---------------------------------------------------------------------------------------------

/// A rejected repeat upload leaves the backend unchanged, so the cache entries survive and both
/// lookups are served without a backend round trip.
#[tokio::test]
async fn if_absent_on_existing_artifact_keeps_cache_entries() {
    let inner = FakeStorage::new();
    let cache = tiered(
        &inner,
        CacheOptions {
            negative_ttl: Some(Duration::from_secs(600)),
            ..options()
        },
    );
    put(cache.as_ref(), b"first", "tag-1").await;
    assert_eq!(
        put_if_absent(cache.as_ref(), KEY, b"second").await,
        PutOutcome::AlreadyExists
    );
    assert!(cache.get_hot(NS, KEY).is_some(), "payload entry dropped");
    assert!(cache.head_hot(NS, KEY).is_some(), "metadata entry dropped");
    assert_eq!(
        read_data(cache.as_ref()).await.as_deref(),
        Some(&b"first"[..])
    );
    assert_eq!(head_tag(cache.as_ref()).await.as_deref(), Some("tag-1"));
    assert_eq!(inner.gets.load(Ordering::SeqCst), 0);
    assert_eq!(inner.heads.load(Ordering::SeqCst), 0);
}

/// An `IfAbsent` write that does store a value still invalidates what readers cached around it:
/// a stale entry (artifact removed out-of-band) and a negative entry read during the write.
#[tokio::test]
async fn if_absent_store_invalidates_stale_and_racing_entries() {
    let inner = FakeStorage::new();
    let cache = tiered(
        &inner,
        CacheOptions {
            negative_ttl: Some(Duration::from_secs(600)),
            ..options()
        },
    );

    // Stale positive entry: cached, then removed from the backend behind the cache's back.
    put(cache.as_ref(), b"old", "tag-old").await;
    inner.remove_direct(KEY);
    assert_eq!(
        put_if_absent(cache.as_ref(), KEY, b"new").await,
        PutOutcome::Stored
    );
    assert_eq!(
        read_data(cache.as_ref()).await.as_deref(),
        Some(&b"new"[..])
    );
    assert_eq!(head_tag(cache.as_ref()).await.as_deref(), Some("if-absent"));

    // Racing negative entry: a GET reads "absent", then an IfAbsent PUT stores the key before
    // the GET gets to cache its miss.
    const OTHER: &str = "other";
    let gate = inner.arm(|g| &mut g.get);
    let reader = tokio::spawn({
        let cache = cache.clone();
        async move { read(cache.as_ref(), OTHER).await }
    });
    gate.wait_entered().await;
    assert_eq!(
        put_if_absent(cache.as_ref(), OTHER, b"fresh").await,
        PutOutcome::Stored
    );
    gate.release();
    assert!(reader.await.unwrap().is_none());
    assert_eq!(
        read(cache.as_ref(), OTHER).await.map(|(d, _)| d).as_deref(),
        Some(&b"fresh"[..]),
        "negative entry from before the write survived it"
    );
}

/// A conditional upload that finds the artifact (stored out-of-band, e.g. by another replica)
/// drops the negative entry, so GET/HEAD stop reporting a miss immediately.
#[tokio::test]
async fn if_absent_already_exists_clears_negative_entry() {
    let inner = FakeStorage::new();
    let cache = tiered(
        &inner,
        CacheOptions {
            negative_ttl: Some(Duration::from_secs(600)),
            ..options()
        },
    );
    assert_eq!(head_tag(cache.as_ref()).await, None);
    inner.insert_direct(KEY, b"remote", "remote-tag");
    // Negative entry still active.
    assert_eq!(head_tag(cache.as_ref()).await, None);

    assert_eq!(
        put_if_absent(cache.as_ref(), KEY, b"local").await,
        PutOutcome::AlreadyExists
    );
    assert_eq!(
        head_tag(cache.as_ref()).await.as_deref(),
        Some("remote-tag")
    );
    assert_eq!(
        read_data(cache.as_ref()).await.as_deref(),
        Some(&b"remote"[..])
    );

    // Race: a HEAD observed the miss before the out-of-band write and only tries to cache it
    // after the conditional upload returned AlreadyExists; that stale miss must not stick.
    const OTHER: &str = "other";
    let gate = inner.arm(|g| &mut g.head);
    let reader = tokio::spawn({
        let cache = cache.clone();
        async move { cache.head(NS, OTHER).await.unwrap() }
    });
    gate.wait_entered().await;
    inner.insert_direct(OTHER, b"remote", "remote-tag");
    assert_eq!(
        put_if_absent(cache.as_ref(), OTHER, b"local").await,
        PutOutcome::AlreadyExists
    );
    gate.release();
    assert!(reader.await.unwrap().is_none());
    let tag = cache.head(NS, OTHER).await.unwrap().and_then(|m| m.tag);
    assert_eq!(tag.as_deref(), Some("remote-tag"));
}

// ---------------------------------------------------------------------------------------------
// TTL and negative caching
// ---------------------------------------------------------------------------------------------

/// Out-of-band deletions (other replica, S3 lifecycle) are noticed once the TTL elapsed, and
/// expirations are reported separately from capacity evictions.
#[tokio::test]
async fn cached_entries_expire_after_ttl() {
    let inner = FakeStorage::new();
    let cache = tiered(
        &inner,
        CacheOptions {
            ttl: Some(Duration::from_millis(50)),
            ..options()
        },
    );
    put(cache.as_ref(), b"payload", "tag").await;
    assert!(cache.head(NS, KEY).await.unwrap().is_some());
    inner.remove_direct(KEY);

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(cache.get_hot(NS, KEY).is_none());
    assert!(cache.head_hot(NS, KEY).is_none());
    let stats = cache.cache_stats().unwrap();
    assert_eq!(stats.payload.expirations, 1);
    assert_eq!(stats.metadata.expirations, 1);
    assert_eq!(stats.payload.evictions, 0);
    assert_eq!(stats.metadata.evictions, 0);

    assert_eq!(head_tag(cache.as_ref()).await, None);
    assert_eq!(read_data(cache.as_ref()).await, None);
}

/// Within the TTL the cache keeps serving, even if the backend lost the artifact.
#[tokio::test]
async fn entries_are_served_from_cache_within_ttl() {
    let inner = FakeStorage::new();
    let cache = tiered(
        &inner,
        CacheOptions {
            ttl: Some(Duration::from_secs(600)),
            ..options()
        },
    );
    put(cache.as_ref(), b"payload", "tag").await;
    inner.remove_direct(KEY);
    assert_eq!(
        read_data(cache.as_ref()).await.as_deref(),
        Some(&b"payload"[..])
    );
    assert_eq!(head_tag(cache.as_ref()).await.as_deref(), Some("tag"));
    assert_eq!(inner.gets.load(Ordering::SeqCst), 0);
}

/// A backend miss observed by `GET` drops cached metadata even without negative caching, so
/// `HEAD` does not keep reporting an artifact the server itself just answered `404` for.
#[tokio::test]
async fn backend_miss_drops_cached_metadata_without_negative_cache() {
    let inner = FakeStorage::new();
    let cache = tiered(
        &inner,
        CacheOptions {
            // Payloads are not cached, metadata is.
            max_bytes: 0,
            ttl: Some(Duration::from_secs(600)),
            negative_ttl: None,
            ..options()
        },
    );
    put(cache.as_ref(), b"payload", "tag").await;
    assert_eq!(head_tag(cache.as_ref()).await.as_deref(), Some("tag"));
    assert_eq!(
        inner.heads.load(Ordering::SeqCst),
        0,
        "HEAD is answered from the metadata cache"
    );

    inner.remove_direct(KEY);
    assert_eq!(read_data(cache.as_ref()).await, None);
    assert!(cache.head_hot(NS, KEY).is_none());
    assert_eq!(head_tag(cache.as_ref()).await, None);
    assert_eq!(inner.heads.load(Ordering::SeqCst), 1);
}

/// The miss-driven removal must not drop what a write that started after the read cached.
#[tokio::test]
async fn backend_miss_before_concurrent_put_keeps_new_entry() {
    let inner = FakeStorage::new();
    let cache = tiered(
        &inner,
        CacheOptions {
            max_bytes: 0,
            ttl: Some(Duration::from_secs(600)),
            ..options()
        },
    );
    let gate = inner.arm(|g| &mut g.get);
    let reader = tokio::spawn({
        let cache = cache.clone();
        async move { read_data(cache.as_ref()).await }
    });
    gate.wait_entered().await;
    put(cache.as_ref(), b"data", "tag").await;
    gate.release();
    assert_eq!(reader.await.unwrap(), None);
    assert_eq!(head_tag(cache.as_ref()).await.as_deref(), Some("tag"));
    assert_eq!(
        inner.heads.load(Ordering::SeqCst),
        0,
        "the PUT's metadata entry survived the older miss"
    );
}

#[tokio::test]
async fn negative_cache_suppresses_backend_lookups_until_put() {
    let inner = FakeStorage::new();
    let cache = tiered(
        &inner,
        CacheOptions {
            negative_ttl: Some(Duration::from_secs(600)),
            ..options()
        },
    );
    assert_eq!(head_tag(cache.as_ref()).await, None);
    assert_eq!(head_tag(cache.as_ref()).await, None);
    assert_eq!(read_data(cache.as_ref()).await, None);
    assert_eq!(inner.heads.load(Ordering::SeqCst), 1);
    assert_eq!(inner.gets.load(Ordering::SeqCst), 0);

    put(cache.as_ref(), b"now-here", "tag").await;
    assert_eq!(head_tag(cache.as_ref()).await.as_deref(), Some("tag"));
    assert_eq!(
        read_data(cache.as_ref()).await.as_deref(),
        Some(&b"now-here"[..])
    );

    // Same for a negative entry recorded by a GET miss after a delete.
    cache.delete(NS, KEY).await.unwrap();
    assert_eq!(read_data(cache.as_ref()).await, None);
    assert_eq!(head_tag(cache.as_ref()).await, None);
    put(cache.as_ref(), b"again", "tag2").await;
    assert_eq!(
        read_data(cache.as_ref()).await.as_deref(),
        Some(&b"again"[..])
    );
}

#[tokio::test]
async fn negative_entries_expire_and_are_off_by_default() {
    let inner = FakeStorage::new();
    let cache = tiered(&inner, options());
    assert_eq!(head_tag(cache.as_ref()).await, None);
    assert_eq!(head_tag(cache.as_ref()).await, None);
    assert_eq!(
        inner.heads.load(Ordering::SeqCst),
        2,
        "negative caching must be opt-in"
    );

    let inner = FakeStorage::new();
    let cache = tiered(
        &inner,
        CacheOptions {
            negative_ttl: Some(Duration::from_millis(30)),
            ..options()
        },
    );
    assert_eq!(head_tag(cache.as_ref()).await, None);
    inner.insert_direct(KEY, b"x", "late");
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert_eq!(head_tag(cache.as_ref()).await.as_deref(), Some("late"));
}

/// A miss observed before a concurrent PUT must not be cached as a negative entry.
#[tokio::test]
async fn negative_entry_is_not_cached_across_a_concurrent_put() {
    let inner = FakeStorage::new();
    let cache = tiered(
        &inner,
        CacheOptions {
            negative_ttl: Some(Duration::from_secs(600)),
            ..options()
        },
    );
    let gate = inner.arm(|g| &mut g.head);
    let reader = tokio::spawn({
        let cache = cache.clone();
        async move { head_tag(cache.as_ref()).await }
    });
    gate.wait_entered().await;
    put(cache.as_ref(), b"data", "tag").await;
    gate.release();
    assert_eq!(reader.await.unwrap(), None);
    assert_eq!(head_tag(cache.as_ref()).await.as_deref(), Some("tag"));
}

// ---------------------------------------------------------------------------------------------
// Single-flight
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn concurrent_get_misses_share_one_backend_request() {
    for streaming in [false, true] {
        let inner = if streaming {
            FakeStorage::streaming()
        } else {
            FakeStorage::new()
        };
        inner.insert_direct(KEY, &[5u8; 3000], "tag");
        let cache = tiered(&inner, options());

        let gate = inner.arm(|g| &mut g.get);
        let readers: Vec<_> = (0..32)
            .map(|_| {
                let cache = cache.clone();
                tokio::spawn(async move { read_data(cache.as_ref()).await })
            })
            .collect();
        gate.wait_entered().await;
        yield_many().await; // let every reader join the flight
        gate.release();
        for reader in readers {
            assert_eq!(reader.await.unwrap(), Some(vec![5u8; 3000]));
        }
        assert_eq!(
            inner.gets.load(Ordering::SeqCst),
            1,
            "streaming = {streaming}"
        );
        assert!(cache.get_hot(NS, KEY).is_some());
    }
}

#[tokio::test]
async fn concurrent_head_misses_share_one_backend_request() {
    let inner = FakeStorage::new();
    inner.insert_direct(KEY, b"x", "tag");
    let cache = tiered(&inner, options());

    let gate = inner.arm(|g| &mut g.head);
    let readers: Vec<_> = (0..32)
        .map(|_| {
            let cache = cache.clone();
            tokio::spawn(async move { head_tag(cache.as_ref()).await })
        })
        .collect();
    gate.wait_entered().await;
    yield_many().await;
    gate.release();
    for reader in readers {
        assert_eq!(reader.await.unwrap().as_deref(), Some("tag"));
    }
    assert_eq!(inner.heads.load(Ordering::SeqCst), 1);
}

/// Artifacts too large for the hot cache are streamed to every waiter (never buffered) and keep
/// their known content length.
#[tokio::test]
async fn large_streamed_artifacts_are_handed_off_unbuffered() {
    let inner = FakeStorage::streaming();
    let big = vec![3u8; 200 << 10];
    inner.insert_direct(KEY, &big, "tag");
    let cache = tiered(&inner, options()); // 64 KiB item limit

    let gate = inner.arm(|g| &mut g.get);
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let cache = cache.clone();
            tokio::spawn(async move {
                let artifact = cache.get(NS, KEY).await.unwrap().unwrap();
                assert_eq!(artifact.content_length, Some(200 << 10));
                let ArtifactPayload::Stream(mut s) = artifact.payload else {
                    panic!("payload was buffered")
                };
                let mut n = 0;
                while let Some(chunk) = s.next().await {
                    n += chunk.unwrap().len();
                }
                n
            })
        })
        .collect();
    gate.wait_entered().await;
    yield_many().await;
    gate.release();
    for reader in readers {
        assert_eq!(reader.await.unwrap(), big.len());
    }
    assert!(cache.get_hot(NS, KEY).is_none());
}

/// A request arriving after a write must not join a flight that started before the write.
#[tokio::test]
async fn requests_after_a_write_do_not_join_older_flights() {
    let inner = FakeStorage::new();
    inner.insert_direct(KEY, b"old", "tag-old");
    let cache = tiered(&inner, options());

    let gate = inner.arm(|g| &mut g.get);
    let first = tokio::spawn({
        let cache = cache.clone();
        async move { read_data(cache.as_ref()).await }
    });
    gate.wait_entered().await;

    put(cache.as_ref(), b"new", "tag-new").await;
    // Evict the freshly cached value so the next GET has to go to the backend.
    cache.payload_cache().remove(NS, KEY);
    let second = tokio::time::timeout(WAIT, read_data(cache.as_ref()))
        .await
        .expect("joined a stale flight");
    assert_eq!(second.as_deref(), Some(&b"new"[..]));

    gate.release();
    first.await.unwrap();
    assert_eq!(
        read_data(cache.as_ref()).await.as_deref(),
        Some(&b"new"[..])
    );
}

/// Cancelling the request that started a flight does not wedge later requests.
#[tokio::test]
async fn cancelled_flight_does_not_block_later_requests() {
    let inner = FakeStorage::new();
    inner.insert_direct(KEY, b"value", "tag");
    let cache = tiered(&inner, options());

    let gate = inner.arm(|g| &mut g.get);
    let first = tokio::spawn({
        let cache = cache.clone();
        async move { read_data(cache.as_ref()).await }
    });
    gate.wait_entered().await;
    first.abort();
    let _ = first.await;

    let second = tokio::time::timeout(WAIT, read_data(cache.as_ref()))
        .await
        .expect("request blocked behind a cancelled flight");
    assert_eq!(second.as_deref(), Some(&b"value"[..]));
    assert_eq!(inner.gets.load(Ordering::SeqCst), 2);
}

/// A backend that panics while serving a shared fill turns into an error for every waiter
/// instead of re-panicking each of them; later requests start a fresh flight.
#[tokio::test]
async fn shared_flight_panic_is_reported_as_error_to_all_waiters() {
    let inner = Arc::new(FakeStorage {
        panic_on_read: true,
        ..FakeStorage::default()
    });
    let cache = tiered(&inner, options());
    let results = futures_util::future::join_all((0..8).map(|_| cache.get(NS, KEY))).await;
    assert!(results.iter().all(|r| r.is_err()));
    assert_eq!(inner.gets.load(Ordering::SeqCst), 1);
    assert!(cache.head(NS, KEY).await.is_err());
    assert!(cache.get(NS, KEY).await.is_err());
    assert_eq!(inner.gets.load(Ordering::SeqCst), 2);
}

/// Errors shared through single-flight GET/HEAD fills reach every waiter unchanged (same
/// variant, message and HTTP status), e.g. a 507 or a 429 must not become a 502.
#[tokio::test]
async fn shared_flight_errors_keep_their_variant_and_status() {
    use fast_turbo_cache_server::AppError;

    let errors = [
        AppError::Unauthorized("u".into()),
        AppError::forbidden("some_code", "f"),
        AppError::BadRequest("b".into()),
        AppError::NotFound("n".into()),
        AppError::RequestTimeout("t".into()),
        AppError::PayloadTooLarge("p".into()),
        AppError::TooManyRequests("busy".into()),
        AppError::Storage("s".into()),
        AppError::InsufficientStorage("full".into()),
        AppError::Internal("i".into()),
        AppError::io(
            "disk",
            std::io::Error::from(std::io::ErrorKind::StorageFull),
        ),
    ];
    let inner = FakeStorage::new();
    let cache = tiered(&inner, options());
    for expected in errors {
        *inner.read_error.lock().unwrap() = Some(expected.clone());
        let gets = futures_util::future::join_all((0..4).map(|_| cache.get(NS, KEY))).await;
        let heads = futures_util::future::join_all((0..4).map(|_| cache.head(NS, KEY))).await;
        let got = gets
            .into_iter()
            .map(|r| r.err().expect("GET must fail"))
            .chain(heads.into_iter().map(|r| r.expect_err("HEAD must fail")));
        for err in got {
            assert_eq!(format!("{err:?}"), format!("{expected:?}"));
            assert_eq!(err.status(), expected.status());
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------------------------

/// Each request is counted exactly once even though routes probe `get_hot`/`head_hot` first.
#[tokio::test]
async fn hot_probe_and_lookup_count_each_request_once() {
    let inner = FakeStorage::new();
    let cache = tiered(&inner, options());
    put(cache.as_ref(), b"abc", "tag").await;

    // Route pattern: hot probe, then regular lookup on a hot miss.
    async fn route_get(cache: &TieredStorage, hash: &str) {
        if cache.get_hot(NS, hash).is_none() {
            let _ = cache.get(NS, hash).await.unwrap();
        }
    }
    async fn route_head(cache: &TieredStorage, hash: &str) {
        if cache.head_hot(NS, hash).is_none() {
            let _ = cache.head(NS, hash).await.unwrap();
        }
    }
    route_get(&cache, KEY).await; // hit
    route_get(&cache, "missing1").await; // miss
    route_get(&cache, "missing2").await; // miss
    route_head(&cache, KEY).await; // hit
    route_head(&cache, "missing1").await; // miss

    let stats = cache.cache_stats().unwrap();
    assert_eq!((stats.payload.hits, stats.payload.misses), (1, 2));
    assert_eq!((stats.metadata.hits, stats.metadata.misses), (1, 1));
    assert_eq!(stats.payload.entries, 1);
    assert_eq!(stats.payload.bytes, 3 + 3);
    assert_eq!(stats.payload.max_bytes, 1 << 20);
    assert_eq!(stats.payload.max_entries, 1000);
    assert_eq!(stats.metadata.entries, 1);
    assert_eq!(stats.metadata.max_entries, 1000);
}
