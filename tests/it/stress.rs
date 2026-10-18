//! Randomized concurrent workload through the HTTP API against the hot cache over the local
//! backend.

use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use axum::http::StatusCode;
use fast_turbo_cache_server::{
    AppState, AuthConfig,
    models::{AnalyticsEvent, ArtifactQueryRequest, CacheEvent, CacheSource},
    storage::{cache::TieredStorage, local::LocalStorage},
};

use crate::support::{Api, Fetch};

/// Seeded xorshift64* generator, so every run issues the same requests per worker.
struct FastRng(u64);

impl FastRng {
    fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn next_range(&mut self, max: usize) -> usize {
        (self.next_u64() as usize) % max.max(1)
    }
}

/// Mixed reads, writes, batch queries, analytics and rejected requests from many workers: the hot
/// cache stays within its byte and entry bounds at every step, concurrent writes to one key never
/// produce a torn read, every GET is counted exactly once, and the cache still serves hits
/// afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn mixed_concurrent_workload_keeps_hot_cache_bounded_and_consistent() {
    let temp_dir = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalStorage::new(temp_dir.path()).await.unwrap());
    let max_hot_bytes = 1024 * 1024;
    let max_hot_entries = 64;
    let max_hot_item_bytes = 32 * 1024;
    let tiered = Arc::new(TieredStorage::new(
        local,
        max_hot_bytes,
        max_hot_entries,
        max_hot_item_bytes,
        256,
    ));
    let auth = AuthConfig::new(vec!["stress-token".into()], false);
    let state = AppState::new(tiered.clone(), auth).with_max_artifact_size(128 * 1024);
    let api = Api::serve(state).await.with_token("stress-token");

    let num_hot_deps = 16usize;
    for i in 0..num_hot_deps {
        let resp = api
            .put(format!("/v8/artifacts/hot_dep_{i:04}?slug=team-0"))
            .header("x-artifact-duration", (100 + i * 10).to_string())
            .body(vec![(i as u8).wrapping_add(17); 4096])
            .fetch()
            .await;
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
    }

    let num_workers = 32usize;
    let ops_per_worker = 40usize;
    let artifact_gets = Arc::new(AtomicU64::new(0));
    let batch_distinct_hashes = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    let mut handles = Vec::with_capacity(num_workers);
    for worker_id in 0..num_workers {
        let api = api.clone();
        let tiered = tiered.clone();
        let artifact_gets = artifact_gets.clone();
        let batch_distinct_hashes = batch_distinct_hashes.clone();

        handles.push(tokio::spawn(async move {
            let mut rng = FastRng::new((worker_id as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15));

            for step in 0..ops_per_worker {
                assert!(tiered.payload_cache().current_bytes() <= max_hot_bytes);
                assert!(tiered.payload_cache().current_entries() <= max_hot_entries);

                let team = format!("team-{}", rng.next_range(3));
                match rng.next_range(100) {
                    // 40%: GET of a frequently used artifact.
                    0..=39 => {
                        let dep = rng.next_range(num_hot_deps);
                        artifact_gets.fetch_add(1, Ordering::Relaxed);
                        let resp = api
                            .get(format!("/v8/artifacts/hot_dep_{dep:04}?slug=team-0"))
                            .fetch()
                            .await;
                        assert_eq!(resp.status(), StatusCode::OK);
                        let bytes = resp.bytes().await.unwrap();
                        assert_eq!(bytes.len(), 4096);
                        assert!(bytes.iter().all(|&b| b == (dep as u8).wrapping_add(17)));
                    }
                    // 20%: PUTs that churn the cache; a quarter is above the item limit.
                    40..=59 => {
                        let cold_id = rng.next_range(300);
                        let size = if rng.next_range(4) == 0 {
                            48 * 1024
                        } else {
                            512 + rng.next_range(8192)
                        };
                        let resp = api
                            .put(format!(
                                "/v8/artifacts/cold_artifact_{cold_id:04}?slug={team}"
                            ))
                            .header("x-artifact-duration", "250")
                            .body(vec![(cold_id & 0xFF) as u8; size])
                            .fetch()
                            .await;
                        assert_eq!(resp.status(), StatusCode::ACCEPTED);
                    }
                    // 15%: batch query of present, absent and duplicate hashes.
                    60..=74 => {
                        let mut hashes = Vec::with_capacity(12);
                        for _ in 0..6 {
                            hashes.push(format!("hot_dep_{:04}", rng.next_range(num_hot_deps)));
                        }
                        for _ in 0..4 {
                            hashes.push(format!("cold_artifact_{:04}", rng.next_range(300)));
                        }
                        hashes.push("non_existent_hash_xyz".to_string());
                        hashes.push(hashes[0].clone());
                        let distinct: HashSet<&String> = hashes.iter().collect();
                        batch_distinct_hashes.fetch_add(distinct.len() as u64, Ordering::Relaxed);

                        let resp = api
                            .post("/v8/artifacts?slug=team-0")
                            .json(&ArtifactQueryRequest { hashes })
                            .fetch()
                            .await;
                        assert_eq!(resp.status(), StatusCode::OK);
                    }
                    // 10%: PUT or GET of one contended key; every body is a single repeated byte,
                    // so a read mixing two writes shows up as differing bytes.
                    75..=84 => {
                        let path = "/v8/artifacts/shared_contended_hash?slug=team-0";
                        if step % 2 == 0 {
                            let resp = api
                                .put(path)
                                .body(vec![(step & 0xFF) as u8; 2048])
                                .fetch()
                                .await;
                            assert_eq!(resp.status(), StatusCode::ACCEPTED);
                        } else {
                            artifact_gets.fetch_add(1, Ordering::Relaxed);
                            let resp = api.get(path).fetch().await;
                            if resp.status() == StatusCode::OK {
                                let data = resp.bytes().await.unwrap();
                                assert_eq!(data.len(), 2048);
                                assert!(data.iter().all(|&b| b == data[0]), "torn read");
                            }
                        }
                    }
                    // 10%: analytics events.
                    85..=94 => {
                        let events = vec![AnalyticsEvent {
                            session_id: Some(format!("sess-{worker_id}")),
                            source: CacheSource::Remote,
                            event: CacheEvent::Hit,
                            hash: format!("hot_dep_{:04}", rng.next_range(num_hot_deps)),
                            duration: 120,
                        }];
                        let resp = api.post("/v8/artifacts/events").json(&events).fetch().await;
                        assert_eq!(resp.status(), StatusCode::OK);
                    }
                    // 5%: rejected requests.
                    _ => match rng.next_range(3) {
                        0 => {
                            let resp = api
                                .put("/v8/artifacts/oversized_hash")
                                .body(vec![0u8; 140 * 1024])
                                .fetch()
                                .await;
                            assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
                        }
                        1 => {
                            let resp = api
                                .with_token("wrong-token")
                                .get("/v8/artifacts/status")
                                .fetch()
                                .await;
                            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
                        }
                        _ => {
                            let resp = api.get("/v8/artifacts/bad$hash!").fetch().await;
                            assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
                        }
                    },
                }
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    let elapsed = start.elapsed();

    // Hit counts depend on scheduling (eviction order under churn), so only
    // scheduling-independent properties are asserted.
    let payload = tiered.payload_cache();
    let metadata = tiered.metadata_cache();
    assert!(payload.current_bytes() <= max_hot_bytes);
    assert!(payload.current_entries() <= max_hot_entries);
    assert_eq!(
        payload.hits() + payload.misses(),
        artifact_gets.load(Ordering::Relaxed),
        "each GET must be counted exactly once"
    );
    assert!(
        metadata.hits() + metadata.misses() >= batch_distinct_hashes.load(Ordering::Relaxed),
        "every distinct hash of a batch query must be looked up"
    );

    // Once quiescent, repeated reads hit the caches again.
    let hot_get = || async {
        let resp = api
            .get("/v8/artifacts/hot_dep_0000?slug=team-0")
            .fetch()
            .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.bytes().await.unwrap().len(), 4096);
    };
    hot_get().await;
    let hits_before = payload.hits();
    hot_get().await;
    assert_eq!(
        payload.hits(),
        hits_before + 1,
        "repeated GET must be a hit"
    );
    let meta_hits_before = metadata.hits();
    let resp = api
        .post("/v8/artifacts?slug=team-0")
        .json(&ArtifactQueryRequest {
            hashes: vec!["hot_dep_0000".to_string()],
        })
        .fetch()
        .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        metadata.hits(),
        meta_hits_before + 1,
        "batch query of a cached artifact must be a metadata hit"
    );

    let total_ops = num_workers * ops_per_worker;
    println!(
        "{total_ops} requests from {num_workers} workers in {elapsed:?} ({:.0} req/s), \
         hot_hits={}, evictions={}",
        total_ops as f64 / elapsed.as_secs_f64(),
        payload.hits(),
        payload.evictions(),
    );
}
