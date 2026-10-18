use std::{
    alloc::{GlobalAlloc, Layout, System},
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
};
use bytes::Bytes;
use fast_turbo_cache_server::{
    AppState, AuthConfig, PutMode, build_router,
    models::{ArtifactMetadata, ArtifactQueryRequest},
    storage::{
        Storage,
        cache::{BoundedBytesCache, BoundedMetadataCache, TieredStorage},
        local::LocalStorage,
    },
};
use http_body_util::BodyExt;
use tokio::net::TcpListener;
use tower::ServiceExt;

struct CountingAllocator;

static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static DEALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static BYTES_ALLOCATED: AtomicU64 = AtomicU64::new(0);
static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
static PEAK_LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            let size = layout.size();
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
            BYTES_ALLOCATED.fetch_add(size as u64, Ordering::Relaxed);
            let prev = LIVE_BYTES.fetch_add(size, Ordering::Relaxed);
            let cur = prev + size;
            let mut peak = PEAK_LIVE_BYTES.load(Ordering::Relaxed);
            while cur > peak {
                match PEAK_LIVE_BYTES.compare_exchange_weak(
                    peak,
                    cur,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(actual) => peak = actual,
                }
            }
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        DEALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
    }
}

#[derive(Clone, Copy)]
struct AllocSnapshot {
    allocs: u64,
    bytes: u64,
}

impl AllocSnapshot {
    fn now() -> Self {
        Self {
            allocs: ALLOC_COUNT.load(Ordering::Relaxed),
            bytes: BYTES_ALLOCATED.load(Ordering::Relaxed),
        }
    }

    fn delta_since(self, earlier: Self) -> (u64, u64) {
        (
            self.allocs.saturating_sub(earlier.allocs),
            self.bytes.saturating_sub(earlier.bytes),
        )
    }
}

fn print_row(name: &str, iters: u64, elapsed: Duration, allocs: u64, bytes: u64) {
    let ns_per_op = elapsed.as_nanos() as f64 / iters as f64;
    let ops_per_sec = 1_000_000_000.0 / ns_per_op;
    let allocs_per_op = allocs as f64 / iters as f64;
    let bytes_per_op = bytes as f64 / iters as f64;
    println!(
        "{:<42} | {:>9.1} ns/op | {:>10.0} op/s | {:>7.2} allocs/op | {:>8.1} B/op",
        name, ns_per_op, ops_per_sec, allocs_per_op, bytes_per_op
    );
}

/// Untimed iterations before each micro benchmark.
const WARMUP_ITERS: u64 = 200_000;

#[tokio::main]
async fn main() {
    println!(
        "=========================================================================================================="
    );
    println!(
        "{:<42} | {:>15} | {:>15} | {:>17} | {:>13}",
        "Benchmark / Hot Path", "Latency", "Throughput", "Heap Allocs", "Heap Bytes"
    );
    println!(
        "=========================================================================================================="
    );

    let sample_meta = ArtifactMetadata {
        duration: 1250,
        size: 16384,
        tag: Some("hmac-sha256-tag-value-1234567890=".into()),
        sha: Some("9c9236efb673e9c2ebffd82940f419a168a84bd3".into()),
        dirty_hash: Some("dirty_hash_abcdef123456".into()),
        client_ci: Some("GITHUB_ACTIONS".into()),
    };
    let sample_payload = Bytes::from(vec![42u8; 16384]);

    // 1. Microbenchmark: BoundedBytesCache::get (hot cache hit)
    let bytes_cache = BoundedBytesCache::new(64 * 1024 * 1024, 4096, 4 * 1024 * 1024);
    bytes_cache.put(
        "acme-team",
        "7a8b9c0d1e2f3a4b5c6d",
        sample_payload.clone(),
        sample_meta.clone(),
    );
    let iters = 1_000_000u64;
    // Every micro benchmark runs a warm-up pass first: a cold 100k-iteration run mostly measures
    // CPU frequency ramp-up and cache warm-up (row 3 used to vary between ~130 and ~260 ns).
    for _ in 0..WARMUP_ITERS {
        std::hint::black_box(bytes_cache.get("acme-team", "7a8b9c0d1e2f3a4b5c6d"));
    }
    let before = AllocSnapshot::now();
    let t0 = Instant::now();
    for _ in 0..iters {
        let res = std::hint::black_box(bytes_cache.get("acme-team", "7a8b9c0d1e2f3a4b5c6d"));
        assert!(res.is_some());
    }
    let elapsed = t0.elapsed();
    let (allocs, bytes) = AllocSnapshot::now().delta_since(before);
    print_row(
        "1. BoundedBytesCache::get (hit)",
        iters,
        elapsed,
        allocs,
        bytes,
    );

    // 2. Microbenchmark: BoundedMetadataCache::get (metadata hit)
    let meta_cache = BoundedMetadataCache::new(10_000);
    meta_cache.put("acme-team", "7a8b9c0d1e2f3a4b5c6d", sample_meta.clone());
    for _ in 0..WARMUP_ITERS {
        std::hint::black_box(meta_cache.get("acme-team", "7a8b9c0d1e2f3a4b5c6d"));
    }
    let before = AllocSnapshot::now();
    let t0 = Instant::now();
    for _ in 0..iters {
        let res = std::hint::black_box(meta_cache.get("acme-team", "7a8b9c0d1e2f3a4b5c6d"));
        assert!(res.is_some());
    }
    let elapsed = t0.elapsed();
    let (allocs, bytes) = AllocSnapshot::now().delta_since(before);
    print_row(
        "2. BoundedMetadataCache::get (hit)",
        iters,
        elapsed,
        allocs,
        bytes,
    );

    // 3. Microbenchmark: TieredStorage::head (hit)
    let temp_dir = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalStorage::new(temp_dir.path()).await.unwrap());
    let tiered = Arc::new(TieredStorage::new(
        local,
        64 * 1024 * 1024,
        4096,
        4 * 1024 * 1024,
        10_000,
    ));
    tiered
        .put(
            "acme-team",
            "7a8b9c0d1e2f3a4b5c6d",
            sample_payload.clone().into(),
            sample_meta.clone(),
            PutMode::Overwrite,
        )
        .await
        .unwrap();
    for i in 0..50 {
        let mut m = sample_meta.clone();
        m.duration = 100 + i;
        tiered
            .put(
                "acme-team",
                &format!("batch_hash_{i:04}"),
                sample_payload.clone().into(),
                m,
                PutMode::Overwrite,
            )
            .await
            .unwrap();
    }

    for _ in 0..WARMUP_ITERS {
        std::hint::black_box(
            tiered
                .head("acme-team", "7a8b9c0d1e2f3a4b5c6d")
                .await
                .unwrap(),
        );
    }
    let before = AllocSnapshot::now();
    let t0 = Instant::now();
    for _ in 0..iters {
        let res = std::hint::black_box(
            tiered
                .head("acme-team", "7a8b9c0d1e2f3a4b5c6d")
                .await
                .unwrap(),
        );
        assert!(res.is_some());
    }
    let elapsed = t0.elapsed();
    let (allocs, bytes) = AllocSnapshot::now().delta_since(before);
    print_row(
        "3. TieredStorage::head (hit)",
        iters,
        elapsed,
        allocs,
        bytes,
    );

    // 4. Microbenchmark: TieredStorage::get (hot payload hit)
    for _ in 0..WARMUP_ITERS {
        std::hint::black_box(
            tiered
                .get("acme-team", "7a8b9c0d1e2f3a4b5c6d")
                .await
                .unwrap(),
        );
    }
    let before = AllocSnapshot::now();
    let t0 = Instant::now();
    for _ in 0..iters {
        let res = std::hint::black_box(
            tiered
                .get("acme-team", "7a8b9c0d1e2f3a4b5c6d")
                .await
                .unwrap(),
        );
        assert!(res.is_some());
    }
    let elapsed = t0.elapsed();
    let (allocs, bytes) = AllocSnapshot::now().delta_since(before);
    print_row(
        "4. TieredStorage::get (hot hit)",
        iters,
        elapsed,
        allocs,
        bytes,
    );

    // 5. Full Axum Router Dispatch: HEAD /v8/artifacts/{hash}
    let auth = AuthConfig::new(vec!["bench-secret-token-12345".into()], false);
    let state = AppState::new(tiered.clone(), auth);
    let app = build_router(state);

    let router_iters = 20_000u64;
    for _ in 0..router_iters / 4 {
        let req = Request::builder()
            .method(Method::HEAD)
            .uri("/v8/artifacts/7a8b9c0d1e2f3a4b5c6d?slug=acme-team")
            .header("Authorization", "Bearer bench-secret-token-12345")
            .body(Body::empty())
            .unwrap();
        std::hint::black_box(app.clone().oneshot(req).await.unwrap());
    }
    let before = AllocSnapshot::now();
    let t0 = Instant::now();
    for _ in 0..router_iters {
        let req = Request::builder()
            .method(Method::HEAD)
            .uri("/v8/artifacts/7a8b9c0d1e2f3a4b5c6d?slug=acme-team")
            .header("Authorization", "Bearer bench-secret-token-12345")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
    let elapsed = t0.elapsed();
    let (allocs, bytes) = AllocSnapshot::now().delta_since(before);
    print_row(
        "5. Axum Router HEAD /v8/artifacts/{hash}",
        router_iters,
        elapsed,
        allocs,
        bytes,
    );

    // 6. Full Axum Router Dispatch: GET /v8/artifacts/{hash} (16 KiB hot hit)
    let before = AllocSnapshot::now();
    let t0 = Instant::now();
    for _ in 0..router_iters {
        let req = Request::builder()
            .method(Method::GET)
            .uri("/v8/artifacts/7a8b9c0d1e2f3a4b5c6d?slug=acme-team")
            .header("Authorization", "Bearer bench-secret-token-12345")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body_bytes.len(), 16384);
    }
    let elapsed = t0.elapsed();
    let (allocs, bytes) = AllocSnapshot::now().delta_since(before);
    print_row(
        "6. Axum Router GET /v8/artifacts/{hash}",
        router_iters,
        elapsed,
        allocs,
        bytes,
    );

    // 7. Full Axum Router Dispatch: POST /v8/artifacts (50-hash batch query)
    let batch_req_body = Bytes::from(
        serde_json::to_vec(&ArtifactQueryRequest {
            hashes: (0..50).map(|i| format!("batch_hash_{i:04}")).collect(),
        })
        .unwrap(),
    );
    let batch_iters = 5_000u64;
    let before = AllocSnapshot::now();
    let t0 = Instant::now();
    for _ in 0..batch_iters {
        let req = Request::builder()
            .method(Method::POST)
            .uri("/v8/artifacts?slug=acme-team")
            .header("Authorization", "Bearer bench-secret-token-12345")
            .header("Content-Type", "application/json")
            .body(Body::from(batch_req_body.clone()))
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let _ = resp.into_body().collect().await.unwrap().to_bytes();
    }
    let elapsed = t0.elapsed();
    let (allocs, bytes) = AllocSnapshot::now().delta_since(before);
    print_row(
        "7. Axum Router POST /v8/artifacts (50x)",
        batch_iters,
        elapsed,
        allocs,
        bytes,
    );

    // 8. Concurrent TCP HTTP Load Test (64 workers x 100 requests = 6,400 requests) with p50/p95/p99
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let server_app = app.clone();
    tokio::spawn(async move {
        use axum::serve::ListenerExt;
        let listener = listener.tap_io(|tcp| {
            let _ = tcp.set_nodelay(true);
        });
        axum::serve(listener, server_app).await.unwrap();
    });
    let base_url = format!("http://{addr}");
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(64)
        .tcp_nodelay(true)
        .build()
        .unwrap();

    // Warm up keep-alive connections across the pool before measuring steady-state latency
    for w in 0..8 {
        let url = format!(
            "{base_url}/v8/artifacts/batch_hash_{:04}?slug=acme-team",
            w % 50
        );
        let _ = client
            .head(&url)
            .bearer_auth("bench-secret-token-12345")
            .send()
            .await
            .unwrap();
    }

    let workers = 64usize;
    let reqs_per_worker = 100usize;
    let mut handles = Vec::with_capacity(workers);
    let t_tcp = Instant::now();
    for w in 0..workers {
        let client = client.clone();
        let url = format!(
            "{base_url}/v8/artifacts/batch_hash_{:04}?slug=acme-team",
            w % 50
        );
        handles.push(tokio::spawn(async move {
            let mut latencies_us = Vec::with_capacity(reqs_per_worker);
            for i in 0..reqs_per_worker {
                let start = Instant::now();
                if i % 3 == 0 {
                    let resp = client
                        .head(&url)
                        .bearer_auth("bench-secret-token-12345")
                        .send()
                        .await
                        .unwrap();
                    assert_eq!(resp.status(), StatusCode::OK);
                } else {
                    let resp = client
                        .get(&url)
                        .bearer_auth("bench-secret-token-12345")
                        .send()
                        .await
                        .unwrap();
                    assert_eq!(resp.status(), StatusCode::OK);
                    let b = resp.bytes().await.unwrap();
                    assert_eq!(b.len(), 16384);
                }
                latencies_us.push(start.elapsed().as_micros() as u64);
            }
            latencies_us
        }));
    }

    let mut all_latencies = Vec::with_capacity(workers * reqs_per_worker);
    for h in handles {
        all_latencies.extend(h.await.unwrap());
    }
    let tcp_elapsed = t_tcp.elapsed();
    all_latencies.sort_unstable();
    let total_reqs = all_latencies.len();
    let p50 = all_latencies[total_reqs * 50 / 100];
    let p95 = all_latencies[total_reqs * 95 / 100];
    let p99 = all_latencies[total_reqs * 99 / 100];
    let rps = total_reqs as f64 / tcp_elapsed.as_secs_f64();
    let peak_mb = PEAK_LIVE_BYTES.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0);

    println!(
        "=========================================================================================================="
    );
    println!(
        "8. Concurrent TCP Load ({total_reqs} reqs, {workers} workers): {:.0} req/s | p50={}µs | p95={}µs | p99={}µs | Peak Live Heap={:.2} MiB",
        rps, p50, p95, p99, peak_mb
    );
    println!(
        "=========================================================================================================="
    );
}
