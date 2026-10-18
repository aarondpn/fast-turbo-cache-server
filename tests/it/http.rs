//! HTTP-layer tests against a served router: upload slots and body timeouts, artifact header
//! validation, auth scoping, CORS, request ids, metrics, unknown-length artifacts, and early
//! rejections that clients still sending their body must receive.

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use axum::http::StatusCode;
use fast_turbo_cache_server::{
    AppState, AuthConfig, CacheOptions, PutMode, PutOutcome, ServerOptions, Storage, build_router,
    models::{AnalyticsEvent, ArtifactMetadata, CacheEvent, CacheSource},
    storage::cache::TieredStorage,
};
use futures_util::stream;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::support::{
    Api, Fetch,
    fake_storage::{FakeStorage, Payloads},
    spawn_server, tok_state,
};

fn options(spool_dir: &std::path::Path) -> ServerOptions {
    ServerOptions {
        spool_dir: spool_dir.to_path_buf(),
        ..ServerOptions::default()
    }
}

async fn read_response(stream: &mut TcpStream, limit: Duration) -> String {
    let mut buf = Vec::new();
    let _ = tokio::time::timeout(limit, async {
        let mut chunk = [0u8; 4096];
        loop {
            match stream.read(&mut chunk).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    buf.extend_from_slice(&chunk[..n]);
                    if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
            }
        }
    })
    .await;
    String::from_utf8_lossy(&buf).into_owned()
}

/// Opens a raw connection and sends a PUT whose body stalls after 10 of 100 bytes.
async fn start_stalled_upload(addr: SocketAddr, slug: &str, token: &str) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "PUT /v8/artifacts/stalledhash?slug={slug} HTTP/1.1\r\nHost: localhost\r\n\
         Authorization: Bearer {token}\r\nContent-Length: 100\r\n\r\n0123456789"
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    stream
}

// A stalled upload holding the only upload slot is cut off by the body idle timeout, so other
// tenants are delayed at most by that timeout.
#[tokio::test]
async fn stalled_upload_does_not_block_other_tenants_beyond_idle_timeout() {
    let spool = tempfile::tempdir().unwrap();
    let state = tok_state(FakeStorage::new())
        .with_max_concurrent_uploads(1)
        .with_server_options(ServerOptions {
            body_idle_timeout: Duration::from_millis(300),
            upload_slot_wait: Duration::from_secs(10),
            ..options(spool.path())
        });
    let api = Api::serve(state).await.with_token("tok");

    let mut stalled = start_stalled_upload(api.addr(), "tenant-a", "tok").await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let started = std::time::Instant::now();
    let other = tokio::time::timeout(
        Duration::from_secs(5),
        api.put("/v8/artifacts/otherhash?slug=tenant-b")
            .body(b"tenant-b-payload".to_vec())
            .fetch(),
    )
    .await
    .expect("other tenant's upload must not be blocked by the stalled upload");
    assert_eq!(other.status(), StatusCode::ACCEPTED);
    assert!(started.elapsed() < Duration::from_secs(3));

    let response = read_response(&mut stalled, Duration::from_secs(5)).await;
    assert!(
        response.starts_with("HTTP/1.1 408"),
        "stalled upload should get 408, got: {response}"
    );
    // The rest of the body never arrives: the connection is closed after the response.
    let mut rest = Vec::new();
    let closed = tokio::time::timeout(Duration::from_secs(5), stalled.read_to_end(&mut rest)).await;
    assert!(closed.is_ok(), "the stalled connection was not closed");
}

// No upload slot frees up within UPLOAD_SLOT_WAIT_SECS: 429 + Retry-After (a 5xx would count
// towards turbo's outage breaker).
#[tokio::test]
async fn upload_slot_wait_timeout_returns_429_with_retry_after() {
    let spool = tempfile::tempdir().unwrap();
    let state = tok_state(FakeStorage::new())
        .with_max_concurrent_uploads(1)
        .with_server_options(ServerOptions {
            body_idle_timeout: Duration::from_secs(30),
            upload_slot_wait: Duration::from_millis(200),
            ..options(spool.path())
        });
    let metrics = state.metrics.clone();
    let api = Api::serve(state).await.with_token("tok");
    let addr = api.addr();

    let _stalled = start_stalled_upload(addr, "tenant-a", "tok").await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let resp = api
        .put("/v8/artifacts/otherhash?slug=tenant-b")
        .body(b"x".to_vec())
        .fetch()
        .await;
    assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(resp.headers().get("retry-after").unwrap(), "1");
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "too_many_requests");
    assert_eq!(metrics.uploads_in_flight.get(), 1);
}

// A client that disconnects after sending its whole body does not free its upload slot while
// the write it started still runs: a retry waits (429) instead of starting a second write.
#[tokio::test]
async fn disconnected_upload_keeps_its_slot_until_the_write_ends() {
    let spool = tempfile::tempdir().unwrap();
    let fake = FakeStorage::new();
    let gate = fake.arm(|g| &mut g.put_before);
    let state = tok_state(fake.clone())
        .with_max_concurrent_uploads(1)
        .with_server_options(ServerOptions {
            upload_slot_wait: Duration::from_millis(200),
            ..options(spool.path())
        });
    let metrics = state.metrics.clone();
    let api = Api::serve(state).await.with_token("tok");

    let mut stream = TcpStream::connect(api.addr()).await.unwrap();
    stream
        .write_all(
            b"PUT /v8/artifacts/abandonedhash?slug=team HTTP/1.1\r\nHost: localhost\r\n\
              Authorization: Bearer tok\r\nContent-Length: 7\r\n\r\npayload",
        )
        .await
        .unwrap();
    gate.wait_entered().await;
    drop(stream);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let resp = api
        .put("/v8/artifacts/retryhash?slug=team")
        .body(b"x".to_vec())
        .fetch()
        .await;
    assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(metrics.uploads_in_flight.get(), 1);

    // The abandoned write still completes and then frees the slot.
    gate.release();
    tokio::time::timeout(Duration::from_secs(5), async {
        while metrics.uploads_in_flight.get() != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the slot was not freed after the write ended");
    assert!(fake.snapshot("abandonedhash").is_some());
    let resp = api
        .put("/v8/artifacts/retryhash?slug=team")
        .body(b"x".to_vec())
        .fetch()
        .await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
}

// x-artifact-tag up to 600 characters (Vercel spec) is accepted and returned unchanged.
#[tokio::test]
async fn tag_of_600_chars_roundtrips() {
    let state = tok_state(FakeStorage::new());
    let api = Api::serve(state).await.with_token("tok");
    let tag = "T".repeat(600);

    let put = api
        .put("/v8/artifacts/taghash")
        .header("x-artifact-tag", &tag)
        .body(b"payload".to_vec())
        .fetch()
        .await;
    assert_eq!(put.status(), StatusCode::ACCEPTED);

    let get = api.get("/v8/artifacts/taghash").fetch().await;
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(get.headers().get("x-artifact-tag").unwrap(), tag.as_str());

    let too_long = api
        .put("/v8/artifacts/taghash2")
        .header("x-artifact-tag", "T".repeat(601))
        .body(b"payload".to_vec())
        .fetch()
        .await;
    assert_eq!(too_long.status(), StatusCode::BAD_REQUEST);
}

// Control characters in `x-artifact-*` values, including the HTAB that RFC 9110 allows, are
// rejected with a 400 and the standard error envelope.
#[tokio::test]
async fn artifact_headers_with_control_characters_are_rejected() {
    let state = tok_state(FakeStorage::new());
    let api = Api::serve(state).await.with_token("tok");

    for header in [
        "x-artifact-tag",
        "x-artifact-client-ci",
        "x-artifact-sha",
        "x-artifact-dirty-hash",
    ] {
        let put = api
            .put("/v8/artifacts/tabhash")
            .header(header, "a\tb")
            .body(b"payload".to_vec())
            .fetch()
            .await;
        assert_eq!(put.status(), StatusCode::BAD_REQUEST, "{header}");
        let body: serde_json::Value = put.json().await.unwrap();
        assert_eq!(body["error"]["code"], "bad_request", "{header}: {body}");
        let message = body["error"]["message"].as_str().unwrap();
        assert!(message.contains(header), "{header}: {message}");
        assert!(message.contains("control characters"), "{message}");
    }
    let get = api.get("/v8/artifacts/tabhash").fetch().await;
    assert_eq!(get.status(), StatusCode::NOT_FOUND, "nothing was stored");

    // Spaces (also repeated ones) are fine and stored verbatim.
    let put = api
        .put("/v8/artifacts/spacehash")
        .header("x-artifact-tag", "a  b")
        .body(b"payload".to_vec())
        .fetch()
        .await;
    assert_eq!(put.status(), StatusCode::ACCEPTED);
    let get = api.get("/v8/artifacts/spacehash").fetch().await;
    assert_eq!(get.headers().get("x-artifact-tag").unwrap(), "a  b");

    // Other bytes outside visible ASCII / SP (here DEL, a non-ASCII byte and a raw tab) are a
    // 400 too. Sent raw because HTTP client libraries refuse to build such headers.
    for bad in [&b"a\x7fb"[..], &b"a\xffb"[..], &b"a\tb"[..]] {
        let mut stream = TcpStream::connect(api.addr()).await.unwrap();
        let mut req = b"PUT /v8/artifacts/badhash HTTP/1.1\r\nHost: localhost\r\n\
            Authorization: Bearer tok\r\nContent-Length: 1\r\nConnection: close\r\n\
            x-artifact-tag: "
            .to_vec();
        req.extend_from_slice(bad);
        req.extend_from_slice(b"\r\n\r\nx");
        stream.write_all(&req).await.unwrap();
        // `Connection: close`: read to EOF so the body is complete.
        let mut raw = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut raw)).await;
        let response = String::from_utf8_lossy(&raw).into_owned();
        assert!(response.starts_with("HTTP/1.1 400"), "{bad:?}: {response}");
        // Tabs and non-ASCII bytes reach the handler and get the standard error envelope;
        // other control bytes (DEL) are refused by hyper's parser with a bare 400.
        if bad != &b"a\x7fb"[..] {
            assert!(response.contains("\"bad_request\""), "{bad:?}: {response}");
        }
    }
}

fn sample_event(hash: &str) -> AnalyticsEvent {
    AnalyticsEvent {
        session_id: Some("session-of-team-a".into()),
        source: CacheSource::Remote,
        event: CacheEvent::Hit,
        hash: hash.into(),
        duration: 100,
    }
}

// Analytics events are stored per namespace; scoped tokens cannot read or write other teams'
// events.
#[tokio::test]
async fn analytics_are_isolated_between_scoped_tokens() {
    let auth = AuthConfig::new(vec![], false)
        .with_scoped_tokens([("team-a", "token-a"), ("team-b", "token-b")]);
    let state = AppState::new(FakeStorage::new(), auth);
    let api = Api::serve(state).await;
    let (team_a, team_b) = (api.with_token("token-a"), api.with_token("token-b"));

    let post = team_a
        .post("/v8/artifacts/events?slug=team-a")
        .json(&vec![sample_event("secret-hash-a")])
        .fetch()
        .await;
    assert_eq!(post.status(), StatusCode::OK);

    // Team B sees nothing of team A.
    let b_events: Vec<AnalyticsEvent> = team_b
        .get("/v8/artifacts/events?slug=team-b")
        .fetch()
        .await
        .json()
        .await
        .unwrap();
    assert!(b_events.is_empty());

    // Team B cannot read team A's namespace, nor the default namespace, nor write to A.
    for path in ["/v8/artifacts/events?slug=team-a", "/v8/artifacts/events"] {
        let resp = team_b.get(path).fetch().await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }
    let poison = team_b
        .post("/v8/artifacts/events?slug=team-a")
        .json(&vec![sample_event("poison")])
        .fetch()
        .await;
    assert_eq!(poison.status(), StatusCode::FORBIDDEN);

    let a_events: Vec<AnalyticsEvent> = team_a
        .get("/v8/artifacts/events?slug=team-a")
        .fetch()
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(a_events, vec![sample_event("secret-hash-a")]);
}

// A read-only token may read, query and send events, sees `readOnly` in the status, and gets a
// 403 on PUT and DELETE.
#[tokio::test]
async fn read_only_token_cannot_upload_or_delete() {
    let auth =
        AuthConfig::new(vec!["rw".into()], false).with_scoped_read_only_tokens([("team-a", "ro")]);
    let state = AppState::new(FakeStorage::new(), auth).with_allow_delete(true);
    let api = Api::serve(state).await;
    let (rw, ro) = (api.with_token("rw"), api.with_token("ro"));
    let artifact = "/v8/artifacts/hash1?slug=team-a";
    let put = rw.put(artifact).body(b"payload".to_vec()).fetch().await;
    assert_eq!(put.status(), StatusCode::ACCEPTED);

    for (token, read_only) in [(&rw, false), (&ro, true)] {
        let status: Value = token
            .get("/v8/artifacts/status?slug=team-a")
            .fetch()
            .await
            .json()
            .await
            .unwrap();
        assert_eq!(
            status,
            json!({ "status": "enabled", "readOnly": read_only })
        );
    }

    assert_eq!(ro.get(artifact).fetch().await.status(), StatusCode::OK);
    assert_eq!(ro.head(artifact).fetch().await.status(), StatusCode::OK);
    let query: HashMap<String, Value> = ro
        .post("/v8/artifacts?slug=team-a")
        .json(&json!({ "hashes": ["hash1"] }))
        .fetch()
        .await
        .json()
        .await
        .unwrap();
    assert!(query["hash1"].is_object(), "{query:?}");
    let events = ro
        .post("/v8/artifacts/events?slug=team-a")
        .json(&vec![sample_event("hash1")])
        .fetch()
        .await;
    assert_eq!(events.status(), StatusCode::OK);
    let events = ro.get("/v8/artifacts/events?slug=team-a").fetch().await;
    assert_eq!(events.status(), StatusCode::OK);

    for write in [
        ro.put(artifact).body(b"poison".to_vec()),
        ro.delete(artifact),
    ] {
        let resp = write.fetch().await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["error"]["code"], "forbidden_read_only");
    }
    let body = rw.get(artifact).fetch().await.bytes().await.unwrap();
    assert_eq!(&body[..], b"payload");
}

// Analytics fields over MAX_EVENT_FIELD_LEN bytes (here ending inside a multi-byte character)
// are rejected with a 400 and nothing of the batch is retained.
#[tokio::test]
async fn oversized_analytics_fields_are_rejected() {
    let state = tok_state(FakeStorage::new());
    let metrics = state.metrics.clone();
    let api = Api::serve(state).await.with_token("tok");

    let boundary = format!("{}é", "a".repeat(255));
    let mut long_session = sample_event("ok");
    long_session.session_id = Some(boundary.clone());
    for event in [sample_event(&boundary), long_session] {
        let resp = api
            .post("/v8/artifacts/events")
            .json(&vec![event])
            .fetch()
            .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["error"]["code"], "bad_request");
    }
    assert!(metrics.recent_events("default").is_empty());
    assert_eq!(metrics.analytics_remote_hits.get(), 0);
}

// CORS headers are on every response (success and error), not only on preflight; the preflight
// carries no Location header.
#[tokio::test]
async fn cors_headers_on_all_responses_and_no_location_on_preflight() {
    let state = tok_state(FakeStorage::new()).with_cors_allow_origin("https://app.example.com");
    let api = Api::serve(state).await.with_token("tok");

    let check = |resp: &reqwest::Response| {
        assert_eq!(
            resp.headers().get("access-control-allow-origin").unwrap(),
            "https://app.example.com",
            "status {}",
            resp.status()
        );
        let expose = resp
            .headers()
            .get("access-control-expose-headers")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(expose.contains("x-artifact-tag"));
        assert!(expose.contains("x-artifact-duration"));
    };

    let put = api
        .put("/v8/artifacts/corshash")
        .body(b"data".to_vec())
        .fetch()
        .await;
    assert_eq!(put.status(), StatusCode::ACCEPTED);
    check(&put);

    let get = api.get("/v8/artifacts/corshash").fetch().await;
    assert_eq!(get.status(), StatusCode::OK);
    check(&get);

    let unauthorized = api.anonymous().get("/v8/artifacts/corshash").fetch().await;
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
    check(&unauthorized);

    let preflight = api
        .anonymous()
        .options("/v8/artifacts/corshash?slug=acme")
        .header("Access-Control-Request-Method", "PUT")
        .fetch()
        .await;
    assert_eq!(preflight.status(), StatusCode::OK);
    assert!(preflight.headers().get("location").is_none());
    assert_eq!(
        preflight
            .headers()
            .get_all("access-control-allow-origin")
            .iter()
            .count(),
        1
    );
    check(&preflight);
}

// JSON endpoints authenticate before reading/parsing the body; parse errors use the standard
// error envelope.
#[tokio::test]
async fn json_endpoints_authenticate_before_parsing() {
    let state = tok_state(FakeStorage::new());
    let api = Api::serve(state).await;

    for path in ["/v8/artifacts", "/v8/artifacts/events"] {
        // No auth + garbage body + wrong content type: 401 envelope.
        let resp = api
            .post(path)
            .header("content-type", "text/plain")
            .body("{not json")
            .fetch()
            .await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "{path}");
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["error"]["code"], "unauthorized");

        // Authenticated garbage: 400 envelope.
        let resp = api
            .with_token("tok")
            .post(path)
            .header("content-type", "application/json")
            .body("{not json")
            .fetch()
            .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "{path}");
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["error"]["code"], "bad_request");
    }
}

// Hashes whose lookup failed or that are invalid are left out of a batch query response:
// turbo parses the whole response as `{hash: null | hit}` and resolves missing hashes with
// single HEAD requests, which report the error.
#[tokio::test]
async fn batch_query_leaves_out_failed_and_invalid_hashes() {
    let storage = Arc::new(FakeStorage {
        failing_head: Some("brokenhash"),
        ..Default::default()
    });
    let state = tok_state(storage);
    let api = Api::serve(state).await.with_token("tok");

    let resp = api
        .post("/v8/artifacts")
        .json(&json!({ "hashes": ["brokenhash", "missinghash", "bad/hash"] }))
        .fetch()
        .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body: HashMap<String, Value> = resp.json().await.unwrap();
    assert_eq!(body.len(), 1, "{body:?}");
    assert!(body["missinghash"].is_null());

    let head = api.head("/v8/artifacts/brokenhash").fetch().await;
    assert_eq!(head.status(), StatusCode::BAD_GATEWAY);
}

// turbo appends `/v8/...` to its API URL verbatim: `TURBO_API=http://host/` must work too.
#[tokio::test]
async fn repeated_slashes_in_the_path_are_collapsed() {
    let state = tok_state(FakeStorage::new());
    let api = Api::serve(state).await.with_token("tok");

    let put = api
        .put("//v8/artifacts/slashhash?slug=team")
        .body(b"payload".to_vec())
        .fetch()
        .await;
    assert_eq!(put.status(), StatusCode::ACCEPTED);

    let get = api.get("/v8/artifacts/slashhash?slug=team").fetch().await;
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(get.bytes().await.unwrap().as_ref(), b"payload");
    let status = api.get("//v8//artifacts/status").fetch().await;
    assert_eq!(status.status(), StatusCode::OK);
}

// Backend HEADs of concurrent batch queries share one server-wide limit.
#[tokio::test]
async fn batch_query_backend_heads_are_globally_bounded() {
    let storage = Arc::new(FakeStorage {
        head_delay: Duration::from_millis(20),
        ..Default::default()
    });
    let state = tok_state(storage.clone()).with_batch_head_concurrency(4);
    let api = Api::serve(state).await.with_token("tok");

    let requests = (0..3).map(|r| {
        let hashes: Vec<String> = (0..40).map(|i| format!("h{r}x{i}")).collect();
        let req = api.post("/v8/artifacts").json(&json!({ "hashes": hashes }));
        async move { req.fetch().await.status() }
    });
    for status in futures_util::future::join_all(requests).await {
        assert_eq!(status, StatusCode::OK);
    }
    assert_eq!(storage.heads.load(Ordering::SeqCst), 120);
    let max = storage.max_heads_in_flight.load(Ordering::SeqCst);
    assert!(max <= 4, "max concurrent backend HEADs was {max}");
}

/// `backend` behind a hot cache whose item limit (1 KiB) is below the 10 KB test artifacts, so
/// those are streamed through instead of buffered.
fn small_hot_cache(backend: Arc<dyn Storage>) -> Arc<dyn Storage> {
    let options = CacheOptions {
        max_bytes: 1 << 20,
        max_entries: 100,
        max_item_bytes: 1024,
        metadata_max_entries: 100,
        ttl: None,
        negative_ttl: None,
    };
    Arc::new(TieredStorage::with_options(backend, options))
}

fn unknown_length_backend(payloads: Payloads) -> (Arc<FakeStorage>, Vec<u8>) {
    let storage = Arc::new(FakeStorage {
        payloads,
        ..Default::default()
    });
    let payload: Vec<u8> = (0..10_000u32).map(|i| (i % 251) as u8).collect();
    let tagged = |len: usize| ArtifactMetadata {
        duration: 1,
        size: len as u64,
        tag: Some("tag".into()),
        ..Default::default()
    };
    storage.insert("bigartifact", &payload, tagged(payload.len()));
    storage.insert("smallartifact", &payload[..100], tagged(100));
    (storage, payload)
}

/// A backend response without a length is sent chunked, without `Content-Length`: directly and
/// through the hot cache (streamed through, as it exceeds the hot item limit). An artifact the
/// hot cache buffers gets its exact length.
#[tokio::test]
async fn unknown_length_artifact_is_sent_chunked() {
    let (storage, payload) = unknown_length_backend(Payloads::UnknownLengthStream);
    let backend: Arc<dyn Storage> = storage;
    let tiered = small_hot_cache(backend.clone());
    for (name, backend) in [("backend", backend), ("tiered", tiered)] {
        let api = Api::serve(tok_state(backend)).await.with_token("tok");
        let resp = api.get("/v8/artifacts/bigartifact").fetch().await;
        assert_eq!(resp.status(), StatusCode::OK, "{name}");
        assert!(
            resp.headers().get("content-length").is_none(),
            "{name}: unexpected Content-Length {:?}",
            resp.headers().get("content-length")
        );
        assert_eq!(
            resp.headers().get("transfer-encoding").unwrap(),
            "chunked",
            "{name}"
        );
        assert_eq!(resp.headers()["x-artifact-tag"], "tag", "{name}");
        assert_eq!(resp.bytes().await.unwrap().as_ref(), &payload[..], "{name}");

        if name == "tiered" {
            // Small enough for the hot cache: buffered, so the exact length is known.
            for _ in 0..2 {
                let resp = api.get("/v8/artifacts/smallartifact").fetch().await;
                assert_eq!(resp.headers()["content-length"], "100");
                assert_eq!(resp.bytes().await.unwrap().as_ref(), &payload[..100]);
            }
        }
    }
}

/// A GET whose backend reports no length takes a hand-off path (a stream longer than the
/// announced size 0, or a claimed size above the hot item limit). That size is unverified and
/// must not be cached: `HEAD` and the batch query still report the backend's exact size.
#[tokio::test]
async fn unverified_size_from_a_get_is_not_cached() {
    for payloads in [
        Payloads::UnknownLengthStream,
        Payloads::ClaimedSizeStream(5000),
    ] {
        let (storage, payload) = unknown_length_backend(payloads);
        let tiered = small_hot_cache(storage);
        let api = Api::serve(tok_state(tiered)).await.with_token("tok");

        let resp = api.get("/v8/artifacts/bigartifact").fetch().await;
        assert_eq!(resp.status(), StatusCode::OK, "{payloads:?}");
        assert!(resp.headers().get("content-length").is_none());
        assert_eq!(resp.bytes().await.unwrap().as_ref(), &payload[..]);

        let resp = api.head("/v8/artifacts/bigartifact").fetch().await;
        assert_eq!(resp.status(), StatusCode::OK, "{payloads:?}");
        assert_eq!(resp.headers()["content-length"], "10000", "{payloads:?}");
        let body: Value = api
            .post("/v8/artifacts")
            .json(&json!({ "hashes": ["bigartifact"] }))
            .fetch()
            .await
            .json()
            .await
            .unwrap();
        assert_eq!(body["bigartifact"]["size"], json!(10_000), "{payloads:?}");
    }
}

// Large uploads reach storage as a spooled file (RAM use not proportional to the body), small
// ones stay in RAM; the spool file is gone after the request.
#[tokio::test]
async fn large_upload_is_spooled_to_disk() {
    let spool = tempfile::tempdir().unwrap();
    let storage = Arc::new(FakeStorage {
        put_outcome: Some(PutOutcome::Stored),
        ..Default::default()
    });
    let state = tok_state(storage.clone()).with_server_options(ServerOptions {
        upload_memory_threshold: 1024,
        ..options(spool.path())
    });
    let api = Api::serve(state).await.with_token("tok");

    // Known length (Content-Length) above the threshold.
    let big = vec![7u8; 256 * 1024];
    let resp = api
        .put("/v8/artifacts/bighash")
        .body(big.clone())
        .fetch()
        .await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    // Unknown length (chunked) above the threshold.
    let chunks: Vec<Result<Vec<u8>, std::io::Error>> =
        (0..64).map(|_| Ok(vec![1u8; 4096])).collect();
    let resp = api
        .put("/v8/artifacts/chunkedhash")
        .body(reqwest::Body::wrap_stream(stream::iter(chunks)))
        .fetch()
        .await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    let resp = api
        .put("/v8/artifacts/smallhash")
        .body(vec![1u8; 100])
        .fetch()
        .await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);

    let puts = storage.put_log.lock().unwrap().clone();
    assert_eq!(puts.len(), 3);
    assert_eq!(puts[0].hash, "bighash");
    assert_eq!(puts[0].spooled_file_existed, Some(true));
    assert_eq!(puts[0].len, big.len() as u64);
    assert_eq!(puts[1].spooled_file_existed, Some(true));
    assert_eq!(puts[1].len, 64 * 4096);
    assert_eq!(puts[2].spooled_file_existed, None);
    assert_eq!(puts[2].len, 100);
    assert_eq!(std::fs::read_dir(spool.path()).unwrap().count(), 0);
}

// Immutable mode uses the atomic `PutMode::IfAbsent` (no separate, racy existence check) and
// maps `AlreadyExists` to 403 `artifact_immutable`.
#[tokio::test]
async fn immutable_mode_uses_atomic_if_absent_put() {
    let storage = Arc::new(FakeStorage {
        put_outcome: Some(PutOutcome::AlreadyExists),
        ..Default::default()
    });
    let state = tok_state(storage.clone()).with_immutable_artifacts(true);
    let api = Api::serve(state).await.with_token("tok");

    let resp = api
        .put("/v8/artifacts/immutablehash?slug=acme")
        .body(b"data".to_vec())
        .fetch()
        .await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["error"]["code"], "artifact_immutable");

    let puts = storage.put_log.lock().unwrap().clone();
    assert_eq!(puts.len(), 1);
    assert_eq!(puts[0].mode, PutMode::IfAbsent);
    assert_eq!(puts[0].namespace, "acme");
    assert_eq!(storage.heads.load(Ordering::SeqCst), 0);
}

// Request ids are generated (or a well-formed client value is kept) and echoed.
#[tokio::test]
async fn request_ids_are_generated_and_propagated() {
    let state = tok_state(FakeStorage::new());
    let api = Api::serve(state).await;

    let resp = api.get("/healthz").fetch().await;
    let generated = resp
        .headers()
        .get("x-request-id")
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(generated.len(), 32);

    let resp = api
        .get("/healthz")
        .header("x-request-id", "client-req-42")
        .fetch()
        .await;
    assert_eq!(resp.headers().get("x-request-id").unwrap(), "client-req-42");

    let resp = api
        .get("/healthz")
        .header("x-request-id", "bad id with spaces")
        .fetch()
        .await;
    assert_ne!(
        resp.headers().get("x-request-id").unwrap(),
        "bad id with spaces"
    );
}

// Storage calls that reach the backend are recorded in the latency histogram and error counter.
#[tokio::test]
async fn metrics_record_backend_latency_and_errors() {
    let storage = Arc::new(FakeStorage {
        failing_head: Some("brokenhash"),
        ..Default::default()
    });
    let api = Api::serve(tok_state(storage)).await.with_token("tok");

    let head = api.head("/v8/artifacts/brokenhash").fetch().await;
    assert_eq!(head.status(), StatusCode::BAD_GATEWAY);

    let text = api
        .anonymous()
        .get("/metrics")
        .fetch()
        .await
        .text()
        .await
        .unwrap();
    for needle in [
        "turbo_cache_storage_operation_duration_seconds_count{backend=\"fake\",operation=\"head\"} 1",
        "turbo_cache_storage_errors_total{backend=\"fake\",operation=\"head\"} 1",
        "turbo_cache_uploads_in_flight{backend=\"fake\"} 0",
    ] {
        assert!(text.contains(needle), "missing `{needle}` in:\n{text}");
    }
}

/// Reads one complete HTTP/1 response (head plus `Content-Length` body) from `stream`.
async fn read_one_response(stream: &mut TcpStream) -> String {
    let mut response = Vec::new();
    let read = tokio::time::timeout(Duration::from_secs(10), async {
        let mut buf = [0u8; 4096];
        loop {
            let text = String::from_utf8_lossy(&response);
            if let Some(head_end) = text.find("\r\n\r\n") {
                let length = text[..head_end]
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap_or(0);
                if response.len() >= head_end + 4 + length {
                    break;
                }
            }
            match stream.read(&mut buf).await {
                Ok(0) => panic!("connection closed: {text}"),
                Ok(n) => response.extend_from_slice(&buf[..n]),
                Err(err) => panic!("reading the response failed ({err}): {text}"),
            }
        }
    })
    .await;
    assert!(read.is_ok(), "no complete response");
    String::from_utf8_lossy(&response).into_owned()
}

/// Serves `build_router(state)` with the production server
/// ([`fast_turbo_cache_server::server::serve`]).
async fn serve_production(state: AppState) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = build_router(state);
    tokio::spawn(async move {
        let options = ServerOptions::default();
        fast_turbo_cache_server::server::serve(listener, app, &options, std::future::pending())
            .await
    });
    addr
}

// A request rejected before its body was read (413 on Content-Length, 401) must not reset the
// connection of a client still sending: the unread body is drained (bounded by time) while the
// response is sent, and a drained connection stays usable. The 64 MiB bodies are larger than any
// socket buffer, so a byte-capped drain fails this test deterministically.
#[tokio::test]
async fn early_rejection_does_not_reset_a_client_still_sending_its_body() {
    let addr = serve_production(tok_state(FakeStorage::new()).with_max_artifact_size(1024)).await;
    for (token, expected, code) in [
        ("tok", "HTTP/1.1 413", "payload_too_large"),
        ("wrong", "HTTP/1.1 401", "unauthorized"),
    ] {
        for body_len in [140 * 1024, 1024 * 1024, 64 * 1024 * 1024] {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            let head = format!(
                "PUT /v8/artifacts/bighash HTTP/1.1\r\nHost: localhost\r\n\
                 Authorization: Bearer {token}\r\nContent-Length: {body_len}\r\n\r\n"
            );
            stream.write_all(head.as_bytes()).await.unwrap();
            // Give the server time to answer and give up on the request.
            tokio::time::sleep(Duration::from_millis(200)).await;
            stream
                .write_all(&vec![0u8; body_len])
                .await
                .unwrap_or_else(|err| {
                    panic!("{token}/{body_len}: sending the rest of the body failed: {err}")
                });
            let response = read_one_response(&mut stream).await;
            assert!(response.starts_with(expected), "{body_len}: {response}");
            assert!(response.contains(code), "{response}");

            // The body was drained completely, so the connection is still in sync.
            stream
                .write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n")
                .await
                .unwrap();
            let next = read_one_response(&mut stream).await;
            assert!(next.starts_with("HTTP/1.1 200"), "{next}");
        }
    }
}

// A JSON body rejected while it is read (over MAX_JSON_BODY_SIZE) is drained like an unread one:
// the client gets the 413 and the connection stays usable. Without the drain, sending the 64 MiB
// body fails.
#[tokio::test]
async fn oversized_json_body_is_drained_after_the_413() {
    let addr = serve_production(tok_state(FakeStorage::new())).await;
    let body_len = 64 * 1024 * 1024;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let head = format!(
        "POST /v8/artifacts/events HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer tok\r\n\
         Content-Type: application/json\r\nContent-Length: {body_len}\r\n\r\n"
    );
    stream.write_all(head.as_bytes()).await.unwrap();
    stream
        .write_all(&vec![b'a'; body_len])
        .await
        .unwrap_or_else(|err| panic!("sending the rest of the body failed: {err}"));
    let response = read_one_response(&mut stream).await;
    assert!(response.starts_with("HTTP/1.1 413"), "{response}");
    stream
        .write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let next = read_one_response(&mut stream).await;
    assert!(next.starts_with("HTTP/1.1 200"), "{next}");
}

// turbo's HTTP client (reqwest) receives the 401/403/413 of a large rejected PUT instead of
// `connection reset by peer`.
#[tokio::test]
async fn reqwest_receives_early_rejections_of_large_puts() {
    let state = AppState::new(
        FakeStorage::new(),
        AuthConfig::new(vec!["tok".into()], false).with_scoped_tokens([("team-a", "tok-a")]),
    )
    .with_max_artifact_size(1024 * 1024);
    let base = format!("http://{}", serve_production(state).await);
    let huge = bytes::Bytes::from(vec![0u8; 100 * 1024 * 1024]);
    for _ in 0..3 {
        for (token, slug, expected) in [
            ("wrong", "acme", StatusCode::UNAUTHORIZED),
            ("tok-a", "team-b", StatusCode::FORBIDDEN),
            ("tok", "acme", StatusCode::PAYLOAD_TOO_LARGE),
        ] {
            // A new client (new connection) for each request: the reset hit mostly the first
            // request of a connection, which pushes the most data before reading the response.
            let response = reqwest::Client::new()
                .put(format!("{base}/v8/artifacts/abc123?slug={slug}"))
                .bearer_auth(token)
                .body(huge.clone())
                .send()
                .await
                .unwrap_or_else(|err| panic!("{expected}: transport error {err:?}"));
            assert_eq!(response.status(), expected);
        }
    }
}

// The drain is bounded by UNREAD_BODY_DRAIN_TIMEOUT: a client that keeps a rejected body
// coming (here: trickling) has its connection closed once the timeout is reached.
#[tokio::test]
async fn unread_body_drain_is_bounded() {
    let state = tok_state(FakeStorage::new()).with_max_artifact_size(1024);
    let (_, addr) = spawn_server(build_router(state)).await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let declared = 1024 * 1024 * 1024;
    let head = format!(
        "PUT /v8/artifacts/bighash HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer tok\r\n\
         Content-Length: {declared}\r\n\r\n"
    );
    stream.write_all(head.as_bytes()).await.unwrap();
    let response = read_response(&mut stream, Duration::from_secs(5)).await;
    assert!(response.starts_with("HTTP/1.1 413"), "{response}");
    let rejected = std::time::Instant::now();
    // Keep the body coming: the server reads it until the drain timeout, then closes.
    let (mut reader, mut writer) = stream.into_split();
    let trickle = tokio::spawn(async move {
        while writer.write_all(&[0u8; 1024]).await.is_ok() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
    let timeout = fast_turbo_cache_server::routes::UNREAD_BODY_DRAIN_TIMEOUT;
    let mut rest = Vec::new();
    let closed = tokio::time::timeout(timeout * 3, reader.read_to_end(&mut rest)).await;
    trickle.abort();
    assert!(closed.is_ok(), "the connection was not closed");
    let elapsed = rejected.elapsed();
    assert!(
        elapsed >= timeout - Duration::from_millis(500),
        "closed before the drain timeout ({elapsed:?})"
    );
}

// Expect: 100-continue clients have not sent the body when the request is rejected; it must not
// be drained (that would make hyper send `100 Continue` and the client upload it for nothing).
#[tokio::test]
async fn expect_continue_body_is_not_requested_after_rejection() {
    let state = tok_state(FakeStorage::new());
    let (_, addr) = spawn_server(build_router(state)).await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            b"PUT /v8/artifacts/somehash HTTP/1.1\r\nHost: localhost\r\n\
              Content-Length: 100\r\nExpect: 100-continue\r\n\r\n",
        )
        .await
        .unwrap();
    let mut response = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response)).await;
    let text = String::from_utf8_lossy(&response);
    assert!(
        text.starts_with("HTTP/1.1 401"),
        "unexpected response: {text}"
    );
    assert!(
        !text.contains("100 Continue"),
        "unexpected 100 Continue: {text}"
    );
}

// A tenant trickling uploads holds at most its per-namespace share of the upload slots, and
// namespaces linked by one scoped token (`team-a=tok-ac,team-c=tok-ac`) share one allowance.
// Other tenants still get slots without waiting.
#[tokio::test]
async fn one_token_cannot_hold_every_upload_slot() {
    let spool = tempfile::tempdir().unwrap();
    let state = AppState::new(
        FakeStorage::new(),
        AuthConfig::new(vec![], false).with_scoped_tokens([
            ("team-a", "tok-ac"),
            ("team-c", "tok-ac"),
            ("team-b", "tok-b"),
        ]),
    )
    .with_max_concurrent_uploads(4)
    .with_max_uploads_per_namespace(2)
    .with_server_options(ServerOptions {
        body_idle_timeout: Duration::from_secs(30),
        upload_slot_wait: Duration::from_millis(300),
        ..options(spool.path())
    });
    let metrics = state.metrics.clone();
    let api = Api::serve(state).await.with_token("tok-b");
    let addr = api.addr();

    // tok-ac trickles one upload into each of its namespaces: its whole share ...
    let _a = start_stalled_upload(addr, "team-a", "tok-ac").await;
    let _c = start_stalled_upload(addr, "team-c", "tok-ac").await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(metrics.uploads_in_flight.get(), 2);

    // ... so a further upload to either namespace waits and gets 429 + Retry-After instead of
    // taking the remaining server-wide slots ...
    for slug in ["team-c", "team-a"] {
        let mut extra = start_stalled_upload(addr, slug, "tok-ac").await;
        let response = read_response(&mut extra, Duration::from_secs(5)).await;
        assert!(response.starts_with("HTTP/1.1 429"), "{slug}: {response}");
        assert!(response.to_ascii_lowercase().contains("retry-after: 1"));
    }
    assert_eq!(metrics.uploads_in_flight.get(), 2);

    // ... and the other tenant still uploads without waiting.
    let resp = api
        .put("/v8/artifacts/otherhash?slug=team-b")
        .body(b"payload".to_vec())
        .fetch()
        .await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
}
