//! End-to-end tests through the HTTP API: the Turborepo client flow on every storage backend,
//! guarantees that need the routes and a real backend together, and the access controls and
//! limits of a configured server.

use std::{collections::HashMap, sync::Arc};

use axum::http::StatusCode;
use fast_turbo_cache_server::{
    AppState, AuthConfig, CacheOptions, CliConfig, LocalOptions, Storage,
    models::{
        AnalyticsEvent, ArtifactQueryHit, ArtifactQueryRequest, CacheEvent, CacheSource,
        CachingStatus, CachingStatusResponse,
    },
    storage::{cache::TieredStorage, local::LocalStorage, s3::S3Storage},
};
use serde_json::Value;

use crate::support::{
    Api, Fetch,
    fake_storage::FakeStorage,
    mock_s3::{Mock, spawn_mock},
};

const TOKEN: &str = "tok";
const HASH: &str = "7a8b9c0d1e2f3a4b5c6d";
const PAYLOAD: &[u8] = b"turborepo-tar-zst-archive-payload-bytes";
const SHA: &str = "9c9236efb673e9c2ebffd82940f419a168a84bd3";

async fn local_storage(dir: &std::path::Path) -> Arc<LocalStorage> {
    Arc::new(
        LocalStorage::with_options(dir, LocalOptions::default())
            .await
            .unwrap(),
    )
}

/// What `turbo` does with a remote cache: readiness and status checks, then upload, query,
/// download and delete one artifact with all its metadata headers in namespace `acme`.
async fn turborepo_lifecycle(api: &Api, backend: &str) {
    let ready = api.anonymous().get("/readyz").fetch().await;
    assert_eq!(ready.status(), StatusCode::OK);
    let ready: Value = ready.json().await.unwrap();
    assert_eq!(ready["backend"], backend);

    let status = api.get("/v8/artifacts/status?slug=acme").fetch().await;
    assert_eq!(status.status(), StatusCode::OK);
    let status: CachingStatusResponse = status.json().await.unwrap();
    assert_eq!(status.status, CachingStatus::Enabled);

    let put = api
        .put(format!("/v8/artifacts/{HASH}?slug=acme"))
        .header("Content-Type", "application/octet-stream")
        .header("x-artifact-duration", "1450")
        .header("x-artifact-tag", "hmac-sha256-signature-tag=")
        .header("x-artifact-sha", SHA)
        .header("x-artifact-dirty-hash", "dirty12345")
        .header("x-artifact-client-ci", "GITHUB_ACTIONS")
        .body(PAYLOAD)
        .fetch()
        .await;
    assert_eq!(put.status(), StatusCode::ACCEPTED);

    let other_team = api
        .head(format!("/v8/artifacts/{HASH}?slug=other-team"))
        .fetch()
        .await;
    assert_eq!(other_team.status(), StatusCode::NOT_FOUND);

    let assert_metadata_headers = |resp: &reqwest::Response| {
        assert_eq!(resp.status(), StatusCode::OK);
        let headers = resp.headers();
        assert_eq!(headers["x-artifact-duration"], "1450");
        assert_eq!(headers["x-artifact-tag"], "hmac-sha256-signature-tag=");
        assert_eq!(headers["x-artifact-sha"], SHA);
        assert_eq!(headers["x-artifact-dirty-hash"], "dirty12345");
    };
    let head = api
        .head(format!("/v8/artifacts/{HASH}?slug=acme"))
        .fetch()
        .await;
    assert_metadata_headers(&head);

    let batch: HashMap<String, Option<ArtifactQueryHit>> = api
        .post("/v8/artifacts?slug=acme")
        .json(&ArtifactQueryRequest {
            hashes: vec![HASH.to_string(), "missinghash".to_string()],
        })
        .fetch()
        .await
        .json()
        .await
        .unwrap();
    let hit = batch[HASH].as_ref().unwrap();
    assert_eq!(hit.task_duration_ms, 1450);
    assert_eq!(hit.size, PAYLOAD.len() as u64);
    assert_eq!(hit.tag.as_deref(), Some("hmac-sha256-signature-tag="));
    assert_eq!(hit.sha.as_deref(), Some(SHA));
    assert_eq!(hit.dirty_hash.as_deref(), Some("dirty12345"));
    assert!(batch["missinghash"].is_none());

    let get = api
        .get(format!("/v8/artifacts/{HASH}?slug=acme"))
        .fetch()
        .await;
    assert_metadata_headers(&get);
    assert_eq!(get.headers()["content-length"], PAYLOAD.len().to_string());
    assert_eq!(get.bytes().await.unwrap().as_ref(), PAYLOAD);

    let delete = api
        .delete(format!("/v8/artifacts/{HASH}?slug=acme"))
        .fetch()
        .await;
    assert_eq!(delete.status(), StatusCode::OK);
    let gone = api
        .get(format!("/v8/artifacts/{HASH}?slug=acme"))
        .fetch()
        .await;
    assert_eq!(gone.status(), StatusCode::NOT_FOUND);
}

async fn serve_with_delete(storage: Arc<dyn Storage>) -> Api {
    let state =
        AppState::new(storage, AuthConfig::new(vec![TOKEN.into()], false)).with_allow_delete(true);
    Api::serve(state).await.with_token(TOKEN)
}

#[tokio::test]
async fn lifecycle_on_local_storage() {
    let dir = tempfile::tempdir().unwrap();
    let api = serve_with_delete(local_storage(dir.path()).await).await;

    // `turbo --preflight` sends an OPTIONS request before every artifact request.
    let preflight = api
        .anonymous()
        .options(format!("/v8/artifacts/{HASH}?slug=acme"))
        .header("Access-Control-Request-Method", "PUT")
        .header(
            "Access-Control-Request-Headers",
            "Authorization, Content-Type, x-artifact-duration, x-artifact-tag",
        )
        .fetch()
        .await;
    assert_eq!(preflight.status(), StatusCode::OK);
    let allow_headers = preflight.headers()["access-control-allow-headers"]
        .to_str()
        .unwrap()
        .to_ascii_lowercase();
    assert!(allow_headers.contains("authorization"), "{allow_headers}");

    turborepo_lifecycle(&api, "local").await;

    // Client-reported analytics feed the time-saved counter next to the server's own counters.
    let events = vec![AnalyticsEvent {
        session_id: Some("sess-1".into()),
        source: CacheSource::Remote,
        event: CacheEvent::Hit,
        hash: HASH.to_string(),
        duration: 1450,
    }];
    let posted = api
        .post("/v8/artifacts/events?slug=acme")
        .json(&events)
        .fetch()
        .await;
    assert_eq!(posted.status(), StatusCode::OK);
    let metrics = api
        .anonymous()
        .get("/metrics")
        .fetch()
        .await
        .text()
        .await
        .unwrap();
    for line in [
        "turbo_cache_get_hits_total{backend=\"local\"} 1",
        "turbo_cache_puts_total{backend=\"local\"} 1",
        "turbo_cache_time_saved_seconds_total{backend=\"local\"} 1.45",
    ] {
        assert!(metrics.contains(line), "missing `{line}` in:\n{metrics}");
    }
}

/// An S3 backend of preset `storage_type`, configured through the CLI as a user would, against
/// a mock S3 server that verifies SigV4 signatures for `region`.
async fn s3_preset(storage_type: &str, region: &str, extra_args: &[&str]) -> Arc<S3Storage> {
    let mock = Mock::with_identity("turbo-bucket", region, "test-access-key", "test-secret");
    let endpoint = spawn_mock(&mock).await;
    let args = [
        "fast-turbo-cache-server",
        "--storage-type",
        storage_type,
        "--s3-bucket",
        "turbo-bucket",
        "--s3-endpoint",
        &endpoint,
        "--s3-access-key-id",
        "test-access-key",
        "--s3-secret-access-key",
        "test-secret",
        "--tokens",
        TOKEN,
    ];
    let cli = CliConfig::try_parse_from_ignoring_env(args.iter().chain(extra_args).copied());
    let s3 = cli.unwrap().resolve_s3_config_with_env(&|_| None).unwrap();
    Arc::new(S3Storage::new(s3).unwrap())
}

/// Garage: region `garage` and path-style addressing from the preset.
#[tokio::test]
async fn lifecycle_on_garage() {
    let storage = s3_preset("garage", "garage", &["--storage-prefix", "turborepo"]).await;
    turborepo_lifecycle(&serve_with_delete(storage).await, "garage").await;
}

/// RustFS: region `us-east-1` and path-style addressing from the preset, with signed payloads.
#[tokio::test]
async fn lifecycle_on_rustfs() {
    let storage = s3_preset("rustfs", "us-east-1", &["--s3-sign-payload"]).await;
    turborepo_lifecycle(&serve_with_delete(storage).await, "rustfs").await;
}

/// Many concurrent immutable PUTs of one hash with different bodies, on every backend: exactly
/// one is accepted (202), all others get 403 `artifact_immutable`, and the stored body and tag
/// are the winner's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_immutable_puts_have_exactly_one_winner() {
    let dir = tempfile::tempdir().unwrap();
    let backends: Vec<(&str, Arc<dyn Storage>)> = vec![
        ("local", local_storage(&dir.path().join("plain")).await),
        (
            "tiered-local",
            Arc::new(TieredStorage::with_options(
                local_storage(&dir.path().join("tiered")).await,
                CacheOptions::default(),
            )),
        ),
        ("s3", s3_preset("s3", "us-east-1", &[]).await),
    ];
    for (name, storage) in backends {
        let state = AppState::new(storage, AuthConfig::new(vec![TOKEN.into()], false))
            .with_immutable_artifacts(true)
            .with_max_concurrent_uploads(64);
        let api = Api::serve(state).await.with_token(TOKEN);
        for round in 0..5 {
            let path = format!("/v8/artifacts/immutable{round}?slug=acme");
            let puts = (0..16).map(|i| {
                let request = api
                    .put(&path)
                    .header("x-artifact-tag", format!("tag-{i}"))
                    .body(format!("body-{i}").repeat(64));
                async move {
                    let resp = request.fetch().await;
                    let status = resp.status();
                    let json: Value = resp.json().await.unwrap();
                    (i, status, json)
                }
            });
            let results = futures_util::future::join_all(puts).await;
            let winners: Vec<_> = results
                .iter()
                .filter(|(_, status, _)| *status == StatusCode::ACCEPTED)
                .map(|(i, _, _)| *i)
                .collect();
            assert_eq!(winners.len(), 1, "{name} round {round}: {results:?}");
            for (_, status, json) in &results {
                if *status != StatusCode::ACCEPTED {
                    assert_eq!(*status, StatusCode::FORBIDDEN, "{name}: {json}");
                    assert_eq!(json["error"]["code"], "artifact_immutable", "{name}");
                }
            }
            let winner = winners[0];
            let resp = api.get(&path).fetch().await;
            assert_eq!(resp.status(), StatusCode::OK, "{name}");
            let tag = resp.headers()["x-artifact-tag"]
                .to_str()
                .unwrap()
                .to_owned();
            assert_eq!(tag, format!("tag-{winner}"), "{name}");
            let body = resp.text().await.unwrap();
            assert_eq!(body, format!("body-{winner}").repeat(64), "{name}");
        }
    }
}

/// `/metrics` reports the real hot-cache counters of `TieredStorage`, counted once per request.
#[tokio::test]
async fn hot_cache_statistics_are_exported_to_metrics() {
    let dir = tempfile::tempdir().unwrap();
    let tiered = Arc::new(TieredStorage::with_options(
        local_storage(dir.path()).await,
        CacheOptions {
            max_bytes: 1 << 20,
            max_entries: 100,
            max_item_bytes: 64 << 10,
            metadata_max_entries: 100,
            ttl: None,
            negative_ttl: None,
        },
    ));
    let state = AppState::new(tiered, AuthConfig::new(vec![TOKEN.into()], false));
    let api = Api::serve(state).await.with_token(TOKEN);

    // One cached PUT, two GET hits, one GET miss, one HEAD hit, one HEAD miss.
    let put = api
        .put("/v8/artifacts/metricshash")
        .body(vec![9u8; 1000])
        .fetch()
        .await;
    assert_eq!(put.status(), StatusCode::ACCEPTED);
    for hash in ["metricshash", "metricshash", "missinghash"] {
        api.get(format!("/v8/artifacts/{hash}")).fetch().await;
    }
    for hash in ["metricshash", "missinghash"] {
        api.head(format!("/v8/artifacts/{hash}")).fetch().await;
    }

    let resp = api.anonymous().get("/metrics").fetch().await;
    assert_eq!(resp.status(), StatusCode::OK);
    let text = resp.text().await.unwrap();
    for line in [
        "turbo_cache_hot_cache_hits_total{backend=\"local\",tier=\"payload\"} 2",
        "turbo_cache_hot_cache_misses_total{backend=\"local\",tier=\"payload\"} 1",
        "turbo_cache_hot_cache_hits_total{backend=\"local\",tier=\"metadata\"} 1",
        "turbo_cache_hot_cache_misses_total{backend=\"local\",tier=\"metadata\"} 1",
        "turbo_cache_hot_cache_entries{backend=\"local\",tier=\"payload\"} 1",
        "turbo_cache_hot_cache_bytes{backend=\"local\",tier=\"payload\"} 1000",
        "turbo_cache_hot_cache_max_bytes{backend=\"local\",tier=\"payload\"} 1048576",
        "turbo_cache_hot_cache_max_entries{backend=\"local\",tier=\"metadata\"} 100",
        "turbo_cache_hot_cache_evictions_total{backend=\"local\",tier=\"payload\"} 0",
        "turbo_cache_hot_cache_expirations_total{backend=\"local\",tier=\"payload\"} 0",
    ] {
        assert!(
            text.lines().any(|l| l == line),
            "missing `{line}` in:\n{text}"
        );
    }
}

#[tokio::test]
async fn caching_disabled_and_request_limits() {
    let storage = FakeStorage::new();
    let auth = AuthConfig::new(vec![TOKEN.into()], false);
    let state = AppState::new(storage.clone(), auth.clone()).with_max_artifact_size(16);
    let api = Api::serve(state).await.with_token(TOKEN);

    let oversized = api.put("/v8/artifacts/hash123").body(vec![0u8; 17]).fetch();
    assert_eq!(oversized.await.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let at_limit = api.put("/v8/artifacts/hash123").body(vec![0u8; 16]).fetch();
    assert_eq!(at_limit.await.status(), StatusCode::ACCEPTED);

    let hashes: Vec<String> = (0..1001).map(|i| format!("hash{i}")).collect();
    let batch = api
        .post("/v8/artifacts")
        .json(&ArtifactQueryRequest { hashes })
        .fetch()
        .await;
    assert_eq!(batch.status(), StatusCode::BAD_REQUEST);
    // A full batch of maximum-length hashes fits the batch query body limit; a larger body
    // is rejected before it is parsed.
    let hashes: Vec<String> = (0..1000).map(|i| format!("{i:0>128}")).collect();
    let batch = api
        .post("/v8/artifacts")
        .json(&ArtifactQueryRequest { hashes });
    assert_eq!(batch.fetch().await.status(), StatusCode::OK);
    let hashes = vec!["a".to_string(); 64 * 1024];
    let batch = api
        .post("/v8/artifacts")
        .json(&ArtifactQueryRequest { hashes });
    assert_eq!(batch.fetch().await.status(), StatusCode::PAYLOAD_TOO_LARGE);

    let events: Vec<AnalyticsEvent> = (0..1001)
        .map(|i| AnalyticsEvent {
            session_id: None,
            source: CacheSource::Remote,
            event: CacheEvent::Hit,
            hash: format!("hash{i}"),
            duration: 1,
        })
        .collect();
    let events = api.post("/v8/artifacts/events").json(&events).fetch().await;
    assert_eq!(events.status(), StatusCode::BAD_REQUEST);

    // Turborepo recognises the `remote_caching_disabled` code and stops using the cache.
    let disabled = AppState::new(storage, auth).with_caching_status(CachingStatus::Disabled);
    let disabled = Api::serve(disabled).await.with_token(TOKEN);
    let put = disabled
        .put("/v8/artifacts/hash123")
        .body(vec![1u8; 4])
        .fetch()
        .await;
    assert_eq!(put.status(), StatusCode::FORBIDDEN);
    let body: Value = put.json().await.unwrap();
    assert_eq!(body["error"]["code"], "remote_caching_disabled");
}

#[tokio::test]
async fn security_controls() {
    let auth = AuthConfig::new(vec!["global-admin-token".into()], false)
        .with_scoped_tokens([("acme", "acme-only-token")]);
    let debug_auth = format!("{auth:?}");
    assert!(!debug_auth.contains("global-admin-token"), "{debug_auth}");
    assert!(!debug_auth.contains("acme-only-token"), "{debug_auth}");

    let state = AppState::new(FakeStorage::new(), auth)
        .with_allow_delete(false)
        .with_protect_observability(true)
        .with_cors_allow_origin("https://turbo.example.com");
    let anonymous = Api::serve(state).await;
    let acme = anonymous.with_token("acme-only-token");
    let admin = anonymous.with_token("global-admin-token");

    assert_eq!(
        anonymous.get("/metrics").fetch().await.status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        anonymous.get("/readyz").fetch().await.status(),
        StatusCode::UNAUTHORIZED
    );
    // Metrics aggregate all tenants: a namespace-scoped token is not enough.
    assert_eq!(
        acme.get("/metrics").fetch().await.status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(admin.get("/metrics").fetch().await.status(), StatusCode::OK);

    let put = acme
        .put("/v8/artifacts/hash_sec_1?slug=acme")
        .body(b"acme-payload".to_vec())
        .fetch()
        .await;
    assert_eq!(put.status(), StatusCode::ACCEPTED);
    let cross_team = acme
        .get("/v8/artifacts/hash_sec_1?slug=other-team")
        .fetch()
        .await;
    assert_eq!(cross_team.status(), StatusCode::FORBIDDEN);
    let body: Value = cross_team.json().await.unwrap();
    assert_eq!(body["error"]["code"], "forbidden_namespace");

    let delete = admin
        .delete("/v8/artifacts/hash_sec_1?slug=acme")
        .fetch()
        .await;
    assert_eq!(delete.status(), StatusCode::FORBIDDEN);
    let body: Value = delete.json().await.unwrap();
    assert_eq!(body["error"]["code"], "delete_disabled");

    let preflight = anonymous
        .options("/v8/artifacts/hash_sec_1?slug=acme")
        .fetch()
        .await;
    let headers = preflight.headers();
    assert_eq!(
        headers["access-control-allow-origin"],
        "https://turbo.example.com"
    );
    let methods = headers["access-control-allow-methods"].to_str().unwrap();
    assert!(!methods.contains("DELETE"), "{methods}");
}
