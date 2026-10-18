//! OIDC token exchange against an in-process mock issuer (discovery document and JWKS) with ID
//! tokens signed by a fixed test key: verification failures, claim rules, and access with the
//! minted cache tokens.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{Json, Router, http::StatusCode, routing::get};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use fast_turbo_cache_server::{
    AppState, AuthConfig,
    oidc::{CacheTokenKey, ClaimRules, OidcConfig, OidcExchange},
};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde_json::{Value, json};

use crate::support::{Api, Fetch, fake_storage::FakeStorage, tok_state};

/// EC P-256 test key of the mock issuer (generated for these tests only).
const ISSUER_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgP9aZesLMcPRQfj1K
ABJUoBF8aSDoDbNe+UBXCVNyYFShRANCAAQ8j3NLAtryxWxiLWiVOKgP02L7skfH
loVUj4X+ql3Mtl6xGRHfDoFx3PPsSFxKqhtWwfYLl9mkbcwRvAxuptz4
-----END PRIVATE KEY-----";
/// Public point of [`ISSUER_KEY_PEM`] (base64url).
const ISSUER_KEY_X: &str = "PI9zSwLa8sVsYi1olTioD9Ni-7JHx5aFVI-F_qpdzLY";
const ISSUER_KEY_Y: &str = "XrEZEd8OgXHc8-xIXEqqG1bB9guX2aRtzBG8DG6m3Pg";
const KID: &str = "test-key";
const AUDIENCE: &str = "https://cache.example.com";

/// What the mock issuer's discovery endpoint answers.
#[derive(Clone, Copy)]
enum Discovery {
    Valid,
    WrongIssuer,
    ServerError,
}

/// Serves the discovery document and JWKS on a loopback port; returns the issuer URL and the
/// number of discovery requests.
async fn spawn_issuer(discovery: Discovery) -> (String, Arc<AtomicUsize>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let issuer = format!("http://{}", listener.local_addr().unwrap());
    let advertised = match discovery {
        Discovery::WrongIssuer => "https://other-issuer.example".to_string(),
        _ => issuer.clone(),
    };
    let document = json!({ "issuer": advertised, "jwks_uri": format!("{issuer}/jwks") });
    let jwks = json!({ "keys": [{
        "kty": "EC", "crv": "P-256", "x": ISSUER_KEY_X, "y": ISSUER_KEY_Y,
        "kid": KID, "alg": "ES256", "use": "sig",
    }] });
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = hits.clone();
    let app = Router::new()
        .route(
            "/.well-known/openid-configuration",
            get(move || async move {
                counter.fetch_add(1, Ordering::SeqCst);
                match discovery {
                    Discovery::ServerError => Err(StatusCode::INTERNAL_SERVER_ERROR),
                    _ => Ok(Json(document)),
                }
            }),
        )
        .route("/jwks", get(move || async move { Json(jwks) }));
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (issuer, hits)
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// An ID token of `issuer` for a push to `main` of `acme/app`; `overrides` replace claims
/// (`null` removes one).
fn id_token(issuer: &str, overrides: Value) -> String {
    let mut claims = json!({
        "iss": issuer,
        "aud": AUDIENCE,
        "sub": "repo:acme/app:ref:refs/heads/main",
        "repository": "acme/app",
        "repository_owner": "acme",
        "ref": "refs/heads/main",
        "iat": now(),
        "nbf": now(),
        "exp": now() + 300,
    });
    for (claim, value) in overrides.as_object().unwrap() {
        match value {
            Value::Null => claims.as_object_mut().unwrap().remove(claim),
            value => claims
                .as_object_mut()
                .unwrap()
                .insert(claim.clone(), value.clone()),
        };
    }
    let header = Header {
        kid: Some(KID.to_string()),
        ..Header::new(Algorithm::ES256)
    };
    let key = EncodingKey::from_ec_pem(ISSUER_KEY_PEM.as_bytes()).unwrap();
    jsonwebtoken::encode(&header, &claims, &key).unwrap()
}

/// A server with the static token `static` and OIDC for a [`spawn_issuer`]; returns its client
/// and the issuer URL.
async fn serve(write_claims: &[&str], namespace_claim: Option<&str>) -> (Api, String) {
    let (issuer, _) = spawn_issuer(Discovery::Valid).await;
    (
        serve_for(&issuer, write_claims, namespace_claim).await,
        issuer,
    )
}

async fn serve_for(issuer: &str, write_claims: &[&str], namespace_claim: Option<&str>) -> Api {
    let write_claims = ClaimRules::parse(write_claims).unwrap();
    let config = OidcConfig {
        issuer: issuer.to_string(),
        audience: AUDIENCE.to_string(),
        required_claims: ClaimRules::parse(&["repository_owner=acme"]).unwrap(),
        write_claims: (!write_claims.is_empty()).then_some(write_claims),
        namespace_claim: namespace_claim.map(str::to_string),
        token_ttl: Duration::from_secs(3600),
        signing_key: CacheTokenKey::new(&[42; 32], issuer, AUDIENCE),
    };
    let auth = AuthConfig::new(vec!["static".into()], false);
    let state = AppState::new(FakeStorage::new(), auth)
        .with_oidc(OidcExchange::new(config).unwrap())
        .with_allow_delete(true);
    Api::serve(state).await
}

/// `POST /oidc/token` with `id_token`; returns the status and the JSON body.
async fn exchange(api: &Api, id_token: &str) -> (StatusCode, Value) {
    let resp = api.with_token(id_token).post("/oidc/token").fetch().await;
    let status = resp.status();
    let text = resp.text().await.unwrap();
    assert!(!text.contains(id_token), "ID token echoed: {text}");
    (status, serde_json::from_str(&text).unwrap())
}

/// Exchanges `id_token` (expecting success) and returns the response.
async fn cache_token(api: &Api, id_token: &str) -> Value {
    let (status, body) = exchange(api, id_token).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body
}

#[tokio::test]
async fn exchanged_token_reads_and_writes_until_tampered_with() {
    let (api, issuer) = serve(&[], None).await;
    let body = cache_token(&api, &id_token(&issuer, json!({}))).await;
    assert_eq!(body["readOnly"], false);
    assert!(body.get("namespace").is_none(), "{body}");
    let expires_at = body["expiresAt"].as_u64().unwrap();
    assert!(expires_at.abs_diff(now() + 3600) <= 5, "{body}");
    // Issuers differ in whether `iss` ends with a slash.
    cache_token(&api, &id_token(&format!("{issuer}/"), json!({}))).await;

    let token = body["token"].as_str().unwrap();
    let minted = api.with_token(token);
    for slug in ["team-a", "team-b"] {
        let path = format!("/v8/artifacts/hash1?slug={slug}");
        let put = minted.put(&path).body(b"payload".to_vec()).fetch().await;
        assert_eq!(put.status(), StatusCode::ACCEPTED);
        assert_eq!(minted.get(&path).fetch().await.status(), StatusCode::OK);
    }
    let status: Value = minted
        .get("/v8/artifacts/status")
        .fetch()
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(status["readOnly"], false);
    // Configured tokens keep working next to minted ones.
    let artifact = "/v8/artifacts/hash1?slug=team-a";
    let resp = api.with_token("static").get(artifact).fetch().await;
    assert_eq!(resp.status(), StatusCode::OK);

    // The payload starts with `{"` (`eyJ`); `f` changes its first byte.
    let tampered = token.replacen("ftc1.e", "ftc1.f", 1);
    assert_ne!(tampered, token);
    let resp = api.with_token(&tampered).get(artifact).fetch().await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // Without OIDC configured, the endpoint does not exist.
    let plain = Api::serve(tok_state(FakeStorage::new())).await;
    let id = id_token(&issuer, json!({}));
    let resp = plain.with_token(&id).post("/oidc/token").fetch().await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn unverifiable_id_tokens_are_rejected_with_401() {
    let (api, issuer) = serve(&[], None).await;
    let valid = id_token(&issuer, json!({}));
    let (signed, _) = valid.rsplit_once('.').unwrap();
    let payload = valid.split('.').nth(1).unwrap();
    let other = id_token(&issuer, json!({ "repository": "acme/other" }));
    let (_, other_signature) = other.rsplit_once('.').unwrap();
    let hs256 = jsonwebtoken::encode(
        &Header {
            kid: Some(KID.to_string()),
            ..Header::new(Algorithm::HS256)
        },
        &json!({ "iss": issuer, "aud": AUDIENCE, "exp": now() + 300, "repository_owner": "acme" }),
        &EncodingKey::from_secret(ISSUER_KEY_X.as_bytes()),
    )
    .unwrap();
    let unknown_kid = {
        let header = Header {
            kid: Some("other-key".to_string()),
            ..Header::new(Algorithm::ES256)
        };
        let key = EncodingKey::from_ec_pem(ISSUER_KEY_PEM.as_bytes()).unwrap();
        let claims = json!({ "iss": issuer, "aud": AUDIENCE, "exp": now() + 300 });
        jsonwebtoken::encode(&header, &claims, &key).unwrap()
    };

    for (case, token) in [
        (
            "wrong audience",
            id_token(&issuer, json!({ "aud": "https://other.example.com" })),
        ),
        (
            "wrong issuer",
            id_token("https://other-issuer.example", json!({})),
        ),
        (
            "expired",
            id_token(
                &issuer,
                json!({ "iat": now() - 900, "nbf": now() - 900, "exp": now() - 600 }),
            ),
        ),
        (
            "not yet valid",
            id_token(&issuer, json!({ "nbf": now() + 600 })),
        ),
        ("without expiry", id_token(&issuer, json!({ "exp": null }))),
        ("bad signature", format!("{signed}.{other_signature}")),
        ("alg none", format!("eyJhbGciOiJub25lIn0.{payload}.")),
        ("HS256", hs256),
        ("unknown key", unknown_kid),
        ("garbage", "not-a-jwt".to_string()),
    ] {
        let (status, body) = exchange(&api, &token).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{case}: {body}");
        assert_eq!(body["error"]["code"], "unauthorized", "{case}");
    }

    // jsonwebtoken quotes the unknown algorithm; the response must not.
    let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"x\ninjected"}"#);
    let (status, body) = exchange(&api, &format!("{header}.{payload}.AAAA")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
    assert!(!body.to_string().contains("injected"), "{body}");

    let resp = api.post("/oidc/token").fetch().await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn claim_rules_decide_access_permission_and_namespace() {
    let (api, issuer) = serve(&["ref=refs/heads/main"], Some("repository_owner")).await;

    let (status, body) = exchange(
        &api,
        &id_token(&issuer, json!({ "repository_owner": "evil" })),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["error"]["code"], "forbidden_claims");

    let main = cache_token(&api, &id_token(&issuer, json!({}))).await;
    assert_eq!(
        (&main["readOnly"], &main["namespace"]),
        (&json!(false), &json!("acme"))
    );
    let feature = id_token(&issuer, json!({ "ref": "refs/heads/feature" }));
    let feature = cache_token(&api, &feature).await;
    assert_eq!(
        (&feature["readOnly"], &feature["namespace"]),
        (&json!(true), &json!("acme"))
    );

    let writer = api.with_token(main["token"].as_str().unwrap());
    let reader = api.with_token(feature["token"].as_str().unwrap());
    let artifact = "/v8/artifacts/hash1?slug=acme";
    let put = writer.put(artifact).body(b"payload".to_vec()).fetch().await;
    assert_eq!(put.status(), StatusCode::ACCEPTED);
    assert_eq!(reader.get(artifact).fetch().await.status(), StatusCode::OK);
    for write in [
        reader.put(artifact).body(b"poison".to_vec()),
        reader.delete(artifact),
    ] {
        let resp = write.fetch().await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["error"]["code"], "forbidden_read_only");
    }
    let status: Value = reader
        .get("/v8/artifacts/status?slug=acme")
        .fetch()
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(status["readOnly"], true);

    // Scoped to the namespace of the claim.
    for token in [&writer, &reader] {
        let resp = token.get("/v8/artifacts/hash1?slug=other").fetch().await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        let body: Value = resp.json().await.unwrap();
        assert_eq!(body["error"]["code"], "forbidden_namespace");
    }
}

#[tokio::test]
async fn failed_key_fetches_are_not_repeated_on_every_exchange() {
    for discovery in [Discovery::ServerError, Discovery::WrongIssuer] {
        let (issuer, hits) = spawn_issuer(discovery).await;
        let api = serve_for(&issuer, &[], None).await;
        let token = id_token(&issuer, json!({}));
        for _ in 0..3 {
            let (status, body) = exchange(&api, &token).await;
            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
        }
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }
}
