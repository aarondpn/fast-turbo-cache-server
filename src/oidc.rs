//! OIDC token exchange (`POST /oidc/token`): a CI job trades an OIDC ID token (e.g. from GitHub
//! Actions) for a cache token minted by this server. ID tokens expire within minutes, but turbo
//! reads `TURBO_TOKEN` once per run, so the minted token lives for `OIDC_TOKEN_TTL_SECS`.

use std::{
    fmt,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::Context as _;
use axum::{Json, extract::State, http::HeaderMap};
use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use hmac::{Hmac, KeyInit, Mac};
use jsonwebtoken::{
    Algorithm, DecodingKey, Validation,
    errors::ErrorKind,
    jwk::{Jwk, JwkSet},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value};
use sha2::Sha256;
use tracing::{debug, info, warn};
use url::{Host, Url};

use crate::{
    auth::{Grant, bearer_token, missing_header},
    error::{AppError, AppResult},
    routes::AppState,
    storage::sanitize_namespace,
};

/// Shortest accepted `OIDC_SIGNING_KEY` in bytes (after base64 decoding).
pub const MIN_SIGNING_KEY_BYTES: usize = 32;
/// Longest accepted `OIDC_TOKEN_TTL_SECS` (one day).
pub const MAX_TOKEN_TTL_SECS: u64 = 86_400;
/// Version prefix of minted cache tokens.
const TOKEN_PREFIX: &str = "ftc1";
/// Clock skew tolerated for `exp`, `nbf` and `iat` of ID tokens.
const LEEWAY_SECS: u64 = 60;
/// Asymmetric algorithms only: with `HS*` anyone knowing the issuer's public key could sign.
const ALGORITHMS: [Algorithm; 9] = [
    Algorithm::RS256,
    Algorithm::RS384,
    Algorithm::RS512,
    Algorithm::PS256,
    Algorithm::PS384,
    Algorithm::PS512,
    Algorithm::ES256,
    Algorithm::ES384,
    Algorithm::EdDSA,
];
/// Earliest refetch of the issuer's keys after a successful fetch, so ID tokens with unknown key
/// ids cannot make the server hammer the issuer.
const JWKS_REFETCH_INTERVAL: Duration = Duration::from_secs(60);
/// Earliest refetch after a failed fetch, so an unavailable issuer is not queried on every
/// exchange.
const JWKS_RETRY_INTERVAL: Duration = Duration::from_secs(10);
/// Largest accepted discovery document or JWKS.
const MAX_RESPONSE_BYTES: usize = 256 * 1024;
/// Cached keys are refreshed after this long, so keys the issuer has withdrawn stop working.
const JWKS_MAX_AGE: Duration = Duration::from_secs(3600);
const HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);

/// `claim=value` rules: rules for the same claim are alternatives, rules for different claims
/// must all match.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClaimRules(Vec<(String, Vec<String>)>);

impl ClaimRules {
    /// Parses `claim=value` entries (split at the first `=`); blank entries are ignored.
    pub fn parse<S: AsRef<str>>(entries: &[S]) -> anyhow::Result<Self> {
        let mut rules: Vec<(String, Vec<String>)> = Vec::new();
        for entry in entries.iter().map(|e| e.as_ref().trim()) {
            if entry.is_empty() {
                continue;
            }
            let (claim, value) = entry
                .split_once('=')
                .map(|(claim, value)| (claim.trim(), value.trim()))
                .filter(|(claim, value)| !claim.is_empty() && !value.is_empty())
                .ok_or_else(|| {
                    anyhow::anyhow!("Invalid claim rule '{entry}' (expected 'claim=value')")
                })?;
            match rules.iter_mut().find(|(name, _)| name == claim) {
                Some((_, values)) => values.push(value.to_string()),
                None => rules.push((claim.to_string(), vec![value.to_string()])),
            }
        }
        Ok(Self(rules))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn matches(&self, claims: &Map<String, Value>) -> bool {
        self.0.iter().all(|(claim, values)| {
            claim_value(claims, claim).is_some_and(|actual| values.contains(&actual))
        })
    }
}

/// A string, number or boolean claim as text.
fn claim_value(claims: &Map<String, Value>, claim: &str) -> Option<String> {
    match claims.get(claim)? {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// HMAC-SHA256 key of the minted cache tokens, derived from `OIDC_SIGNING_KEY`.
#[derive(Clone)]
pub struct CacheTokenKey(Arc<[u8]>);

impl fmt::Debug for CacheTokenKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt("[REDACTED]", f)
    }
}

/// Payload of a minted cache token.
#[derive(Serialize, Deserialize)]
struct CacheTokenPayload {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ns: Option<String>,
    ro: bool,
    exp: u64,
}

impl CacheTokenKey {
    /// Binds `secret` to the deployment's `issuer` and `audience`: tokens minted by another
    /// deployment sharing the secret are not valid here.
    pub fn new(secret: &[u8], issuer: &str, audience: &str) -> Self {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("HMAC takes any key length");
        for part in [TOKEN_PREFIX, issuer, audience] {
            mac.update(&(part.len() as u64).to_be_bytes());
            mac.update(part.as_bytes());
        }
        Self(mac.finalize().into_bytes().to_vec().into())
    }

    fn mac(&self, data: &[u8]) -> Hmac<Sha256> {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.0).expect("HMAC takes any key length");
        mac.update(data);
        mac
    }

    /// `ftc1.<base64url(payload)>.<base64url(HMAC-SHA256 of everything before the last dot)>`,
    /// valid for `namespace` (`None`: every namespace) until `expires_at` (unix seconds).
    pub fn mint(&self, namespace: Option<&str>, read_only: bool, expires_at: u64) -> String {
        let payload = CacheTokenPayload {
            ns: namespace.map(str::to_string),
            ro: read_only,
            exp: expires_at,
        };
        let payload = serde_json::to_vec(&payload).expect("payload serializes");
        let signed = format!("{TOKEN_PREFIX}.{}", URL_SAFE_NO_PAD.encode(payload));
        let tag = self.mac(signed.as_bytes()).finalize().into_bytes();
        format!("{signed}.{}", URL_SAFE_NO_PAD.encode(tag))
    }

    /// The access granted by `token` if it was minted with this key and has not expired.
    pub fn verify(&self, token: &str) -> Option<Grant> {
        self.verify_at(token, unix_now())
    }

    fn verify_at(&self, token: &str, now: u64) -> Option<Grant> {
        let (signed, tag) = token.rsplit_once('.')?;
        let payload = signed.strip_prefix(TOKEN_PREFIX)?.strip_prefix('.')?;
        let tag = URL_SAFE_NO_PAD.decode(tag).ok()?;
        // Constant-time comparison.
        self.mac(signed.as_bytes()).verify_slice(&tag).ok()?;
        let payload = URL_SAFE_NO_PAD.decode(payload).ok()?;
        let payload: CacheTokenPayload = serde_json::from_slice(&payload).ok()?;
        (payload.exp > now).then(|| Grant::minted(payload.ns, payload.ro))
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// Decodes `OIDC_SIGNING_KEY` (standard base64 of at least [`MIN_SIGNING_KEY_BYTES`] bytes). The
/// error never contains the key.
pub fn decode_signing_key(raw: &str) -> anyhow::Result<Vec<u8>> {
    STANDARD
        .decode(raw.trim())
        .ok()
        .filter(|key| key.len() >= MIN_SIGNING_KEY_BYTES)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "OIDC_SIGNING_KEY must be base64 encoding at least {MIN_SIGNING_KEY_BYTES} random bytes (e.g. `openssl rand -base64 32`)"
            )
        })
}

/// Checks an issuer or JWKS URL: https (http only for loopback hosts, i.e. tests), no
/// credentials, query or fragment.
pub fn validate_url(raw: &str) -> anyhow::Result<()> {
    let url = Url::parse(raw).map_err(|e| anyhow::anyhow!("Invalid URL: {e}"))?;
    let loopback = match url.host() {
        Some(Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
        Some(Host::Ipv4(ip)) => ip.is_loopback(),
        Some(Host::Ipv6(ip)) => ip.is_loopback(),
        None => false,
    };
    if !(url.scheme() == "https" || (url.scheme() == "http" && loopback)) {
        anyhow::bail!("The URL must use https:// (http:// only for loopback hosts)");
    }
    if url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        anyhow::bail!("The URL must have a host and no credentials, query or fragment");
    }
    Ok(())
}

/// Resolved `OIDC_*` options (see `CliConfig::oidc_config`).
#[derive(Debug, Clone)]
pub struct OidcConfig {
    pub issuer: String,
    pub audience: String,
    pub required_claims: ClaimRules,
    /// `None`: every accepted ID token gets a read-write cache token.
    pub write_claims: Option<ClaimRules>,
    pub namespace_claim: Option<String>,
    pub token_ttl: Duration,
    pub signing_key: CacheTokenKey,
}

/// Successful `POST /oidc/token` response.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExchangeResponse {
    pub token: String,
    /// Unix seconds.
    pub expires_at: u64,
    pub read_only: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

#[derive(Default)]
struct KeyCache {
    jwks: Option<(JwkSet, Instant)>,
    /// No fetch before this instant.
    next_fetch: Option<Instant>,
}

/// Verifies ID tokens and mints cache tokens.
pub struct OidcExchange {
    config: OidcConfig,
    validation: Validation,
    client: reqwest::Client,
    /// Fetched lazily, so startup does not depend on the issuer.
    keys: Mutex<KeyCache>,
    /// Held during a fetch (`keys` is not), so concurrent exchanges wait for one fetch instead of
    /// starting their own.
    fetch_lock: tokio::sync::Mutex<()>,
}

impl OidcExchange {
    pub fn new(config: OidcConfig) -> anyhow::Result<Self> {
        // The algorithm is set per ID token (see `verify`).
        let mut validation = Validation::new(Algorithm::ES256);
        // Issuers differ in whether `iss` ends with a slash; both forms name the same issuer.
        let issuer = config.issuer.trim_end_matches('/');
        validation.set_issuer(&[issuer.to_string(), format!("{issuer}/")]);
        validation.set_audience(&[&config.audience]);
        validation.set_required_spec_claims(&["exp", "iss", "aud"]);
        validation.validate_nbf = true;
        validation.leeway = LEEWAY_SECS;
        let client = reqwest::Client::builder()
            .connect_timeout(HTTP_CONNECT_TIMEOUT)
            .timeout(HTTP_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .context("Failed to build the OIDC HTTP client")?;
        Ok(Self {
            config,
            validation,
            client,
            keys: Mutex::default(),
            fetch_lock: tokio::sync::Mutex::default(),
        })
    }

    pub fn signing_key(&self) -> &CacheTokenKey {
        &self.config.signing_key
    }

    /// Verifies `id_token` and mints a cache token according to the configured rules.
    pub async fn exchange(&self, id_token: &str) -> AppResult<ExchangeResponse> {
        let claims = match self.verify(id_token).await {
            Ok(claims) => claims,
            Err(err) => {
                // Anyone can send unverifiable ID tokens; the access log has the 401.
                if matches!(err, AppError::Unauthorized(_)) {
                    debug!(error = %err, "OIDC token exchange rejected");
                } else {
                    warn!(error = %err, "OIDC token exchange failed");
                }
                return Err(err);
            }
        };
        let subject = claim_value(&claims, "sub");
        let repository = claim_value(&claims, "repository");
        let result = self.mint(&claims);
        match &result {
            Ok(minted) => info!(
                subject,
                repository,
                namespace = minted.namespace.as_deref().unwrap_or("*"),
                read_only = minted.read_only,
                expires_at = minted.expires_at,
                "OIDC token exchange succeeded"
            ),
            Err(err) => warn!(subject, repository, error = %err, "OIDC token exchange rejected"),
        }
        result
    }

    async fn verify(&self, id_token: &str) -> AppResult<Map<String, Value>> {
        let header = jsonwebtoken::decode_header(id_token).map_err(invalid_id_token)?;
        if !ALGORITHMS.contains(&header.alg) {
            return Err(AppError::Unauthorized(format!(
                "ID token algorithm {:?} is not accepted",
                header.alg
            )));
        }
        // jsonwebtoken requires all accepted algorithms to belong to the key's family.
        let mut validation = self.validation.clone();
        validation.algorithms = vec![header.alg];
        let kid = header
            .kid
            .ok_or_else(|| AppError::Unauthorized("ID token has no key id (kid)".to_string()))?;
        let key = self.decoding_key(&kid).await?;
        let claims = jsonwebtoken::decode::<Map<String, Value>>(id_token, &key, &validation)
            .map_err(invalid_id_token)?
            .claims;
        let iat = claims.get("iat").and_then(Value::as_u64);
        if iat.is_some_and(|iat| iat > unix_now() + LEEWAY_SECS) {
            return Err(AppError::Unauthorized(
                "ID token is issued in the future".to_string(),
            ));
        }
        Ok(claims)
    }

    async fn decoding_key(&self, kid: &str) -> AppResult<DecodingKey> {
        let jwk = match self.cached_key(kid, true) {
            Some(jwk) => jwk,
            None => self.refresh_keys(kid).await?,
        };
        DecodingKey::from_jwk(&jwk).map_err(|_| {
            AppError::Unauthorized("ID token is signed with an unsupported key".to_string())
        })
    }

    fn key_cache(&self) -> MutexGuard<'_, KeyCache> {
        self.keys.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The cached key `kid`; with `fresh`, only from keys younger than [`JWKS_MAX_AGE`].
    fn cached_key(&self, kid: &str, fresh: bool) -> Option<Jwk> {
        let cache = self.key_cache();
        let (jwks, fetched) = cache.jwks.as_ref()?;
        if fresh && fetched.elapsed() >= JWKS_MAX_AGE {
            return None;
        }
        jwks.find(kid).cloned()
    }

    /// Refetches the keys unless a fetch was attempted recently, then looks up `kid` (in the
    /// previous keys if the fetch failed).
    async fn refresh_keys(&self, kid: &str) -> AppResult<Jwk> {
        let _fetching = self.fetch_lock.lock().await;
        // Another exchange may have fetched the keys while this one waited.
        if let Some(jwk) = self.cached_key(kid, true) {
            return Ok(jwk);
        }
        let next_fetch = self.key_cache().next_fetch;
        if next_fetch.is_none_or(|at| Instant::now() >= at) {
            let result = self.fetch_jwks().await;
            let mut cache = self.key_cache();
            let now = Instant::now();
            match result {
                Ok(jwks) => {
                    cache.jwks = Some((jwks, now));
                    cache.next_fetch = Some(now + JWKS_REFETCH_INTERVAL);
                }
                Err(err) => {
                    cache.next_fetch = Some(now + JWKS_RETRY_INTERVAL);
                    if cache.jwks.is_none() {
                        return Err(AppError::Internal(format!(
                            "Failed to fetch the OIDC signing keys: {err:#}"
                        )));
                    }
                    warn!(
                        error = format!("{err:#}"),
                        "Failed to refresh the OIDC signing keys; using the cached keys"
                    );
                }
            }
        }
        match &self.key_cache().jwks {
            Some((jwks, _)) => jwks.find(kid).cloned().ok_or_else(|| {
                AppError::Unauthorized("ID token is signed with an unknown key".to_string())
            }),
            None => Err(AppError::Internal(
                "The OIDC signing keys are unavailable (the last fetch failed)".to_string(),
            )),
        }
    }

    async fn fetch_jwks(&self) -> anyhow::Result<JwkSet> {
        #[derive(Deserialize)]
        struct Discovery {
            issuer: String,
            jwks_uri: String,
        }
        let issuer = self.config.issuer.trim_end_matches('/');
        let discovery: Discovery = self
            .get_json(&format!("{issuer}/.well-known/openid-configuration"))
            .await?;
        if discovery.issuer.trim_end_matches('/') != issuer {
            anyhow::bail!(
                "The discovery document names the issuer {:?}, not OIDC_ISSUER",
                discovery.issuer
            );
        }
        validate_url(&discovery.jwks_uri).context("Invalid jwks_uri")?;
        self.get_json(&discovery.jwks_uri).await
    }

    async fn get_json<T: DeserializeOwned>(&self, url: &str) -> anyhow::Result<T> {
        let mut response = self
            .client
            .get(url)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .with_context(|| format!("GET {url}"))?;
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .with_context(|| format!("GET {url}"))?
        {
            if body.len() + chunk.len() > MAX_RESPONSE_BYTES {
                anyhow::bail!("GET {url}: the response exceeds {MAX_RESPONSE_BYTES} bytes");
            }
            body.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&body).with_context(|| format!("Invalid JSON from {url}"))
    }

    /// Applies the claim rules to verified `claims` and mints the cache token.
    fn mint(&self, claims: &Map<String, Value>) -> AppResult<ExchangeResponse> {
        if !self.config.required_claims.matches(claims) {
            return Err(AppError::forbidden(
                "forbidden_claims",
                "ID token does not match OIDC_REQUIRED_CLAIMS",
            ));
        }
        let read_only = self
            .config
            .write_claims
            .as_ref()
            .is_some_and(|rules| !rules.matches(claims));
        let namespace = match &self.config.namespace_claim {
            Some(claim) => Some(claim_namespace(claims, claim)?),
            None => None,
        };
        let expires_at = unix_now() + self.config.token_ttl.as_secs();
        Ok(ExchangeResponse {
            token: self
                .config
                .signing_key
                .mint(namespace.as_deref(), read_only, expires_at),
            expires_at,
            read_only,
            namespace,
        })
    }
}

/// The namespace named by `claim` (`OIDC_NAMESPACE_CLAIM`).
fn claim_namespace(claims: &Map<String, Value>, claim: &str) -> AppResult<String> {
    let value = claim_value(claims, claim).unwrap_or_default();
    match sanitize_namespace(&value) {
        // An empty value would map to the `default` namespace.
        Ok(namespace) if !value.trim().is_empty() => Ok(namespace.to_string()),
        _ => Err(AppError::forbidden(
            "forbidden_namespace",
            format!("ID token claim '{claim}' is not a valid namespace"),
        )),
    }
}

/// A fixed reason: jsonwebtoken's messages can quote parts of the token, which would end up
/// verbatim in logs and responses.
fn invalid_id_token(err: jsonwebtoken::errors::Error) -> AppError {
    let reason = match err.kind() {
        ErrorKind::ExpiredSignature => "expired",
        ErrorKind::ImmatureSignature => "not valid yet",
        ErrorKind::InvalidSignature => "invalid signature",
        ErrorKind::InvalidIssuer => "wrong issuer",
        ErrorKind::InvalidAudience => "wrong audience",
        ErrorKind::MissingRequiredClaim(_) => "missing exp, iss or aud claim",
        ErrorKind::InvalidAlgorithm => "the algorithm does not match the key",
        _ => "malformed",
    };
    AppError::Unauthorized(format!("Invalid ID token: {reason}"))
}

/// `POST /oidc/token` (routed only when OIDC is configured): the ID token is the bearer token.
pub async fn token_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> AppResult<Json<ExchangeResponse>> {
    let exchange = state
        .oidc
        .as_deref()
        .ok_or_else(|| AppError::NotFound("OIDC is not configured".to_string()))?;
    let id_token = bearer_token(&headers)?.ok_or_else(missing_header)?;
    exchange.exchange(id_token).await.map(Json)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn claims(value: Value) -> Map<String, Value> {
        value.as_object().unwrap().clone()
    }

    #[test]
    fn claim_rules_and_across_claims_or_within_a_claim() {
        let rules = ClaimRules::parse(&[
            "repository_owner=acme",
            " ref = refs/heads/main ",
            "ref=refs/heads/release",
            "",
        ])
        .unwrap();
        let token = |owner: &str, git_ref: &str| {
            claims(json!({ "repository_owner": owner, "ref": git_ref }))
        };
        assert!(rules.matches(&token("acme", "refs/heads/main")));
        assert!(rules.matches(&token("acme", "refs/heads/release")));
        assert!(!rules.matches(&token("acme", "refs/heads/feature")));
        assert!(!rules.matches(&token("evil", "refs/heads/main")));
        assert!(!rules.matches(&claims(json!({ "ref": "refs/heads/main" }))));

        // Numeric claims compare by their text.
        let ids = ClaimRules::parse(&["repository_owner_id=123"]).unwrap();
        assert!(ids.matches(&claims(json!({ "repository_owner_id": 123 }))));
        assert!(ids.matches(&claims(json!({ "repository_owner_id": "123" }))));
        assert!(!ids.matches(&claims(json!({ "repository_owner_id": "1234" }))));

        // Values may contain `=`.
        let rules = ClaimRules::parse(&["sub=a=b"]).unwrap();
        assert!(rules.matches(&claims(json!({ "sub": "a=b" }))));

        for bad in ["novalue", "=x", "claim=", " = "] {
            assert!(ClaimRules::parse(&[bad]).is_err(), "{bad}");
        }
        assert!(ClaimRules::parse::<&str>(&[]).unwrap().is_empty());
    }

    #[test]
    fn minted_tokens_roundtrip_and_reject_tampering_expiry_and_other_keys() {
        const ISSUER: &str = "https://issuer.example";
        const AUDIENCE: &str = "https://cache.example";
        let key = CacheTokenKey::new(&[7; 32], ISSUER, AUDIENCE);
        let now = 1_000_000;
        let scoped = key.mint(Some("team-a"), true, now + 60);
        assert!(scoped.starts_with("ftc1."));
        let grant = key.verify_at(&scoped, now).unwrap();
        assert!(!grant.unscoped && grant.read_only);
        assert_eq!(&*grant.namespaces, [Box::from("team-a")]);
        let unscoped = key
            .verify_at(&key.mint(None, false, now + 60), now)
            .unwrap();
        assert!(unscoped.unscoped && !unscoped.read_only);

        assert!(key.verify_at(&scoped, now + 60).is_none(), "expired");
        // Another secret, or the same secret of a deployment with another issuer or audience.
        for (secret, issuer, audience) in [
            ([8; 32], ISSUER, AUDIENCE),
            ([7; 32], "https://other-issuer.example", AUDIENCE),
            ([7; 32], ISSUER, "https://other-cache.example"),
        ] {
            let other = CacheTokenKey::new(&secret, issuer, audience);
            assert!(
                other.verify_at(&scoped, now).is_none(),
                "{issuer} {audience}"
            );
        }

        // A payload of another token with this token's MAC.
        let (_, tag) = scoped.rsplit_once('.').unwrap();
        let unscoped = key.mint(None, false, now + 60);
        let (payload, _) = unscoped.rsplit_once('.').unwrap();
        let forged = format!("{payload}.{tag}");
        assert!(key.verify_at(&forged, now).is_none());
        for bad in [
            "",
            "ftc1",
            "ftc1..",
            "ftc2.e30.AAAA",
            &scoped[..scoped.len() - 1],
        ] {
            assert!(key.verify_at(bad, now).is_none(), "{bad}");
        }
    }

    #[test]
    fn signing_keys_are_base64_of_at_least_32_bytes() {
        let key = STANDARD.encode([7; 32]);
        assert_eq!(decode_signing_key(&format!(" {key}\n")).unwrap(), [7; 32]);
        for bad in [
            STANDARD.encode([7; 31]),
            // 32 characters, but only 24 bytes.
            "k".repeat(32),
            "not base64 but long enough to pass a length check".to_string(),
        ] {
            let err = decode_signing_key(&bad).unwrap_err().to_string();
            assert!(err.contains("openssl rand -base64 32"), "{err}");
        }
    }

    #[test]
    fn issuer_urls_must_use_https_except_on_loopback() {
        for ok in [
            "https://token.actions.githubusercontent.com",
            "http://127.0.0.1:8080",
            "http://localhost/issuer",
            "http://[::1]:1",
        ] {
            validate_url(ok).unwrap();
        }
        for bad in [
            "http://token.actions.githubusercontent.com",
            "https://u:p@issuer.example",
            "https://issuer.example?x=1",
            "ftp://issuer.example",
            "not a url",
        ] {
            assert!(validate_url(bad).is_err(), "{bad}");
        }
    }
}
