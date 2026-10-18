use std::{
    borrow::Cow,
    collections::HashMap,
    convert::Infallible,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, ready},
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    body::Body,
    extract::{Request, State},
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode, Uri, Version, header},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use bytes::Bytes;
use futures_util::{StreamExt, stream};
use percent_encoding::percent_decode_str;
use serde::de::DeserializeOwned;
use serde_json::json;
use tokio::sync::Semaphore;
use tower::{Layer, Service, ServiceExt};

use crate::{
    auth::{AuthConfig, Principal},
    config::ServerOptions,
    error::{AppError, AppResult},
    metrics::{CacheMetrics, StorageOp, UploadRejection, validate_analytics_events},
    models::{
        AnalyticsEvent, ArtifactMetadata, ArtifactQueryHit, ArtifactQueryRequest, CachingStatus,
        CachingStatusResponse, PutArtifactResponse,
    },
    oidc::{self, OidcExchange},
    storage::{
        ArtifactPayload, PutMode, PutOutcome, Storage, cache::run_to_completion,
        sanitize_namespace, validate_hash,
    },
    upload::{
        SlotUnavailable, UploadLimits, UploadSlots, UploadStream, read_body_limited, receive_upload,
    },
};

const HEADER_ARTIFACT_DURATION: HeaderName = HeaderName::from_static("x-artifact-duration");
const HEADER_ARTIFACT_TAG: HeaderName = HeaderName::from_static("x-artifact-tag");
const HEADER_ARTIFACT_SHA: HeaderName = HeaderName::from_static("x-artifact-sha");
const HEADER_ARTIFACT_DIRTY_HASH: HeaderName = HeaderName::from_static("x-artifact-dirty-hash");
const HEADER_ARTIFACT_CLIENT_CI: HeaderName = HeaderName::from_static("x-artifact-client-ci");
pub const HEADER_REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");

/// Response headers that browsers may read on cross-origin responses.
const CORS_EXPOSE_HEADERS: HeaderValue = HeaderValue::from_static(
    "x-artifact-duration, x-artifact-tag, x-artifact-sha, x-artifact-dirty-hash, x-artifact-client-ci, x-request-id, retry-after",
);
const CORS_ALLOW_HEADERS: HeaderValue = HeaderValue::from_static(
    "Authorization, Content-Type, User-Agent, x-artifact-duration, x-artifact-tag, x-artifact-sha, x-artifact-dirty-hash, x-artifact-client-ci, x-ai-agent, x-request-id",
);

pub const MAX_BATCH_QUERY_HASHES: usize = 1000;
pub const MAX_EVENTS_PER_REQUEST: usize = 1000;
pub const MAX_JSON_BODY_SIZE: usize = 2 * 1024 * 1024;
/// Body limit of the batch query: 1000 hashes of 128 characters are about 130 KiB as JSON. The
/// body is parsed before the hash count is checked, so a smaller limit bounds that allocation.
pub const MAX_BATCH_QUERY_BODY_SIZE: usize = 256 * 1024;
/// Maximum length of `x-artifact-sha`, `x-artifact-dirty-hash` and `x-artifact-client-ci`.
pub const MAX_METADATA_HEADER_LEN: usize = 256;
/// Maximum length of `x-artifact-tag` (the Vercel Remote Cache spec allows up to 600).
pub const MAX_TAG_HEADER_LEN: usize = 600;
pub const DEFAULT_MAX_CONCURRENT_UPLOADS: usize = 32;
/// Concurrent backend HEADs issued by one batch query.
const BATCH_QUERY_CONCURRENCY: usize = 32;
/// Concurrent backend HEADs issued by all batch queries together (server-wide).
pub const DEFAULT_BATCH_HEAD_CONCURRENCY: usize = 64;
/// Longest accepted client-supplied `x-request-id`.
const MAX_REQUEST_ID_LEN: usize = 128;

#[derive(Clone)]
pub struct AppState {
    pub storage: Arc<dyn Storage>,
    pub auth: AuthConfig,
    pub metrics: Arc<CacheMetrics>,
    pub caching_status: CachingStatus,
    pub isolate_teams: bool,
    pub max_artifact_size: usize,
    /// Server-wide and per-namespace upload slots.
    pub upload_slots: Arc<UploadSlots>,
    /// Server-wide limit for backend HEADs issued by `POST /v8/artifacts`.
    pub batch_head_semaphore: Arc<Semaphore>,
    pub immutable_artifacts: bool,
    pub allow_delete: bool,
    pub protect_observability: bool,
    pub cors_allow_origin: HeaderValue,
    /// Timeouts, spooling and connection options (see `ServerOptions`).
    pub server_options: ServerOptions,
    /// OIDC token exchange (`POST /oidc/token`); `None` when OIDC is not configured.
    pub oidc: Option<Arc<OidcExchange>>,
}

impl AppState {
    pub fn new(storage: Arc<dyn Storage>, auth: AuthConfig) -> Self {
        Self {
            storage,
            auth,
            metrics: Arc::new(CacheMetrics::new()),
            caching_status: CachingStatus::Enabled,
            isolate_teams: true,
            max_artifact_size: 512 * 1024 * 1024,
            upload_slots: Arc::new(UploadSlots::new(DEFAULT_MAX_CONCURRENT_UPLOADS, 0)),
            batch_head_semaphore: Arc::new(Semaphore::new(DEFAULT_BATCH_HEAD_CONCURRENCY)),
            immutable_artifacts: false,
            allow_delete: false,
            protect_observability: false,
            cors_allow_origin: HeaderValue::from_static("*"),
            server_options: ServerOptions::default(),
            oidc: None,
        }
    }

    /// Enables `POST /oidc/token` and accepts the cache tokens it mints.
    pub fn with_oidc(mut self, exchange: OidcExchange) -> Self {
        self.auth = self
            .auth
            .with_cache_token_key(exchange.signing_key().clone());
        self.oidc = Some(Arc::new(exchange));
        self
    }

    pub fn with_server_options(mut self, options: ServerOptions) -> Self {
        self.server_options = options;
        self
    }

    pub fn with_caching_status(mut self, status: CachingStatus) -> Self {
        self.caching_status = status;
        self
    }

    pub fn with_isolate_teams(mut self, isolate: bool) -> Self {
        self.isolate_teams = isolate;
        self
    }

    pub fn with_max_artifact_size(mut self, max_size: usize) -> Self {
        self.max_artifact_size = max_size;
        self
    }

    pub fn with_max_concurrent_uploads(mut self, max_uploads: usize) -> Self {
        let per_namespace = self.upload_slots.per_namespace().unwrap_or(0);
        self.upload_slots = Arc::new(UploadSlots::new(max_uploads, per_namespace));
        self
    }

    /// Most uploads one namespace may run at the same time (`0` = only the server-wide
    /// `MAX_CONCURRENT_UPLOADS` limit applies).
    pub fn with_max_uploads_per_namespace(mut self, per_namespace: usize) -> Self {
        let total = self.upload_slots.total();
        self.upload_slots = Arc::new(UploadSlots::new(total, per_namespace));
        self
    }

    /// Server-wide cap on concurrent backend HEADs issued by batch queries.
    pub fn with_batch_head_concurrency(mut self, max_heads: usize) -> Self {
        self.batch_head_semaphore = Arc::new(Semaphore::new(max_heads.max(1)));
        self
    }

    pub fn with_immutable_artifacts(mut self, immutable: bool) -> Self {
        self.immutable_artifacts = immutable;
        self
    }

    pub fn with_allow_delete(mut self, allow: bool) -> Self {
        self.allow_delete = allow;
        self
    }

    pub fn with_protect_observability(mut self, protect: bool) -> Self {
        self.protect_observability = protect;
        self
    }

    /// Sets `Access-Control-Allow-Origin` (empty = `*`); an invalid header value is ignored
    /// (`CliConfig::validate` rejects it at startup).
    pub fn with_cors_allow_origin(mut self, origin: &str) -> Self {
        let trimmed = origin.trim();
        if let Ok(val) = HeaderValue::from_str(if trimmed.is_empty() { "*" } else { trimmed }) {
            self.cors_allow_origin = val;
        }
        self
    }

    #[inline]
    fn resolve_namespace_from_uri<'a>(&self, uri: &'a Uri) -> AppResult<Cow<'a, str>> {
        if !self.isolate_teams {
            return Ok(Cow::Borrowed("default"));
        }
        let Some(query) = uri.query() else {
            return Ok(Cow::Borrowed("default"));
        };

        let mut slug: Option<&'a str> = None;
        let mut team_id: Option<&'a str> = None;
        let mut team: Option<&'a str> = None;

        for pair in query.split('&') {
            if let Some(val) = pair.strip_prefix("slug=") {
                if !val.is_empty() {
                    slug = Some(val);
                    break;
                }
            } else if let Some(val) = pair.strip_prefix("teamId=") {
                if !val.is_empty() && team_id.is_none() {
                    team_id = Some(val);
                }
            } else if let Some(val) = pair.strip_prefix("team=")
                && !val.is_empty()
                && team.is_none()
            {
                team = Some(val);
            }
        }

        let Some(raw) = slug.or(team_id).or(team) else {
            return Ok(Cow::Borrowed("default"));
        };

        if raw.contains('%') || raw.contains('+') {
            let decoded = percent_decode_str(raw).decode_utf8_lossy();
            let sanitized = sanitize_namespace(&decoded)?;
            Ok(Cow::Owned(sanitized.to_string()))
        } else {
            let sanitized = sanitize_namespace(raw)?;
            Ok(Cow::Borrowed(sanitized))
        }
    }

    /// Hash, namespace and authorization shared by the `/v8/artifacts/{hash}` handlers.
    fn artifact_request<'a>(
        &self,
        uri: &'a Uri,
        headers: &HeaderMap,
    ) -> AppResult<(&'a str, Cow<'a, str>, Principal<'_>)> {
        let hash = extract_artifact_hash(uri)?;
        let namespace = self.resolve_namespace_from_uri(uri)?;
        let principal = self
            .auth
            .verify_headers_for_namespace(headers, Some(&namespace))?;
        self.ensure_cache_enabled()?;
        Ok((hash, namespace, principal))
    }

    fn ensure_cache_enabled(&self) -> AppResult<()> {
        match self.caching_status {
            CachingStatus::Enabled => Ok(()),
            CachingStatus::Disabled => Err(AppError::forbidden(
                "remote_caching_disabled",
                "Remote caching is disabled on this server",
            )),
            CachingStatus::OverLimit => Err(AppError::forbidden(
                "remote_caching_over_limit",
                "Remote caching usage limit exceeded",
            )),
            CachingStatus::Paused => Err(AppError::forbidden(
                "remote_caching_paused",
                "Remote caching is currently paused",
            )),
        }
    }

    fn upload_limits(&self) -> UploadLimits {
        UploadLimits {
            max_size: self.max_artifact_size as u64,
            memory_threshold: self.server_options.upload_memory_threshold,
            idle_timeout: self.server_options.body_idle_timeout,
            total_timeout: self.server_options.body_total_timeout,
            spool_dir: self.server_options.spool_dir.clone(),
        }
    }

    /// Reads and parses a JSON request body (call only after authentication). The rest of a body
    /// rejected while it is read (too large, timed out) is drained when `body` is dropped.
    async fn read_json<T: DeserializeOwned>(
        &self,
        body: &mut RequestBody,
        limit: usize,
    ) -> AppResult<T> {
        let bytes = read_body_limited(
            body.as_mut(),
            limit,
            self.server_options.body_idle_timeout,
            self.server_options.body_total_timeout,
        )
        .await?;
        serde_json::from_slice(&bytes)
            .map_err(|e| AppError::BadRequest(format!("Invalid JSON request body: {e}")))
    }

    /// Runs a storage call and records its latency and outcome.
    async fn timed<T>(
        &self,
        op: StorageOp,
        fut: impl Future<Output = AppResult<T>>,
    ) -> AppResult<T> {
        let started = Instant::now();
        let result = fut.await;
        self.metrics
            .observe_storage(op, started.elapsed(), result.is_ok());
        result
    }
}

pub fn build_router(state: AppState) -> Router {
    let context = RequestContextLayer {
        cors_origin: state.cors_allow_origin.clone(),
    };

    // Artifact bodies are streamed by `upload::receive_upload` and JSON bodies are read by
    // `AppState::read_json`, both with explicit size limits; no `DefaultBodyLimit` needed.
    let mut router = Router::new()
        .route("/health", get(health_handler))
        .route("/healthz", get(health_handler))
        .route("/readyz", get(ready_handler))
        .route("/metrics", get(metrics_handler))
        .route(
            "/v8/artifacts/status",
            get(caching_status_handler).options(preflight_handler),
        )
        .route(
            "/v8/artifacts/events",
            post(record_events_handler)
                .get(get_events_handler)
                .options(preflight_handler),
        )
        .route(
            "/v8/artifacts",
            post(query_artifacts_handler).options(preflight_handler),
        )
        .route(
            "/v8/artifacts/{hash}",
            put(put_artifact_handler)
                .get(get_artifact_handler)
                .head(head_artifact_handler)
                .delete(delete_artifact_handler)
                .options(preflight_handler),
        );
    if state.oidc.is_some() {
        router = router.route("/oidc/token", post(oidc::token_handler));
    }
    let router = router
        // One layer for request id, request span + access log and CORS: separate `from_fn`
        // middlewares would cost a boxed future and a service clone each.
        .layer(context)
        .with_state(state);

    // `Router::layer` runs after routing, so the path is normalized in front of the router.
    Router::new().fallback_service(router.map_request(collapse_slashes))
}

/// Collapses repeated slashes in the request path. turbo appends `/v8/...` to its API URL
/// verbatim, so `TURBO_API=https://cache.example/` requests `//v8/artifacts/...`.
fn collapse_slashes(mut req: Request) -> Request {
    let uri = req.uri();
    if !uri.path().contains("//") {
        return req;
    }
    let mut path_and_query = String::with_capacity(uri.path().len());
    for segment in uri.path().split('/').filter(|s| !s.is_empty()) {
        path_and_query.push('/');
        path_and_query.push_str(segment);
    }
    if uri.path().ends_with('/') || path_and_query.is_empty() {
        path_and_query.push('/');
    }
    if let Some(query) = uri.query() {
        path_and_query.push('?');
        path_and_query.push_str(query);
    }
    let Ok(path_and_query) = path_and_query.parse() else {
        return req;
    };
    let mut parts = uri.clone().into_parts();
    parts.path_and_query = Some(path_and_query);
    if let Ok(uri) = Uri::from_parts(parts) {
        *req.uri_mut() = uri;
    }
    req
}

/// `tracing` target of the per-request span (`method`, `path`, `request_id` on every log line of
/// the request) and the access log line. `RUST_LOG=info,fast_turbo_cache_server::access=off`
/// disables both, which is the cheapest setting for high request rates.
pub const ACCESS_LOG_TARGET: &str = "fast_turbo_cache_server::access";

/// Endpoints polled by orchestrators/monitoring: successful requests are logged at DEBUG instead
/// of INFO so health checks and scrapes do not flood the access log.
fn is_probe_path(path: &str) -> bool {
    matches!(path, "/health" | "/healthz" | "/readyz" | "/metrics")
}

fn is_valid_request_id(value: &HeaderValue) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= MAX_REQUEST_ID_LEN
        && bytes
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':'))
}

/// Random 128-bit request id as 32 lowercase hex characters.
fn generate_request_id() -> HeaderValue {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut buf = [0u8; 32];
    let mut n = fastrand::u128(..);
    for byte in buf.iter_mut().rev() {
        *byte = HEX[(n & 0xf) as usize];
        n >>= 4;
    }
    HeaderValue::from_maybe_shared(Bytes::copy_from_slice(&buf))
        .expect("hex string is a valid header value")
}

fn content_length(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok())
}

/// Request-wide context around every route (see [`RequestContext`]).
#[derive(Clone)]
struct RequestContextLayer {
    cors_origin: HeaderValue,
}

impl<S> Layer<S> for RequestContextLayer {
    type Service = RequestContext<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RequestContext {
            inner,
            cors_origin: self.cors_origin.clone(),
        }
    }
}

/// Adds, for every request (including errors, 404s and preflights):
///
/// * **Request id**: a well-formed client `x-request-id` is kept, otherwise a random one is
///   generated; it is recorded on the request span and echoed on the response.
/// * **Request span and access log** (target [`ACCESS_LOG_TARGET`]): handler logs carry
///   `method`, `path` and `request_id`; one `finished processing request` line is logged per
///   request, at INFO (DEBUG for successful [probe](is_probe_path) requests).
/// * **CORS**: `Access-Control-Allow-Origin` and `Access-Control-Expose-Headers`, consistent
///   with [`preflight_handler`].
#[derive(Clone)]
struct RequestContext<S> {
    inner: S,
    cors_origin: HeaderValue,
}

impl<S> Service<Request> for RequestContext<S>
where
    S: Service<Request, Response = Response, Error = Infallible>,
{
    type Response = Response;
    type Error = Infallible;
    type Future = RequestContextFuture<S::Future>;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let started = Instant::now();
        let request_id = match req.headers().get(&HEADER_REQUEST_ID) {
            Some(value) if is_valid_request_id(value) => value.clone(),
            _ => generate_request_id(),
        };
        let probe = is_probe_path(req.uri().path());
        let id_str = request_id.to_str().unwrap_or("-");
        // INFO also for probes: a failed probe is logged at INFO and needs the span's fields.
        let span = tracing::info_span!(
            target: ACCESS_LOG_TARGET,
            "request",
            method = %req.method(),
            path = %req.uri().path(),
            request_id = %id_str,
        );

        let inner = {
            let _entered = span.enter();
            tracing::debug!(target: ACCESS_LOG_TARGET, "started processing request");
            self.inner.call(req)
        };
        RequestContextFuture {
            inner,
            span,
            request_id: Some(request_id),
            cors_origin: self.cors_origin.clone(),
            started,
            probe,
        }
    }
}

pin_project_lite::pin_project! {
    struct RequestContextFuture<F> {
        #[pin]
        inner: F,
        span: tracing::Span,
        request_id: Option<HeaderValue>,
        cors_origin: HeaderValue,
        started: Instant,
        probe: bool,
    }
}

impl<F> Future for RequestContextFuture<F>
where
    F: Future<Output = Result<Response, Infallible>>,
{
    type Output = Result<Response, Infallible>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let _entered = this.span.enter();
        let Ok(mut response) = ready!(this.inner.poll(cx));

        let headers = response.headers_mut();
        headers.insert(
            header::ACCESS_CONTROL_ALLOW_ORIGIN,
            this.cors_origin.clone(),
        );
        headers.insert(header::ACCESS_CONTROL_EXPOSE_HEADERS, CORS_EXPOSE_HEADERS);
        if let Some(id) = this.request_id.take() {
            headers.insert(HEADER_REQUEST_ID, id);
        }

        let status = response.status().as_u16();
        let latency_ms = latency_ms(this.started.elapsed());
        if *this.probe && response.status().is_success() {
            tracing::debug!(target: ACCESS_LOG_TARGET, status, latency_ms, "finished processing request");
        } else {
            tracing::info!(target: ACCESS_LOG_TARGET, status, latency_ms, "finished processing request");
        }
        Poll::Ready(Ok(response))
    }
}

/// Access log latency in milliseconds with at most three decimals: `micros / 1000.0` prints
/// cleanly, `secs_f64 * 1000.0` prints float noise such as `3.5929170000000004`.
fn latency_ms(elapsed: Duration) -> f64 {
    elapsed.as_micros() as f64 / 1000.0
}

/// Longest time spent draining an unread request body (see [`RequestBody`]).
pub const UNREAD_BODY_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Request body extractor for handlers that may reject a request before reading its body (auth,
/// size or upload-slot checks). Dropped unread or partially read (e.g. a JSON body over its size
/// limit), the rest is drained in the background for at most [`UNREAD_BODY_DRAIN_TIMEOUT`] while
/// the response is sent. Otherwise hyper closes the connection with unread data in the socket,
/// the kernel answers the still-sending client with a TCP reset that can discard the response,
/// and clients such as turbo (reqwest) see `connection reset` instead of the `401`/`403`/`413`.
///
/// The drain is bounded by time only: a fast client pushes several MiB before it reads the
/// response, so any byte cap brings the reset back for large bodies, and it limits nothing an
/// attacker could not get by sending more requests. A drained connection stays usable; one still
/// sending after the timeout is closed. Not done for clients waiting for `100 Continue` (reading
/// the body would ask for it) or for HTTP/2 (unread streams are reset individually).
struct RequestBody {
    body: Option<Body>,
    drain_unread: bool,
}

impl RequestBody {
    /// Takes the body for reading; it is no longer drained on drop.
    fn into_inner(mut self) -> Body {
        self.body.take().unwrap_or_else(Body::empty)
    }

    /// The body for reading in place; whatever is left unread is drained on drop.
    fn as_mut(&mut self) -> &mut Body {
        self.body.get_or_insert_with(Body::empty)
    }
}

impl<S: Send + Sync> axum::extract::FromRequest<S> for RequestBody {
    type Rejection = Infallible;

    async fn from_request(req: Request, _state: &S) -> Result<Self, Self::Rejection> {
        let drain_unread =
            req.version() <= Version::HTTP_11 && !req.headers().contains_key(header::EXPECT);
        Ok(Self {
            body: Some(req.into_body()),
            drain_unread,
        })
    }
}

impl Drop for RequestBody {
    fn drop(&mut self) {
        let Some(body) = self.body.take() else {
            return;
        };
        if !self.drain_unread || axum::body::HttpBody::is_end_stream(&body) {
            return;
        }
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let mut stream = body.into_data_stream();
                let _ = tokio::time::timeout(UNREAD_BODY_DRAIN_TIMEOUT, async {
                    while let Some(Ok(_)) = stream.next().await {}
                })
                .await;
            });
        }
    }
}

async fn health_handler() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({ "status": "ok" })))
}

async fn ready_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> AppResult<impl IntoResponse> {
    if state.protect_observability {
        state.auth.verify_headers(&headers)?;
    }
    state.storage.health_check().await?;
    Ok((
        StatusCode::OK,
        Json(json!({
            "status": "ready",
            "backend": state.storage.backend_name(),
        })),
    ))
}

async fn metrics_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> AppResult<impl IntoResponse> {
    if state.protect_observability {
        // Metrics aggregate all tenants: namespace-scoped tokens must not read them.
        state.auth.verify_unscoped(&headers)?;
    }
    let body = state
        .metrics
        .render_prometheus(state.storage.backend_name(), state.storage.cache_stats());
    Ok((
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    ))
}

/// Handles `OPTIONS` preflight requests made by Turborepo when `--preflight` is enabled.
///
/// No `Location` header is sent: Turborepo would use it as the URL of the actual request, and
/// a path-only value computed here is wrong behind path-prefixed reverse proxies.
async fn preflight_handler(State(state): State<AppState>) -> impl IntoResponse {
    let mut headers = HeaderMap::new();
    let methods = if state.allow_delete {
        HeaderValue::from_static("GET, HEAD, PUT, POST, DELETE, OPTIONS")
    } else {
        HeaderValue::from_static("GET, HEAD, PUT, POST, OPTIONS")
    };
    headers.insert(header::ACCESS_CONTROL_ALLOW_METHODS, methods);
    headers.insert(header::ACCESS_CONTROL_ALLOW_HEADERS, CORS_ALLOW_HEADERS);
    // `Access-Control-Allow-Origin` is added by the CORS response layer.
    (StatusCode::OK, headers)
}

/// `GET /v8/artifacts/status`
async fn caching_status_handler(
    State(state): State<AppState>,
    uri: Uri,
    headers: HeaderMap,
) -> AppResult<impl IntoResponse> {
    let ns_cow = if uri.query().is_some() {
        Some(state.resolve_namespace_from_uri(&uri)?)
    } else {
        None
    };
    let principal = state
        .auth
        .verify_headers_for_namespace(&headers, ns_cow.as_deref())?;
    Ok(Json(CachingStatusResponse {
        status: state.caching_status,
        read_only: principal.is_read_only(),
    }))
}

/// `POST /v8/artifacts/events`: events are stored per namespace.
async fn record_events_handler(
    State(state): State<AppState>,
    uri: Uri,
    headers: HeaderMap,
    mut body: RequestBody,
) -> AppResult<impl IntoResponse> {
    let namespace = state.resolve_namespace_from_uri(&uri)?;
    // Authenticate before reading/parsing the body.
    state
        .auth
        .verify_headers_for_namespace(&headers, Some(&namespace))?;
    let events: Vec<AnalyticsEvent> = state.read_json(&mut body, MAX_JSON_BODY_SIZE).await?;
    if events.len() > MAX_EVENTS_PER_REQUEST {
        return Err(AppError::BadRequest(format!(
            "Analytics batch contains {} events, exceeding maximum of {MAX_EVENTS_PER_REQUEST}",
            events.len()
        )));
    }
    validate_analytics_events(&events).map_err(AppError::BadRequest)?;
    state.metrics.record_analytics(&namespace, events);
    Ok(StatusCode::OK)
}

/// `GET /v8/artifacts/events`: returns only the requesting namespace's events.
async fn get_events_handler(
    State(state): State<AppState>,
    uri: Uri,
    headers: HeaderMap,
) -> AppResult<impl IntoResponse> {
    let namespace = state.resolve_namespace_from_uri(&uri)?;
    state
        .auth
        .verify_headers_for_namespace(&headers, Some(&namespace))?;
    Ok(Json(state.metrics.recent_events(&namespace)))
}

#[inline]
fn extract_artifact_hash(uri: &Uri) -> AppResult<&str> {
    // Valid hashes only contain unreserved characters, so they never need percent-encoding:
    // a `%` is simply an invalid character.
    validate_hash(
        uri.path()
            .strip_prefix("/v8/artifacts/")
            .unwrap_or_default(),
    )
}

/// `POST /v8/artifacts` (Batch metadata query)
async fn query_artifacts_handler(
    State(state): State<AppState>,
    uri: Uri,
    headers: HeaderMap,
    mut body: RequestBody,
) -> AppResult<impl IntoResponse> {
    let namespace_cow = state.resolve_namespace_from_uri(&uri)?;
    let namespace = namespace_cow.as_ref();
    state
        .auth
        .verify_headers_for_namespace(&headers, Some(namespace))?;
    state.ensure_cache_enabled()?;
    let req: ArtifactQueryRequest = state
        .read_json(&mut body, MAX_BATCH_QUERY_BODY_SIZE)
        .await?;

    if req.hashes.len() > MAX_BATCH_QUERY_HASHES {
        return Err(AppError::BadRequest(format!(
            "Batch query contains {} hashes, exceeding maximum of {MAX_BATCH_QUERY_HASHES}",
            req.hashes.len()
        )));
    }

    // Hashes that are invalid or whose lookup failed are left out of the response: turbo parses
    // the whole map as `{hash: null | hit}` (an error entry would fail the entire batch) and
    // resolves missing hashes with single `HEAD` requests, which report the error.
    let mut results: HashMap<String, Option<ArtifactQueryHit>> =
        HashMap::with_capacity(req.hashes.len());
    let mut cold_misses: Vec<String> = Vec::new();

    // Fast path: answer hot-cache hits synchronously, without a future per hash.
    for hash in req.hashes {
        if results.contains_key(&hash) {
            continue;
        }
        if validate_hash(&hash).is_err() {
            continue;
        }
        if let Some(meta) = state.storage.head_hot(namespace, &hash) {
            results.insert(hash, Some(meta.into()));
        } else {
            // Placeholder so duplicate hashes are only looked up once.
            results.insert(hash.clone(), None);
            cold_misses.push(hash);
        }
    }

    if !cold_misses.is_empty() {
        let state_ref = &state;
        let mut cold_stream = stream::iter(cold_misses)
            .map(|hash| async move {
                // Server-wide cap: concurrent batch queries cannot multiply backend load.
                let Ok(_permit) = state_ref.batch_head_semaphore.acquire().await else {
                    return (hash, Err(()));
                };
                let result = state_ref
                    .timed(StorageOp::Head, state_ref.storage.head(namespace, &hash))
                    .await;
                let entry = match result {
                    Ok(meta) => Ok(meta.map(ArtifactQueryHit::from)),
                    Err(err) => {
                        tracing::warn!(hash = %hash, error = %err, "Batch query lookup failed");
                        Err(())
                    }
                };
                (hash, entry)
            })
            .buffer_unordered(BATCH_QUERY_CONCURRENCY);

        while let Some((hash, entry)) = cold_stream.next().await {
            match entry {
                Ok(hit) => results.insert(hash, hit),
                Err(()) => results.remove(&hash),
            };
        }
    }

    Ok(Json(results))
}

fn parse_duration_header(headers: &HeaderMap) -> AppResult<u64> {
    let Some(val) = headers.get(HEADER_ARTIFACT_DURATION) else {
        return Ok(0);
    };
    let s = val.to_str().map_err(|_| {
        AppError::BadRequest("Invalid characters in x-artifact-duration header".to_string())
    })?;
    s.parse::<u64>().map_err(|_| {
        AppError::BadRequest(format!(
            "Invalid numeric value '{s}' in x-artifact-duration header"
        ))
    })
}

/// Whether an upload failed because of its request body (see [`UploadStream::next_chunk`]).
fn is_body_error(err: &AppError) -> bool {
    matches!(
        err,
        AppError::RequestTimeout(_) | AppError::PayloadTooLarge(_) | AppError::BadRequest(_)
    )
}

fn record_body_rejection(state: &AppState, err: &AppError) {
    match err {
        AppError::RequestTimeout(_) => state
            .metrics
            .record_upload_rejected(UploadRejection::Timeout),
        AppError::PayloadTooLarge(_) => state
            .metrics
            .record_upload_rejected(UploadRejection::TooLarge),
        _ => {}
    }
}

/// `PUT /v8/artifacts/{hash}`
async fn put_artifact_handler(
    State(state): State<AppState>,
    uri: Uri,
    headers: HeaderMap,
    body: RequestBody,
) -> AppResult<impl IntoResponse> {
    let (hash, namespace_cow, principal) = state.artifact_request(&uri, &headers)?;
    principal.ensure_writable()?;
    let namespace = namespace_cow.as_ref();

    let content_length = content_length(&headers);
    if let Some(len) = content_length
        && len > state.max_artifact_size as u64
    {
        state
            .metrics
            .record_upload_rejected(UploadRejection::TooLarge);
        return Err(AppError::PayloadTooLarge(format!(
            "Artifact size {len} bytes exceeds maximum allowed size of {} bytes",
            state.max_artifact_size
        )));
    }

    let duration = parse_duration_header(&headers)?;
    let tag = extract_optional_header(&headers, HEADER_ARTIFACT_TAG, MAX_TAG_HEADER_LEN)?;
    let sha = extract_optional_header(&headers, HEADER_ARTIFACT_SHA, MAX_METADATA_HEADER_LEN)?;
    let dirty_hash = extract_optional_header(
        &headers,
        HEADER_ARTIFACT_DIRTY_HASH,
        MAX_METADATA_HEADER_LEN,
    )?;
    let client_ci =
        extract_optional_header(&headers, HEADER_ARTIFACT_CLIENT_CI, MAX_METADATA_HEADER_LEN)?;

    // Immutability is enforced atomically by the backend (`PutMode::IfAbsent`); a separate
    // existence check here would only add a backend round trip and a TOCTOU window.

    // Bounded wait for an upload slot: a burst of slow uploads yields 429 instead of queueing
    // every other tenant indefinitely. The per-namespace share is counted per
    // `AuthConfig::upload_share_key`.
    let slot_wait = state.server_options.upload_slot_wait;
    let share_key = state.auth.upload_share_key(namespace);
    let upload_slot = match state.upload_slots.acquire(share_key, slot_wait).await {
        Ok(slot) => slot,
        Err(SlotUnavailable) => {
            state
                .metrics
                .record_upload_rejected(UploadRejection::SlotUnavailable);
            return Err(AppError::TooManyRequests(
                "All upload slots are busy; retry later".to_string(),
            ));
        }
    };
    // A started write runs to completion even if the request is cancelled (`run_to_completion`),
    // so the write task owns the slot and the in-flight gauge: a client that disconnects or
    // times out cannot free its slot while its write still runs.
    let guards = (upload_slot, state.metrics.upload_started());

    let limits = state.upload_limits();
    let mode = if state.immutable_artifacts {
        PutMode::IfAbsent
    } else {
        PutMode::Overwrite
    };
    let metadata = |size| ArtifactMetadata {
        duration,
        size,
        tag,
        sha,
        dirty_hash,
        client_ci,
    };
    // Large overwrite-mode uploads with a known length are stored while they arrive (S3
    // multipart); everything else is received completely first (RAM or spool file). The body is
    // read under idle/total deadlines either way, so a stalled client releases its slot.
    let streamed = mode == PutMode::Overwrite
        && state.storage.streams_uploads()
        && content_length.is_some_and(|len| len > limits.memory_threshold as u64);
    let (size, result) = if streamed {
        let size = content_length.unwrap_or(0);
        let upload = UploadStream::new(body.into_inner(), content_length, &limits)?;
        let storage = Arc::clone(&state.storage);
        let (namespace, hash) = (namespace.to_owned(), hash.to_owned());
        let metadata = metadata(size);
        let started = Instant::now();
        let result = run_to_completion(async move {
            let _guards = guards;
            storage
                .put_stream(&namespace, &hash, upload, metadata)
                .await
        })
        .await;
        // Body errors (timeouts, size, client gone) are the client's, not the backend's.
        let backend_ok = result.as_ref().map_or_else(is_body_error, |_| true);
        state
            .metrics
            .observe_storage(StorageOp::Put, started.elapsed(), backend_ok);
        (size, result)
    } else {
        let upload = match receive_upload(body.into_inner(), content_length, &limits).await {
            Ok(upload) => upload,
            Err(err) => {
                record_body_rejection(&state, &err);
                return Err(err);
            }
        };
        let size = upload.len();
        let storage = Arc::clone(&state.storage);
        let (namespace, hash) = (namespace.to_owned(), hash.to_owned());
        let metadata = metadata(size);
        let result = state
            .timed(
                StorageOp::Put,
                run_to_completion(async move {
                    let _guards = guards;
                    storage.put(&namespace, &hash, upload, metadata, mode).await
                }),
            )
            .await;
        (size, result)
    };
    let outcome = result.inspect_err(|err| record_body_rejection(&state, err))?;
    if outcome == PutOutcome::AlreadyExists {
        return Err(AppError::forbidden(
            "artifact_immutable",
            format!("Artifact '{hash}' already exists and immutable mode is enabled"),
        ));
    }
    state.metrics.record_put(size);

    Ok((
        StatusCode::ACCEPTED,
        Json(PutArtifactResponse {
            urls: vec![format!("{namespace}/{hash}")],
        }),
    ))
}

/// `GET /v8/artifacts/{hash}`
async fn get_artifact_handler(
    State(state): State<AppState>,
    uri: Uri,
    headers: HeaderMap,
) -> AppResult<Response> {
    let (hash, namespace_cow, _) = state.artifact_request(&uri, &headers)?;
    let namespace = namespace_cow.as_ref();

    // Synchronous hot-cache fast path: no boxed future or stream.
    if let Some((bytes, metadata)) = state.storage.get_hot(namespace, hash) {
        state.metrics.record_get_hit(bytes.len() as u64);
        let response_headers = build_artifact_headers(&metadata, Some(bytes.len() as u64))?;
        return Ok((StatusCode::OK, response_headers, Body::from(bytes)).into_response());
    }

    let Some(stored) = state
        .timed(StorageOp::Get, state.storage.get(namespace, hash))
        .await?
    else {
        state.metrics.record_get_miss();
        return Err(AppError::NotFound(format!("Artifact '{hash}' not found")));
    };

    let (body, content_length) = match stored.payload {
        ArtifactPayload::Bytes(bytes) => {
            let len = bytes.len() as u64;
            (Body::from(bytes), Some(len))
        }
        // `None` = unknown length: no Content-Length, the body is sent chunked.
        ArtifactPayload::Stream(stream) => (Body::from_stream(stream), stored.content_length),
    };
    state
        .metrics
        .record_get_hit(content_length.unwrap_or(stored.metadata.size));
    let response_headers = build_artifact_headers(&stored.metadata, content_length)?;

    Ok((StatusCode::OK, response_headers, body).into_response())
}

/// `HEAD /v8/artifacts/{hash}`
async fn head_artifact_handler(
    State(state): State<AppState>,
    uri: Uri,
    headers: HeaderMap,
) -> AppResult<impl IntoResponse> {
    let (hash, namespace_cow, _) = state.artifact_request(&uri, &headers)?;
    let namespace = namespace_cow.as_ref();

    if let Some(metadata) = state.storage.head_hot(namespace, hash) {
        state.metrics.record_head_hit();
        let response_headers = build_artifact_headers(&metadata, Some(metadata.size))?;
        return Ok((StatusCode::OK, response_headers));
    }

    let Some(metadata) = state
        .timed(StorageOp::Head, state.storage.head(namespace, hash))
        .await?
    else {
        state.metrics.record_head_miss();
        return Err(AppError::NotFound(format!("Artifact '{hash}' not found")));
    };

    state.metrics.record_head_hit();
    let response_headers = build_artifact_headers(&metadata, Some(metadata.size))?;

    Ok((StatusCode::OK, response_headers))
}

/// `DELETE /v8/artifacts/{hash}`
async fn delete_artifact_handler(
    State(state): State<AppState>,
    uri: Uri,
    headers: HeaderMap,
) -> AppResult<impl IntoResponse> {
    let (hash, namespace_cow, principal) = state.artifact_request(&uri, &headers)?;
    principal.ensure_writable()?;
    let namespace = namespace_cow.as_ref();

    if !state.allow_delete {
        return Err(AppError::forbidden(
            "delete_disabled",
            "Artifact deletion is disabled on this server",
        ));
    }

    let deleted = state
        .timed(StorageOp::Delete, state.storage.delete(namespace, hash))
        .await?;
    if deleted {
        Ok((
            StatusCode::OK,
            Json(json!({ "deleted": true, "hash": hash })),
        ))
    } else {
        Err(AppError::NotFound(format!("Artifact '{hash}' not found")))
    }
}

fn extract_optional_header(
    headers: &HeaderMap,
    name: HeaderName,
    max_len: usize,
) -> AppResult<Option<Arc<str>>> {
    let Some(val) = headers.get(&name) else {
        return Ok(None);
    };
    let s = val
        .to_str()
        .map_err(|_| AppError::BadRequest(format!("Invalid characters in {name} header")))?
        .trim();
    if s.is_empty() {
        return Ok(None);
    }
    if s.len() > max_len {
        return Err(AppError::BadRequest(format!(
            "Header {name} exceeds maximum length of {max_len} characters"
        )));
    }
    // Visible ASCII and space only. Control characters, including the HTAB that RFC 9110
    // allows in field values, are rejected (`to_str` already rejects the rest): every backend
    // must store identical bytes, and S3-compatible stores disagree on how SigV4 signs tabs.
    if !val.as_bytes().iter().all(|b| (0x20..0x7f).contains(b)) {
        return Err(AppError::BadRequest(format!(
            "Header {name} contains control characters (including tabs) or non-ASCII bytes"
        )));
    }
    Ok(Some(Arc::from(s)))
}

/// Builds artifact response headers. `content_length = None` (unknown length) omits
/// `Content-Length` so the body is sent with chunked encoding instead of a wrong length.
fn build_artifact_headers(
    metadata: &ArtifactMetadata,
    content_length: Option<u64>,
) -> AppResult<HeaderMap> {
    let mut headers = HeaderMap::with_capacity(7);
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    if let Some(len) = content_length {
        headers.insert(header::CONTENT_LENGTH, HeaderValue::from(len));
    }
    headers.insert(
        HEADER_ARTIFACT_DURATION,
        HeaderValue::from(metadata.duration),
    );

    let optional = [
        (HEADER_ARTIFACT_TAG, &metadata.tag),
        (HEADER_ARTIFACT_SHA, &metadata.sha),
        (HEADER_ARTIFACT_DIRTY_HASH, &metadata.dirty_hash),
        (HEADER_ARTIFACT_CLIENT_CI, &metadata.client_ci),
    ];
    for (name, value) in optional {
        if let Some(value) = value {
            let val = HeaderValue::from_str(value).map_err(|e| {
                AppError::internal_with_source(format!("Invalid stored {name} header"), &e)
            })?;
            headers.insert(name, val);
        }
    }

    Ok(headers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn access_log_latency_has_at_most_three_decimals() {
        assert_eq!(
            latency_ms(Duration::from_nanos(3_592_917)).to_string(),
            "3.592"
        );
        assert_eq!(latency_ms(Duration::from_millis(12)).to_string(), "12");
        assert_eq!(latency_ms(Duration::ZERO).to_string(), "0");
        for nanos in (0..20_000_000u64).step_by(997) {
            let text = latency_ms(Duration::from_nanos(nanos)).to_string();
            let decimals = text.split_once('.').map_or(0, |(_, frac)| frac.len());
            assert!(decimals <= 3, "{nanos} ns -> {text}");
        }
    }

    #[test]
    fn request_ids_are_validated() {
        assert!(is_valid_request_id(&HeaderValue::from_static(
            "abc-123_x.y:z"
        )));
        assert!(!is_valid_request_id(&HeaderValue::from_static("")));
        assert!(!is_valid_request_id(&HeaderValue::from_static("a b")));
        let long = "a".repeat(MAX_REQUEST_ID_LEN + 1);
        assert!(!is_valid_request_id(&HeaderValue::from_str(&long).unwrap()));
        let generated = generate_request_id();
        assert_eq!(generated.len(), 32);
        assert!(is_valid_request_id(&generated));
    }

    #[test]
    fn hash_extraction_rejects_percent_encoding() {
        let uri: Uri = "/v8/artifacts/abc%2e".parse().unwrap();
        assert!(matches!(
            extract_artifact_hash(&uri),
            Err(AppError::BadRequest(_))
        ));
        let uri: Uri = "/v8/artifacts/abc123".parse().unwrap();
        assert_eq!(extract_artifact_hash(&uri).unwrap(), "abc123");
    }
}
