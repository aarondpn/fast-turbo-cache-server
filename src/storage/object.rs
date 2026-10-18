//! The storage backend shared by the object stores (S3, Google Cloud Storage, Azure Blob
//! Storage), built on an `object_store` client, which signs requests, retries transient failures
//! (5xx, 408, 429, transport errors; `STORAGE_MAX_RETRIES`, jittered exponential backoff), loads
//! credentials and runs multipart uploads. This module adds the HTTP client policy, the error
//! classification and the upload strategy; `s3`, `gcs` and `azure` configure the stores.
//!
//! - Objects live at `[prefix/]<namespace>/<hash>`; the artifact metadata is stored as object
//!   metadata (`x-amz-meta-*`, `x-goog-meta-*`, `x-ms-meta-*`). A store or proxy that drops it
//!   loses the metadata (the artifact itself is still served).
//! - Bodies up to one part (8 MiB) are a single `PUT`. Larger overwrite-mode uploads are multipart
//!   uploads, streamed from the request when it has a `Content-Length` (memory per upload bounded
//!   by `(STORAGE_PARTS_IN_FLIGHT + 1) x 8 MiB`), otherwise sent from the spool file. A multipart
//!   upload is completed only after the whole body arrived and aborted on any error, timeout or
//!   cancellation, so nothing partial becomes visible.
//! - `PutMode::IfAbsent` is always one conditional `PUT` (`If-None-Match: *`, on GCS
//!   `x-goog-if-generation-match: 0`).
//! - Redirects are never followed (they could forward signed headers and tokens). Request
//!   attempts and response bodies are time-bounded (see [`TimedService`]), so a stalled store
//!   cannot hang a request.
//! - Only a missing object is a miss; a missing bucket or container and credential problems are
//!   storage errors, also for body-less `HEAD` answers (see `head_object`).
//! - Readiness probes (a `LIST` of the prefix with at most one key, or a 1-byte `GET` of a probe
//!   key) are not retried, bounded by 5s (including credential loading), cached for 5s (failures
//!   too) and single-flight.
//! - Retention (`STORAGE_MAX_AGE_SECS`, `STORAGE_MAX_BYTES`) is enforced by a background janitor
//!   that lists the prefix (see `janitor`).

use std::{
    fmt, io,
    pin::Pin,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures_util::{Stream, StreamExt, TryStreamExt};
use http::{StatusCode, header};
use object_store::{
    Attribute, AttributeValue, Attributes, BackoffConfig, ClientConfigKey, ClientOptions,
    GetOptions, GetRange, GetResult, MultipartUpload, ObjectStore, ObjectStoreExt,
    PutMultipartOptions, PutOptions, PutPayload, RetryConfig,
    client::{
        HttpClient, HttpConnector, HttpError, HttpErrorKind, HttpRequest, HttpResponse,
        HttpResponseBody, HttpService,
    },
    list::{PaginatedListOptions, PaginatedListStore},
    path::Path as ObjectPath,
};
use tokio::task::JoinSet;

use crate::{
    error::{AppError, AppResult},
    models::ArtifactMetadata,
    storage::{PutMode, PutOutcome, StoredArtifact, UploadBody, sanitize_namespace, validate_hash},
    upload::UploadStream,
};

use super::Storage;

mod janitor;

pub use janitor::CleanupReport;

/// Size of every multipart upload part except the last one.
pub const MULTIPART_PART_SIZE: usize = 8 * 1024 * 1024;
/// Most parts S3 and GCS accept for one multipart upload.
pub const MAX_MULTIPART_PARTS: u64 = 10_000;
/// Largest artifact a multipart upload with [`MULTIPART_PART_SIZE`] parts can store.
pub const MAX_MULTIPART_UPLOAD_BYTES: u64 = MAX_MULTIPART_PARTS * MULTIPART_PART_SIZE as u64;

const CONTENT_TYPE: &str = "application/octet-stream";

/// Readiness results (successes AND failures) are cached for this long.
const HEALTH_CHECK_CACHE_TTL: Duration = Duration::from_secs(5);
/// Upper bound for one readiness probe, so `/readyz` answers quickly even while the store hangs.
const HEALTH_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
/// Read size for streaming spooled files into multipart uploads.
const FILE_CHUNK_BYTES: usize = 256 * 1024;
/// Request attempts may take `connect + read timeout + body size / this rate` until the response
/// headers arrive, so a stalled upload cannot hang forever.
const MIN_UPLOAD_BYTES_PER_SEC: u64 = 256 * 1024;
/// A code-less `HEAD` 404 is trusted as "missing object" (not a missing bucket) for this long
/// after the bucket was last seen to exist.
const BUCKET_CONFIRM_TTL: Duration = Duration::from_secs(60);
const RETRY_BASE_DELAY: Duration = Duration::from_millis(100);
const RETRY_MAX_DELAY: Duration = Duration::from_secs(5);

/// Names of the artifact metadata entries (`x-amz-meta-<name>`, ...). `size` is written for
/// compatibility only (reads take the `Content-Length`).
pub(crate) struct MetadataNames {
    pub duration: &'static str,
    pub size: &'static str,
    pub tag: &'static str,
    pub sha: &'static str,
    pub dirty_hash: &'static str,
    pub client_ci: &'static str,
}

pub(crate) const METADATA_NAMES: MetadataNames = MetadataNames {
    duration: "duration",
    size: "size",
    tag: "tag",
    sha: "sha",
    dirty_hash: "dirty-hash",
    client_ci: "client-ci",
};

/// The error codes in a store's XML error bodies that tell a missing object from a missing
/// bucket (or container).
pub(crate) struct ErrorCodes {
    pub missing_object: &'static str,
    pub missing_bucket: &'static str,
}

/// What differs between the stores and deployments of an [`ObjectStorage`].
pub(crate) struct StoreOptions {
    /// Backend name (`s3`, `rustfs`, `garage`, `gcs`, `azure`), also used in errors and logs.
    pub name: &'static str,
    /// Validated key prefix without surrounding `/` (empty: none).
    pub prefix: String,
    /// Validated key probed by readiness checks instead of listing the prefix.
    pub health_check_key: Option<String>,
    pub parts_in_flight: usize,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    /// Largest body of a single `PUT` (conditional writes are always one).
    pub max_single_put: u64,
    pub metadata: &'static MetadataNames,
    pub codes: ErrorCodes,
    /// Which 403 answers to lookups are misses (`S3_TREAT_403_AS_MISS`); `None`: none.
    pub forbidden_miss: Option<fn(&Failure) -> bool>,
    /// Error code of a multipart commit whose parts were discarded because a concurrent upload of
    /// the same key committed first (Azure: committing a blob drops all its uncommitted blocks).
    pub lost_commit: Option<&'static str>,
}

/// An `object_store` store that can also list one page with a key limit (readiness probes).
pub(crate) trait Store: ObjectStore + PaginatedListStore {}

impl<T: ObjectStore + PaginatedListStore> Store for T {}

type HealthResult = (Instant, Result<(), String>);

/// Cached readiness result plus a lock held by the (single) in-flight probe.
#[derive(Default)]
struct HealthState {
    last: RwLock<Option<HealthResult>>,
    probe: tokio::sync::Mutex<()>,
}

impl HealthState {
    fn last(&self) -> Option<HealthResult> {
        self.last.read().ok().and_then(|guard| guard.clone())
    }

    fn fresh(&self) -> Option<Result<(), String>> {
        self.last()
            .filter(|(at, _)| at.elapsed() < HEALTH_CHECK_CACHE_TTL)
            .map(|(_, result)| result)
    }
}

/// Artifact storage on an object store (see the module documentation).
#[derive(Clone)]
pub struct ObjectStorage {
    store: Arc<dyn Store>,
    /// Same store without retries, sharing the credentials of `store` (readiness probes).
    probe_store: Arc<dyn Store>,
    options: Arc<StoreOptions>,
    health: Arc<HealthState>,
    /// When the bucket was last seen to exist (see `head_object`).
    bucket_seen: Arc<BucketSeen>,
}

impl fmt::Debug for ObjectStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The store's `Display` names the bucket only (its `Debug` may show credentials).
        f.debug_struct("ObjectStorage")
            .field("name", &self.options.name)
            .field("store", &format_args!("{}", self.store))
            .field("prefix", &self.options.prefix)
            .finish_non_exhaustive()
    }
}

impl ObjectStorage {
    pub(crate) fn new(
        store: impl Store + 'static,
        probe_store: impl Store + 'static,
        options: StoreOptions,
    ) -> Self {
        Self {
            store: Arc::new(store),
            probe_store: Arc::new(probe_store),
            options: Arc::new(options),
            health: Arc::new(HealthState::default()),
            bucket_seen: Arc::new(BucketSeen::default()),
        }
    }

    /// Builds the full object key inside the bucket: `[prefix/]<namespace>/<hash>`.
    pub fn object_key(&self, namespace: &str, hash: &str) -> AppResult<String> {
        let ns = sanitize_namespace(namespace)?;
        let hash = validate_hash(hash)?;

        if self.options.prefix.is_empty() {
            Ok(format!("{ns}/{hash}"))
        } else {
            Ok(format!("{}/{ns}/{hash}", self.options.prefix))
        }
    }

    pub(crate) fn object_path(&self, namespace: &str, hash: &str) -> AppResult<ObjectPath> {
        let key = self.object_key(namespace, hash)?;
        // The prefix was validated at construction; namespace and hash are plain identifiers.
        parse_key(&key).map_err(AppError::Internal)
    }

    fn bucket_recently_seen(&self) -> bool {
        self.bucket_seen.recently_seen()
    }

    /// Converts a store result, recording what it tells about the bucket.
    fn observe<T>(&self, result: object_store::Result<T>) -> Result<T, Failure> {
        match result {
            Ok(value) => {
                self.bucket_seen.exists();
                Ok(value)
            }
            Err(err) => {
                let failure = Failure::new(err);
                let codes = &self.options.codes;
                match failure.code.as_deref() {
                    Some(code) if code == codes.missing_object => self.bucket_seen.exists(),
                    Some(code) if code == codes.missing_bucket => self.bucket_seen.missing(),
                    _ => {}
                }
                Err(failure)
            }
        }
    }

    /// Whether a failed GET/HEAD means "object does not exist". Only the object request's own
    /// answer can be a miss: a 404 or 403 of a credential endpoint (an instance without a role or
    /// service account) is a `Generic` error whose text merely contains that status.
    pub(crate) fn is_miss(&self, failure: &Failure) -> bool {
        match *failure.error {
            object_store::Error::NotFound { .. } => {
                failure.code.as_deref() != Some(self.options.codes.missing_bucket)
            }
            object_store::Error::PermissionDenied { .. } => self
                .options
                .forbidden_miss
                .is_some_and(|is_miss| is_miss(failure)),
            _ => false,
        }
    }

    fn failure_error(&self, op: &str, key: &ObjectPath, failure: &Failure) -> AppError {
        AppError::Storage(format!(
            "{} {op} '{key}' failed: {failure}",
            self.options.name
        ))
    }

    /// `HEAD` on an object; `None` when it does not exist.
    ///
    /// `HEAD` answers have no body and thus no error code, so a bare 403 or 404 is ambiguous and
    /// re-checked with a 1-byte ranged `GET`, whose body carries `<Code>`:
    /// - a 403 that may be a miss (`S3_TREAT_403_AS_MISS`) could be a missing object without
    ///   `s3:ListBucket` or a signature/credential problem, which must never be reported as a miss;
    /// - a 404 could also be a missing bucket. It is re-checked only when the bucket was not seen
    ///   to exist within [`BUCKET_CONFIRM_TTL`], so this costs at most one extra request per
    ///   window, and a bucket deleted while the server runs is reported as misses by `HEAD` for
    ///   at most that long (`GET` and the readiness probe detect it at once).
    async fn head_object(&self, path: &ObjectPath) -> AppResult<Option<GetResult>> {
        let options = GetOptions {
            head: true,
            ..GetOptions::default()
        };
        match self.observe(self.store.get_opts(path, options).await) {
            Ok(result) => Ok(Some(result)),
            Err(failure)
                if failure.is_forbidden()
                    && failure.code.is_none()
                    && self.options.forbidden_miss.is_some() =>
            {
                match self.ranged_get(path).await {
                    Ok(()) => Err(AppError::Storage(format!(
                        "{} HEAD '{path}' was denied (HTTP 403) but GET is allowed; grant s3:GetObject consistently",
                        self.options.name
                    ))),
                    Err(failure) if self.is_miss(&failure) => Ok(None),
                    Err(failure) => Err(self.failure_error("HEAD", path, &failure)),
                }
            }
            Err(failure)
                if failure.is_not_found()
                    && failure.code.is_none()
                    && !self.bucket_recently_seen() =>
            {
                match self.ranged_get(path).await {
                    // Created between the two requests; the HEAD answer (missing) is still valid.
                    Ok(()) => Ok(None),
                    Err(failure) if self.is_miss(&failure) => Ok(None),
                    Err(failure) => Err(self.failure_error("HEAD", path, &failure)),
                }
            }
            Err(failure) if self.is_miss(&failure) => Ok(None),
            Err(failure) => Err(self.failure_error("HEAD", path, &failure)),
        }
    }

    /// 1-byte ranged `GET`, whose error responses carry the error code (unlike `HEAD`).
    async fn ranged_get(&self, path: &ObjectPath) -> Result<(), Failure> {
        self.observe(self.store.get_opts(path, first_byte()).await)
            .map(drop)
    }

    /// Deletes an artifact object; `false` when it does not exist. S3 answers the delete of a
    /// missing key with success as well, so a `HEAD` asks first. Only a 404 counts as missing:
    /// for any other error status (also a 403 that `S3_TREAT_403_AS_MISS` takes as a miss) the
    /// delete is sent anyway. When the `HEAD` gets no answer at all (transport errors, timeouts,
    /// credentials that cannot be loaded), its error is returned: the delete would fail the same
    /// way after another full round of retries.
    async fn delete_existing_object(&self, path: &ObjectPath) -> AppResult<bool> {
        let options = GetOptions {
            head: true,
            ..GetOptions::default()
        };
        match self.observe(self.store.get_opts(path, options).await) {
            Err(failure) if failure.is_not_found() && self.is_miss(&failure) => {
                if failure.code.is_some() || self.bucket_recently_seen() {
                    return Ok(false);
                }
                // A code-less 404 may also mean a missing bucket (see `head_object`); the delete
                // answers that.
                self.delete_object(path).await.map(|_| false)
            }
            Err(failure) if failure.status.is_none() => {
                Err(self.failure_error("HEAD", path, &failure))
            }
            _ => self.delete_object(path).await,
        }
    }

    async fn delete_object(&self, path: &ObjectPath) -> AppResult<bool> {
        let location = path.clone();
        let mut results = self
            .store
            .delete_stream(futures_util::stream::once(async move { Ok(location) }).boxed());
        let result = match results.next().await {
            Some(result) => result.map(drop),
            None => Ok(()),
        };
        match self.observe(result) {
            Ok(()) => Ok(true),
            Err(failure) if failure.is_not_found() && self.is_miss(&failure) => {
                // Azure reports the deletes of a batch with bare statuses, so a 404 may also mean
                // a missing container: re-checked like `head_object` does.
                if failure.code.is_none()
                    && !self.bucket_recently_seen()
                    && let Err(failure) = self.ranged_get(path).await
                    && !self.is_miss(&failure)
                {
                    return Err(self.failure_error("DELETE", path, &failure));
                }
                Ok(false)
            }
            Err(failure) => Err(self.failure_error("DELETE", path, &failure)),
        }
    }

    /// Single `PUT` (conditional for `PutMode::IfAbsent`).
    async fn put_single(
        &self,
        path: &ObjectPath,
        payload: PutPayload,
        attributes: Attributes,
        mode: PutMode,
    ) -> AppResult<PutOutcome> {
        if payload.content_length() as u64 > self.options.max_single_put {
            return Err(AppError::PayloadTooLarge(format!(
                "Artifact of {} bytes exceeds the single-request upload limit of {} ({} bytes)",
                payload.content_length(),
                self.options.name,
                self.options.max_single_put
            )));
        }
        let options = PutOptions {
            mode: match mode {
                PutMode::Overwrite => object_store::PutMode::Overwrite,
                PutMode::IfAbsent => object_store::PutMode::Create,
            },
            attributes,
            ..PutOptions::default()
        };
        let result = self.store.put_opts(path, payload, options).await;
        match result {
            // 412 (or 409 for a concurrent conditional write): also the answer to a retried
            // attempt whose earlier attempt was applied but whose response was lost.
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) if mode == PutMode::IfAbsent => {
                self.bucket_seen.exists();
                Ok(PutOutcome::AlreadyExists)
            }
            result => match self.observe(result) {
                Ok(_) => Ok(PutOutcome::Stored),
                Err(failure) => Err(self.failure_error("PUT", path, &failure)),
            },
        }
    }

    /// Multipart upload of `chunks` (overwrite mode). Parts are only completed after the stream
    /// ended without error; otherwise (and when this future is dropped) the upload is aborted.
    async fn put_multipart<S>(
        &self,
        path: &ObjectPath,
        len: u64,
        attributes: Attributes,
        chunks: S,
    ) -> AppResult<PutOutcome>
    where
        S: Stream<Item = AppResult<Bytes>> + Unpin,
    {
        let options = PutMultipartOptions {
            attributes,
            ..PutMultipartOptions::default()
        };
        let upload = self
            .observe(self.store.put_multipart_opts(path, options).await)
            .map_err(|failure| self.failure_error("CreateMultipartUpload", path, &failure))?;
        let mut parts = PartWriter::new(upload, self.options.parts_in_flight);
        let sent = self.send_parts(&mut parts, path, chunks).await;
        let result = match sent {
            Ok(()) => parts.complete().await,
            Err(err) => {
                parts.abort().await;
                return Err(err);
            }
        };
        match self.observe(result) {
            Ok(()) => Ok(PutOutcome::Stored),
            // Keys are content hashes: the upload that won stored the same artifact.
            Err(failure)
                if self
                    .options
                    .lost_commit
                    .is_some_and(|code| failure.code.as_deref() == Some(code))
                    && self
                        .store
                        .head(path)
                        .await
                        .is_ok_and(|meta| meta.size == len) =>
            {
                Ok(PutOutcome::Stored)
            }
            Err(failure) => Err(self.failure_error("multipart upload", path, &failure)),
        }
    }

    /// Cuts `chunks` into [`MULTIPART_PART_SIZE`] parts and sends them.
    async fn send_parts<S>(
        &self,
        parts: &mut PartWriter,
        path: &ObjectPath,
        mut chunks: S,
    ) -> AppResult<()>
    where
        S: Stream<Item = AppResult<Bytes>> + Unpin,
    {
        let mut part = BytesMut::new();
        while let Some(mut chunk) = chunks.try_next().await? {
            while !chunk.is_empty() {
                if part.capacity() == 0 {
                    part.reserve(MULTIPART_PART_SIZE);
                }
                let take = (MULTIPART_PART_SIZE - part.len()).min(chunk.len());
                part.extend_from_slice(&chunk.split_to(take));
                if part.len() == MULTIPART_PART_SIZE {
                    let full = std::mem::take(&mut part).freeze();
                    parts
                        .send(full)
                        .await
                        .map_err(|e| self.failure_error("UploadPart", path, &Failure::new(e)))?;
                }
            }
        }
        if !part.is_empty() || parts.sent == 0 {
            parts
                .send(part.freeze())
                .await
                .map_err(|e| self.failure_error("UploadPart", path, &Failure::new(e)))?;
        }
        Ok(())
    }

    async fn probe_health(&self) -> Result<(), String> {
        let result = match &self.options.health_check_key {
            // Prefix-scoped policies may not allow listing: probe a (possibly missing) object
            // instead. GET (not HEAD) so a missing bucket is distinguishable by its error code;
            // the 1-byte range keeps existing objects cheap.
            Some(key) => {
                let path = parse_key(key)?;
                match self.observe(self.probe_store.get_opts(&path, first_byte()).await) {
                    Ok(_) => Ok(()),
                    Err(failure)
                        if failure.status == Some(StatusCode::RANGE_NOT_SATISFIABLE)
                            || self.is_miss(&failure) =>
                    {
                        Ok(())
                    }
                    Err(failure) => Err(("health GET", failure)),
                }
            }
            None => {
                let prefix =
                    (!self.options.prefix.is_empty()).then(|| format!("{}/", self.options.prefix));
                let options = PaginatedListOptions {
                    max_keys: Some(1),
                    ..PaginatedListOptions::default()
                };
                let listed = self
                    .probe_store
                    .list_paginated(prefix.as_deref(), options)
                    .await;
                self.observe(listed)
                    .map(drop)
                    .map_err(|failure| ("LIST", failure))
            }
        };
        result.map_err(|(op, failure)| {
            format!(
                "{} health check ({op}) failed: {failure}",
                self.options.name
            )
        })
    }
}

fn first_byte() -> GetOptions {
    GetOptions {
        range: Some(GetRange::Bounded(0..1)),
        ..GetOptions::default()
    }
}

/// Sends multipart upload parts with at most `limit` (`STORAGE_PARTS_IN_FLIGHT`) in flight.
/// Dropping it before [`PartWriter::complete`] or [`PartWriter::abort`] (request cancelled, e.g.
/// by a client disconnect) cancels the part uploads and aborts the upload in the background.
struct PartWriter {
    upload: Option<Box<dyn MultipartUpload>>,
    in_flight: JoinSet<object_store::Result<()>>,
    limit: usize,
    sent: usize,
}

impl PartWriter {
    fn new(upload: Box<dyn MultipartUpload>, limit: usize) -> Self {
        Self {
            upload: Some(upload),
            in_flight: JoinSet::new(),
            limit: limit.max(1),
            sent: 0,
        }
    }

    /// Waits for a free in-flight slot, then starts uploading `part`.
    async fn send(&mut self, part: Bytes) -> object_store::Result<()> {
        while self.in_flight.len() >= self.limit {
            self.join_one().await?;
        }
        let upload = self
            .upload
            .as_mut()
            .expect("upload is only taken when finished");
        self.in_flight
            .spawn(upload.put_part(PutPayload::from(part)));
        self.sent += 1;
        Ok(())
    }

    async fn join_one(&mut self) -> object_store::Result<()> {
        match self.in_flight.join_next().await {
            None | Some(Ok(Ok(()))) => Ok(()),
            Some(Ok(Err(err))) => Err(err),
            Some(Err(err)) if err.is_panic() => std::panic::resume_unwind(err.into_panic()),
            Some(Err(err)) => Err(object_store::Error::JoinError { source: err }),
        }
    }

    /// Waits for the remaining parts and completes the upload (aborting it on failure).
    async fn complete(mut self) -> object_store::Result<()> {
        while !self.in_flight.is_empty() {
            if let Err(err) = self.join_one().await {
                self.abort().await;
                return Err(err);
            }
        }
        let mut upload = self
            .upload
            .take()
            .expect("upload is only taken when finished");
        match upload.complete().await {
            Ok(_) => Ok(()),
            Err(err) => {
                abort_upload(upload).await;
                Err(err)
            }
        }
    }

    async fn abort(mut self) {
        self.in_flight.shutdown().await;
        if let Some(upload) = self.upload.take() {
            abort_upload(upload).await;
        }
    }
}

impl Drop for PartWriter {
    fn drop(&mut self) {
        let Some(upload) = self.upload.take() else {
            return;
        };
        self.in_flight.abort_all();
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(abort_upload(upload));
            }
            Err(_) => tracing::warn!(
                "Multipart upload cancelled without a runtime; its parts stay until a lifecycle rule removes them"
            ),
        }
    }
}

async fn abort_upload(mut upload: Box<dyn MultipartUpload>) {
    if let Err(err) = upload.abort().await {
        tracing::warn!(
            error = %Failure::new(err),
            "Failed to abort a multipart upload; its parts stay until a lifecycle rule (abort incomplete multipart uploads) removes them"
        );
    }
}

/// When the bucket was last seen to exist, as milliseconds since `base` plus one (0: not seen, or
/// seen missing). An atomic instead of a lock: every request records it.
struct BucketSeen {
    base: Instant,
    at: AtomicU64,
}

impl Default for BucketSeen {
    fn default() -> Self {
        Self {
            base: Instant::now(),
            at: AtomicU64::new(0),
        }
    }
}

impl BucketSeen {
    fn now(&self) -> u64 {
        u64::try_from(self.base.elapsed().as_millis()).unwrap_or(u64::MAX - 1) + 1
    }

    fn exists(&self) {
        let now = self.now();
        // Skip the store (a shared cache line written by every request) while the last record is
        // younger than a second.
        let at = self.at.load(Ordering::Relaxed);
        if at == 0 || now.saturating_sub(at) >= 1000 {
            self.at.store(now, Ordering::Relaxed);
        }
    }

    fn missing(&self) {
        self.at.store(0, Ordering::Relaxed);
    }

    fn recently_seen(&self) -> bool {
        let at = self.at.load(Ordering::Relaxed);
        at != 0 && Duration::from_millis(self.now().saturating_sub(at)) < BUCKET_CONFIRM_TTL
    }
}

/// What a failed `object_store` call tells about the store's answer.
///
/// `object_store` does not expose the error body as data, so the status and error code are taken
/// from the error text, which carries the response body (S3, GCS and Azure all answer with an
/// XML `<Error><Code>`). Misses are the hot error path: construction formats the error once and
/// reads only the code; the message and the full error chain are built only when the failure is
/// displayed. The displayed text never contains URL query strings (an Azure SAS token's `sig`).
#[derive(Debug)]
pub(crate) struct Failure {
    /// HTTP status of the final answer (`None` for transport errors and timeouts), for `Generic`
    /// errors taken from their text, which may also be a credential endpoint's answer.
    pub status: Option<StatusCode>,
    /// Error code from the XML error body (never present for `HEAD`).
    pub code: Option<String>,
    /// The error text (`object_store` includes the response body in it).
    text: String,
    error: Box<object_store::Error>,
}

impl Failure {
    pub fn new(error: object_store::Error) -> Self {
        let text = error.to_string();
        let status = match &error {
            object_store::Error::NotFound { .. } => Some(StatusCode::NOT_FOUND),
            object_store::Error::PermissionDenied { .. } => Some(StatusCode::FORBIDDEN),
            object_store::Error::Unauthenticated { .. } => Some(StatusCode::UNAUTHORIZED),
            object_store::Error::Precondition { .. } => Some(StatusCode::PRECONDITION_FAILED),
            object_store::Error::AlreadyExists { .. } => Some(StatusCode::CONFLICT),
            object_store::Error::NotModified { .. } => Some(StatusCode::NOT_MODIFIED),
            _ => status_in(&text),
        };
        let code = Self::element(&text, "Code");
        Self {
            status,
            code,
            text,
            error: Box::new(error),
        }
    }

    /// An element of the XML error body, the last part of the error text.
    fn element(text: &str, name: &str) -> Option<String> {
        let body = &text[text.find('<')?..];
        // Cheap pre-check before parsing (HEAD answers and transport errors have no body).
        if !body.contains(name) {
            return None;
        }
        xml_element_text(body.as_bytes(), name).filter(|v| !v.is_empty())
    }

    /// The error message from the XML error body.
    pub fn message(&self) -> Option<String> {
        Self::element(&self.text, "Message")
    }

    /// The object request was answered with 404.
    fn is_not_found(&self) -> bool {
        matches!(*self.error, object_store::Error::NotFound { .. })
    }

    /// The object request was answered with 403.
    fn is_forbidden(&self) -> bool {
        matches!(*self.error, object_store::Error::PermissionDenied { .. })
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.status, &self.code) {
            (Some(status), Some(code)) => {
                write!(f, "HTTP {} {code}", status.as_u16())?;
                if let Some(message) = self.message() {
                    write!(f, ": {message}")?;
                }
                // The request part of the error text (method, URL, elapsed time, retries).
                let context = self
                    .text
                    .split(" - Server returned")
                    .next()
                    .unwrap_or(&self.text);
                write!(f, " ({})", redact_query_strings(context))
            }
            _ => f.write_str(&error_chain(&*self.error)),
        }
    }
}

/// The error and all its sources not already contained in its text, without URL query strings.
fn error_chain(err: &(dyn std::error::Error + 'static)) -> String {
    let mut out = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        let text = cause.to_string();
        if !out.contains(&text) {
            out.push_str(": ");
            out.push_str(&text);
        }
        source = cause.source();
    }
    redact_query_strings(&out)
}

/// Replaces the query strings of the URLs in `text`: request URLs may carry credentials (Azure
/// sends the SAS token, including its signature, in the query of every request).
fn redact_query_strings(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(scheme) = rest.find("://") {
        let end = rest[scheme..]
            .find(char::is_whitespace)
            .map_or(rest.len(), |len| scheme + len);
        match rest[scheme..end].find('?') {
            Some(query) => {
                out.push_str(&rest[..scheme + query]);
                out.push_str("?<redacted>");
            }
            None => out.push_str(&rest[..end]),
        }
        rest = &rest[end..];
    }
    out.push_str(rest);
    out
}

/// The HTTP status in `object_store`'s "non-2xx status code: <status>" error text.
fn status_in(detail: &str) -> Option<StatusCode> {
    let (_, rest) = detail.split_once("status code: ")?;
    rest.get(..3)?
        .parse::<u16>()
        .ok()
        .and_then(|code| StatusCode::from_u16(code).ok())
}

/// Artifact metadata parsed from object attributes.
fn metadata_from(result: &GetResult, names: &MetadataNames) -> ArtifactMetadata {
    let text = |name: &'static str| {
        result
            .attributes
            .get(&Attribute::Metadata(name.into()))
            .map(|v| v.as_ref())
            .filter(|s| !s.is_empty())
    };
    let shared = |name: &'static str| text(name).map(Arc::from);
    ArtifactMetadata {
        duration: text(names.duration)
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0),
        // The exact length from `Content-Length` (`object_store` requires it).
        size: result.meta.size,
        tag: shared(names.tag),
        sha: shared(names.sha),
        dirty_hash: shared(names.dirty_hash),
        client_ci: shared(names.client_ci),
    }
}

/// Builds the attributes (metadata headers and content type) of an upload. Optional values must
/// be visible ASCII or spaces: the HTTP layer already rejects anything else, and this check keeps
/// library callers that use `Storage::put` directly from sending raw tabs or control bytes in a
/// signed header (S3-compatible stores disagree on how SigV4 signs them, and the stored bytes
/// would differ from what other backends store).
fn metadata_attributes(
    metadata: &ArtifactMetadata,
    names: &MetadataNames,
) -> AppResult<Attributes> {
    let mut attributes = Attributes::with_capacity(7);
    attributes.insert(Attribute::ContentType, AttributeValue::from(CONTENT_TYPE));
    let mut insert = |name: &'static str, value: String| {
        attributes.insert(Attribute::Metadata(name.into()), value.into());
    };
    insert(names.duration, metadata.duration.to_string());
    insert(names.size, metadata.size.to_string());
    let optional = [
        (names.tag, &metadata.tag),
        (names.sha, &metadata.sha),
        (names.dirty_hash, &metadata.dirty_hash),
        (names.client_ci, &metadata.client_ci),
    ];
    for (name, value) in optional {
        if let Some(value) = value {
            if !value.bytes().all(|b| (0x20..0x7f).contains(&b)) {
                return Err(AppError::BadRequest(format!(
                    "Artifact metadata '{name}' contains control characters (including tabs) or \
                     non-ASCII bytes"
                )));
            }
            insert(name, value.to_string());
        }
    }
    Ok(attributes)
}

fn storage_body_error(err: object_store::Error) -> io::Error {
    io::Error::other(format!("Storage stream read error: {}", error_chain(&err)))
}

#[async_trait]
impl Storage for ObjectStorage {
    fn backend_name(&self) -> &'static str {
        self.options.name
    }

    fn streams_uploads(&self) -> bool {
        true
    }

    async fn put(
        &self,
        namespace: &str,
        hash: &str,
        body: UploadBody,
        mut metadata: ArtifactMetadata,
        mode: PutMode,
    ) -> AppResult<PutOutcome> {
        let path = self.object_path(namespace, hash)?;
        metadata.size = body.len();
        let attributes = metadata_attributes(&metadata, self.options.metadata)?;
        let part = MULTIPART_PART_SIZE as u64;
        match body {
            UploadBody::Bytes(bytes) => {
                self.put_single(&path, PutPayload::from(bytes), attributes, mode)
                    .await
            }
            UploadBody::File(file) if mode == PutMode::Overwrite && file.len() > part => {
                // A descriptor opened once: the spool sweep may remove the path of a slow upload
                // (see `SpoolHandle`).
                let handle = file
                    .open_handle()
                    .await
                    .map_err(|e| AppError::io("Failed to open spooled upload", e))?;
                let chunks = handle
                    .stream(FILE_CHUNK_BYTES)
                    .map_err(|e| AppError::io("Failed to read spooled upload", e));
                self.put_multipart(&path, file.len(), attributes, Box::pin(chunks))
                    .await
            }
            UploadBody::File(file) => {
                let payload = if file.len() <= part {
                    UploadBody::File(file)
                        .into_bytes(MULTIPART_PART_SIZE)
                        .await?
                } else {
                    // A conditional write must be one `PUT`: map the spool file instead of
                    // copying up to 5 GiB into RAM.
                    file.map()
                        .await
                        .map_err(|e| AppError::io("Failed to map spooled upload", e))?
                };
                self.put_single(&path, PutPayload::from(payload), attributes, mode)
                    .await
            }
        }
    }

    async fn put_stream(
        &self,
        namespace: &str,
        hash: &str,
        body: UploadStream,
        mut metadata: ArtifactMetadata,
    ) -> AppResult<PutOutcome> {
        let Some(len) = body.content_length() else {
            // Without a length the size metadata of the multipart upload is unknown when it
            // starts: receive (spool) the body first.
            let body = body.receive().await?;
            return self
                .put(namespace, hash, body, metadata, PutMode::Overwrite)
                .await;
        };
        let path = self.object_path(namespace, hash)?;
        metadata.size = len;
        let attributes = metadata_attributes(&metadata, self.options.metadata)?;
        if len <= MULTIPART_PART_SIZE as u64 {
            let data = body.into_bytes().await?;
            return self
                .put_single(
                    &path,
                    PutPayload::from(data),
                    attributes,
                    PutMode::Overwrite,
                )
                .await;
        }
        self.put_multipart(&path, len, attributes, Box::pin(body.into_chunks()))
            .await
    }

    async fn get(&self, namespace: &str, hash: &str) -> AppResult<Option<StoredArtifact>> {
        let path = self.object_path(namespace, hash)?;
        // The body streams for as long as the client reads it (idle timeout per read).
        let result = match self.observe(self.store.get_opts(&path, GetOptions::default()).await) {
            Ok(result) => result,
            Err(failure) if self.is_miss(&failure) => return Ok(None),
            Err(failure) => return Err(self.failure_error("GET", &path, &failure)),
        };
        let metadata = metadata_from(&result, self.options.metadata);
        let stream = result.into_stream().map_err(storage_body_error);
        Ok(Some(StoredArtifact::from_stream(
            metadata,
            Box::pin(stream),
        )))
    }

    async fn head(&self, namespace: &str, hash: &str) -> AppResult<Option<ArtifactMetadata>> {
        let path = self.object_path(namespace, hash)?;
        Ok(self
            .head_object(&path)
            .await?
            .map(|result| metadata_from(&result, self.options.metadata)))
    }

    async fn delete(&self, namespace: &str, hash: &str) -> AppResult<bool> {
        let path = self.object_path(namespace, hash)?;
        self.delete_existing_object(&path).await
    }

    async fn health_check(&self) -> AppResult<()> {
        // Anonymous `/readyz` traffic causes at most one backend request per cache window, even
        // while the store is down. Callers arriving during a probe get the previous result
        // instead of queueing behind it; only the very first probe is waited for.
        if let Some(result) = self.health.fresh() {
            return result.map_err(AppError::Storage);
        }
        let _probe = match self.health.probe.try_lock() {
            Ok(guard) => guard,
            Err(_) => {
                if let Some((_, previous)) = self.health.last() {
                    return previous.map_err(AppError::Storage);
                }
                let guard = self.health.probe.lock().await;
                if let Some(result) = self.health.fresh() {
                    return result.map_err(AppError::Storage);
                }
                guard
            }
        };
        if let Some(result) = self.health.fresh() {
            return result.map_err(AppError::Storage);
        }
        // The cap covers credential loading too (a hanging metadata/token endpoint), not only the
        // request itself.
        let cap =
            (self.options.connect_timeout + self.options.read_timeout).min(HEALTH_PROBE_TIMEOUT);
        let result = match tokio::time::timeout(cap, self.probe_health()).await {
            Ok(result) => result,
            Err(_) => Err(format!(
                "{} health check timed out after {}ms",
                self.options.name,
                cap.as_millis()
            )),
        };
        if let Ok(mut last) = self.health.last.write() {
            *last = Some((Instant::now(), result.clone()));
        }
        result.map_err(AppError::Storage)
    }
}

/// Retry policy for `STORAGE_MAX_RETRIES` retries (`0` for readiness probes).
pub(crate) fn retry_config(max_retries: u32) -> RetryConfig {
    RetryConfig {
        max_retries: max_retries as usize,
        backoff: BackoffConfig {
            init_backoff: RETRY_BASE_DELAY,
            max_backoff: RETRY_MAX_DELAY,
            base: 2.0,
        },
        ..RetryConfig::default()
    }
}

// ---------------------------------------------------------------------------------------------
// HTTP clients
// ---------------------------------------------------------------------------------------------

/// Builds the reqwest clients of a store and of its credential providers: redirects are never
/// followed, there is no total request timeout (downloads stream for as long as the client
/// reads), and every attempt is bounded by [`TimedService`].
#[derive(Debug)]
pub(crate) struct Connector {
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
}

impl HttpConnector for Connector {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        // `object_store` allows plain HTTP only for the clients of the link-local/loopback
        // credential endpoints (instance metadata, container credentials): those never use a
        // proxy. The store and token clients honour the proxy environment variables. Endpoint
        // schemes are validated by the backends instead (plain HTTP endpoints are allowed).
        let metadata = options
            .get_config_value(&ClientConfigKey::AllowHttp)
            .is_some_and(|value| value == "true");
        let mut builder = reqwest::Client::builder()
            .connect_timeout(self.connect_timeout)
            .tcp_nodelay(true)
            .redirect(reqwest::redirect::Policy::none());
        builder = if metadata {
            builder.no_proxy()
        } else {
            builder.pool_max_idle_per_host(64)
        };
        let client = builder.build().map_err(|e| object_store::Error::Generic {
            store: "HTTP client",
            source: format!("cannot build the HTTP client: {}", error_chain(&e)).into(),
        })?;
        Ok(HttpClient::new(TimedService {
            client,
            connect_timeout: self.connect_timeout,
            read_timeout: self.read_timeout,
        }))
    }
}

/// Bounds every request attempt: response headers must arrive within `connect + read timeout +
/// body size / 256 KiB/s` (reqwest's read timeout would also count the time spent sending the
/// body), and the response body fails when no data arrives for the read timeout.
#[derive(Debug)]
struct TimedService {
    client: reqwest::Client,
    connect_timeout: Duration,
    read_timeout: Duration,
}

#[async_trait]
impl HttpService for TimedService {
    async fn call(&self, mut request: HttpRequest) -> Result<HttpResponse, HttpError> {
        // Container credentials without a token: `object_store` reads the token from a file and
        // sends whatever it contains, also nothing.
        if request
            .headers()
            .get(header::AUTHORIZATION)
            .is_some_and(|value| value.is_empty())
        {
            request.headers_mut().remove(header::AUTHORIZATION);
        }
        let len = request.body().content_length() as u64;
        let deadline = self.connect_timeout
            + self.read_timeout
            + Duration::from_secs(len.div_ceil(MIN_UPLOAD_BYTES_PER_SEC));
        let response = match tokio::time::timeout(deadline, self.client.call(request)).await {
            Ok(response) => response?,
            Err(_) => {
                return Err(HttpError::new(
                    HttpErrorKind::Timeout,
                    io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("no response headers within {}s", deadline.as_secs_f32()),
                    ),
                ));
            }
        };
        let (parts, body) = response.into_parts();
        let body = IdleTimeoutStream::new(
            http_body_util::BodyStream::new(body),
            self.read_timeout,
            |timeout| {
                HttpError::new(
                    HttpErrorKind::Timeout,
                    io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!(
                            "storage response stalled: no data for {}s",
                            timeout.as_secs_f32()
                        ),
                    ),
                )
            },
        );
        Ok(HttpResponse::from_parts(
            parts,
            HttpResponseBody::new(http_body_util::StreamBody::new(body)),
        ))
    }
}

/// Fails a streamed body when the server sends no data for `timeout` (instead of a total
/// deadline, which would cut off large downloads to slow clients).
///
/// Only time spent waiting for the server counts: the timer is (re)started whenever the consumer
/// polls for the next chunk, not when the previous chunk arrived, so a slow or stalled HTTP client
/// (backpressure: hyper stops polling the body while the socket is full) cannot make the
/// download look stalled.
struct IdleTimeoutStream<S, E> {
    inner: S,
    timeout: Duration,
    sleep: Pin<Box<tokio::time::Sleep>>,
    /// The previous poll returned `Pending`, i.e. the timer is running for the current wait.
    waiting: bool,
    done: bool,
    stalled: fn(Duration) -> E,
}

impl<S, E> IdleTimeoutStream<S, E> {
    fn new(inner: S, timeout: Duration, stalled: fn(Duration) -> E) -> Self {
        Self {
            inner,
            timeout,
            sleep: Box::pin(tokio::time::sleep(timeout)),
            waiting: false,
            done: false,
            stalled,
        }
    }
}

impl<S, T, E> Stream for IdleTimeoutStream<S, E>
where
    S: Stream<Item = Result<T, E>> + Unpin,
{
    type Item = Result<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Every field is `Unpin` (the timer is boxed).
        let this = Pin::into_inner(self);
        if this.done {
            return Poll::Ready(None);
        }
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(item)) => {
                this.waiting = false;
                Poll::Ready(Some(item))
            }
            Poll::Ready(None) => {
                this.done = true;
                Poll::Ready(None)
            }
            Poll::Pending => {
                if !this.waiting {
                    // A new wait for the server starts now (the consumer may have been busy).
                    this.waiting = true;
                    let deadline = tokio::time::Instant::now() + this.timeout;
                    this.sleep.as_mut().reset(deadline);
                }
                if this.sleep.as_mut().poll(cx).is_ready() {
                    this.done = true;
                    Poll::Ready(Some(Err((this.stalled)(this.timeout))))
                } else {
                    Poll::Pending
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Configuration helpers
// ---------------------------------------------------------------------------------------------

/// Validates an endpoint URL (`what`: e.g. "S3 endpoint") without ever echoing it (it might
/// contain credentials).
pub(crate) fn parse_endpoint(endpoint: &str, what: &str) -> anyhow::Result<url::Url> {
    let url = url::Url::parse(endpoint).map_err(|e| anyhow::anyhow!("Invalid {what} URL: {e}"))?;
    if !url.username().is_empty() || url.password().is_some() {
        anyhow::bail!(
            "Invalid {what} URL: credentials in the URL (user:pass@) are not allowed; configure them in their own options"
        );
    }
    if !matches!(url.scheme(), "http" | "https") {
        anyhow::bail!("Invalid {what} URL: only http and https are supported");
    }
    if url.host_str().is_none_or(str::is_empty) {
        anyhow::bail!("Invalid {what} URL: missing host");
    }
    if url.query().is_some() || url.fragment().is_some() {
        anyhow::bail!("Invalid {what} URL: query strings and fragments are not supported");
    }
    Ok(url)
}

pub(crate) fn is_loopback_host(url: &url::Url) -> bool {
    match url.host() {
        Some(url::Host::Domain(domain)) => {
            let domain = domain.trim_end_matches('.').to_ascii_lowercase();
            domain == "localhost" || domain.ends_with(".localhost")
        }
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        None => false,
    }
}

/// Parses an object key (or key prefix) into an `object_store` path without re-encoding it, so
/// keys stay byte-identical to existing objects. `object_store` paths cannot represent empty
/// segments, `.`/`..` segments and control characters.
pub(crate) fn parse_key(key: &str) -> Result<ObjectPath, String> {
    let invalid = || {
        format!(
            "'{key}' must not contain empty segments ('//'), '.' or '..' segments or control characters"
        )
    };
    let path = ObjectPath::parse(key).map_err(|_| invalid())?;
    if path.as_ref() != key {
        return Err(invalid());
    }
    Ok(path)
}

/// Returns the text content of the first element named `name` (namespace prefix ignored), or
/// `None` when the document is not valid XML or has no such element.
pub(crate) fn xml_element_text(xml: &[u8], name: &str) -> Option<String> {
    use quick_xml::events::Event;

    if xml.is_empty() {
        return None;
    }
    let text = std::str::from_utf8(xml).ok()?;
    let mut reader = quick_xml::Reader::from_str(text);
    // Nesting depth inside the matched element.
    let mut depth: Option<usize> = None;
    let mut out = String::new();
    loop {
        match reader.read_event().ok()? {
            Event::Start(e) => match depth.as_mut() {
                Some(d) => *d += 1,
                None if e.local_name().as_ref() == name => depth = Some(0),
                None => {}
            },
            Event::End(_) => match depth.as_mut() {
                Some(0) => return Some(out.trim().to_string()),
                Some(d) => *d -= 1,
                None => {}
            },
            Event::Empty(e) if depth.is_none() && e.local_name().as_ref() == name => {
                return Some(String::new());
            }
            Event::Text(t) if depth.is_some() => out.push_str(&t),
            Event::CData(t) if depth.is_some() => out.push_str(&t),
            Event::GeneralRef(r) if depth.is_some() => {
                if let Ok(Some(ch)) = r.resolve_char_ref() {
                    out.push(ch);
                } else {
                    match &*r {
                        "amp" => out.push('&'),
                        "lt" => out.push('<'),
                        "gt" => out.push('>'),
                        "quot" => out.push('"'),
                        "apos" => out.push('\''),
                        _ => {}
                    }
                }
            }
            Event::Eof => return None,
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_attributes_reject_control_characters_for_library_callers() {
        for bad in ["a\tb", "a\u{7f}b", "a\nb", "caf\u{e9}"] {
            for field in 0..4 {
                let mut metadata = ArtifactMetadata::default();
                let value = Some(Arc::from(bad));
                match field {
                    0 => metadata.tag = value,
                    1 => metadata.sha = value,
                    2 => metadata.dirty_hash = value,
                    _ => metadata.client_ci = value,
                }
                let err = metadata_attributes(&metadata, &METADATA_NAMES).unwrap_err();
                assert!(
                    matches!(&err, AppError::BadRequest(m) if m.contains("control characters")),
                    "{bad:?} in field {field}: {err:?}"
                );
            }
        }
        let ok = ArtifactMetadata {
            tag: Some(Arc::from("visible ~ ASCII and  spaces")),
            ..Default::default()
        };
        let attributes = metadata_attributes(&ok, &METADATA_NAMES).unwrap();
        assert_eq!(
            attributes
                .get(&Attribute::Metadata(METADATA_NAMES.tag.into()))
                .unwrap()
                .as_ref(),
            "visible ~ ASCII and  spaces"
        );
    }

    #[test]
    fn xml_error_code_parsing() {
        let xml = br#"<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>NoSuchBucket</Code><Message>The specified bucket does not exist &amp; more</Message><BucketName>b</BucketName></Error>"#;
        assert_eq!(
            xml_element_text(xml, "Code").as_deref(),
            Some("NoSuchBucket")
        );
        assert_eq!(
            xml_element_text(xml, "Message").as_deref(),
            Some("The specified bucket does not exist & more")
        );
        assert_eq!(xml_element_text(xml, "Missing"), None);
        assert_eq!(xml_element_text(b"not xml at all", "Code"), None);
        assert_eq!(xml_element_text(b"", "Code"), None);
        // Character references, CDATA, nesting, namespace prefixes and empty elements.
        let xml = br#"<s3:Error xmlns:s3="urn:x"><s3:Code>A&#38;B&#x3c;&lt;&apos;&quot;</s3:Code><Message><![CDATA[x < y]]> <b>in</b>ner</Message><Empty/></s3:Error>"#;
        assert_eq!(xml_element_text(xml, "Code").as_deref(), Some("A&B<<'\""));
        assert_eq!(
            xml_element_text(xml, "Message").as_deref(),
            Some("x < y inner")
        );
        assert_eq!(xml_element_text(xml, "Empty").as_deref(), Some(""));
    }

    /// Azure sends deletes as a batch `POST` with the SAS token in the URL: failures never show it.
    #[test]
    fn displayed_failures_never_contain_url_query_strings() {
        let url = "https://acct.blob.core.windows.net/c?restype=container&comp=batch&sv=2022-11-02&sig=c2VjcmV0%3D";
        let generic = |text: String| {
            Failure::new(object_store::Error::Generic {
                store: "MicrosoftAzure",
                source: text.into(),
            })
        };
        let transport = generic(format!(
            "Error performing POST {url} in 3ms, after 2 retries - HTTP error: error sending request for url ({url})"
        ));
        let answer = generic(format!(
            "Error performing POST {url} in 3ms - Server returned non-2xx status code: 403 Forbidden: <Error><Code>AuthenticationFailed</Code><Message>m</Message></Error>"
        ));
        assert_eq!(answer.code.as_deref(), Some("AuthenticationFailed"));
        for failure in [transport, answer] {
            let shown = failure.to_string();
            assert!(!shown.contains("sig"), "{shown}");
            assert!(
                shown.contains("POST https://acct.blob.core.windows.net/c?<redacted> in 3ms"),
                "{shown}"
            );
        }
    }

    /// Time the consumer spends not polling (slow/stalled client, backpressure) must not
    /// count as store idle time.
    #[tokio::test(start_paused = true)]
    async fn idle_timeout_stream_ignores_consumer_stalls() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Result<Bytes, io::Error>>();
        let inner = futures_util::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        });
        let mut stream =
            IdleTimeoutStream::new(Box::pin(inner), Duration::from_secs(2), stalled_io);

        tx.send(Ok(Bytes::from_static(b"a"))).unwrap();
        assert_eq!(stream.next().await.unwrap().unwrap(), "a");
        // The client stops reading for much longer than the timeout ...
        tokio::time::advance(Duration::from_secs(10)).await;
        // ... and polls again while the store has nothing buffered: not a stall yet.
        let mut next = stream.next();
        assert!(futures_util::poll!(&mut next).is_pending());
        tokio::time::advance(Duration::from_secs(1)).await;
        tx.send(Ok(Bytes::from_static(b"b"))).unwrap();
        assert_eq!(next.await.unwrap().unwrap(), "b");

        // A real store stall (consumer waiting) still fails after the timeout.
        let started = tokio::time::Instant::now();
        let item = stream.next().await.unwrap();
        assert_eq!(item.unwrap_err().kind(), io::ErrorKind::TimedOut);
        assert_eq!(started.elapsed(), Duration::from_secs(2));
        drop(tx);
    }

    fn stalled_io(timeout: Duration) -> io::Error {
        io::Error::new(io::ErrorKind::TimedOut, format!("stalled {timeout:?}"))
    }
}
