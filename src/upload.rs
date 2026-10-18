//! Streaming reception of request bodies with size limits and timeouts.
//!
//! Artifact uploads are never buffered twice: bodies up to `UPLOAD_MEMORY_THRESHOLD` are copied
//! into one owned, exactly sized buffer (so cached payloads never pin larger hyper read buffers),
//! larger bodies are streamed to a temp file in `SPOOL_DIR` and handed to storage as
//! [`UploadBody::File`]. The temp file is owned by a [`SpooledFile`], so it is removed on every
//! error path and when the request future is cancelled (client disconnect, shutdown). Backends
//! that stream uploads (S3) read the body as an [`UploadStream`] instead, under the same limits.

use std::{
    collections::HashMap,
    io::{self, Write as _},
    path::PathBuf,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use axum::body::Body;
use bytes::{Bytes, BytesMut};
use http_body_util::BodyExt;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};

use crate::{
    error::{AppError, AppResult},
    storage::{SpooledFile, UploadBody},
};

/// Spooled chunks are written to disk in batches of about this size (one blocking-pool hop
/// per batch instead of one per hyper frame).
const SPOOL_WRITE_BATCH: usize = 1024 * 1024;
/// Initial buffer capacity when the body length is not announced.
const UNKNOWN_LENGTH_INITIAL_CAPACITY: usize = 64 * 1024;

/// Limits applied while receiving an upload body.
#[derive(Debug, Clone)]
pub struct UploadLimits {
    /// Maximum accepted body size in bytes (413 beyond).
    pub max_size: u64,
    /// Bodies up to this many bytes stay in RAM; larger ones are spooled to `spool_dir`.
    pub memory_threshold: usize,
    /// Abort (408) when no body bytes arrive for this long.
    pub idle_timeout: Duration,
    /// Abort (408) when waiting for body data took this long in total (time the server spends
    /// on received chunks, e.g. sending a part to S3, does not count).
    pub total_timeout: Duration,
    /// Directory for spooled bodies.
    pub spool_dir: PathBuf,
}

/// Upload concurrency limits: `MAX_CONCURRENT_UPLOADS` slots server-wide, of which one namespace
/// may hold at most `per_namespace` at a time (`MAX_CONCURRENT_UPLOADS_PER_NAMESPACE`).
///
/// A client that trickles data keeps its slot until the total body timeout. The per-namespace
/// share stops one tenant from occupying every slot that way: the other namespaces keep at least
/// `total - per_namespace` slots. Callers pass `AuthConfig::upload_share_key` as the namespace,
/// so namespaces linked by one scoped token share one allowance.
pub struct UploadSlots {
    total: usize,
    /// `None` = no per-namespace limit.
    per_namespace: Option<usize>,
    global: Arc<Semaphore>,
    /// One semaphore per namespace with uploads in flight or waiting (removed when unused, so
    /// the map is bounded by the number of concurrent upload requests).
    namespaces: Mutex<HashMap<Box<str>, Arc<Semaphore>>>,
}

/// No upload slot became free within the wait time (global or namespace share exhausted).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotUnavailable;

/// An acquired upload slot; released on drop.
pub struct UploadSlot {
    _global: OwnedSemaphorePermit,
    _namespace: Option<NamespaceSlot>,
}

/// Per-namespace permit; drops the namespace's semaphore from the map once nobody uses it.
struct NamespaceSlot {
    slots: Arc<UploadSlots>,
    namespace: Box<str>,
    semaphore: Option<Arc<Semaphore>>,
    permit: Option<OwnedSemaphorePermit>,
}

impl Drop for NamespaceSlot {
    fn drop(&mut self) {
        // Release our references first; every other reference is created under the map lock,
        // so "only the map holds it" checked under the lock is race-free.
        drop(self.permit.take());
        drop(self.semaphore.take());
        let mut map = self
            .slots
            .namespaces
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if map
            .get(&*self.namespace)
            .is_some_and(|sem| Arc::strong_count(sem) == 1)
        {
            map.remove(&*self.namespace);
        }
    }
}

impl UploadSlots {
    /// `total` server-wide slots (at least 1); `per_namespace` = 0 or >= `total` disables the
    /// per-namespace limit.
    pub fn new(total: usize, per_namespace: usize) -> Self {
        let total = total.max(1);
        Self {
            total,
            per_namespace: (per_namespace > 0 && per_namespace < total).then_some(per_namespace),
            global: Arc::new(Semaphore::new(total)),
            namespaces: Mutex::new(HashMap::new()),
        }
    }

    /// Server-wide slot count.
    pub fn total(&self) -> usize {
        self.total
    }

    /// Effective per-namespace limit (`None` = only the server-wide limit applies).
    pub fn per_namespace(&self) -> Option<usize> {
        self.per_namespace
    }

    /// Number of namespaces currently tracked (uploading or waiting).
    pub fn tracked_namespaces(&self) -> usize {
        self.namespaces
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    fn namespace_semaphore(&self, namespace: &str, limit: usize) -> Arc<Semaphore> {
        let mut map = self
            .namespaces
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(sem) = map.get(namespace) {
            return Arc::clone(sem);
        }
        let sem = Arc::new(Semaphore::new(limit));
        map.insert(namespace.into(), Arc::clone(&sem));
        sem
    }

    /// Waits up to `wait` for a slot: first the namespace share (so a namespace at its limit
    /// does not occupy server-wide slots while it waits), then a server-wide slot.
    pub async fn acquire(
        self: &Arc<Self>,
        namespace: &str,
        wait: Duration,
    ) -> Result<UploadSlot, SlotUnavailable> {
        let deadline = Instant::now()
            .checked_add(wait)
            .unwrap_or_else(|| Instant::now() + Duration::from_secs(86_400));
        let namespace_slot = match self.per_namespace {
            None => None,
            Some(limit) => {
                let semaphore = self.namespace_semaphore(namespace, limit);
                let mut slot = NamespaceSlot {
                    slots: Arc::clone(self),
                    namespace: namespace.into(),
                    semaphore: Some(Arc::clone(&semaphore)),
                    permit: None,
                };
                slot.permit = Some(acquire_until(deadline, semaphore).await?);
                Some(slot)
            }
        };
        Ok(UploadSlot {
            _global: acquire_until(deadline, Arc::clone(&self.global)).await?,
            _namespace: namespace_slot,
        })
    }
}

async fn acquire_until(
    deadline: Instant,
    semaphore: Arc<Semaphore>,
) -> Result<OwnedSemaphorePermit, SlotUnavailable> {
    let permit = tokio::time::timeout_at(deadline, semaphore.acquire_owned())
        .await
        .map_err(|_| SlotUnavailable)?;
    Ok(permit.expect("upload semaphores are never closed"))
}

/// Idle + total timeout tracking for a body read. Both count only the time spent waiting for body
/// data: while the consumer handles a chunk (a streamed upload waits for S3 to accept a part, a
/// spool write) the clocks stand still, so a slow backend is never reported as a slow client.
struct Deadlines {
    idle: Duration,
    /// What is left of the total timeout.
    remaining: Duration,
}

impl Deadlines {
    fn new(idle: Duration, total: Duration) -> Self {
        Self {
            idle,
            remaining: total,
        }
    }
}

/// Reads the next data chunk of `body`. `Ok(None)` = end of body.
async fn next_chunk(body: &mut Body, deadlines: &mut Deadlines) -> AppResult<Option<Bytes>> {
    loop {
        let started = Instant::now();
        let far = || started + Duration::from_secs(86_400 * 365);
        let total_deadline = started.checked_add(deadlines.remaining).unwrap_or_else(far);
        let idle_deadline = started
            .checked_add(deadlines.idle)
            .unwrap_or_else(far)
            .min(total_deadline);
        let frame = tokio::time::timeout_at(idle_deadline, body.frame()).await;
        deadlines.remaining = deadlines.remaining.saturating_sub(started.elapsed());
        let frame = match frame {
            Ok(frame) => frame,
            Err(_) => {
                let which = if idle_deadline >= total_deadline {
                    "Request body was not received within the total body timeout"
                } else {
                    "No request body data received within the body idle timeout"
                };
                return Err(AppError::RequestTimeout(which.to_string()));
            }
        };
        match frame {
            None => return Ok(None),
            Some(Err(err)) => {
                tracing::debug!(error = %err, "Failed to read request body");
                return Err(AppError::BadRequest(
                    "Failed to read request body".to_string(),
                ));
            }
            Some(Ok(frame)) => {
                // Trailers are ignored; empty data frames carry nothing.
                if let Ok(data) = frame.into_data()
                    && !data.is_empty()
                {
                    return Ok(Some(data));
                }
            }
        }
    }
}

fn too_large(max: u64) -> AppError {
    AppError::PayloadTooLarge(format!(
        "Request body exceeds maximum allowed size of {max} bytes"
    ))
}

/// Copies `buf` into an exactly sized allocation when its spare capacity is significant, so the
/// returned `Bytes` does not keep unused memory alive.
fn into_owned_exact(buf: BytesMut) -> Bytes {
    if buf.capacity() - buf.len() > buf.len() / 8 {
        Bytes::copy_from_slice(&buf)
    } else {
        buf.freeze()
    }
}

/// Reads a whole (small) body into memory, enforcing `limit` and the body timeouts.
/// Used for JSON request bodies.
pub async fn read_body_limited(
    body: &mut Body,
    limit: usize,
    idle_timeout: Duration,
    total_timeout: Duration,
) -> AppResult<Bytes> {
    let mut deadlines = Deadlines::new(idle_timeout, total_timeout);
    let mut buf = BytesMut::new();
    while let Some(chunk) = next_chunk(body, &mut deadlines).await? {
        if buf.len() + chunk.len() > limit {
            return Err(too_large(limit as u64));
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(into_owned_exact(buf))
}

/// Temp-file sink for spooled uploads. Writes happen on the blocking pool; the `SpooledFile`
/// travels with the file handle into each blocking job, so if the request future is dropped
/// mid-write the job still finishes and then drops (= deletes) the temp file.
struct FileSink {
    file: Option<(SpooledFile, std::fs::File)>,
    pending: Vec<Bytes>,
    pending_len: usize,
    written: u64,
}

impl FileSink {
    async fn create(dir: &std::path::Path) -> AppResult<Self> {
        let (spooled, file) = SpooledFile::create_in(dir)
            .await
            .map_err(|e| AppError::io("Failed to create upload spool file", e))?;
        let file = file.into_std().await;
        Ok(Self {
            file: Some((spooled, file)),
            pending: Vec::new(),
            pending_len: 0,
            written: 0,
        })
    }

    async fn push(&mut self, chunk: Bytes) -> AppResult<()> {
        self.pending_len += chunk.len();
        self.pending.push(chunk);
        if self.pending_len >= SPOOL_WRITE_BATCH {
            self.flush().await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> AppResult<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let (spooled, mut file) = self
            .file
            .take()
            .ok_or_else(|| AppError::Internal("Upload spool file already failed".to_string()))?;
        let chunks = std::mem::take(&mut self.pending);
        let len = std::mem::take(&mut self.pending_len);
        let result = tokio::task::spawn_blocking(move || -> io::Result<_> {
            for chunk in &chunks {
                file.write_all(chunk)?;
            }
            Ok((spooled, file))
        })
        .await
        .map_err(|e| AppError::internal_with_source("Upload spool writer failed", &e))?;
        // On a write error the closure already dropped (and thereby deleted) the spool file.
        let pair = result.map_err(|e| AppError::io("Failed to write upload spool file", e))?;
        self.file = Some(pair);
        self.written += len as u64;
        Ok(())
    }

    async fn finish(mut self) -> AppResult<SpooledFile> {
        self.flush().await?;
        let (mut spooled, file) = self
            .file
            .take()
            .ok_or_else(|| AppError::Internal("Upload spool file already failed".to_string()))?;
        drop(file);
        spooled.set_len(self.written);
        Ok(spooled)
    }
}

enum Sink {
    Memory(BytesMut),
    File(FileSink),
}

/// An artifact upload body read as it arrives: every chunk is subject to `limits.max_size` and
/// the idle/total body timeouts, and an announced `Content-Length` must match the body exactly.
///
/// [`UploadStream::receive`] stores the whole body (RAM or spool file); backends that stream
/// uploads (S3 multipart) read it chunk by chunk with [`UploadStream::next_chunk`] instead.
pub struct UploadStream {
    body: Body,
    deadlines: Deadlines,
    announced: Option<u64>,
    received: u64,
    limits: UploadLimits,
}

impl UploadStream {
    /// Starts receiving `body`. An announced `content_length` larger than `limits.max_size` is
    /// rejected up front; the streamed byte count is enforced independently (chunked uploads).
    pub fn new(body: Body, content_length: Option<u64>, limits: &UploadLimits) -> AppResult<Self> {
        if let Some(len) = content_length
            && len > limits.max_size
        {
            return Err(too_large(limits.max_size));
        }
        Ok(Self {
            body,
            deadlines: Deadlines::new(limits.idle_timeout, limits.total_timeout),
            announced: content_length,
            received: 0,
            limits: limits.clone(),
        })
    }

    /// The announced `Content-Length`, if any.
    pub fn content_length(&self) -> Option<u64> {
        self.announced
    }

    /// Reads the next data chunk; `Ok(None)` at the end of the body.
    ///
    /// Errors: [`AppError::RequestTimeout`] (idle/total timeout), [`AppError::PayloadTooLarge`],
    /// [`AppError::BadRequest`] (read error, body longer or shorter than its `Content-Length`).
    pub async fn next_chunk(&mut self) -> AppResult<Option<Bytes>> {
        let Some(chunk) = next_chunk(&mut self.body, &mut self.deadlines).await? else {
            if let Some(len) = self.announced
                && self.received != len
            {
                return Err(AppError::BadRequest(format!(
                    "Request body ended after {} of {len} bytes (Content-Length)",
                    self.received
                )));
            }
            return Ok(None);
        };
        self.received += chunk.len() as u64;
        if self.received > self.limits.max_size {
            return Err(too_large(self.limits.max_size));
        }
        if let Some(len) = self.announced
            && self.received > len
        {
            return Err(AppError::BadRequest(format!(
                "Request body is longer than its Content-Length of {len} bytes"
            )));
        }
        Ok(Some(chunk))
    }

    /// The body as a stream of chunks (see [`UploadStream::next_chunk`]).
    pub fn into_chunks(self) -> impl futures_util::Stream<Item = AppResult<Bytes>> + Send {
        futures_util::stream::try_unfold(self, |mut upload| async move {
            Ok(upload.next_chunk().await?.map(|chunk| (chunk, upload)))
        })
    }

    /// Reads the whole body into one buffer, sized by the announced length. For bodies whose
    /// size the caller has bounded (`max_size` applies, too).
    pub async fn into_bytes(mut self) -> AppResult<Bytes> {
        let capacity = self.announced.unwrap_or(0).min(self.limits.max_size) as usize;
        let mut buf = BytesMut::with_capacity(capacity);
        while let Some(chunk) = self.next_chunk().await? {
            buf.extend_from_slice(&chunk);
        }
        Ok(into_owned_exact(buf))
    }

    /// Receives the whole body: bodies whose (announced or actual) size is <= `memory_threshold`
    /// end up in one owned, exactly sized `Bytes`; larger bodies are spooled to `spool_dir` ->
    /// [`UploadBody::File`]. Any error or cancellation removes the spool file.
    pub async fn receive(mut self) -> AppResult<UploadBody> {
        let threshold = self.limits.memory_threshold as u64;
        let mut sink = match self.announced {
            Some(len) if len > threshold => {
                Sink::File(FileSink::create(&self.limits.spool_dir).await?)
            }
            // `len <= threshold` fits in usize because the threshold is a usize.
            Some(len) => Sink::Memory(BytesMut::with_capacity(len as usize)),
            None => Sink::Memory(BytesMut::with_capacity(
                UNKNOWN_LENGTH_INITIAL_CAPACITY.min(self.limits.memory_threshold),
            )),
        };

        while let Some(chunk) = self.next_chunk().await? {
            match &mut sink {
                Sink::Memory(buf) if self.received <= threshold => buf.extend_from_slice(&chunk),
                Sink::Memory(buf) => {
                    // Crossed the RAM threshold: move what we have to a spool file.
                    let mut file = FileSink::create(&self.limits.spool_dir).await?;
                    let buffered = std::mem::take(buf).freeze();
                    if !buffered.is_empty() {
                        file.push(buffered).await?;
                    }
                    file.push(chunk).await?;
                    sink = Sink::File(file);
                }
                Sink::File(file) => file.push(chunk).await?,
            }
        }

        match sink {
            Sink::Memory(buf) => Ok(UploadBody::Bytes(into_owned_exact(buf))),
            Sink::File(file) => Ok(UploadBody::File(file.finish().await?)),
        }
    }
}

/// Receives a whole artifact upload body (see [`UploadStream::new`] and
/// [`UploadStream::receive`]).
pub async fn receive_upload(
    body: Body,
    content_length: Option<u64>,
    limits: &UploadLimits,
) -> AppResult<UploadBody> {
    UploadStream::new(body, content_length, limits)?
        .receive()
        .await
}

#[cfg(test)]
mod tests {
    use std::{path::Path, pin::Pin, task::Poll};

    use futures_util::stream;

    use super::*;

    fn limits(dir: &Path, threshold: usize) -> UploadLimits {
        UploadLimits {
            max_size: 1024 * 1024,
            memory_threshold: threshold,
            idle_timeout: Duration::from_secs(5),
            total_timeout: Duration::from_secs(30),
            spool_dir: dir.to_path_buf(),
        }
    }

    fn chunked(chunks: Vec<&'static [u8]>) -> Body {
        Body::from_stream(stream::iter(
            chunks
                .into_iter()
                .map(|c| Ok::<_, io::Error>(Bytes::from_static(c))),
        ))
    }

    fn dir_entries(dir: &Path) -> usize {
        std::fs::read_dir(dir).unwrap().count()
    }

    #[tokio::test]
    async fn large_body_is_spooled_and_cleaned_up_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        // Unknown length: starts in RAM, crosses the 8-byte threshold mid-stream.
        let body = receive_upload(
            chunked(vec![b"0123", b"4567", b"89abcdef"]),
            None,
            &limits(dir.path(), 8),
        )
        .await
        .unwrap();
        let UploadBody::File(file) = &body else {
            panic!("expected spooled body")
        };
        assert_eq!(file.len(), 16);
        assert_eq!(std::fs::read(file.path()).unwrap(), b"0123456789abcdef");
        assert_eq!(dir_entries(dir.path()), 1);
        drop(body);
        assert_eq!(dir_entries(dir.path()), 0);

        // Announced length above the threshold: spooled from the first byte.
        let body = receive_upload(
            chunked(vec![b"0123456789"]),
            Some(10),
            &limits(dir.path(), 8),
        )
        .await
        .unwrap();
        assert!(matches!(body, UploadBody::File(_)));
        assert_eq!(body.into_bytes(100).await.unwrap(), &b"0123456789"[..]);
        assert_eq!(dir_entries(dir.path()), 0);
    }

    #[tokio::test]
    async fn oversized_and_failing_bodies_leave_no_spool_files() {
        let dir = tempfile::tempdir().unwrap();
        let mut l = limits(dir.path(), 4);
        l.max_size = 10;
        let err = receive_upload(chunked(vec![b"012345", b"6789ab"]), None, &l)
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::PayloadTooLarge(_)));
        assert!(matches!(
            receive_upload(chunked(vec![]), Some(11), &l).await,
            Err(AppError::PayloadTooLarge(_))
        ));

        let failing = Body::from_stream(stream::iter(vec![
            Ok(Bytes::from_static(b"0123456789")),
            Err(io::Error::other("client went away")),
        ]));
        let err = receive_upload(failing, None, &limits(dir.path(), 4))
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::BadRequest(_)));
        assert_eq!(dir_entries(dir.path()), 0);
    }

    #[tokio::test]
    async fn stalled_body_times_out_and_cancellation_removes_spool_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut sent = false;
        let stalled = Body::from_stream(stream::poll_fn(move |_| {
            if sent {
                Poll::Pending
            } else {
                sent = true;
                Poll::Ready(Some(Ok::<_, io::Error>(Bytes::from_static(b"0123456789"))))
            }
        }));
        let mut l = limits(dir.path(), 4);
        l.idle_timeout = Duration::from_millis(50);
        let started = std::time::Instant::now();
        let err = receive_upload(stalled, None, &l).await.unwrap_err();
        assert!(
            matches!(&err, AppError::RequestTimeout(m) if m.contains("body idle timeout")),
            "{err}"
        );
        assert!(started.elapsed() < Duration::from_secs(5));
        assert_eq!(dir_entries(dir.path()), 0);

        // Cancellation (request future dropped) after spooling started.
        let mut sent = false;
        let stalled = Body::from_stream(stream::poll_fn(move |_| {
            if sent {
                Poll::Pending
            } else {
                sent = true;
                Poll::Ready(Some(Ok::<_, io::Error>(Bytes::from_static(b"0123456789"))))
            }
        }));
        let l = limits(dir.path(), 4);
        let mut fut: Pin<Box<_>> = Box::pin(receive_upload(stalled, None, &l));
        let _ = tokio::time::timeout(Duration::from_millis(100), &mut fut).await;
        assert_eq!(dir_entries(dir.path()), 1);
        drop(fut);
        assert_eq!(dir_entries(dir.path()), 0);
    }

    /// Time the consumer spends on received chunks (a streamed upload waiting for S3 to accept a
    /// part) does not count against the total timeout; waiting for the client does.
    #[tokio::test]
    async fn total_timeout_counts_only_waiting_for_data() {
        let dir = tempfile::tempdir().unwrap();
        let paced = || {
            Body::from_stream(stream::unfold(0, |n| async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                (n < 4).then(|| (Ok::<_, io::Error>(Bytes::from_static(b"x")), n + 1))
            }))
        };
        let mut l = limits(dir.path(), 1024);
        l.total_timeout = Duration::from_millis(150);
        let mut upload = UploadStream::new(paced(), None, &l).unwrap();
        let mut received = 0;
        while upload.next_chunk().await.unwrap().is_some() {
            received += 1;
            tokio::time::sleep(Duration::from_millis(60)).await;
        }
        assert_eq!(received, 4);

        // The waiting time still adds up across chunks.
        l.total_timeout = Duration::from_millis(25);
        let mut upload = UploadStream::new(paced(), None, &l).unwrap();
        let err = loop {
            match upload.next_chunk().await {
                Ok(Some(_)) => tokio::time::sleep(Duration::from_millis(60)).await,
                Ok(None) => panic!("the total timeout must expire"),
                Err(err) => break err,
            }
        };
        assert!(
            matches!(&err, AppError::RequestTimeout(m) if m.contains("total body timeout")),
            "{err}"
        );
    }

    async fn collect(body: Body, len: Option<u64>, l: &UploadLimits) -> AppResult<Vec<u8>> {
        let mut upload = UploadStream::new(body, len, l)?;
        let mut out = Vec::new();
        while let Some(chunk) = upload.next_chunk().await? {
            out.extend_from_slice(&chunk);
        }
        Ok(out)
    }

    #[tokio::test]
    async fn upload_stream_enforces_the_announced_length() {
        let dir = tempfile::tempdir().unwrap();
        let l = limits(dir.path(), 4);
        let ok = collect(chunked(vec![b"0123", b"456"]), Some(7), &l)
            .await
            .unwrap();
        assert_eq!(ok, b"0123456");
        let short = collect(chunked(vec![b"0123"]), Some(7), &l)
            .await
            .unwrap_err();
        assert!(
            matches!(&short, AppError::BadRequest(m) if m.contains("ended after 4 of 7")),
            "{short}"
        );
        let long = collect(chunked(vec![b"0123", b"4567"]), Some(7), &l)
            .await
            .unwrap_err();
        assert!(
            matches!(&long, AppError::BadRequest(m) if m.contains("longer")),
            "{long}"
        );
        // Unknown length: only MAX_ARTIFACT_SIZE applies.
        let mut small = limits(dir.path(), 4);
        small.max_size = 5;
        let mut upload = UploadStream::new(chunked(vec![b"0123", b"45"]), None, &small).unwrap();
        assert!(upload.next_chunk().await.unwrap().is_some());
        assert!(matches!(
            upload.next_chunk().await,
            Err(AppError::PayloadTooLarge(_))
        ));
        assert!(matches!(
            UploadStream::new(chunked(vec![]), Some(6), &small),
            Err(AppError::PayloadTooLarge(_))
        ));
    }

    #[tokio::test]
    async fn namespace_share_limits_one_namespace_only() {
        let slots = Arc::new(UploadSlots::new(4, 2));
        assert_eq!(slots.per_namespace(), Some(2));
        let wait = Duration::from_millis(50);
        let a1 = slots.acquire("team-a", wait).await.unwrap();
        let a2 = slots.acquire("team-a", wait).await.unwrap();
        // team-a is at its share although two server-wide slots are free.
        assert_eq!(
            slots.acquire("team-a", wait).await.err(),
            Some(SlotUnavailable)
        );
        // Other namespaces still get the remaining slots.
        let b1 = slots.acquire("team-b", wait).await.unwrap();
        let c1 = slots.acquire("team-c", wait).await.unwrap();
        // Server-wide limit reached.
        assert_eq!(
            slots.acquire("team-d", wait).await.err(),
            Some(SlotUnavailable)
        );
        drop(a1);
        let a3 = slots.acquire("team-a", wait).await.unwrap();
        drop((a2, a3, b1, c1));
        // Unused namespaces are forgotten.
        assert_eq!(slots.tracked_namespaces(), 0);
    }

    #[tokio::test]
    async fn waiting_upload_gets_namespace_slot_when_released() {
        let slots = Arc::new(UploadSlots::new(4, 1));
        let first = slots
            .acquire("team-a", Duration::from_secs(1))
            .await
            .unwrap();
        let waiter = {
            let slots = Arc::clone(&slots);
            tokio::spawn(async move { slots.acquire("team-a", Duration::from_secs(5)).await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(slots.tracked_namespaces(), 1);
        drop(first);
        let second = waiter.await.unwrap().unwrap();
        drop(second);
        assert_eq!(slots.tracked_namespaces(), 0);
    }

    #[tokio::test]
    async fn no_namespace_limit_when_disabled_or_not_below_total() {
        for per_namespace in [0, 3, 10] {
            let slots = Arc::new(UploadSlots::new(3, per_namespace));
            assert_eq!(slots.per_namespace(), None);
            let held: Vec<_> = futures_util::future::join_all(
                (0..3).map(|_| slots.acquire("team-a", Duration::from_millis(50))),
            )
            .await;
            assert!(held.iter().all(Result::is_ok));
            assert_eq!(
                slots
                    .acquire("team-b", Duration::from_millis(20))
                    .await
                    .err(),
                Some(SlotUnavailable)
            );
            assert_eq!(slots.tracked_namespaces(), 0);
        }
    }
}
