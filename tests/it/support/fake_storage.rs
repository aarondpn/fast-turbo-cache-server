//! A configurable in-memory [`Storage`] double for the HTTP-layer and hot-cache tests: call
//! counters, a log of what reached `put`, one-shot [`Gate`]s that pause an operation at a
//! well-defined point, and explicit fault knobs (all off by default).

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use fast_turbo_cache_server::{
    AppError, AppResult, PutMode, PutOutcome, Storage, UploadBody, models::ArtifactMetadata,
    storage::StoredArtifact,
};
use futures_util::stream;
use tokio::sync::{Semaphore, watch};

/// How long a test waits for a backend operation to reach a gate.
const WAIT: Duration = Duration::from_secs(10);

/// A pause point: the backend operation signals `entered` and blocks until `release`.
#[derive(Clone)]
pub struct Gate {
    entered: Arc<Semaphore>,
    released: Arc<watch::Sender<bool>>,
}

impl Gate {
    fn new() -> Self {
        Self {
            entered: Arc::new(Semaphore::new(0)),
            released: Arc::new(watch::channel(false).0),
        }
    }

    async fn pass(&self) {
        let mut rx = self.released.subscribe();
        self.entered.add_permits(1);
        rx.wait_for(|released| *released).await.unwrap();
    }

    pub async fn wait_entered(&self) {
        tokio::time::timeout(WAIT, self.entered.acquire())
            .await
            .expect("backend operation did not reach the gate")
            .unwrap()
            .forget();
    }

    pub fn release(&self) {
        self.released.send_replace(true);
    }
}

#[derive(Default)]
pub struct Gates {
    /// Paused after the backend value was read (so the read returns the old state).
    pub get: Option<Gate>,
    pub head: Option<Gate>,
    /// Paused before / after the backend write.
    pub put_before: Option<Gate>,
    pub put_after: Option<Gate>,
}

/// What reached `put`.
#[derive(Debug, Clone)]
pub struct PutRecord {
    pub namespace: String,
    pub hash: String,
    pub mode: PutMode,
    /// `Some(exists)` for a spooled body: whether its file existed when `put` was called.
    pub spooled_file_existed: Option<bool>,
    pub len: u64,
}

/// How `get` returns a stored payload.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub enum Payloads {
    /// One buffer.
    #[default]
    Bytes,
    /// A stream of 3 chunks (to exercise buffering) with a known `content_length`.
    Stream,
    /// A stream of unknown length (`content_length: None`, metadata size 0), like a backend
    /// response without `Content-Length`.
    UnknownLengthStream,
    /// A stream of unknown length whose metadata claims this size (e.g. a stale size header).
    ClaimedSizeStream(u64),
}

/// In-memory fake backend (`backend_name` "fake"). Objects are keyed by hash only.
#[derive(Default)]
pub struct FakeStorage {
    pub payloads: Payloads,
    /// `put` records the call and returns this outcome without storing anything (the body is
    /// dropped unread). `None`: `put` stores the object.
    pub put_outcome: Option<PutOutcome>,
    /// `put` and `delete` run on a detached blocking task that sleeps this long first (like
    /// `spawn_blocking` file I/O): the work completes even if the calling future is dropped.
    pub detached_writes: Option<Duration>,
    /// Every operation yields a random number of times before and after touching the store, so
    /// concurrent operations interleave at arbitrary points.
    pub random_yields: bool,
    /// `head` sleeps this long (concurrent calls are tracked in `max_heads_in_flight`).
    pub head_delay: Duration,
    /// `head` of this hash fails with a storage error whose message must not leak.
    pub failing_head: Option<&'static str>,
    /// `get` and `head` fail with this error.
    pub read_error: Mutex<Option<AppError>>,
    /// `get` (after letting concurrent callers join its flight) and `head` panic.
    pub panic_on_read: bool,

    pub objects: Arc<Mutex<HashMap<String, (Bytes, ArtifactMetadata)>>>,
    pub gates: Mutex<Gates>,
    pub put_log: Mutex<Vec<PutRecord>>,
    pub gets: AtomicUsize,
    pub heads: AtomicUsize,
    pub puts: AtomicUsize,
    pub heads_in_flight: AtomicUsize,
    pub max_heads_in_flight: AtomicUsize,
}

/// Metadata of an artifact of `size` bytes with duration 1 and `tag`.
pub fn metadata(size: usize, tag: &str) -> ArtifactMetadata {
    ArtifactMetadata {
        duration: 1,
        size: size as u64,
        tag: Some(tag.into()),
        ..Default::default()
    }
}

impl FakeStorage {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn streaming() -> Arc<Self> {
        Arc::new(Self {
            payloads: Payloads::Stream,
            ..Self::default()
        })
    }

    /// Stores `data` with [`metadata`]`(data.len(), tag)`, bypassing `put`.
    pub fn insert_direct(&self, hash: &str, data: &[u8], tag: &str) {
        self.insert(hash, data, metadata(data.len(), tag));
    }

    /// Stores `data` with `meta`, bypassing `put`.
    pub fn insert(&self, hash: &str, data: &[u8], meta: ArtifactMetadata) {
        self.objects
            .lock()
            .unwrap()
            .insert(hash.to_string(), (Bytes::copy_from_slice(data), meta));
    }

    pub fn remove_direct(&self, hash: &str) {
        self.objects.lock().unwrap().remove(hash);
    }

    /// Arms the gate selected by `f`; the next matching operation pauses there.
    pub fn arm(&self, f: impl FnOnce(&mut Gates) -> &mut Option<Gate>) -> Gate {
        let gate = Gate::new();
        *f(&mut self.gates.lock().unwrap()) = Some(gate.clone());
        gate
    }

    fn take(&self, f: impl FnOnce(&mut Gates) -> &mut Option<Gate>) -> Option<Gate> {
        f(&mut self.gates.lock().unwrap()).take()
    }

    pub fn snapshot(&self, hash: &str) -> Option<(Bytes, ArtifactMetadata)> {
        self.objects.lock().unwrap().get(hash).cloned()
    }

    async fn maybe_yield(&self) {
        if self.random_yields {
            for _ in 0..fastrand::usize(0..4) {
                tokio::task::yield_now().await;
            }
        }
    }

    fn read_error(&self) -> AppResult<()> {
        match &*self.read_error.lock().unwrap() {
            Some(err) => Err(err.clone()),
            None => Ok(()),
        }
    }
}

#[async_trait]
impl Storage for FakeStorage {
    fn backend_name(&self) -> &'static str {
        "fake"
    }

    async fn put(
        &self,
        namespace: &str,
        hash: &str,
        body: UploadBody,
        mut metadata: ArtifactMetadata,
        mode: PutMode,
    ) -> AppResult<PutOutcome> {
        self.puts.fetch_add(1, Ordering::SeqCst);
        self.put_log.lock().unwrap().push(PutRecord {
            namespace: namespace.to_string(),
            hash: hash.to_string(),
            mode,
            spooled_file_existed: match &body {
                UploadBody::Bytes(_) => None,
                UploadBody::File(f) => Some(f.path().exists()),
            },
            len: body.len(),
        });
        if let Some(outcome) = self.put_outcome {
            return Ok(outcome);
        }
        self.maybe_yield().await;
        if let Some(gate) = self.take(|g| &mut g.put_before) {
            gate.pass().await;
        }
        let data = body.into_bytes(usize::MAX).await?;
        metadata.size = data.len() as u64;
        let store = {
            let objects = Arc::clone(&self.objects);
            let hash = hash.to_string();
            move || {
                let mut objects = objects.lock().unwrap();
                if mode == PutMode::IfAbsent && objects.contains_key(&hash) {
                    return PutOutcome::AlreadyExists;
                }
                objects.insert(hash, (data, metadata));
                PutOutcome::Stored
            }
        };
        let outcome = match self.detached_writes {
            Some(delay) => tokio::task::spawn_blocking(move || {
                std::thread::sleep(delay);
                store()
            })
            .await
            .unwrap(),
            None => store(),
        };
        if outcome == PutOutcome::Stored
            && let Some(gate) = self.take(|g| &mut g.put_after)
        {
            gate.pass().await;
        }
        self.maybe_yield().await;
        Ok(outcome)
    }

    async fn get(&self, _namespace: &str, hash: &str) -> AppResult<Option<StoredArtifact>> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        if self.panic_on_read {
            // Let every concurrent caller join the flight before it fails.
            for _ in 0..10 {
                tokio::task::yield_now().await;
            }
            panic!("backend bug");
        }
        self.read_error()?;
        self.maybe_yield().await;
        let snapshot = self.snapshot(hash);
        if let Some(gate) = self.take(|g| &mut g.get) {
            gate.pass().await;
        }
        self.maybe_yield().await;
        Ok(snapshot.map(|(data, mut meta)| {
            let third = data.len() / 3;
            let chunks = vec![
                Ok(data.slice(..third)),
                Ok(data.slice(third..2 * third)),
                Ok(data.slice(2 * third..)),
            ];
            let len = data.len() as u64;
            match self.payloads {
                Payloads::Bytes => StoredArtifact::from_bytes(meta, data),
                Payloads::Stream => {
                    StoredArtifact::from_stream(meta, Box::pin(stream::iter(chunks)))
                        .with_content_length(Some(len))
                }
                Payloads::UnknownLengthStream => {
                    meta.size = 0;
                    StoredArtifact::from_stream(meta, Box::pin(stream::iter(chunks)))
                        .with_content_length(None)
                }
                Payloads::ClaimedSizeStream(size) => {
                    meta.size = size;
                    StoredArtifact::from_stream(meta, Box::pin(stream::iter(chunks)))
                        .with_content_length(None)
                }
            }
        }))
    }

    async fn head(&self, _namespace: &str, hash: &str) -> AppResult<Option<ArtifactMetadata>> {
        self.heads.fetch_add(1, Ordering::SeqCst);
        if self.panic_on_read {
            panic!("backend bug");
        }
        self.read_error()?;
        let now = self.heads_in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_heads_in_flight.fetch_max(now, Ordering::SeqCst);
        if !self.head_delay.is_zero() {
            tokio::time::sleep(self.head_delay).await;
        }
        self.heads_in_flight.fetch_sub(1, Ordering::SeqCst);
        if Some(hash) == self.failing_head {
            return Err(AppError::Storage(
                "s3 exploded: secret-internal-detail".into(),
            ));
        }
        self.maybe_yield().await;
        let snapshot = self.snapshot(hash).map(|(_, meta)| meta);
        if let Some(gate) = self.take(|g| &mut g.head) {
            gate.pass().await;
        }
        self.maybe_yield().await;
        Ok(snapshot)
    }

    async fn delete(&self, _namespace: &str, hash: &str) -> AppResult<bool> {
        self.maybe_yield().await;
        let objects = Arc::clone(&self.objects);
        let hash = hash.to_string();
        let remove = move || objects.lock().unwrap().remove(&hash).is_some();
        let removed = match self.detached_writes {
            Some(delay) => tokio::task::spawn_blocking(move || {
                std::thread::sleep(delay);
                remove()
            })
            .await
            .unwrap(),
            None => remove(),
        };
        self.maybe_yield().await;
        Ok(removed)
    }

    async fn health_check(&self) -> AppResult<()> {
        Ok(())
    }
}
