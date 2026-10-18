pub mod azure;
pub mod cache;
pub mod gcs;
pub mod local;
pub mod object;
pub mod s3;

use std::{
    fmt, io,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::Stream;

use crate::{
    config::{CliConfig, StorageBackendKind},
    error::{AppError, AppResult},
    models::ArtifactMetadata,
    upload::UploadStream,
};

pub type ArtifactStream = Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>;

/// Payload representation for a retrieved artifact.
/// `Bytes` avoids boxing a `dyn Stream` for in-memory hot-cache hits.
pub enum ArtifactPayload {
    Bytes(Bytes),
    Stream(ArtifactStream),
}

pub struct StoredArtifact {
    pub metadata: ArtifactMetadata,
    pub payload: ArtifactPayload,
    /// Exact payload length, if known. With `None`, HTTP responses must not send
    /// `Content-Length` (chunked transfer encoding instead).
    pub content_length: Option<u64>,
}

impl StoredArtifact {
    /// In-memory payload; `content_length` is the exact buffer length.
    #[inline]
    pub fn from_bytes(metadata: ArtifactMetadata, data: Bytes) -> Self {
        let len = data.len() as u64;
        Self {
            metadata,
            payload: ArtifactPayload::Bytes(data),
            content_length: Some(len),
        }
    }

    /// Streamed payload whose length is taken from `metadata.size`.
    #[inline]
    pub fn from_stream(metadata: ArtifactMetadata, stream: ArtifactStream) -> Self {
        let len = metadata.size;
        Self {
            metadata,
            payload: ArtifactPayload::Stream(stream),
            content_length: Some(len),
        }
    }

    /// Overrides the known payload length (`None` = unknown length).
    #[inline]
    pub fn with_content_length(mut self, content_length: Option<u64>) -> Self {
        self.content_length = content_length;
        self
    }
}

/// Write semantics for [`Storage::put`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PutMode {
    /// Replace any existing artifact with the same hash.
    #[default]
    Overwrite,
    /// Store only if no artifact with this hash exists yet. Backends must implement this
    /// atomically (no check-then-write race) where the underlying store allows it.
    IfAbsent,
}

/// Result of a [`Storage::put`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutOutcome {
    /// The artifact was written.
    Stored,
    /// `PutMode::IfAbsent` was requested and an artifact already existed; nothing was written.
    AlreadyExists,
}

/// An upload body that was spooled to a temporary file on disk.
///
/// The value owns the file: dropping it deletes the file unless ownership was released with
/// [`SpooledFile::take_path`].
pub struct SpooledFile {
    path: Option<PathBuf>,
    len: u64,
}

impl SpooledFile {
    /// Takes ownership of an existing file at `path` containing `len` bytes.
    pub fn new(path: PathBuf, len: u64) -> Self {
        Self {
            path: Some(path),
            len,
        }
    }

    /// Creates a new, uniquely named (unpredictable, `O_EXCL`, mode 0600 on unix) empty temp file
    /// inside `dir` and returns the owning handle plus a writable file. Update the length with
    /// [`SpooledFile::set_len`] after writing.
    pub async fn create_in(dir: &Path) -> io::Result<(Self, tokio::fs::File)> {
        let dir = dir.to_path_buf();
        let (file, path) = tokio::task::spawn_blocking(move || {
            tempfile::Builder::new()
                .prefix(SPOOL_FILE_PREFIX)
                .suffix(SPOOL_FILE_SUFFIX)
                .tempfile_in(&dir)?
                .keep()
                .map_err(|e| e.error)
        })
        .await
        .map_err(io::Error::other)??;
        Ok((Self::new(path, 0), tokio::fs::File::from_std(file)))
    }

    pub fn path(&self) -> &Path {
        self.path
            .as_deref()
            .expect("SpooledFile path is only taken by consuming methods")
    }

    /// Number of payload bytes in the file.
    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Records the number of bytes written to the file.
    pub fn set_len(&mut self, len: u64) {
        self.len = len;
    }

    /// Opens the spooled file once for any number of sequential or concurrent full reads (see
    /// [`SpoolHandle`]).
    pub async fn open_handle(&self) -> io::Result<SpoolHandle> {
        let path = self.path().to_path_buf();
        let file = tokio::task::spawn_blocking(move || std::fs::File::open(path))
            .await
            .map_err(io::Error::other)??;
        Ok(SpoolHandle {
            file: Arc::new(file),
            len: self.len,
        })
    }

    /// Maps the spooled file read-only into memory, e.g. for a single S3 `PUT` of a large upload
    /// without copying it into RAM (the pages are file-backed and can be evicted). Fails with
    /// `UnexpectedEof` if the file is shorter than its recorded length.
    pub async fn map(&self) -> io::Result<Bytes> {
        let path = self.path().to_path_buf();
        let len = self.len;
        tokio::task::spawn_blocking(move || {
            let file = std::fs::File::open(path)?;
            if file.metadata()?.len() < len {
                return Err(spool_truncated());
            }
            if len == 0 {
                return Ok(Bytes::new());
            }
            let len = usize::try_from(len).map_err(io::Error::other)?;
            // SAFETY: a spool file is private to this process (created with `O_EXCL` and mode
            // 0600 by `create_in`, owned by this `SpooledFile`) and is never written or truncated
            // once the upload was received. The orphan sweep may only unlink it, which keeps the
            // mapping valid. Another process truncating it would make reads fault (SIGBUS).
            let map = unsafe { memmap2::MmapOptions::new().len(len).map(&file)? };
            Ok(Bytes::from_owner(map))
        })
        .await
        .map_err(io::Error::other)?
    }

    /// Releases ownership: the file is no longer deleted on drop. The caller becomes responsible
    /// for removing or publishing it.
    pub fn take_path(mut self) -> PathBuf {
        self.path
            .take()
            .expect("SpooledFile path is only taken by consuming methods")
    }
}

/// An open spooled upload that can be streamed from the start any number of times with
/// positional reads, so streams never share a file offset.
///
/// The orphan sweep ([`purge_stale_spool_files`]) only sees the mtime, which stops changing once
/// the body is received, so a slow upload that is still being sent (e.g. S3 multipart) may lose
/// its path. Reading from a descriptor opened before the upload starts keeps the data readable
/// (on Windows an open file cannot be deleted, so the sweep skips it).
#[derive(Clone)]
pub struct SpoolHandle {
    file: Arc<std::fs::File>,
    len: u64,
}

impl SpoolHandle {
    /// Number of payload bytes every stream yields.
    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Streams the payload from the start in chunks of at most `chunk_size` bytes (read on the
    /// blocking pool). Fails with `UnexpectedEof` if the file is shorter than its recorded
    /// length, so a truncated file is never sent as a short body.
    pub fn stream(
        &self,
        chunk_size: usize,
    ) -> impl Stream<Item = io::Result<Bytes>> + Send + 'static {
        let file = Arc::clone(&self.file);
        let len = self.len;
        let chunk_size = chunk_size.max(1) as u64;
        futures_util::stream::try_unfold(0u64, move |offset| {
            let file = Arc::clone(&file);
            async move {
                if offset >= len {
                    return Ok(None);
                }
                let want = (len - offset).min(chunk_size) as usize;
                let chunk = tokio::task::spawn_blocking(move || {
                    let mut buf = vec![0u8; want];
                    read_exact_at(&file, &mut buf, offset)?;
                    Ok::<_, io::Error>(Bytes::from(buf))
                })
                .await
                .map_err(io::Error::other)??;
                Ok(Some((chunk, offset + want as u64)))
            }
        })
    }
}

impl fmt::Debug for SpoolHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpoolHandle")
            .field("len", &self.len)
            .finish()
    }
}

/// Fills `buf` from `offset` without moving a shared file cursor.
#[cfg(unix)]
fn read_exact_at(file: &std::fs::File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::read_exact_at(file, buf, offset).map_err(|err| {
        if err.kind() == io::ErrorKind::UnexpectedEof {
            spool_truncated()
        } else {
            err
        }
    })
}

/// Fills `buf` from `offset` (each read names its offset, so streams do not interfere).
#[cfg(windows)]
fn read_exact_at(file: &std::fs::File, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        match file.seek_read(buf, offset) {
            Ok(0) => return Err(spool_truncated()),
            Ok(n) => {
                buf = &mut buf[n..];
                offset += n as u64;
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

fn spool_truncated() -> io::Error {
    io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "spooled upload is shorter than its recorded length",
    )
}

impl fmt::Debug for SpooledFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpooledFile")
            .field("path", &self.path)
            .field("len", &self.len)
            .finish()
    }
}

impl Drop for SpooledFile {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// File name prefix of spool files created by [`SpooledFile::create_in`].
pub const SPOOL_FILE_PREFIX: &str = ".turbo-spool-";
/// File name suffix of spool files created by [`SpooledFile::create_in`].
pub const SPOOL_FILE_SUFFIX: &str = ".tmp";

/// Deletes orphaned spool files (left behind when the process crashed or was killed mid-upload)
/// in `dir`: only regular files named `.turbo-spool-*.tmp` directly inside `dir` (no recursion,
/// symlinks are never followed) whose modification time is at least `max_age` old. Live spool
/// files are written continuously while an upload is received, so their mtime stays fresh; later
/// readers use [`SpoolHandle`] or [`SpooledFile::map`], which survive the path's removal.
///
/// Returns the number of removed files; a missing `dir` yields `Ok(0)`.
pub async fn purge_stale_spool_files(dir: &Path, max_age: Duration) -> io::Result<usize> {
    let dir = dir.to_path_buf();
    tokio::task::spawn_blocking(move || purge_stale_spool_files_blocking(&dir, max_age))
        .await
        .map_err(io::Error::other)?
}

fn purge_stale_spool_files_blocking(dir: &Path, max_age: Duration) -> io::Result<usize> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(err) => return Err(err),
    };
    let now = std::time::SystemTime::now();
    let mut removed = 0;
    for entry in entries {
        let Ok(entry) = entry else { continue };
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !(name.starts_with(SPOOL_FILE_PREFIX) && name.ends_with(SPOOL_FILE_SUFFIX)) {
            continue;
        }
        // `DirEntry::metadata` does not follow symlinks.
        let Ok(meta) = entry.metadata() else { continue };
        if !meta.file_type().is_file() {
            continue;
        }
        let age = meta
            .modified()
            .ok()
            .and_then(|modified| now.duration_since(modified).ok())
            .unwrap_or_default();
        if age < max_age {
            continue;
        }
        match std::fs::remove_file(entry.path()) {
            Ok(()) => removed += 1,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => {
                tracing::warn!(path = %entry.path().display(), error = %err, "Failed to remove stale spool file");
            }
        }
    }
    Ok(removed)
}

/// Artifact upload body handed to [`Storage::put`]: small bodies stay in RAM, large ones are
/// spooled to a temp file (see `UPLOAD_MEMORY_THRESHOLD`). Streamed uploads use
/// [`Storage::put_stream`] instead.
#[derive(Debug)]
pub enum UploadBody {
    Bytes(Bytes),
    File(SpooledFile),
}

impl UploadBody {
    /// Payload length in bytes.
    pub fn len(&self) -> u64 {
        match self {
            UploadBody::Bytes(b) => b.len() as u64,
            UploadBody::File(f) => f.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the in-memory bytes, if this body is held in RAM.
    pub fn as_bytes(&self) -> Option<&Bytes> {
        match self {
            UploadBody::Bytes(b) => Some(b),
            UploadBody::File(_) => None,
        }
    }

    /// Materializes the body in memory. Fails with `PayloadTooLarge` when the body is larger than
    /// `limit` bytes (checked before reading a spooled file). Spooled files are deleted afterwards.
    pub async fn into_bytes(self, limit: usize) -> AppResult<Bytes> {
        if self.len() > limit as u64 {
            return Err(AppError::PayloadTooLarge(format!(
                "Artifact of {} bytes exceeds the in-memory limit of {limit} bytes",
                self.len()
            )));
        }
        match self {
            UploadBody::Bytes(b) => Ok(b),
            UploadBody::File(f) => {
                let data = tokio::fs::read(f.path())
                    .await
                    .map_err(|e| AppError::io("Failed to read spooled upload", e))?;
                if data.len() as u64 != f.len() {
                    return Err(AppError::Internal(format!(
                        "Spooled upload length mismatch: expected {} bytes, found {}",
                        f.len(),
                        data.len()
                    )));
                }
                Ok(Bytes::from(data))
            }
        }
    }
}

impl From<Bytes> for UploadBody {
    fn from(b: Bytes) -> Self {
        UploadBody::Bytes(b)
    }
}

impl From<Vec<u8>> for UploadBody {
    fn from(v: Vec<u8>) -> Self {
        UploadBody::Bytes(Bytes::from(v))
    }
}

impl From<&'static [u8]> for UploadBody {
    fn from(v: &'static [u8]) -> Self {
        UploadBody::Bytes(Bytes::from_static(v))
    }
}

impl From<SpooledFile> for UploadBody {
    fn from(f: SpooledFile) -> Self {
        UploadBody::File(f)
    }
}

/// Counters for one in-memory cache tier.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheTierStats {
    pub hits: u64,
    pub misses: u64,
    /// Entries removed to make room (capacity evictions; expiry is counted in `expirations`).
    pub evictions: u64,
    /// Entries dropped because their TTL elapsed (`CACHE_TTL_SECS` / `NEGATIVE_CACHE_TTL_SECS`).
    pub expirations: u64,
    /// Bytes currently held (0 for metadata-only tiers if not tracked).
    pub bytes: u64,
    /// Entries currently held.
    pub entries: u64,
    /// Configured byte budget (0 = not applicable).
    pub max_bytes: u64,
    /// Configured entry budget.
    pub max_entries: u64,
}

/// Point-in-time statistics of the in-memory caches of a storage backend.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheStatsSnapshot {
    pub payload: CacheTierStats,
    pub metadata: CacheTierStats,
}

/// Default hot-cache TTL (`CACHE_TTL_SECS`).
pub const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(300);

/// Sizing and expiry options for the in-memory payload + metadata caches (`TieredStorage`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheOptions {
    /// Total payload byte budget (0 disables the payload cache for `TieredStorage`).
    pub max_bytes: usize,
    /// Maximum number of payload entries.
    pub max_entries: usize,
    /// Largest single payload eligible for caching.
    pub max_item_bytes: usize,
    /// Maximum number of metadata entries (0 disables the metadata cache).
    pub metadata_max_entries: usize,
    /// Expiry of payload and metadata entries (`None` = never expire).
    pub ttl: Option<Duration>,
    /// Expiry of cached misses (`None` = misses are not cached).
    pub negative_ttl: Option<Duration>,
}

impl Default for CacheOptions {
    fn default() -> Self {
        Self {
            max_bytes: 128 * 1024 * 1024,
            max_entries: 10_000,
            max_item_bytes: 4 * 1024 * 1024,
            metadata_max_entries: 50_000,
            ttl: Some(DEFAULT_CACHE_TTL),
            negative_ttl: None,
        }
    }
}

/// Retention/cleanup options for `LocalStorage`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalOptions {
    /// Total on-disk byte budget; oldest entries are evicted beyond it (`None` = unlimited).
    pub max_bytes: Option<u64>,
    /// Entries older than this are deleted (`None` = never).
    pub max_age: Option<Duration>,
    /// How often the background janitor runs.
    pub cleanup_interval: Duration,
    /// Orphaned temp files older than this are deleted.
    pub temp_max_age: Duration,
}

impl Default for LocalOptions {
    fn default() -> Self {
        Self {
            max_bytes: None,
            max_age: None,
            cleanup_interval: Duration::from_secs(600),
            temp_max_age: Duration::from_secs(3600),
        }
    }
}

/// Retention options for the object store backends (see `object::janitor`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionOptions {
    /// Total byte budget; the oldest artifacts are evicted beyond it (`None` = unlimited).
    pub max_bytes: Option<u64>,
    /// Artifacts uploaded longer ago than this are deleted (`None` = never).
    pub max_age: Option<Duration>,
    /// How often the janitor runs.
    pub interval: Duration,
}

#[async_trait]
pub trait Storage: Send + Sync {
    /// Identifier of the storage backend (e.g. `"local"`, `"s3"`, `"rustfs"`, `"garage"`).
    fn backend_name(&self) -> &'static str;

    /// Synchronous fast-path for in-memory hot cache `GET` hits.
    /// Avoids boxing an async Future or stream when the artifact is already in RAM.
    #[inline]
    fn get_hot(&self, _namespace: &str, _hash: &str) -> Option<(Bytes, ArtifactMetadata)> {
        None
    }

    /// Synchronous fast-path for in-memory metadata/hot cache `HEAD` hits.
    /// Avoids boxing an async Future when metadata is already in RAM.
    #[inline]
    fn head_hot(&self, _namespace: &str, _hash: &str) -> Option<ArtifactMetadata> {
        None
    }

    /// Stores an artifact and its associated metadata. `metadata.size` is set from `body.len()`.
    /// With [`PutMode::IfAbsent`] nothing is written when the artifact already exists and
    /// [`PutOutcome::AlreadyExists`] is returned.
    async fn put(
        &self,
        namespace: &str,
        hash: &str,
        body: UploadBody,
        metadata: ArtifactMetadata,
        mode: PutMode,
    ) -> AppResult<PutOutcome>;

    /// Whether [`Storage::put_stream`] stores bodies while they are received (instead of
    /// receiving them completely first). The server only streams overwrite-mode uploads with a
    /// `Content-Length` above `UPLOAD_MEMORY_THRESHOLD`.
    fn streams_uploads(&self) -> bool {
        false
    }

    /// Stores an artifact in [`PutMode::Overwrite`] mode while its body is still being received.
    /// `metadata.size` is set from the announced length. Nothing may become visible unless the
    /// whole body arrived. The default receives the body completely (RAM or spool file, see
    /// [`UploadStream::receive`]) and calls [`Storage::put`].
    async fn put_stream(
        &self,
        namespace: &str,
        hash: &str,
        body: UploadStream,
        metadata: ArtifactMetadata,
    ) -> AppResult<PutOutcome> {
        let body = body.receive().await?;
        self.put(namespace, hash, body, metadata, PutMode::Overwrite)
            .await
    }

    /// Retrieves an artifact payload and its associated metadata.
    async fn get(&self, namespace: &str, hash: &str) -> AppResult<Option<StoredArtifact>>;

    /// Checks if an artifact exists and returns its metadata without fetching the body.
    async fn head(&self, namespace: &str, hash: &str) -> AppResult<Option<ArtifactMetadata>>;

    /// Deletes an artifact and its metadata if it exists.
    async fn delete(&self, namespace: &str, hash: &str) -> AppResult<bool>;

    /// Checks whether the storage backend is reachable and healthy.
    async fn health_check(&self) -> AppResult<()>;

    /// Statistics of in-memory caches, if this backend has any.
    fn cache_stats(&self) -> Option<CacheStatsSnapshot> {
        None
    }
}

/// Longest accepted artifact hash or namespace (see [`validate_hash`], [`sanitize_namespace`]).
pub const MAX_IDENTIFIER_LEN: usize = 128;

/// Validates an artifact hash to prevent path traversal or key injection.
#[inline]
pub fn validate_hash(hash: &str) -> AppResult<&str> {
    if hash.is_empty() {
        return Err(AppError::BadRequest(
            "Artifact hash cannot be empty".to_string(),
        ));
    }
    if hash.len() > MAX_IDENTIFIER_LEN {
        return Err(AppError::BadRequest(format!(
            "Artifact hash exceeds maximum length of {MAX_IDENTIFIER_LEN} characters"
        )));
    }
    if !hash
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(AppError::BadRequest(format!(
            "Invalid artifact hash '{hash}': only alphanumeric characters, '-' and '_' are allowed"
        )));
    }
    Ok(hash)
}

/// Validates and normalizes a team/slug namespace identifier without allocating.
#[inline]
pub fn sanitize_namespace(namespace: &str) -> AppResult<&str> {
    let trimmed = namespace.trim();
    if trimmed.is_empty() {
        return Ok("default");
    }
    // Leading dots are rejected: they would collide with hidden/internal directories such as the
    // local backend's `.spool` upload directory and the `.`/`..` path components.
    if trimmed.len() > MAX_IDENTIFIER_LEN || trimmed.starts_with('.') {
        return Err(AppError::BadRequest(format!(
            "Invalid team/slug namespace '{trimmed}'"
        )));
    }
    if !trimmed
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
    {
        return Err(AppError::BadRequest(format!(
            "Invalid characters in team/slug namespace '{trimmed}'"
        )));
    }
    Ok(trimmed)
}

/// Builds a storage instance from the CLI configuration, wrapping disk/object store backends
/// with a strictly bounded in-memory hot LRU cache when configured.
pub async fn build_storage(config: &CliConfig) -> anyhow::Result<Arc<dyn Storage>> {
    let base: Arc<dyn Storage> = match config.storage_type {
        StorageBackendKind::Local => Arc::new(
            local::LocalStorage::with_options(&config.local_storage_path, config.local_options())
                .await?,
        ),
        StorageBackendKind::S3 | StorageBackendKind::Rustfs | StorageBackendKind::Garage => {
            let storage = s3::S3Storage::new(config.resolve_s3_config()?)?;
            storage
                .object_storage()
                .spawn_janitor(config.retention_options());
            Arc::new(storage)
        }
        StorageBackendKind::Gcs => {
            let storage = gcs::new(config.resolve_gcs_config()?)?;
            storage.spawn_janitor(config.retention_options());
            Arc::new(storage)
        }
        StorageBackendKind::Azure => {
            let storage = azure::new(config.resolve_azure_config()?)?;
            storage.spawn_janitor(config.retention_options());
            Arc::new(storage)
        }
    };
    Ok(wrap_with_tiered_cache(base, config))
}

fn wrap_with_tiered_cache(base: Arc<dyn Storage>, config: &CliConfig) -> Arc<dyn Storage> {
    let opts = config.cache_options();
    if opts.max_bytes == 0 && opts.metadata_max_entries == 0 {
        return base;
    }
    Arc::new(cache::TieredStorage::with_options(base, opts))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{SpooledFile, UploadBody, sanitize_namespace, validate_hash};
    use crate::error::AppError;

    #[tokio::test]
    async fn purge_stale_spool_files_removes_only_old_spool_files() {
        use std::time::{Duration, SystemTime};

        let dir = tempfile::tempdir().unwrap();
        let old = SystemTime::now() - Duration::from_secs(7200);
        let set_old = |path: &std::path::Path| {
            let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
            file.set_modified(old).unwrap();
        };

        let stale = dir.path().join(".turbo-spool-stale.tmp");
        std::fs::write(&stale, b"x").unwrap();
        set_old(&stale);
        let fresh = dir.path().join(".turbo-spool-fresh.tmp");
        std::fs::write(&fresh, b"x").unwrap();
        let foreign = dir.path().join("other-file.tmp");
        std::fs::write(&foreign, b"x").unwrap();
        set_old(&foreign);
        let nested_dir = dir.path().join(".turbo-spool-dir.tmp");
        std::fs::create_dir(&nested_dir).unwrap();
        // Symlinks are never followed or removed, even if the name matches.
        #[cfg(unix)]
        let link = {
            let link = dir.path().join(".turbo-spool-link.tmp");
            std::os::unix::fs::symlink(&foreign, &link).unwrap();
            link
        };
        // A live spool file created through the regular API is fresh and must survive.
        let (live, _file) = SpooledFile::create_in(dir.path()).await.unwrap();

        let removed = super::purge_stale_spool_files(dir.path(), Duration::from_secs(3600))
            .await
            .unwrap();
        assert_eq!(removed, 1);
        assert!(!stale.exists());
        assert!(fresh.exists());
        assert!(foreign.exists());
        assert!(nested_dir.exists());
        assert!(live.path().exists());
        #[cfg(unix)]
        assert!(link.symlink_metadata().is_ok());

        let missing = dir.path().join("does-not-exist");
        assert_eq!(
            super::purge_stale_spool_files(&missing, Duration::ZERO)
                .await
                .unwrap(),
            0
        );
    }

    async fn spooled(dir: &std::path::Path, content: &[u8]) -> SpooledFile {
        use tokio::io::AsyncWriteExt;
        let (mut spool, mut file) = SpooledFile::create_in(dir).await.unwrap();
        file.write_all(content).await.unwrap();
        file.flush().await.unwrap();
        spool.set_len(content.len() as u64);
        spool
    }

    async fn collect_spool(handle: &super::SpoolHandle, chunk: usize) -> std::io::Result<Vec<u8>> {
        use futures_util::TryStreamExt;
        let chunks: Vec<Bytes> = handle.stream(chunk).try_collect().await?;
        assert!(chunks.iter().all(|c| c.len() <= chunk));
        Ok(chunks.concat())
    }

    #[tokio::test]
    async fn spool_handle_streams_are_independent_and_survive_removal() {
        let dir = tempfile::tempdir().unwrap();
        let data: Vec<u8> = (0..10_000u32).map(|i| (i % 253) as u8).collect();
        let spool = spooled(dir.path(), &data).await;
        let handle = spool.open_handle().await.unwrap();
        assert_eq!(handle.len(), data.len() as u64);

        // Interleaved streams do not share a file offset.
        let (a, b) = tokio::join!(collect_spool(&handle, 777), collect_spool(&handle, 4096));
        assert_eq!(a.unwrap(), data);
        assert_eq!(b.unwrap(), data);

        // The path disappearing (orphan sweep) does not affect the open handle on Unix.
        #[cfg(unix)]
        {
            std::fs::remove_file(spool.path()).unwrap();
            assert_eq!(collect_spool(&handle, 1000).await.unwrap(), data);
        }
        drop(spool);
    }

    #[tokio::test]
    async fn spool_handle_reports_truncated_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut spool = spooled(dir.path(), b"short").await;
        spool.set_len(10);
        let handle = spool.open_handle().await.unwrap();
        let err = collect_spool(&handle, 4).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);

        let empty = spooled(dir.path(), b"").await;
        let handle = empty.open_handle().await.unwrap();
        assert!(handle.is_empty());
        assert!(collect_spool(&handle, 4).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn spooled_file_map_reads_the_recorded_length() {
        let dir = tempfile::tempdir().unwrap();
        let data: Vec<u8> = (0..100_000u32).map(|i| (i % 249) as u8).collect();
        let spool = spooled(dir.path(), &data).await;
        let mapped = spool.map().await.unwrap();
        assert_eq!(&mapped[..], &data[..]);
        // The mapping survives the orphan sweep removing the path (Unix).
        #[cfg(unix)]
        {
            std::fs::remove_file(spool.path()).unwrap();
            assert_eq!(&mapped[..], &data[..]);
        }
        drop(spool);

        let mut short = spooled(dir.path(), b"short").await;
        short.set_len(10);
        let err = short.map().await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
        let empty = spooled(dir.path(), b"").await;
        assert!(empty.map().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn spooled_file_is_private_and_removed_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        let spool = spooled(dir.path(), b"hello").await;
        let path = spool.path().to_path_buf();
        assert!(path.starts_with(dir.path()));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(
                mode & 0o077,
                0,
                "spool files must not be group/world accessible"
            );
        }
        drop(spool);
        assert!(!path.exists());

        let spool = spooled(dir.path(), b"abc").await;
        let path = spool.take_path();
        assert!(path.exists(), "take_path must keep the file");
    }

    #[tokio::test]
    async fn upload_body_into_bytes_enforces_limit() {
        let dir = tempfile::tempdir().unwrap();
        let body = UploadBody::from(Bytes::from_static(b"in-memory"));
        assert_eq!(
            body.into_bytes(9).await.unwrap(),
            Bytes::from_static(b"in-memory")
        );

        let spool = spooled(dir.path(), b"on-disk").await;
        let path = spool.path().to_path_buf();
        let body = UploadBody::from(spool);
        assert_eq!(
            body.into_bytes(1024).await.unwrap(),
            Bytes::from_static(b"on-disk")
        );
        assert!(
            !path.exists(),
            "into_bytes consumes and removes the spooled file"
        );

        let spool = spooled(dir.path(), b"too large").await;
        let path = spool.path().to_path_buf();
        let err = UploadBody::File(spool).into_bytes(3).await.unwrap_err();
        assert!(matches!(err, AppError::PayloadTooLarge(_)));
        assert!(!path.exists());
        let err = UploadBody::from(vec![0u8; 4])
            .into_bytes(3)
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::PayloadTooLarge(_)));
    }

    #[test]
    fn validate_hash_rejects_traversal_and_invalid_chars() {
        assert!(validate_hash("1234567890abcdef").is_ok());
        assert!(validate_hash("turbo_hash-2026").is_ok());
        assert!(validate_hash(&"a".repeat(128)).is_ok());
        assert!(validate_hash("").is_err());
        assert!(validate_hash("../etc/passwd").is_err());
        assert!(validate_hash("abc/def").is_err());
        assert!(validate_hash("abc.def").is_err());
        assert!(validate_hash(&"a".repeat(129)).is_err());
    }

    #[test]
    fn sanitize_namespace_rejects_traversal_and_hidden_names() {
        assert_eq!(sanitize_namespace("").unwrap(), "default");
        assert_eq!(sanitize_namespace("team_123").unwrap(), "team_123");
        assert_eq!(sanitize_namespace("my-org.prod").unwrap(), "my-org.prod");
        assert!(sanitize_namespace("..").is_err());
        assert!(sanitize_namespace(".").is_err());
        assert!(sanitize_namespace(".spool").is_err());
        assert!(sanitize_namespace("a/b").is_err());
    }
}
