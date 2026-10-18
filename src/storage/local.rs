//! Local filesystem storage backend.
//!
//! # On-disk layout (format v1)
//!
//! `<base>/<ns>/<shard>/<hash>.artifact`, where `<shard>` is the first two characters of the hash
//! and every component is case-escaped ([`escape_component`]), so identifiers differing only in
//! case never share a file on case-insensitive filesystems (macOS/Windows defaults).
//!
//! Each artifact is a single file, published with one atomic rename (or no-replace link for
//! [`PutMode::IfAbsent`]), so payload and metadata can never be mixed between concurrent writers:
//!
//! ```text
//! payload (payload_len bytes) | metadata JSON (meta_len bytes) | trailer (24 bytes)
//! trailer = payload_len: u64 LE | meta_len: u32 LE | version: u16 LE | flags: u16 LE | b"TURBOCRS"
//! ```
//!
//! The metadata lives *after* the payload so a spooled upload (`UploadBody::File`) is published by
//! appending metadata + trailer to the spool file and renaming it into place, without copying the
//! payload. Readers validate the trailer against the file length; truncated or corrupt files are
//! treated as misses and removed.
//!
//! # Retention
//!
//! A background janitor (spawned by the constructor) deletes orphaned temp files older than
//! `temp_max_age`, artifacts whose mtime is older than `max_age`, and evicts the oldest artifacts
//! (by mtime) while the store exceeds `max_bytes`, then removes empty shard/namespace
//! directories. GET hits refresh the mtime at most once per touch interval (1 hour, or a quarter
//! of `max_age` if smaller), approximating LRU. Removals never delete a file that was replaced
//! concurrently (see [`remove_if_unchanged`]).

use std::{
    borrow::Cow,
    collections::BTreeMap,
    fs::{self as stdfs, File, Metadata},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use tokio::io::AsyncReadExt;
use tokio_util::io::ReaderStream;

use crate::{
    error::{AppError, AppResult},
    models::ArtifactMetadata,
    storage::{
        MAX_IDENTIFIER_LEN, PutMode, PutOutcome, StoredArtifact, UploadBody, sanitize_namespace,
        validate_hash,
    },
};

pub use super::LocalOptions;

use super::Storage;

/// File extension of artifacts in the current single-file format.
pub const ARTIFACT_EXTENSION: &str = "artifact";
const MAGIC: &[u8; 8] = b"TURBOCRS";
const FORMAT_VERSION: u16 = 1;
/// Size of the fixed trailer at the end of every artifact file.
pub const TRAILER_LEN: usize = 24;
/// Upper bound for the serialized metadata block (real metadata is well below 4 KiB).
pub const MAX_METADATA_LEN: usize = 64 * 1024;
/// Read buffer of GET streams (a 4 KiB default would cost one blocking-pool hop per 4 KiB).
pub const STREAM_BUFFER_SIZE: usize = 256 * 1024;
/// Maximum interval between mtime refreshes of an artifact that is being read.
const MAX_TOUCH_INTERVAL: Duration = Duration::from_secs(3600);
/// Empty directories younger than this are kept (avoids churn with puts that just created them).
const EMPTY_DIR_MIN_AGE: Duration = Duration::from_secs(60);
/// Prefix/suffix of temp files written by this backend inside shard directories.
const TEMP_PREFIX: &str = ".tcrs-";
const TEMP_SUFFIX: &str = ".tmp";

/// Serializes the no-replace fallback for filesystems without hard-link support.
static NOCLOBBER_FALLBACK_LOCK: Mutex<()> = Mutex::new(());
/// Whether the non-atomic no-replace fallback has been logged already.
static NOCLOBBER_FALLBACK_WARNED: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Clone)]
pub struct LocalStorage {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    base_dir: PathBuf,
    options: LocalOptions,
    touch_interval: Duration,
    cleanup_lock: Mutex<()>,
}

/// Result of one janitor pass (see [`LocalStorage::run_cleanup`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CleanupReport {
    /// Orphaned temp files removed.
    pub temp_files_removed: u64,
    /// Artifacts removed because they were older than `max_age`.
    pub expired_removed: u64,
    /// Artifacts evicted to get below `max_bytes`.
    pub evicted_removed: u64,
    /// Artifacts remaining after the pass.
    pub remaining_entries: u64,
    /// Bytes (file sizes) remaining after the pass.
    pub remaining_bytes: u64,
    /// Empty namespace/shard directories removed.
    pub dirs_removed: u64,
}

impl LocalStorage {
    pub async fn new(base_dir: impl AsRef<Path>) -> anyhow::Result<Self> {
        Self::with_options(base_dir, LocalOptions::default()).await
    }

    /// Creates the store and spawns its background janitor (which runs immediately and then
    /// every `options.cleanup_interval`, until the last clone of the store is dropped).
    pub async fn with_options(
        base_dir: impl AsRef<Path>,
        options: LocalOptions,
    ) -> anyhow::Result<Self> {
        let base_dir = base_dir.as_ref().to_path_buf();
        let base_dir = tokio::task::spawn_blocking(move || -> io::Result<PathBuf> {
            create_private_dir_all(&base_dir)?;
            Ok(base_dir)
        })
        .await??;

        let touch_interval = options
            .max_age
            .map_or(MAX_TOUCH_INTERVAL, |age| (age / 4).min(MAX_TOUCH_INTERVAL));
        let interval = options.cleanup_interval.max(Duration::from_millis(1));
        let storage = Self {
            inner: Arc::new(Inner {
                base_dir,
                options,
                touch_interval,
                cleanup_lock: Mutex::new(()),
            }),
        };
        spawn_janitor(Arc::downgrade(&storage.inner), interval);
        Ok(storage)
    }

    /// Runs one janitor pass now (the background task calls this periodically).
    pub async fn run_cleanup(&self) -> AppResult<CleanupReport> {
        let inner = self.inner.clone();
        run_blocking(move || {
            inner
                .cleanup()
                .map_err(|e| io_error("Local storage cleanup failed", e))
        })
        .await
    }

    /// Path of the artifact file for `namespace`/`hash` in the current format.
    pub fn artifact_path(&self, namespace: &str, hash: &str) -> AppResult<PathBuf> {
        let ns = sanitize_namespace(namespace)?;
        let hash = validate_hash(hash)?;
        Ok(self.inner.paths(ns, hash).target)
    }
}

fn spawn_janitor(inner: Weak<Inner>, interval: Duration) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let Some(inner) = inner.upgrade() else {
                break;
            };
            let storage = LocalStorage { inner };
            match storage.run_cleanup().await {
                Ok(report)
                    if report.temp_files_removed
                        + report.expired_removed
                        + report.evicted_removed
                        + report.dirs_removed
                        > 0 =>
                {
                    tracing::info!(
                        temp_files_removed = report.temp_files_removed,
                        expired_removed = report.expired_removed,
                        evicted_removed = report.evicted_removed,
                        dirs_removed = report.dirs_removed,
                        remaining_entries = report.remaining_entries,
                        remaining_bytes = report.remaining_bytes,
                        "Local storage cleanup finished"
                    );
                }
                Ok(report) => tracing::debug!(?report, "Local storage cleanup finished"),
                Err(e) => tracing::warn!(error = %e, "Local storage cleanup failed"),
            }
        }
    });
}

async fn run_blocking<T, F>(f: F) -> AppResult<T>
where
    F: FnOnce() -> AppResult<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| AppError::Internal(format!("Local storage task failed: {e}")))?
}

/// Shorthand for [`AppError::io`]: out-of-space errors become 507, everything else 500.
pub fn io_error(context: &str, e: io::Error) -> AppError {
    AppError::io(context, e)
}

/// Portable file-name limit (`NAME_MAX` on Linux/macOS, the component limit on Windows/NTFS).
const NAME_MAX: usize = 255;
// Every escaped artifact file name of a valid hash fits within `NAME_MAX`.
const _: () = assert!(
    MAX_IDENTIFIER_LEN + 1 + MAX_IDENTIFIER_LEN.div_ceil(4) + 1 + ARTIFACT_EXTENSION.len()
        <= NAME_MAX
);

/// Escapes a validated namespace/hash path component so that it is unique on case-insensitive
/// filesystems: components without ASCII uppercase letters are returned unchanged, others become
/// `lowercase(s) + "^" + bitmap`, where the bitmap has one lowercase hex digit per 4 characters
/// and bit `i % 4` of digit `i / 4` is set iff character `i` is uppercase (`AbC` → `abc^5`).
///
/// Inputs never contain `^`, so the mapping is injective even after case folding, and the output
/// is at most `len + 1 + ceil(len / 4)` bytes long (161 for a 128-character identifier).
pub fn escape_component(s: &str) -> Cow<'_, str> {
    if !s.bytes().any(|b| b.is_ascii_uppercase()) {
        return Cow::Borrowed(s);
    }
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len() + 1 + s.len().div_ceil(4));
    out.push_str(&s.to_ascii_lowercase());
    out.push('^');
    for chunk in bytes.chunks(4) {
        let nibble = chunk
            .iter()
            .enumerate()
            .filter(|(_, b)| b.is_ascii_uppercase())
            .fold(0u32, |acc, (i, _)| acc | (1 << i));
        out.push(char::from_digit(nibble, 16).expect("nibble < 16"));
    }
    Cow::Owned(out)
}

fn shard_of(hash: &str) -> &str {
    // Hashes are validated ASCII, so byte slicing is on a char boundary.
    &hash[..hash.len().min(2)]
}

struct ArtifactPaths {
    ns_dir: PathBuf,
    shard_dir: PathBuf,
    target: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileId {
    #[cfg(unix)]
    dev: u64,
    #[cfg(unix)]
    ino: u64,
    len: u64,
    modified: Option<SystemTime>,
}

fn file_id(meta: &Metadata) -> FileId {
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;
    FileId {
        #[cfg(unix)]
        dev: meta.dev(),
        #[cfg(unix)]
        ino: meta.ino(),
        len: meta.len(),
        modified: meta.modified().ok(),
    }
}

/// Removes `path` only if it still is the file identified by `expected`, so a file that was
/// concurrently replaced by an upload or touched by a read is kept.
///
/// A plain stat-then-unlink would delete a file renamed into place between the two calls.
/// Instead the name is atomically renamed to a private temp name, the moved file is verified, and
/// a different (newer) file is linked back unless an even newer one was published meanwhile.
/// Readers may see a brief miss, never a loss of the newest version. Crash leftovers use the temp
/// naming and are collected by the janitor.
fn remove_if_unchanged(path: &Path, expected: &FileId) -> io::Result<bool> {
    remove_if_unchanged_with(path, expected, || {})
}

/// [`remove_if_unchanged`] with a hook between the identity check and the rename (tests use it
/// to inject a concurrent replacement).
fn remove_if_unchanged_with(
    path: &Path,
    expected: &FileId,
    after_check: impl FnOnce(),
) -> io::Result<bool> {
    match stdfs::symlink_metadata(path) {
        Ok(m) if file_id(&m) == *expected => {}
        Ok(_) => return Ok(false),
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e),
    }
    after_check();
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let made = tempfile::Builder::new()
        .prefix(TEMP_PREFIX)
        .suffix(TEMP_SUFFIX)
        .rand_bytes(16)
        .make_in(dir, |grave| stdfs::rename(path, grave));
    let grave = match made {
        Ok(named) => named.keep().map_err(|e| e.error)?.1,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e),
    };
    let grave = TempGuard(Some(grave));
    let is_expected = stdfs::symlink_metadata(grave.path()).is_ok_and(|m| file_id(&m) == *expected);
    if is_expected {
        // Dropping the guard unlinks the grave name.
        return Ok(true);
    }
    // A newer file was renamed in between our stat and rename: put it back (no-replace, so an
    // even newer upload published meanwhile wins).
    if let Err(e) = link_noclobber(grave.path(), path)
        && e.kind() != io::ErrorKind::AlreadyExists
    {
        tracing::warn!(path = %path.display(), error = %e, "Failed to restore replaced file");
    }
    Ok(false)
}

fn remove_ignore_missing(path: &Path) -> io::Result<bool> {
    match stdfs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

/// Creates `dir` and missing parents; new directories are private to the owner (0700 on unix).
fn create_private_dir_all(dir: &Path) -> io::Result<()> {
    let mut builder = stdfs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(dir)
}

/// fsyncs a directory so a completed rename/link survives a crash (no-op on non-unix).
fn sync_dir(dir: &Path) {
    #[cfg(unix)]
    if let Err(e) = File::open(dir).and_then(|d| d.sync_all()) {
        tracing::warn!(dir = %dir.display(), error = %e, "Failed to fsync local storage directory");
    }
    #[cfg(not(unix))]
    let _ = dir;
}

/// Owns a temp file path and removes it on drop unless disarmed.
struct TempGuard(Option<PathBuf>);

impl TempGuard {
    fn path(&self) -> &Path {
        self.0.as_deref().expect("temp guard path present")
    }

    fn disarm(mut self) {
        self.0 = None;
    }
}

impl Drop for TempGuard {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            let _ = stdfs::remove_file(path);
        }
    }
}

/// Creates a new temp file inside `dir`: unpredictable name, `O_EXCL` (never follows or reuses
/// an existing path), mode 0600 on unix.
fn create_temp_in(dir: &Path) -> io::Result<(File, TempGuard)> {
    let (file, path) = tempfile::Builder::new()
        .prefix(TEMP_PREFIX)
        .suffix(TEMP_SUFFIX)
        .tempfile_in(dir)?
        .keep()
        .map_err(|e| e.error)?;
    Ok((file, TempGuard(Some(path))))
}

/// `ENOTSUP`/`EOPNOTSUPP` values of the supported platforms. std reports them as an
/// uncategorized error kind, so they have to be matched by number.
#[cfg(any(target_os = "linux", target_os = "android"))]
const NOT_SUPPORTED_ERRNOS: &[i32] = &[95];
#[cfg(any(target_os = "macos", target_os = "ios"))]
const NOT_SUPPORTED_ERRNOS: &[i32] = &[45, 102];
#[cfg(target_os = "freebsd")]
const NOT_SUPPORTED_ERRNOS: &[i32] = &[45];
#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd"
)))]
const NOT_SUPPORTED_ERRNOS: &[i32] = &[];

/// Whether a `link(2)` failure means "this filesystem cannot create hard links" (FAT/exFAT
/// return `ENOTSUP`/`EPERM`, FUSE/NFS/SMB may return `EOPNOTSUPP`/`ENOSYS`), as opposed to a
/// real outcome (`EEXIST`) or error (`ENOENT`, `EXDEV`, `ENOSPC`, `EIO`, ...).
fn link_unsupported(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::Unsupported | io::ErrorKind::PermissionDenied
    ) || e
        .raw_os_error()
        .is_some_and(|code| NOT_SUPPORTED_ERRNOS.contains(&code))
}

/// Creates the name `dst` for the file at `src` iff `dst` does not exist yet (atomic via
/// `link(2)`). The caller still owns (and must remove) `src`.
fn link_noclobber(src: &Path, dst: &Path) -> io::Result<()> {
    link_noclobber_with(src, dst, |s, d| stdfs::hard_link(s, d))
}

fn link_noclobber_with(
    src: &Path,
    dst: &Path,
    link: impl FnOnce(&Path, &Path) -> io::Result<()>,
) -> io::Result<()> {
    match link(src, dst) {
        Ok(()) => Ok(()),
        // Filesystems without hard links (e.g. FAT/exFAT, some SMB/NFS shares): fall back to a
        // check-then-rename, serialized within this process only. `IfAbsent` is therefore not
        // atomic across processes on such a volume (std has no portable `RENAME_NOREPLACE`);
        // operators are warned once.
        Err(e) if link_unsupported(&e) => {
            if !NOCLOBBER_FALLBACK_WARNED.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    dir = %dst.parent().unwrap_or(dst).display(),
                    error = %e,
                    "Local storage filesystem does not support hard links; immutable (IfAbsent) \
                     uploads are only atomic within this process, not across processes"
                );
            }
            let _lock = NOCLOBBER_FALLBACK_LOCK
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            match stdfs::symlink_metadata(dst) {
                Ok(_) => Err(io::Error::from(io::ErrorKind::AlreadyExists)),
                Err(m) if m.kind() == io::ErrorKind::NotFound => {
                    // Rename moves `src`; the caller's later remove of `src` is a no-op.
                    stdfs::rename(src, dst)
                }
                Err(_) => Err(e),
            }
        }
        Err(e) => Err(e),
    }
}

/// Publishes a fully written and synced temp file at `target`.
fn publish(temp: TempGuard, target: &Path, mode: PutMode) -> io::Result<PutOutcome> {
    match mode {
        PutMode::Overwrite => {
            stdfs::rename(temp.path(), target)?;
            temp.disarm();
            Ok(PutOutcome::Stored)
        }
        // The guard's drop removes the temp name after linking.
        PutMode::IfAbsent => link_if_absent(temp.path(), target),
    }
}

/// Whether `path` holds a complete, valid artifact. A corrupt file is removed (unless it was
/// concurrently replaced) and reported as absent, so it never blocks an `IfAbsent` re-upload.
fn existing_artifact_is_valid(path: &Path) -> io::Result<bool> {
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e),
    };
    let meta = file.metadata()?;
    match read_artifact_header(&mut file, meta.len())? {
        Ok(_) => Ok(true),
        Err(reason) => {
            tracing::warn!(path = %path.display(), reason, "Removing corrupt local artifact");
            remove_if_unchanged(path, &file_id(&meta))?;
            Ok(false)
        }
    }
}

/// Publishes the finished file at `src` as `target` unless a valid artifact already exists
/// there (no-replace link). A corrupt or concurrently deleted target is retried a few times.
/// The caller still owns (and must remove) `src`.
fn link_if_absent(src: &Path, target: &Path) -> io::Result<PutOutcome> {
    const ATTEMPTS: usize = 3;
    for _ in 0..ATTEMPTS {
        match link_noclobber(src, target) {
            Ok(()) => return Ok(PutOutcome::Stored),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                if existing_artifact_is_valid(target)? {
                    return Ok(PutOutcome::AlreadyExists);
                }
            }
            Err(e) => return Err(e),
        }
    }
    // Something keeps (re)creating invalid files at `target`; do not fight over it.
    Ok(PutOutcome::AlreadyExists)
}

fn encode_trailer(payload_len: u64, meta_len: u32) -> [u8; TRAILER_LEN] {
    let mut t = [0u8; TRAILER_LEN];
    t[0..8].copy_from_slice(&payload_len.to_le_bytes());
    t[8..12].copy_from_slice(&meta_len.to_le_bytes());
    t[12..14].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    // t[14..16]: flags, reserved (0).
    t[16..24].copy_from_slice(MAGIC);
    t
}

/// Validates a trailer against the total file length; returns `(payload_len, meta_len)`.
fn decode_trailer(t: &[u8; TRAILER_LEN], file_len: u64) -> Result<(u64, usize), &'static str> {
    if &t[16..24] != MAGIC {
        return Err("bad magic");
    }
    let version = u16::from_le_bytes([t[12], t[13]]);
    if version != FORMAT_VERSION {
        return Err("unsupported format version");
    }
    let payload_len = u64::from_le_bytes(t[0..8].try_into().expect("8 bytes"));
    let meta_len = u32::from_le_bytes(t[8..12].try_into().expect("4 bytes")) as usize;
    if meta_len > MAX_METADATA_LEN {
        return Err("metadata block too large");
    }
    let expected = payload_len
        .checked_add(meta_len as u64 + TRAILER_LEN as u64)
        .ok_or("length overflow")?;
    if expected != file_len {
        return Err("length mismatch (truncated or corrupt file)");
    }
    Ok((payload_len, meta_len))
}

/// Reads and validates the metadata of an open artifact file; leaves the cursor at offset 0.
/// `Ok(Err(reason))` means the file is corrupt.
fn read_artifact_header(
    file: &mut File,
    file_len: u64,
) -> io::Result<Result<ArtifactMetadata, &'static str>> {
    if file_len < TRAILER_LEN as u64 {
        return Ok(Err("file shorter than trailer"));
    }
    let mut trailer = [0u8; TRAILER_LEN];
    file.seek(SeekFrom::Start(file_len - TRAILER_LEN as u64))?;
    if let Err(e) = file.read_exact(&mut trailer) {
        return if e.kind() == io::ErrorKind::UnexpectedEof {
            Ok(Err("file shrank while reading"))
        } else {
            Err(e)
        };
    }
    let (payload_len, meta_len) = match decode_trailer(&trailer, file_len) {
        Ok(v) => v,
        Err(reason) => return Ok(Err(reason)),
    };
    let mut meta = vec![0u8; meta_len];
    file.seek(SeekFrom::Start(payload_len))?;
    if let Err(e) = file.read_exact(&mut meta) {
        return if e.kind() == io::ErrorKind::UnexpectedEof {
            Ok(Err("file shrank while reading"))
        } else {
            Err(e)
        };
    }
    let Ok(mut metadata) = serde_json::from_slice::<ArtifactMetadata>(&meta) else {
        return Ok(Err("invalid metadata JSON"));
    };
    metadata.size = payload_len;
    file.seek(SeekFrom::Start(0))?;
    Ok(Ok(metadata))
}

/// An artifact found on disk. `file` is positioned at the payload start; exactly
/// `metadata.size` payload bytes follow.
struct Found {
    file: File,
    metadata: ArtifactMetadata,
}

enum JanitorItem {
    Temp {
        path: PathBuf,
        meta: Metadata,
    },
    Artifact {
        path: PathBuf,
        meta: Metadata,
        size: u64,
    },
}

fn age_of(meta: &Metadata, now: SystemTime) -> Duration {
    meta.modified()
        .ok()
        .and_then(|m| now.duration_since(m).ok())
        .unwrap_or_default()
}

fn minute_of(meta: &Metadata) -> u64 {
    meta.modified()
        .ok()
        .and_then(|m| m.duration_since(UNIX_EPOCH).ok())
        .map_or(0, |d| d.as_secs() / 60)
}

impl Inner {
    fn paths(&self, ns: &str, hash: &str) -> ArtifactPaths {
        let ns_dir = self.base_dir.join(&*escape_component(ns));
        let shard_dir = ns_dir.join(&*escape_component(shard_of(hash)));
        let mut name = escape_component(hash).into_owned();
        name.push('.');
        name.push_str(ARTIFACT_EXTENSION);
        // Guaranteed for every valid hash by the compile-time check next to `NAME_MAX`.
        debug_assert!(name.len() <= NAME_MAX, "file name too long: {name}");
        let target = shard_dir.join(name);
        ArtifactPaths {
            ns_dir,
            shard_dir,
            target,
        }
    }

    fn maybe_touch(&self, file: &File, meta: &Metadata) {
        let now = SystemTime::now();
        if age_of(meta, now) >= self.touch_interval
            && let Err(e) = file.set_modified(now)
        {
            tracing::debug!(error = %e, "Failed to refresh artifact mtime");
        }
    }

    fn lookup(&self, ns: &str, hash: &str, touch: bool) -> io::Result<Option<Found>> {
        let paths = self.paths(ns, hash);
        match File::open(&paths.target) {
            Ok(mut file) => {
                let meta = file.metadata()?;
                match read_artifact_header(&mut file, meta.len())? {
                    Ok(metadata) => {
                        if touch {
                            self.maybe_touch(&file, &meta);
                        }
                        Ok(Some(Found { file, metadata }))
                    }
                    Err(reason) => {
                        tracing::warn!(
                            path = %paths.target.display(),
                            reason,
                            "Removing corrupt local artifact"
                        );
                        remove_if_unchanged(&paths.target, &file_id(&meta))?;
                        Ok(None)
                    }
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Ensures the shard directory exists; returns whether it had to be created.
    fn ensure_shard_dir(&self, paths: &ArtifactPaths) -> io::Result<bool> {
        if paths.shard_dir.is_dir() {
            return Ok(false);
        }
        create_private_dir_all(&paths.shard_dir)?;
        Ok(true)
    }

    /// Runs `op`, recreating the shard directory and retrying if it failed with `NotFound`
    /// because the janitor removed the (empty) directory concurrently.
    fn retry_if_dir_removed<T>(
        &self,
        paths: &ArtifactPaths,
        created_dirs: &mut bool,
        mut op: impl FnMut() -> io::Result<T>,
    ) -> io::Result<T> {
        const ATTEMPTS: usize = 3;
        let mut attempt = 1;
        loop {
            match op() {
                Err(e)
                    if e.kind() == io::ErrorKind::NotFound
                        && attempt < ATTEMPTS
                        && !paths.shard_dir.is_dir() =>
                {
                    attempt += 1;
                    create_private_dir_all(&paths.shard_dir)?;
                    *created_dirs = true;
                }
                other => return other,
            }
        }
    }

    fn put(
        &self,
        ns: &str,
        hash: &str,
        body: UploadBody,
        meta_json: &[u8],
        mode: PutMode,
    ) -> AppResult<PutOutcome> {
        let paths = self.paths(ns, hash);
        if mode == PutMode::IfAbsent {
            // Cheap early exit (avoids writing the body); the publish step below is the atomic
            // decision. Corrupt entries are removed by the lookup and do not count as existing.
            let exists = self
                .lookup(ns, hash, false)
                .map_err(|e| io_error("Failed to check existing artifact", e))?
                .is_some();
            if exists {
                return Ok(PutOutcome::AlreadyExists);
            }
        }

        let payload_len = body.len();
        let meta_len = u32::try_from(meta_json.len())
            .map_err(|_| AppError::BadRequest("Artifact metadata too large".to_string()))?;
        let trailer = encode_trailer(payload_len, meta_len);
        let mut created_dirs = self
            .ensure_shard_dir(&paths)
            .map_err(|e| io_error("Failed to create storage directory", e))?;

        let outcome = match body {
            UploadBody::Bytes(data) => self
                .retry_if_dir_removed(&paths, &mut created_dirs, || {
                    let (file, temp) = create_temp_in(&paths.shard_dir)?;
                    write_artifact(file, &[&data, meta_json, &trailer])?;
                    // The shard dir now contains the temp file, so the janitor cannot remove it.
                    publish(temp, &paths.target, mode)
                })
                .map_err(|e| io_error("Failed to write artifact", e))?,
            UploadBody::File(spool) => {
                let mut file = stdfs::OpenOptions::new()
                    .append(true)
                    .open(spool.path())
                    .map_err(|e| io_error("Failed to open spooled upload", e))?;
                let on_disk = file
                    .metadata()
                    .map_err(|e| io_error("Failed to stat spooled upload", e))?
                    .len();
                if on_disk != payload_len {
                    return Err(AppError::Internal(format!(
                        "Spooled upload length mismatch: expected {payload_len} bytes, found {on_disk}"
                    )));
                }
                file.write_all(meta_json)
                    .and_then(|()| file.write_all(&trailer))
                    .and_then(|()| file.sync_all())
                    .map_err(|e| io_error("Failed to write artifact", e))?;
                drop(file);
                let spool_path = spool.path();
                // Returns the outcome and whether the spool file itself was renamed into place.
                let result = self.retry_if_dir_removed(&paths, &mut created_dirs, || {
                    let published = match mode {
                        PutMode::Overwrite => stdfs::rename(spool_path, &paths.target)
                            .map(|()| (PutOutcome::Stored, true)),
                        // Links a second name; the spool name is removed when `spool` drops.
                        PutMode::IfAbsent => {
                            link_if_absent(spool_path, &paths.target).map(|o| (o, false))
                        }
                    };
                    match published {
                        Err(e) if e.kind() == io::ErrorKind::CrossesDevices => {
                            copy_and_publish(spool_path, &paths, mode).map(|o| (o, false))
                        }
                        other => other,
                    }
                });
                let result = result.map(|(outcome, moved)| {
                    if moved {
                        // Release ownership without deleting the (now published) file.
                        let _ = spool.take_path();
                    }
                    outcome
                });
                result.map_err(|e| io_error("Failed to publish artifact", e))?
            }
        };

        if outcome == PutOutcome::Stored {
            sync_dir(&paths.shard_dir);
            if created_dirs {
                sync_dir(&paths.ns_dir);
                sync_dir(&self.base_dir);
            }
        }
        Ok(outcome)
    }

    fn delete(&self, ns: &str, hash: &str) -> io::Result<bool> {
        remove_ignore_missing(&self.paths(ns, hash).target)
    }

    /// Visits temp files and artifacts (unreadable entries are skipped).
    fn walk(&self, visit: &mut dyn FnMut(JanitorItem)) -> io::Result<()> {
        for ns_entry in stdfs::read_dir(&self.base_dir)? {
            let Ok(ns_entry) = ns_entry else {
                continue;
            };
            let Ok(file_type) = ns_entry.file_type() else {
                continue;
            };
            let ns_name = ns_entry.file_name();
            let ns_name = ns_name.to_string_lossy();
            // Skips `.spool` and other hidden/internal entries; never follows symlinks.
            if ns_name.starts_with('.') || !file_type.is_dir() {
                continue;
            }
            let Ok(shards) = stdfs::read_dir(ns_entry.path()) else {
                continue;
            };
            for shard_entry in shards {
                let Ok(shard_entry) = shard_entry else {
                    continue;
                };
                if !shard_entry.file_type().is_ok_and(|t| t.is_dir()) {
                    continue;
                }
                let Ok(files) = stdfs::read_dir(shard_entry.path()) else {
                    continue;
                };
                for file_entry in files {
                    let Ok(file_entry) = file_entry else {
                        continue;
                    };
                    let Ok(meta) = file_entry.metadata() else {
                        continue;
                    };
                    if !meta.file_type().is_file() {
                        continue;
                    }
                    let name = file_entry.file_name();
                    let Some(name) = name.to_str() else { continue };
                    let path = file_entry.path();
                    if name.starts_with('.') {
                        // Hidden files are never ours unless they are our temp files
                        // (artifact names cannot start with '.'); leave foreign
                        // files such as AppleDouble `._*` untouched.
                        if name.starts_with(TEMP_PREFIX) && name.ends_with(TEMP_SUFFIX) {
                            visit(JanitorItem::Temp { path, meta });
                        }
                    } else if name
                        .strip_suffix(ARTIFACT_EXTENSION)
                        .is_some_and(|s| s.ends_with('.'))
                    {
                        let size = meta.len();
                        visit(JanitorItem::Artifact { path, meta, size });
                    }
                }
            }
        }
        Ok(())
    }

    fn remove_artifact_item(path: &Path, meta: &Metadata) -> bool {
        match remove_if_unchanged(path, &file_id(meta)) {
            Ok(removed) => removed,
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "Failed to remove local artifact");
                false
            }
        }
    }

    fn cleanup(&self) -> io::Result<CleanupReport> {
        let _running = self.cleanup_lock.lock().unwrap_or_else(|p| p.into_inner());
        let opts = &self.options;
        let now = SystemTime::now();
        let mut report = CleanupReport::default();
        let mut total: u64 = 0;
        // Bytes per mtime minute, used to find the eviction cutoff without holding every entry.
        let mut buckets: BTreeMap<u64, u64> = BTreeMap::new();

        self.walk(&mut |item| match item {
            JanitorItem::Temp { path, meta } => {
                if age_of(&meta, now) >= opts.temp_max_age
                    && remove_if_unchanged(&path, &file_id(&meta)).unwrap_or(false)
                {
                    report.temp_files_removed += 1;
                }
            }
            JanitorItem::Artifact { path, meta, size } => {
                if let Some(max_age) = opts.max_age
                    && age_of(&meta, now) >= max_age
                    && Self::remove_artifact_item(&path, &meta)
                {
                    report.expired_removed += 1;
                    return;
                }
                total += size;
                report.remaining_entries += 1;
                *buckets.entry(minute_of(&meta)).or_default() += size;
            }
        })?;

        if let Some(max_bytes) = opts.max_bytes
            && total > max_bytes
        {
            let to_free = total - max_bytes;
            let mut acc = 0u64;
            let mut cutoff = u64::MAX;
            for (&minute, &bytes) in &buckets {
                acc += bytes;
                if acc >= to_free {
                    cutoff = minute;
                    break;
                }
            }
            // Entries in the cutoff minute are evicted oldest-first only as far as needed.
            let mut boundary: Vec<(SystemTime, PathBuf, Metadata, u64)> = Vec::new();
            self.walk(&mut |item| {
                let JanitorItem::Artifact { path, meta, size } = item else {
                    return;
                };
                let minute = minute_of(&meta);
                if minute < cutoff {
                    if Self::remove_artifact_item(&path, &meta) {
                        total = total.saturating_sub(size);
                        report.evicted_removed += 1;
                        report.remaining_entries = report.remaining_entries.saturating_sub(1);
                    }
                } else if minute == cutoff {
                    let mtime = meta.modified().unwrap_or(UNIX_EPOCH);
                    boundary.push((mtime, path, meta, size));
                }
            })?;
            boundary.sort_by_key(|entry| entry.0);
            for (_, path, meta, size) in boundary {
                if total <= max_bytes {
                    break;
                }
                if Self::remove_artifact_item(&path, &meta) {
                    total = total.saturating_sub(size);
                    report.evicted_removed += 1;
                    report.remaining_entries = report.remaining_entries.saturating_sub(1);
                }
            }
        }

        report.remaining_bytes = total;
        report.dirs_removed = self.remove_empty_dirs(now);
        Ok(report)
    }

    /// Removes empty shard directories idle for [`EMPTY_DIR_MIN_AGE`], and a namespace directory
    /// once its last shard directory was removed. `rmdir` only succeeds on empty directories; a
    /// put that loses the race recreates the directory (see `retry_if_dir_removed`).
    ///
    /// Only shard-shaped names (at most 4 bytes: two characters plus case escape) are removed,
    /// and a namespace directory only after one of its shards was, so a spool directory inside
    /// the storage tree (which has no subdirectories) is never removed.
    fn remove_empty_dirs(&self, now: SystemTime) -> u64 {
        const MAX_SHARD_NAME_LEN: usize = 4;
        let is_idle_dir = |entry: &stdfs::DirEntry| {
            entry.file_type().is_ok_and(|t| t.is_dir())
                && entry
                    .metadata()
                    .is_ok_and(|m| age_of(&m, now) >= EMPTY_DIR_MIN_AGE)
        };
        let mut removed = 0;
        let Ok(namespaces) = stdfs::read_dir(&self.base_dir) else {
            return 0;
        };
        for ns_entry in namespaces.flatten() {
            if ns_entry.file_name().to_string_lossy().starts_with('.')
                || !ns_entry.file_type().is_ok_and(|t| t.is_dir())
            {
                continue;
            }
            let ns_path = ns_entry.path();
            let mut removed_shards = 0;
            if let Ok(shards) = stdfs::read_dir(&ns_path) {
                for shard_entry in shards.flatten() {
                    let name = shard_entry.file_name();
                    let name = name.to_string_lossy();
                    if !name.starts_with('.')
                        && name.len() <= MAX_SHARD_NAME_LEN
                        && is_idle_dir(&shard_entry)
                        && stdfs::remove_dir(shard_entry.path()).is_ok()
                    {
                        removed_shards += 1;
                    }
                }
            }
            removed += removed_shards;
            if removed_shards > 0 && stdfs::remove_dir(&ns_path).is_ok() {
                removed += 1;
            }
        }
        removed
    }
}

/// Writes all parts to a fresh temp file and fsyncs it.
fn write_artifact(mut file: File, parts: &[&[u8]]) -> io::Result<()> {
    for part in parts {
        file.write_all(part)?;
    }
    file.sync_all()
}

/// Cross-filesystem fallback: copies the finished file at `src` into a temp file next to the
/// target, fsyncs it and publishes it.
fn copy_and_publish(src: &Path, paths: &ArtifactPaths, mode: PutMode) -> io::Result<PutOutcome> {
    let (mut file, temp) = create_temp_in(&paths.shard_dir)?;
    io::copy(&mut File::open(src)?, &mut file)?;
    file.sync_all()?;
    drop(file);
    publish(temp, &paths.target, mode)
}

#[async_trait]
impl Storage for LocalStorage {
    fn backend_name(&self) -> &'static str {
        "local"
    }

    async fn put(
        &self,
        namespace: &str,
        hash: &str,
        body: UploadBody,
        mut metadata: ArtifactMetadata,
        mode: PutMode,
    ) -> AppResult<PutOutcome> {
        let ns = sanitize_namespace(namespace)?.to_owned();
        let hash = validate_hash(hash)?.to_owned();
        metadata.size = body.len();
        let meta_json = serde_json::to_vec(&metadata)
            .map_err(|e| AppError::Internal(format!("Failed to serialize metadata: {e}")))?;
        if meta_json.len() > MAX_METADATA_LEN {
            return Err(AppError::BadRequest(
                "Artifact metadata too large".to_string(),
            ));
        }
        let inner = self.inner.clone();
        run_blocking(move || inner.put(&ns, &hash, body, &meta_json, mode)).await
    }

    async fn get(&self, namespace: &str, hash: &str) -> AppResult<Option<StoredArtifact>> {
        let ns = sanitize_namespace(namespace)?.to_owned();
        let hash = validate_hash(hash)?.to_owned();
        let inner = self.inner.clone();
        let found = run_blocking(move || {
            inner
                .lookup(&ns, &hash, true)
                .map_err(|e| io_error("Failed to read artifact", e))
        })
        .await?;
        let Some(Found { file, metadata }) = found else {
            return Ok(None);
        };
        // Header and payload come from the same open file, so they always belong together.
        let reader = tokio::fs::File::from_std(file).take(metadata.size);
        let stream = Box::pin(ReaderStream::with_capacity(reader, STREAM_BUFFER_SIZE));
        Ok(Some(StoredArtifact::from_stream(metadata, stream)))
    }

    async fn head(&self, namespace: &str, hash: &str) -> AppResult<Option<ArtifactMetadata>> {
        let ns = sanitize_namespace(namespace)?.to_owned();
        let hash = validate_hash(hash)?.to_owned();
        let inner = self.inner.clone();
        let found = run_blocking(move || {
            inner
                .lookup(&ns, &hash, false)
                .map_err(|e| io_error("Failed to read artifact metadata", e))
        })
        .await?;
        Ok(found.map(|f| f.metadata))
    }

    async fn delete(&self, namespace: &str, hash: &str) -> AppResult<bool> {
        let ns = sanitize_namespace(namespace)?.to_owned();
        let hash = validate_hash(hash)?.to_owned();
        let inner = self.inner.clone();
        run_blocking(move || {
            inner
                .delete(&ns, &hash)
                .map_err(|e| io_error("Failed to delete artifact", e))
        })
        .await
    }

    async fn health_check(&self) -> AppResult<()> {
        let meta = tokio::fs::metadata(&self.inner.base_dir)
            .await
            .map_err(|e| io_error("Local storage directory is inaccessible", e))?;
        if !meta.is_dir() {
            return Err(AppError::Internal(
                "Local storage path is not a directory".to_string(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escape_component_is_injective_for_case_variants() {
        assert_eq!(escape_component("abc-1_z"), "abc-1_z");
        assert!(matches!(escape_component("abc"), Cow::Borrowed(_)));
        assert_eq!(escape_component("Ab"), "ab^1");
        assert_eq!(escape_component("aB"), "ab^2");
        assert_eq!(escape_component("ABC"), "abc^7");
        assert_eq!(escape_component("AbC"), "abc^5");
        assert_eq!(escape_component("abcdE"), "abcde^01");
        assert_eq!(escape_component("Team.X"), "team.x^12");

        // Exhaustive over all case variants of a 6-letter word plus a few extras: all escaped
        // names are distinct even after case folding (case-insensitive filesystems).
        let mut seen = std::collections::HashSet::new();
        for mask in 0u32..64 {
            let variant: String = "abcdef"
                .chars()
                .enumerate()
                .map(|(i, c)| {
                    if mask & (1 << i) != 0 {
                        c.to_ascii_uppercase()
                    } else {
                        c
                    }
                })
                .collect();
            assert!(seen.insert(escape_component(&variant).to_ascii_lowercase()));
        }
        for extra in ["abcdef1", "abcde", "abcdef_1"] {
            assert!(seen.insert(escape_component(extra).to_ascii_lowercase()));
        }
    }

    #[test]
    fn trailer_roundtrip_and_validation() {
        let t = encode_trailer(10, 5);
        assert_eq!(decode_trailer(&t, 10 + 5 + TRAILER_LEN as u64), Ok((10, 5)));
        assert!(decode_trailer(&t, 10 + 5 + TRAILER_LEN as u64 - 1).is_err());
        let mut bad = t;
        bad[16] ^= 0xff;
        assert!(decode_trailer(&bad, 10 + 5 + TRAILER_LEN as u64).is_err());
        let mut bad_version = t;
        bad_version[12] = 9;
        assert!(decode_trailer(&bad_version, 10 + 5 + TRAILER_LEN as u64).is_err());
        let huge = encode_trailer(0, (MAX_METADATA_LEN + 1) as u32);
        assert!(decode_trailer(&huge, (MAX_METADATA_LEN + 1 + TRAILER_LEN) as u64).is_err());
        let overflow = encode_trailer(u64::MAX, 1);
        assert!(decode_trailer(&overflow, 0).is_err());
    }

    fn valid_artifact(body: &[u8]) -> Vec<u8> {
        let meta = serde_json::to_vec(&ArtifactMetadata::default()).unwrap();
        let mut raw = body.to_vec();
        raw.extend_from_slice(&meta);
        raw.extend_from_slice(&encode_trailer(body.len() as u64, meta.len() as u32));
        raw
    }

    #[test]
    fn cross_device_spool_is_copied_and_published() {
        // A real cross-device rename (EXDEV) cannot be forced in a unit test, so the fallback is
        // called directly.
        let src_dir = tempfile::tempdir().unwrap();
        let dst_dir = tempfile::tempdir().unwrap();
        let src = src_dir.path().join("spool");
        let content = valid_artifact(b"payload");
        stdfs::write(&src, &content).unwrap();
        let paths = ArtifactPaths {
            ns_dir: dst_dir.path().to_path_buf(),
            shard_dir: dst_dir.path().to_path_buf(),
            target: dst_dir.path().join("x.artifact"),
        };
        assert_eq!(
            copy_and_publish(&src, &paths, PutMode::Overwrite).unwrap(),
            PutOutcome::Stored
        );
        assert_eq!(stdfs::read(&paths.target).unwrap(), content);
        assert_eq!(
            copy_and_publish(&src, &paths, PutMode::IfAbsent).unwrap(),
            PutOutcome::AlreadyExists
        );
        let leftovers: Vec<_> = stdfs::read_dir(dst_dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(leftovers.len(), 1, "no temp files left: {leftovers:?}");
    }

    #[test]
    fn link_unsupported_matches_only_missing_link_support() {
        let unsupported = [io::ErrorKind::Unsupported, io::ErrorKind::PermissionDenied]
            .map(io::Error::from)
            .into_iter()
            .chain(
                NOT_SUPPORTED_ERRNOS
                    .iter()
                    .map(|&c| io::Error::from_raw_os_error(c)),
            );
        for e in unsupported {
            assert!(link_unsupported(&e), "{e:?}");
        }
        // EEXIST, ENOENT, EIO, EXDEV, ENOSPC: real outcomes/errors, never the fallback.
        for code in [17, 2, 5, 18, 28] {
            assert!(
                !link_unsupported(&io::Error::from_raw_os_error(code)),
                "{code}"
            );
        }
    }

    #[test]
    fn noclobber_fallback_without_hard_links_never_replaces() {
        let Some(&enotsup) = NOT_SUPPORTED_ERRNOS.first() else {
            return;
        };
        let no_links = |_: &Path, _: &Path| Err(io::Error::from_raw_os_error(enotsup));
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("t.artifact");
        let first = dir.path().join("first");
        stdfs::write(&first, b"first").unwrap();
        link_noclobber_with(&first, &target, no_links).unwrap();
        assert_eq!(stdfs::read(&target).unwrap(), b"first");
        assert!(!first.exists(), "fallback renames the source into place");

        let second = dir.path().join("second");
        stdfs::write(&second, b"second").unwrap();
        let err = link_noclobber_with(&second, &target, no_links).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(stdfs::read(&target).unwrap(), b"first");

        // Real errors are passed through, not turned into the fallback.
        let enospc = |_: &Path, _: &Path| Err(io::Error::from_raw_os_error(28));
        let fresh = dir.path().join("fresh");
        let err = link_noclobber_with(&second, &fresh, enospc).unwrap_err();
        assert_eq!(err.raw_os_error(), Some(28));
        assert!(!fresh.exists());
    }

    fn dir_names(dir: &Path) -> Vec<String> {
        let mut names: Vec<_> = stdfs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        names
    }

    #[test]
    fn remove_if_unchanged_keeps_file_replaced_after_the_check() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("t.artifact");
        stdfs::write(&target, b"old").unwrap();
        let old_id = file_id(&stdfs::symlink_metadata(&target).unwrap());

        // A fresh upload is renamed over the target between the identity check and the
        // removal: the fresh file must survive.
        let fresh = dir.path().join("fresh");
        stdfs::write(&fresh, b"fresh").unwrap();
        let removed = remove_if_unchanged_with(&target, &old_id, || {
            stdfs::rename(&fresh, &target).unwrap();
        })
        .unwrap();
        assert!(!removed);
        assert_eq!(stdfs::read(&target).unwrap(), b"fresh");
        assert_eq!(dir_names(dir.path()), ["t.artifact"], "no grave files left");

        // Unchanged file: removed, nothing left behind.
        let id = file_id(&stdfs::symlink_metadata(&target).unwrap());
        assert!(remove_if_unchanged(&target, &id).unwrap());
        assert!(dir_names(dir.path()).is_empty());
        assert!(!remove_if_unchanged(&target, &id).unwrap());
    }

    #[test]
    fn retry_if_dir_removed_recreates_the_shard_dir() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let base = tempfile::tempdir().unwrap();
        let store = rt
            .block_on(LocalStorage::new(base.path().join("store")))
            .unwrap();
        let paths = store.inner.paths("team", "abcdef");
        let mut created = store.inner.ensure_shard_dir(&paths).unwrap();
        assert!(created);
        created = false;
        let mut calls = 0;
        let outcome = store
            .inner
            .retry_if_dir_removed(&paths, &mut created, || {
                calls += 1;
                if calls == 1 {
                    // The janitor removes the (empty) shard dir right after `ensure_shard_dir`.
                    stdfs::remove_dir(&paths.shard_dir).unwrap();
                }
                let (mut file, temp) = create_temp_in(&paths.shard_dir)?;
                file.write_all(&valid_artifact(b"x"))?;
                drop(file);
                publish(temp, &paths.target, PutMode::Overwrite)
            })
            .unwrap();
        assert_eq!(outcome, PutOutcome::Stored);
        assert_eq!(calls, 2);
        assert!(created, "recreated dirs are reported for fsync");
        assert!(paths.target.is_file());

        // Other NotFound errors (e.g. a vanished source) are not retried.
        let mut calls = 0;
        let err = store
            .inner
            .retry_if_dir_removed(&paths, &mut created, || {
                calls += 1;
                stdfs::rename(base.path().join("missing"), &paths.target)
            })
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        assert_eq!(calls, 1);
    }
}
