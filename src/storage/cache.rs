//! Bounded, sharded in-memory LRU caches and the [`TieredStorage`] decorator.
//!
//! # Consistency model of [`TieredStorage`]
//!
//! The backend is authoritative; the caches only ever hold values that were observed in the
//! backend (or written to it) without a concurrent write to the same key:
//!
//! * Every key maps onto one of [`GENERATION_STRIPES`] generation counters. Writers (`put`,
//!   `delete`) bump the counter and invalidate both caches when they start **and** again when
//!   the backend call has finished (also when it fails). Conditional writes
//!   (`PutMode::IfAbsent`) only do the finish step, and skip it when the backend reports
//!   `AlreadyExists`, so repeated uploads of an existing artifact keep it cached.
//! * Writes run on their own task (`run_to_completion`): a client disconnect or request timeout
//!   does **not** abort a write that has reached the backend; it completes in the background and
//!   the caches are updated afterwards. Such writes are not tracked by graceful shutdown; at
//!   runtime shutdown they are dropped, which still invalidates the key.
//! * Readers capture the generation *before* calling the backend and only insert what they read
//!   if the generation is unchanged; the check and the insert happen under the shard lock, so a
//!   write that ends after the check is guaranteed to invalidate the inserted entry. Payload
//!   entries remember their generation and are only promoted into the metadata cache while it is
//!   still current.
//! * A writer only caches its own value when no other writer overlapped it (exactly one bump
//!   between its start and its end), so concurrent writers can never leave the caches pointing at
//!   a value the backend no longer holds.
//! * Entries expire after `CACHE_TTL_SECS` so out-of-band changes (other replicas, S3 lifecycle
//!   rules) are picked up eventually. Optional negative entries (`NEGATIVE_CACHE_TTL_SECS`)
//!   remember misses and are dropped by every write to the key.
//! * Concurrent cache-filling `GET`s / `HEAD`s of the same key share one backend request
//!   (single-flight); a request never joins a flight that started before a write to the key.

use std::{
    borrow::Borrow,
    collections::{HashMap, hash_map::RandomState},
    fmt,
    hash::{BuildHasher, BuildHasherDefault, Hash, Hasher},
    panic::AssertUnwindSafe,
    sync::{
        Arc, Mutex, MutexGuard, OnceLock, TryLockError,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{
    FutureExt, StreamExt,
    future::{BoxFuture, Shared, WeakShared},
    stream,
};
use lru::LruCache;

use crate::{
    error::{AppError, AppResult},
    models::ArtifactMetadata,
    storage::{
        ArtifactPayload, CacheStatsSnapshot, CacheTierStats, PutMode, PutOutcome, Storage,
        StoredArtifact, UploadBody, sanitize_namespace, validate_hash,
    },
    upload::UploadStream,
};

pub use super::CacheOptions;

/// Upper bound for the number of shards of a cache.
const MAX_SHARDS: usize = 16;
/// Each shard must be able to hold at least this many entries before the cache is split.
const MIN_ENTRIES_PER_SHARD: usize = 4;
/// Headroom on top of the largest payload for its metadata strings (tag, sha, ...), which are
/// accounted against the byte budget together with the payload. Larger than the biggest possible
/// metadata (600-byte tag + 3 x 256-byte headers).
pub const METADATA_SLACK_BYTES: usize = 4 * 1024;
/// Payloads up to this size are copied into an exactly sized buffer before they are cached, so a
/// small artifact can never pin a much larger (e.g. hyper read) buffer it was sliced from.
const OWNED_COPY_MAX_BYTES: usize = 512 * 1024;
/// How often an insert retries while the whole budget is reserved by concurrent inserts and
/// nothing is left to evict before it gives up with [`InsertOutcome::Busy`]. Reservations are
/// only held for the duration of one shard-locked insert, so a few rounds are enough.
const RESERVE_SPIN_ROUNDS: usize = 128;
/// Number of per-key generation counters (keys are striped onto them by hash).
const GENERATION_STRIPES: usize = 4096;
/// Stack buffer size used to build composite keys without an intermediate `String`.
const INLINE_KEY_BYTES: usize = 256;

#[inline]
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[inline]
fn is_expired(expires_at: Option<Instant>) -> bool {
    expires_at.is_some_and(|at| Instant::now() >= at)
}

#[inline]
fn floor_pow2(n: usize) -> usize {
    debug_assert!(n > 0);
    1 << (usize::BITS - 1 - n.leading_zeros())
}

/// Share of `total` assigned to shard `index` of `shards` (remainder spread over the first ones),
/// so the shard budgets add up to exactly `total`.
#[inline]
fn split_budget(total: usize, shards: usize, index: usize) -> usize {
    total / shards + usize::from(index < total % shards)
}

/// Copies small payloads into an owned, exactly sized allocation (see [`OWNED_COPY_MAX_BYTES`]).
#[inline]
fn own_small(data: Bytes) -> Bytes {
    if data.is_empty() || data.len() > OWNED_COPY_MAX_BYTES {
        data
    } else {
        Bytes::copy_from_slice(&data)
    }
}

static SHARD_HASHER: OnceLock<RandomState> = OnceLock::new();

#[inline]
fn shard_hasher() -> &'static RandomState {
    SHARD_HASHER.get_or_init(RandomState::new)
}

// ---------------------------------------------------------------------------------------------
// Keys
// ---------------------------------------------------------------------------------------------

/// Hash of a `(namespace, hash)` key, computed once per operation and reused for shard
/// selection, the generation stripe and the in-shard hash map.
///
/// Uses a per-process randomly seeded SipHash, so attackers cannot precompute keys that collide
/// on one shard or one hash bucket.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct KeyHash(u64);

impl KeyHash {
    #[inline]
    fn of(namespace: &str, hash: &str) -> Self {
        Self(shard_hasher().hash_one((namespace, hash)))
    }

    /// Shard index from the middle bits: the in-shard map uses the low bits (bucket index) and the
    /// top 7 bits (control tag), which stay fully random within a shard.
    #[inline]
    fn shard(self, num_shards: usize) -> usize {
        debug_assert!(num_shards.is_power_of_two());
        ((self.0 >> 32) as usize) & (num_shards - 1)
    }

    #[inline]
    fn stripe(self) -> usize {
        ((self.0 >> 44) as usize) & (GENERATION_STRIPES - 1)
    }
}

/// Borrowed composite key with its precomputed hash.
#[derive(Clone, Copy, Debug)]
struct KeyRef<'a> {
    kh: KeyHash,
    namespace: &'a str,
    hash: &'a str,
}

impl<'a> KeyRef<'a> {
    #[inline]
    fn new(namespace: &'a str, hash: &'a str) -> Self {
        Self {
            kh: KeyHash::of(namespace, hash),
            namespace,
            hash,
        }
    }
}

/// Enables zero-allocation lookups of [`StoredKey`] entries by [`KeyRef`].
trait KeyPair {
    fn prehash(&self) -> u64;
    fn pair(&self) -> (&str, &str);
}

impl KeyPair for KeyRef<'_> {
    #[inline]
    fn prehash(&self) -> u64 {
        self.kh.0
    }

    #[inline]
    fn pair(&self) -> (&str, &str) {
        (self.namespace, self.hash)
    }
}

impl PartialEq for dyn KeyPair + '_ {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.prehash() == other.prehash() && self.pair() == other.pair()
    }
}

impl Eq for dyn KeyPair + '_ {}

impl Hash for dyn KeyPair + '_ {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.prehash());
    }
}

/// Owned composite key: one shared allocation holding `namespace ++ hash` plus the precomputed
/// hash. Cloning (e.g. for the single-flight table) only bumps a reference count.
#[derive(Clone, Debug)]
struct StoredKey {
    prehash: u64,
    ns_len: usize,
    joined: Arc<str>,
}

impl StoredKey {
    fn new(key: KeyRef<'_>) -> Self {
        let (ns, hash) = (key.namespace, key.hash);
        let total = ns.len() + hash.len();
        let joined: Arc<str> = if total <= INLINE_KEY_BYTES {
            let mut buf = [0u8; INLINE_KEY_BYTES];
            buf[..ns.len()].copy_from_slice(ns.as_bytes());
            buf[ns.len()..total].copy_from_slice(hash.as_bytes());
            // The concatenation of two valid UTF-8 strings is valid UTF-8.
            Arc::from(std::str::from_utf8(&buf[..total]).expect("concatenated keys are UTF-8"))
        } else {
            Arc::from(format!("{ns}{hash}"))
        };
        Self {
            prehash: key.kh.0,
            ns_len: ns.len(),
            joined,
        }
    }

    #[inline]
    fn as_key_ref(&self) -> KeyRef<'_> {
        let (namespace, hash) = self.joined.split_at(self.ns_len);
        KeyRef {
            kh: KeyHash(self.prehash),
            namespace,
            hash,
        }
    }
}

impl KeyPair for StoredKey {
    #[inline]
    fn prehash(&self) -> u64 {
        self.prehash
    }

    #[inline]
    fn pair(&self) -> (&str, &str) {
        self.joined.split_at(self.ns_len)
    }
}

impl PartialEq for StoredKey {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.prehash == other.prehash && self.ns_len == other.ns_len && self.joined == other.joined
    }
}

impl Eq for StoredKey {}

impl Hash for StoredKey {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.prehash);
    }
}

impl<'a> Borrow<dyn KeyPair + 'a> for StoredKey {
    #[inline]
    fn borrow(&self) -> &(dyn KeyPair + 'a) {
        self
    }
}

/// Pass-through hasher: keys hash themselves once with [`KeyHash`]; the map reuses that value.
#[derive(Default)]
struct PrehashedHasher(u64);

impl Hasher for PrehashedHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline]
    fn write_u64(&mut self, n: u64) {
        self.0 = n;
    }

    fn write(&mut self, bytes: &[u8]) {
        // Not used by our key types; FNV-1a keeps the hasher correct for any other input.
        for &b in bytes {
            self.0 = (self.0 ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
}

type PrehashedState = BuildHasherDefault<PrehashedHasher>;

type Lru<V> = LruCache<StoredKey, V, PrehashedState>;

#[inline]
fn new_lru<V>() -> Lru<V> {
    LruCache::unbounded_with_hasher(PrehashedState::default())
}

#[inline]
fn dk<'a, 'k>(key: &'a KeyRef<'k>) -> &'a (dyn KeyPair + 'k) {
    key
}

// ---------------------------------------------------------------------------------------------
// Shard slots and statistics
// ---------------------------------------------------------------------------------------------

/// Per-shard counters. They live next to the shard lock in a cache-line padded slot, so updates
/// (made while or right after holding that shard's lock) never contend with other shards.
#[derive(Default)]
struct SlotStats {
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    expirations: AtomicU64,
    bytes: AtomicU64,
    entries: AtomicU64,
}

/// Published LRU-tail access time of a payload shard (see [`Budget::insert_tick`]; [`EMPTY_SHARD`]
/// when the shard is empty). Written under the shard lock, read without it, so eviction can pick
/// the globally oldest shard tail without locking every shard.
struct TailClock(AtomicU64);

const EMPTY_SHARD: u64 = u64::MAX;

impl Default for TailClock {
    fn default() -> Self {
        Self(AtomicU64::new(EMPTY_SHARD))
    }
}

impl SlotStats {
    #[inline]
    fn add(counter: &AtomicU64, n: u64) {
        if n > 0 {
            counter.fetch_add(n, Ordering::Relaxed);
        }
    }

    #[inline]
    fn publish(&self, bytes: usize, entries: usize) {
        self.bytes.store(bytes as u64, Ordering::Relaxed);
        self.entries.store(entries as u64, Ordering::Relaxed);
    }
}

/// One shard plus its counters, aligned to 128 bytes (two cache lines, covering adjacent-line
/// prefetching) so neighbouring shards never share a cache line.
#[repr(align(128))]
struct Slot<S> {
    shard: Mutex<S>,
    stats: SlotStats,
    /// Only maintained for payload shards.
    tail: TailClock,
}

impl<S> Slot<S> {
    fn new(shard: S) -> Self {
        Self {
            shard: Mutex::new(shard),
            stats: SlotStats::default(),
            tail: TailClock::default(),
        }
    }
}

fn sum_stat<S>(slots: &[Slot<S>], field: impl Fn(&SlotStats) -> &AtomicU64) -> u64 {
    slots
        .iter()
        .map(|slot| field(&slot.stats).load(Ordering::Relaxed))
        .sum()
}

/// Which lookup outcomes are recorded in the hit/miss counters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Count {
    /// Internal re-probes: record nothing.
    Off,
    /// Synchronous hot-path probes (`get_hot`/`head_hot`): a miss is followed by the regular
    /// lookup, which records it, so only hits are counted here.
    Hits,
    /// Authoritative lookups: record hits and misses.
    All,
}

impl Count {
    #[inline]
    fn hits(self) -> bool {
        self != Count::Off
    }

    #[inline]
    fn record(self, stats: &SlotStats, hit: bool) {
        match (self, hit) {
            (Count::Off, _) | (Count::Hits, false) => {}
            (_, true) => SlotStats::add(&stats.hits, 1),
            (Count::All, false) => SlotStats::add(&stats.misses, 1),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Payload cache
// ---------------------------------------------------------------------------------------------

struct BytesEntry {
    data: Bytes,
    metadata: ArtifactMetadata,
    size_bytes: usize,
    expires_at: Option<Instant>,
    /// Write generation the entry was cached at (`None` for untracked inserts via `put`). Used to
    /// decide whether the entry may be promoted into the metadata cache.
    generation: Option<u64>,
    /// Logical time of the last access (see [`Budget::insert_tick`]); the eviction victim is the
    /// shard tail with the oldest access (global LRU).
    touched: u64,
}

/// Global byte and entry budget shared by all shards of a payload cache.
///
/// Space is reserved *before* an entry is inserted and released when an entry is removed, so the
/// resident bytes and entries never exceed the limits even transiently, while the shards still
/// lock independently. Because the budget is global, a single item may use the whole budget no
/// matter how many shards there are.
#[repr(align(128))]
struct Budget {
    bytes: AtomicUsize,
    entries: AtomicUsize,
    /// Logical LRU clock, advanced by every insert (it shares the cache line inserts already
    /// write). Hits only read it, so they need neither a clock syscall nor a shared write.
    clock: AtomicU64,
    max_bytes: usize,
    max_entries: usize,
}

impl Budget {
    fn new(max_bytes: usize, max_entries: usize) -> Self {
        Self {
            bytes: AtomicUsize::new(0),
            entries: AtomicUsize::new(0),
            clock: AtomicU64::new(0),
            max_bytes,
            max_entries,
        }
    }

    /// Access time of a newly inserted entry: even and strictly increasing.
    #[inline]
    fn insert_tick(&self) -> u64 {
        self.clock.fetch_add(2, Ordering::Relaxed) + 2
    }

    /// Access time of a hit: odd, i.e. more recent than every insert so far (hits between the
    /// same two inserts tie across shards; within a shard the list order stays exact).
    #[inline]
    fn touch_tick(&self) -> u64 {
        self.clock.load(Ordering::Relaxed) + 1
    }

    /// Atomically reserves `bytes` and `entries` if both fit; reserves nothing otherwise.
    fn try_reserve(&self, bytes: usize, entries: usize) -> bool {
        // A CAS loop rather than `fetch_update`, which newer Rust deprecates in favour of
        // `try_update` (not available on the MSRV).
        fn add(counter: &AtomicUsize, n: usize, max: usize) -> bool {
            if n == 0 {
                return true;
            }
            let mut cur = counter.load(Ordering::Acquire);
            loop {
                let Some(next) = cur.checked_add(n).filter(|&next| next <= max) else {
                    return false;
                };
                match counter.compare_exchange_weak(cur, next, Ordering::AcqRel, Ordering::Acquire)
                {
                    Ok(_) => return true,
                    Err(actual) => cur = actual,
                }
            }
        }
        if !add(&self.bytes, bytes, self.max_bytes) {
            return false;
        }
        if !add(&self.entries, entries, self.max_entries) {
            self.release(bytes, 0);
            return false;
        }
        true
    }

    #[inline]
    fn release(&self, bytes: usize, entries: usize) {
        if bytes > 0 {
            self.bytes.fetch_sub(bytes, Ordering::AcqRel);
        }
        if entries > 0 {
            self.entries.fetch_sub(entries, Ordering::AcqRel);
        }
    }
}

struct BytesShard {
    lru: Lru<BytesEntry>,
}

impl BytesShard {
    fn new() -> Self {
        Self { lru: new_lru() }
    }

    /// Reads the live entry for `key` (marked as most recently used); expired entries are
    /// dropped and their budget released.
    #[inline]
    fn live<R>(
        &mut self,
        key: KeyRef<'_>,
        stats: &SlotStats,
        budget: &Budget,
        read: impl FnOnce(&BytesEntry) -> R,
    ) -> Option<R> {
        let entry = self.lru.get_mut(dk(&key))?;
        if !entry.expires_at.is_some_and(|at| Instant::now() >= at) {
            entry.touched = budget.touch_tick();
            return Some(read(entry));
        }
        let expired = self.lru.pop(dk(&key)).expect("present");
        budget.release(expired.size_bytes, 1);
        SlotStats::add(&stats.expirations, 1);
        None
    }

    fn remove(&mut self, key: KeyRef<'_>, budget: &Budget) -> bool {
        let Some(prev) = self.lru.pop(dk(&key)) else {
            return false;
        };
        budget.release(prev.size_bytes, 1);
        true
    }

    /// [`TailClock`] value for the current LRU tail.
    #[inline]
    fn tail_clock(&self) -> u64 {
        self.lru
            .peek_lru()
            .map_or(EMPTY_SHARD, |(_, entry)| entry.touched)
    }
}

/// Locked payload shard. Publishes the shard's LRU-tail clock when the lock is released, so
/// [`BoundedBytesCache::evict_lru`] sees every change to the tail (touch, insert, removal).
struct ShardGuard<'a> {
    shard: MutexGuard<'a, BytesShard>,
    tail: &'a TailClock,
}

impl std::ops::Deref for ShardGuard<'_> {
    type Target = BytesShard;

    #[inline]
    fn deref(&self) -> &BytesShard {
        &self.shard
    }
}

impl std::ops::DerefMut for ShardGuard<'_> {
    #[inline]
    fn deref_mut(&mut self) -> &mut BytesShard {
        &mut self.shard
    }
}

impl Drop for ShardGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        self.tail
            .0
            .store(self.shard.tail_clock(), Ordering::Relaxed);
    }
}

#[derive(Clone, Copy, Debug)]
struct InsertOptions {
    expires_at: Option<Instant>,
    generation: Option<u64>,
    /// Copy small payloads into an exactly sized buffer (see [`own_small`]). Callers that
    /// assembled the buffer themselves pass `false`.
    copy_small: bool,
}

impl InsertOptions {
    fn new(expires_at: Option<Instant>) -> Self {
        Self {
            expires_at,
            generation: None,
            copy_small: true,
        }
    }
}

/// Result of an insert into a [`BoundedBytesCache`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InsertOutcome {
    Inserted,
    /// The item exceeds the item limit (or the cache is disabled).
    Rejected,
    /// The caller's precondition failed (a concurrent write happened); nothing changed.
    Skipped,
    /// The budget is held by concurrent in-flight inserts and nothing is left to evict.
    Busy,
}

/// Thread-safe, sharded, strictly byte-bounded and entry-bounded LRU cache for artifact payloads.
///
/// The byte and entry limits are global (see [`Budget`]); shards only partition the lookup
/// locks. Eviction removes the least recently used entry across all shards.
pub struct BoundedBytesCache {
    slots: Box<[Slot<BytesShard>]>,
    budget: Budget,
    max_bytes: usize,
    max_entries: usize,
    max_item_bytes: usize,
    ttl: Option<Duration>,
}

impl fmt::Debug for BoundedBytesCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedBytesCache")
            .field("shards", &self.slots.len())
            .field("max_bytes", &self.max_bytes)
            .field("max_entries", &self.max_entries)
            .field("max_item_bytes", &self.max_item_bytes)
            .field("ttl", &self.ttl)
            .field("current_bytes", &self.current_bytes())
            .field("current_entries", &self.current_entries())
            .finish()
    }
}

/// Number of payload shards: a power of two ≤ [`MAX_SHARDS`] with at least
/// [`MIN_ENTRIES_PER_SHARD`] entries of budget per shard. Independent of the item size, since the
/// byte budget is shared by all shards.
fn payload_shard_count(max_entries: usize) -> usize {
    floor_pow2((max_entries / MIN_ENTRIES_PER_SHARD).clamp(1, MAX_SHARDS))
}

/// Largest admissible payload: `max_item_bytes`, but small enough that an item of exactly that
/// size plus typical metadata strings (up to [`METADATA_SLACK_BYTES`], or 1/16 of tiny budgets)
/// still fits the byte budget.
fn effective_item_limit(max_bytes: usize, max_item_bytes: usize) -> usize {
    let slack = METADATA_SLACK_BYTES.min(max_bytes / 16);
    max_item_bytes.min(max_bytes - slack)
}

impl BoundedBytesCache {
    /// Creates a cache without entry expiry.
    pub fn new(max_bytes: usize, max_entries: usize, max_item_bytes: usize) -> Self {
        Self::with_ttl(max_bytes, max_entries, max_item_bytes, None)
    }

    /// Creates a cache whose entries expire `ttl` after insertion (`None` = never).
    ///
    /// `max_item_bytes` is clamped to [`effective_item_limit`], with a warning whenever that
    /// lowers it: e.g. `HOT_CACHE_MAX_ITEM_BYTES` = `MEMORY_CACHE_MAX_BYTES` = 1 MiB gives an
    /// effective limit of 1 MiB - 4 KiB.
    pub fn with_ttl(
        max_bytes: usize,
        max_entries: usize,
        max_item_bytes: usize,
        ttl: Option<Duration>,
    ) -> Self {
        let _ = shard_hasher();
        let enabled = max_bytes > 0 && max_entries > 0;
        let effective_max_item_bytes = effective_item_limit(max_bytes, max_item_bytes);
        if enabled && effective_max_item_bytes < max_item_bytes {
            tracing::warn!(
                configured_max_item_bytes = max_item_bytes,
                effective_max_item_bytes,
                max_bytes,
                "Cache item limit does not fit the cache byte budget (minus metadata headroom); clamped"
            );
        }
        let max_item_bytes = effective_max_item_bytes;
        let num_shards = if enabled {
            payload_shard_count(max_entries)
        } else {
            1
        };
        let slots = (0..num_shards)
            .map(|_| Slot::new(BytesShard::new()))
            .collect();
        Self {
            slots,
            budget: Budget::new(max_bytes, max_entries),
            max_bytes,
            max_entries,
            max_item_bytes,
            ttl,
        }
    }

    #[inline]
    fn enabled(&self) -> bool {
        self.max_bytes > 0 && self.max_entries > 0
    }

    #[inline]
    fn slot(&self, kh: KeyHash) -> &Slot<BytesShard> {
        &self.slots[kh.shard(self.slots.len())]
    }

    #[inline]
    fn lock_slot<'a>(&'a self, slot: &'a Slot<BytesShard>) -> ShardGuard<'a> {
        ShardGuard {
            shard: lock(&slot.shard),
            tail: &slot.tail,
        }
    }

    /// Like [`lock_slot`](Self::lock_slot), but `None` if the shard is currently locked.
    #[inline]
    fn try_lock_slot<'a>(&'a self, slot: &'a Slot<BytesShard>) -> Option<ShardGuard<'a>> {
        let shard = match slot.shard.try_lock() {
            Ok(guard) => guard,
            Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(TryLockError::WouldBlock) => return None,
        };
        Some(ShardGuard {
            shard,
            tail: &slot.tail,
        })
    }

    /// Expiry instant for an entry inserted now.
    #[inline]
    fn expiry(&self) -> Option<Instant> {
        self.ttl.map(|ttl| Instant::now() + ttl)
    }

    fn lookup(&self, key: KeyRef<'_>, count: Count) -> Option<(Bytes, ArtifactMetadata)> {
        if !self.enabled() {
            return None;
        }
        let slot = self.slot(key.kh);
        let mut shard = self.lock_slot(slot);
        let found = shard.live(key, &slot.stats, &self.budget, |entry| {
            (entry.data.clone(), entry.metadata.clone())
        });
        drop(shard);
        count.record(&slot.stats, found.is_some());
        found
    }

    /// Metadata, expiry and cache generation of a live entry (uncounted).
    fn peek_metadata(
        &self,
        key: KeyRef<'_>,
    ) -> Option<(ArtifactMetadata, Option<Instant>, Option<u64>)> {
        if !self.enabled() {
            return None;
        }
        let slot = self.slot(key.kh);
        let mut shard = self.lock_slot(slot);
        shard.live(key, &slot.stats, &self.budget, |entry| {
            (entry.metadata.clone(), entry.expires_at, entry.generation)
        })
    }

    /// Locks the shard with the oldest published LRU tail, or the next-oldest one that is not
    /// locked right now if that one is busy (all busy: waits for the oldest). Returns the tail
    /// clock seen when choosing, the shard index and the guard; `None` if every shard is empty.
    fn lock_victim(&self) -> Option<(u64, usize, ShardGuard<'_>)> {
        let clock = |i: usize| self.slots[i].tail.0.load(Ordering::Relaxed);
        let (seen, oldest) = (0..self.slots.len())
            .map(|i| (clock(i), i))
            .min()
            .filter(|&(at, _)| at != EMPTY_SHARD)?;
        if let Some(guard) = self.try_lock_slot(&self.slots[oldest]) {
            return Some((seen, oldest, guard));
        }
        let mut tails = [(EMPTY_SHARD, 0usize); MAX_SHARDS];
        for (i, tail) in tails.iter_mut().enumerate().take(self.slots.len()) {
            *tail = (clock(i), i);
        }
        let tails = &mut tails[..self.slots.len()];
        tails.sort_unstable();
        tails
            .iter()
            .take_while(|&&(at, _)| at != EMPTY_SHARD)
            .filter(|&&(_, i)| i != oldest)
            .find_map(|&(at, i)| self.try_lock_slot(&self.slots[i]).map(|g| (at, i, g)))
            .or_else(|| Some((seen, oldest, self.lock_slot(&self.slots[oldest]))))
    }

    /// Evicts the least recently used entry across all shards. Returns `false` when every shard
    /// is empty.
    ///
    /// The victim shard is chosen from the published tail clocks without taking any lock, and
    /// only the victim shard is locked. If its tail was touched in the meantime the choice is
    /// re-made (a bounded number of times). When the oldest shard is locked by someone else the
    /// next-oldest unlocked one is used instead, so concurrent inserters do not all queue on the
    /// same victim shard: without contention eviction is exact global LRU, under heavy churn it
    /// is approximately LRU. Never holds more than one lock.
    fn evict_lru(&self) -> bool {
        let n = self.slots.len();
        for attempt in 0..=n {
            let Some((seen, i, mut shard)) = self.lock_victim() else {
                break;
            };
            let current = shard.tail_clock();
            if current == EMPTY_SHARD || (current != seen && attempt < n) {
                // Stale published clock; the guard republishes it on drop.
                continue;
            }
            if let Some((_, old)) = shard.lru.pop_lru() {
                self.budget.release(old.size_bytes, 1);
                SlotStats::add(&self.slots[i].stats.evictions, 1);
                return true;
            }
        }
        // Published clocks say every shard is empty (or kept changing): confirm under the locks.
        for slot in self.slots.iter() {
            let mut shard = self.lock_slot(slot);
            if let Some((_, old)) = shard.lru.pop_lru() {
                self.budget.release(old.size_bytes, 1);
                SlotStats::add(&slot.stats.evictions, 1);
                return true;
            }
        }
        false
    }

    /// Inserts `data` if `precondition` holds when evaluated under the shard lock.
    ///
    /// Space is reserved under the key's shard lock, immediately before the entry is inserted:
    /// an overwrite only needs to reserve the size *difference* to the entry it replaces. When
    /// the budget is full, LRU entries are evicted one at a time (without holding this shard's
    /// lock) and the reservation is retried, so an overwrite evicts only as much as it needs.
    fn insert(
        &self,
        key: KeyRef<'_>,
        data: Bytes,
        metadata: ArtifactMetadata,
        opts: InsertOptions,
        precondition: impl Fn() -> bool,
    ) -> InsertOutcome {
        if !self.enabled() {
            return InsertOutcome::Rejected;
        }
        let item_bytes = data.len().saturating_add(metadata.allocated_bytes());
        let slot = self.slot(key.kh);

        if data.len() > self.max_item_bytes || item_bytes > self.max_bytes {
            let mut shard = self.lock_slot(slot);
            if !precondition() {
                return InsertOutcome::Skipped;
            }
            // The key now has a value we cannot hold, so the old one must go.
            shard.remove(key, &self.budget);
            return InsertOutcome::Rejected;
        }

        // Copy before taking the lock.
        let data = if opts.copy_small {
            own_small(data)
        } else {
            data
        };
        let mut entry = BytesEntry {
            data,
            metadata,
            size_bytes: item_bytes,
            expires_at: opts.expires_at,
            generation: opts.generation,
            touched: 0,
        };

        let mut idle_rounds = 0;
        loop {
            {
                let mut shard = self.lock_slot(slot);
                if !precondition() {
                    return InsertOutcome::Skipped;
                }
                let prev_bytes = shard.live(key, &slot.stats, &self.budget, |existing| {
                    existing.size_bytes
                });
                let (need_bytes, need_entries) = match prev_bytes {
                    Some(prev) => (item_bytes.saturating_sub(prev), 0),
                    None => (item_bytes, 1),
                };
                if self.budget.try_reserve(need_bytes, need_entries) {
                    if let Some(prev) = prev_bytes {
                        self.budget.release(prev.saturating_sub(item_bytes), 0);
                    }
                    entry.touched = self.budget.insert_tick();
                    shard.lru.put(StoredKey::new(key), entry);
                    return InsertOutcome::Inserted;
                }
            }
            if self.evict_lru() {
                idle_rounds = 0;
                continue;
            }
            // Nothing resident although the budget is exhausted: the budget is held by inserts
            // that reserved under another shard's lock and are inserting right now.
            idle_rounds += 1;
            if idle_rounds > RESERVE_SPIN_ROUNDS {
                return InsertOutcome::Busy;
            }
            std::thread::yield_now();
        }
    }

    fn remove_key(&self, key: KeyRef<'_>) -> bool {
        self.remove_key_if(key, || true)
    }

    /// Removes the entry for `key` if `precondition` holds when evaluated under the shard lock.
    fn remove_key_if(&self, key: KeyRef<'_>, precondition: impl FnOnce() -> bool) -> bool {
        if !self.enabled() {
            return false;
        }
        let slot = self.slot(key.kh);
        let mut shard = self.lock_slot(slot);
        precondition() && shard.remove(key, &self.budget)
    }

    /// Looks up a payload, recording a hit or a miss.
    #[inline]
    pub fn get(&self, namespace: &str, hash: &str) -> Option<(Bytes, ArtifactMetadata)> {
        self.lookup(KeyRef::new(namespace, hash), Count::All)
    }

    /// Returns the metadata of a cached payload without recording a hit or miss.
    #[inline]
    pub fn head(&self, namespace: &str, hash: &str) -> Option<ArtifactMetadata> {
        self.peek_metadata(KeyRef::new(namespace, hash))
            .map(|(meta, _, _)| meta)
    }

    /// Caches a payload. When it cannot be cached (larger than the item limit) any existing
    /// entry for the key is removed, so a stale payload is never served afterwards.
    pub fn put(
        &self,
        namespace: &str,
        hash: &str,
        data: Bytes,
        metadata: ArtifactMetadata,
    ) -> bool {
        let opts = InsertOptions::new(self.expiry());
        self.insert(KeyRef::new(namespace, hash), data, metadata, opts, || true)
            == InsertOutcome::Inserted
    }

    pub fn remove(&self, namespace: &str, hash: &str) -> bool {
        self.remove_key(KeyRef::new(namespace, hash))
    }

    /// Test hook: holds `bytes` of the budget like an insert in flight on another shard would.
    #[cfg(test)]
    pub(crate) fn reserve_for_test(&self, bytes: usize) -> bool {
        self.budget.try_reserve(bytes, 0)
    }

    /// Test hook: releases a reservation taken with [`reserve_for_test`](Self::reserve_for_test).
    #[cfg(test)]
    pub(crate) fn release_for_test(&self, bytes: usize) {
        self.budget.release(bytes, 0);
    }

    /// Bytes accounted against the budget (never above [`max_bytes`](Self::max_bytes); equal to
    /// the resident bytes whenever no insert is in flight).
    pub fn current_bytes(&self) -> usize {
        self.budget.bytes.load(Ordering::Acquire)
    }

    /// Entries accounted against the budget (see [`current_bytes`](Self::current_bytes)).
    pub fn current_entries(&self) -> usize {
        self.budget.entries.load(Ordering::Acquire)
    }

    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    pub fn max_entries(&self) -> usize {
        self.max_entries
    }

    /// Largest payload admitted (after clamping to the byte budget).
    pub fn max_item_bytes(&self) -> usize {
        self.max_item_bytes
    }

    /// Number of shards the lookup locks are split across.
    pub fn num_shards(&self) -> usize {
        self.slots.len()
    }

    pub fn hits(&self) -> u64 {
        sum_stat(&self.slots, |s| &s.hits)
    }

    pub fn misses(&self) -> u64 {
        sum_stat(&self.slots, |s| &s.misses)
    }

    /// Entries evicted to make room (capacity evictions; expiry is counted separately).
    pub fn evictions(&self) -> u64 {
        sum_stat(&self.slots, |s| &s.evictions)
    }

    /// Entries dropped because their TTL elapsed.
    pub fn expirations(&self) -> u64 {
        sum_stat(&self.slots, |s| &s.expirations)
    }

    pub fn stats(&self) -> CacheTierStats {
        CacheTierStats {
            hits: self.hits(),
            misses: self.misses(),
            evictions: self.evictions(),
            expirations: self.expirations(),
            bytes: self.current_bytes() as u64,
            entries: self.current_entries() as u64,
            max_bytes: self.max_bytes as u64,
            max_entries: self.max_entries as u64,
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Metadata cache (with negative entries)
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug)]
enum MetaValue {
    Present(ArtifactMetadata),
    /// Negative entry: the backend reported the artifact as missing.
    Absent,
}

impl MetaValue {
    fn heap_bytes(&self) -> usize {
        match self {
            MetaValue::Present(meta) => meta.allocated_bytes(),
            MetaValue::Absent => 0,
        }
    }
}

struct MetaEntry {
    value: MetaValue,
    expires_at: Option<Instant>,
    size_bytes: usize,
}

struct MetaShard {
    lru: Lru<MetaEntry>,
    current_bytes: usize,
    max_entries: usize,
}

impl MetaShard {
    fn new(max_entries: usize) -> Self {
        Self {
            lru: new_lru(),
            current_bytes: 0,
            max_entries,
        }
    }

    #[inline]
    fn publish(&self, stats: &SlotStats) {
        stats.publish(self.current_bytes, self.lru.len());
    }

    fn lookup(&mut self, key: KeyRef<'_>, stats: &SlotStats) -> Option<MetaValue> {
        let entry = self.lru.get(dk(&key))?;
        if !is_expired(entry.expires_at) {
            return Some(entry.value.clone());
        }
        let expired = self.lru.pop(dk(&key)).expect("present");
        self.current_bytes -= expired.size_bytes;
        SlotStats::add(&stats.expirations, 1);
        self.publish(stats);
        None
    }

    fn insert(&mut self, key: KeyRef<'_>, entry: MetaEntry, stats: &SlotStats) {
        if self.max_entries == 0 {
            return;
        }
        if let Some(slot) = self.lru.get_mut(dk(&key)) {
            self.current_bytes = self.current_bytes - slot.size_bytes + entry.size_bytes;
            *slot = entry;
        } else {
            let mut evicted = 0u64;
            while self.lru.len() >= self.max_entries {
                let Some((_, old)) = self.lru.pop_lru() else {
                    break;
                };
                self.current_bytes -= old.size_bytes;
                evicted += 1;
            }
            self.current_bytes += entry.size_bytes;
            self.lru.push(StoredKey::new(key), entry);
            SlotStats::add(&stats.evictions, evicted);
        }
        self.publish(stats);
    }

    fn remove(&mut self, key: KeyRef<'_>, stats: &SlotStats) {
        if let Some(prev) = self.lru.pop(dk(&key)) {
            self.current_bytes -= prev.size_bytes;
            self.publish(stats);
        }
    }

    /// Removes the entry for `key` only if it is a negative (`Absent`) entry.
    fn remove_negative(&mut self, key: KeyRef<'_>, stats: &SlotStats) {
        if self
            .lru
            .peek(dk(&key))
            .is_some_and(|e| matches!(e.value, MetaValue::Absent))
        {
            let prev = self.lru.pop(dk(&key)).expect("present");
            self.current_bytes -= prev.size_bytes;
            self.publish(stats);
        }
    }
}

/// Result of a metadata cache probe.
#[derive(Clone, Debug)]
enum MetaLookup {
    Hit(ArtifactMetadata),
    /// A live negative entry: the artifact is known to be missing.
    Negative,
    Miss,
}

/// Bounded in-memory metadata LRU cache for accelerating `HEAD` and batch `POST /v8/artifacts`
/// queries. Optionally also remembers misses (negative entries) for `negative_ttl`.
pub struct BoundedMetadataCache {
    slots: Box<[Slot<MetaShard>]>,
    max_entries: usize,
    ttl: Option<Duration>,
    negative_ttl: Option<Duration>,
}

impl fmt::Debug for BoundedMetadataCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundedMetadataCache")
            .field("shards", &self.slots.len())
            .field("max_entries", &self.max_entries)
            .field("ttl", &self.ttl)
            .field("negative_ttl", &self.negative_ttl)
            .field("current_entries", &self.current_entries())
            .finish()
    }
}

impl BoundedMetadataCache {
    /// Creates a metadata cache without expiry and without negative entries.
    pub fn new(max_entries: usize) -> Self {
        Self::with_ttl(max_entries, None, None)
    }

    /// Creates a metadata cache whose entries expire after `ttl` and that caches misses for
    /// `negative_ttl` (`None` disables negative caching).
    pub fn with_ttl(
        max_entries: usize,
        ttl: Option<Duration>,
        negative_ttl: Option<Duration>,
    ) -> Self {
        let _ = shard_hasher();
        let num_shards = floor_pow2((max_entries / MIN_ENTRIES_PER_SHARD).clamp(1, MAX_SHARDS));
        let slots = (0..num_shards)
            .map(|i| Slot::new(MetaShard::new(split_budget(max_entries, num_shards, i))))
            .collect();
        Self {
            slots,
            max_entries,
            ttl,
            negative_ttl,
        }
    }

    #[inline]
    fn enabled(&self) -> bool {
        self.max_entries > 0
    }

    #[inline]
    fn slot(&self, kh: KeyHash) -> &Slot<MetaShard> {
        &self.slots[kh.shard(self.slots.len())]
    }

    #[inline]
    fn positive_expiry(&self) -> Option<Instant> {
        self.ttl.map(|ttl| Instant::now() + ttl)
    }

    /// Expiry for a new negative entry, or `None` when negative caching is disabled.
    #[inline]
    fn negative_expiry(&self) -> Option<Instant> {
        if !self.enabled() {
            return None;
        }
        self.negative_ttl.map(|ttl| Instant::now() + ttl)
    }

    /// Uncounted probe.
    #[inline]
    fn lookup(&self, key: KeyRef<'_>) -> MetaLookup {
        if !self.enabled() {
            return MetaLookup::Miss;
        }
        let slot = self.slot(key.kh);
        match lock(&slot.shard).lookup(key, &slot.stats) {
            Some(MetaValue::Present(meta)) => MetaLookup::Hit(meta),
            Some(MetaValue::Absent) => MetaLookup::Negative,
            None => MetaLookup::Miss,
        }
    }

    /// Inserts if `precondition` holds under the shard lock; returns whether it was inserted.
    fn insert(
        &self,
        key: KeyRef<'_>,
        value: MetaValue,
        expires_at: Option<Instant>,
        precondition: impl FnOnce() -> bool,
    ) -> bool {
        if !self.enabled() {
            return false;
        }
        let entry = MetaEntry {
            size_bytes: value.heap_bytes(),
            value,
            expires_at,
        };
        let slot = self.slot(key.kh);
        let mut shard = lock(&slot.shard);
        if !precondition() {
            return false;
        }
        shard.insert(key, entry, &slot.stats);
        true
    }

    fn remove_key(&self, key: KeyRef<'_>) {
        self.remove_key_if(key, || true);
    }

    /// Removes the entry for `key` if `precondition` holds when evaluated under the shard lock.
    fn remove_key_if(&self, key: KeyRef<'_>, precondition: impl FnOnce() -> bool) {
        if !self.enabled() {
            return;
        }
        let slot = self.slot(key.kh);
        let mut shard = lock(&slot.shard);
        if precondition() {
            shard.remove(key, &slot.stats);
        }
    }

    /// Whether misses are cached at all (`NEGATIVE_CACHE_TTL_SECS` > 0).
    #[inline]
    fn caches_misses(&self) -> bool {
        self.enabled() && self.negative_ttl.is_some()
    }

    /// Drops a negative entry for `key` (positive entries are kept).
    fn remove_negative(&self, key: KeyRef<'_>) {
        if !self.caches_misses() {
            return;
        }
        let slot = self.slot(key.kh);
        lock(&slot.shard).remove_negative(key, &slot.stats);
    }

    #[inline]
    fn record_hit(&self, kh: KeyHash) {
        if self.enabled() {
            SlotStats::add(&self.slot(kh).stats.hits, 1);
        }
    }

    #[inline]
    fn record_miss(&self, kh: KeyHash) {
        if self.enabled() {
            SlotStats::add(&self.slot(kh).stats.misses, 1);
        }
    }

    /// Looks up metadata, recording a hit or a miss. A negative entry counts as a hit and
    /// returns `None`.
    #[inline]
    pub fn get(&self, namespace: &str, hash: &str) -> Option<ArtifactMetadata> {
        let key = KeyRef::new(namespace, hash);
        match self.lookup(key) {
            MetaLookup::Hit(meta) => {
                self.record_hit(key.kh);
                Some(meta)
            }
            MetaLookup::Negative => {
                self.record_hit(key.kh);
                None
            }
            MetaLookup::Miss => {
                self.record_miss(key.kh);
                None
            }
        }
    }

    pub fn put(&self, namespace: &str, hash: &str, meta: ArtifactMetadata) {
        self.insert(
            KeyRef::new(namespace, hash),
            MetaValue::Present(meta),
            self.positive_expiry(),
            || true,
        );
    }

    pub fn remove(&self, namespace: &str, hash: &str) {
        self.remove_key(KeyRef::new(namespace, hash));
    }

    pub fn hits(&self) -> u64 {
        sum_stat(&self.slots, |s| &s.hits)
    }

    pub fn misses(&self) -> u64 {
        sum_stat(&self.slots, |s| &s.misses)
    }

    pub fn evictions(&self) -> u64 {
        sum_stat(&self.slots, |s| &s.evictions)
    }

    pub fn expirations(&self) -> u64 {
        sum_stat(&self.slots, |s| &s.expirations)
    }

    /// Entries currently held (positive and negative).
    pub fn current_entries(&self) -> usize {
        sum_stat(&self.slots, |s| &s.entries) as usize
    }

    /// Heap bytes of the cached metadata strings.
    pub fn current_bytes(&self) -> usize {
        sum_stat(&self.slots, |s| &s.bytes) as usize
    }

    pub fn max_entries(&self) -> usize {
        self.max_entries
    }

    pub fn stats(&self) -> CacheTierStats {
        CacheTierStats {
            hits: self.hits(),
            misses: self.misses(),
            evictions: self.evictions(),
            expirations: self.expirations(),
            bytes: self.current_bytes() as u64,
            entries: self.current_entries() as u64,
            max_bytes: 0,
            max_entries: self.max_entries as u64,
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Generations
// ---------------------------------------------------------------------------------------------

/// Striped per-key write generations. Colliding keys only cause conservative cache skips.
struct Generations {
    stripes: Box<[AtomicU64]>,
}

impl Generations {
    fn new() -> Self {
        Self {
            stripes: (0..GENERATION_STRIPES).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    #[inline]
    fn current(&self, kh: KeyHash) -> u64 {
        self.stripes[kh.stripe()].load(Ordering::Acquire)
    }

    /// Increments the generation and returns the new value.
    #[inline]
    fn bump(&self, kh: KeyHash) -> u64 {
        self.stripes[kh.stripe()]
            .fetch_add(1, Ordering::AcqRel)
            .wrapping_add(1)
    }
}

// ---------------------------------------------------------------------------------------------
// Single-flight
// ---------------------------------------------------------------------------------------------

/// Cloneable error shared by all requests waiting on one flight.
#[derive(Clone, Debug)]
struct SharedError(Arc<AppError>);

impl SharedError {
    fn new(err: AppError) -> Self {
        Self(Arc::new(err))
    }

    /// The original error, unchanged, so e.g. a 507 or 408 from the backend does not become a
    /// 502 for the requests that joined the flight.
    fn to_app_error(&self) -> AppError {
        self.0.as_ref().clone()
    }
}

type FlightResult<T> = Result<T, SharedError>;
type FlightFuture<T> = BoxFuture<'static, FlightResult<T>>;

struct FlightEntry<T> {
    id: u64,
    generation: u64,
    weak: WeakShared<FlightFuture<T>>,
}

type FlightTable<T> = Mutex<HashMap<StoredKey, FlightEntry<T>, PrehashedState>>;

/// In-flight cache fills keyed by artifact. Entries only hold weak references: when every waiting
/// request is cancelled the backend request is dropped too.
struct FlightMap<T> {
    table: Arc<FlightTable<T>>,
    next_id: AtomicU64,
}

/// Moved into the flight future; removes the map entry when the flight completes or is dropped.
struct FlightGuard<T> {
    table: Arc<FlightTable<T>>,
    key: StoredKey,
    id: u64,
}

impl<T> Drop for FlightGuard<T> {
    fn drop(&mut self) {
        let mut table = lock(&self.table);
        if table
            .get(&self.key)
            .is_some_and(|entry| entry.id == self.id)
        {
            table.remove(&self.key);
        }
    }
}

impl<T: Clone + Send + Sync + 'static> FlightMap<T> {
    fn new() -> Self {
        Self {
            table: Arc::new(Mutex::new(HashMap::default())),
            next_id: AtomicU64::new(0),
        }
    }

    /// Joins the running flight for `key` if it started at the same `generation` (i.e. no write
    /// to the key happened since), otherwise starts a new one with `start`.
    fn join_or_start(
        &self,
        key: KeyRef<'_>,
        generation: u64,
        start: impl FnOnce(FlightGuard<T>) -> FlightFuture<T>,
    ) -> Shared<FlightFuture<T>> {
        let mut table = lock(&self.table);
        if let Some(entry) = table.get(&key as &dyn KeyPair)
            && entry.generation == generation
            && let Some(flight) = entry.weak.upgrade()
        {
            return flight;
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let stored = StoredKey::new(key);
        let guard = FlightGuard {
            table: Arc::clone(&self.table),
            key: stored.clone(),
            id,
        };
        // A panicking backend must not re-panic every waiter polling the shared future: turn the
        // panic into an error for this flight (the guard still removes the entry).
        let flight = AssertUnwindSafe(start(guard))
            .catch_unwind()
            .map(|result| {
                result.unwrap_or_else(|_| {
                    tracing::error!("storage backend panicked during a shared cache fill");
                    Err(SharedError::new(AppError::Internal(
                        "storage backend panicked".to_string(),
                    )))
                })
            })
            .boxed()
            .shared();
        if let Some(weak) = flight.downgrade() {
            table.insert(
                stored,
                FlightEntry {
                    id,
                    generation,
                    weak,
                },
            );
        }
        flight
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        lock(&self.table).len()
    }
}

/// Outcome of a shared cache-filling `GET`.
#[derive(Clone)]
enum GetFill {
    Missing,
    /// In-memory payload, shared by every waiter.
    Ready(Bytes, ArtifactMetadata),
    /// A streamed payload too large to buffer: the first waiter takes the stream, the others
    /// stream from the backend themselves.
    Handoff(Arc<Mutex<Option<StoredArtifact>>>),
}

impl GetFill {
    fn handoff(artifact: StoredArtifact) -> Self {
        GetFill::Handoff(Arc::new(Mutex::new(Some(artifact))))
    }
}

// ---------------------------------------------------------------------------------------------
// TieredStorage
// ---------------------------------------------------------------------------------------------

struct TieredCore {
    inner: Arc<dyn Storage>,
    payload: BoundedBytesCache,
    metadata: BoundedMetadataCache,
    generations: Generations,
    get_flights: FlightMap<GetFill>,
    head_flights: FlightMap<Option<ArtifactMetadata>>,
}

/// Brackets a backend write: bumps the key's generation and invalidates both caches on start and
/// on finish. `Drop` runs the finish step if the task is torn down (runtime shutdown, panic).
///
/// Conditional writes (`PutMode::IfAbsent`) skip the start step and, if the backend reports
/// `AlreadyExists`, the finish step too. This is safe because the start step only keeps a value
/// cached *before* a write from being served after it, which the finish step alone already
/// guarantees (readers that started before it fail their generation check, entries they inserted
/// earlier are invalidated by it); writers still detect every overlapping write that changed the
/// backend, since each such write bumps the generation when it finishes.
struct WriteGuard<'a> {
    core: &'a TieredCore,
    key: KeyRef<'a>,
    /// Generation after the start step (the current one for conditional writes).
    start: u64,
    finished: bool,
}

impl<'a> WriteGuard<'a> {
    fn begin(core: &'a TieredCore, key: KeyRef<'a>, mode: PutMode) -> Self {
        let start = match mode {
            PutMode::Overwrite => {
                let start = core.generations.bump(key.kh);
                core.invalidate(key);
                start
            }
            PutMode::IfAbsent => core.generations.current(key.kh),
        };
        Self {
            core,
            key,
            start,
            finished: false,
        }
    }

    fn end(&self) -> u64 {
        let end = self.core.generations.bump(self.key.kh);
        self.core.invalidate(self.key);
        end
    }

    /// Ends the write. Returns the end generation if no other write to the key overlapped this
    /// one, i.e. it is safe to cache the written value while the generation stays unchanged.
    fn finish(mut self) -> Option<u64> {
        self.finished = true;
        let end = self.end();
        (end == self.start.wrapping_add(1)).then_some(end)
    }

    /// Ends a conditional write that found the key: positive entries stay valid and are kept. A
    /// negative entry is now known to be wrong (e.g. another replica uploaded the artifact): it
    /// is dropped, after bumping the generation so a reader that observed the miss earlier
    /// cannot re-insert it.
    fn finish_unchanged(mut self) {
        self.finished = true;
        let core = self.core;
        if core.metadata.caches_misses() {
            core.generations.bump(self.key.kh);
            core.metadata.remove_negative(self.key);
        }
    }
}

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.end();
        }
    }
}

impl TieredCore {
    #[inline]
    fn unchanged(&self, kh: KeyHash, generation: u64) -> impl Fn() -> bool + Copy + '_ {
        move || self.generations.current(kh) == generation
    }

    fn invalidate(&self, key: KeyRef<'_>) {
        self.payload.remove_key(key);
        self.metadata.remove_key(key);
    }

    fn cache_metadata(&self, key: KeyRef<'_>, metadata: &ArtifactMetadata, generation: u64) {
        self.metadata.insert(
            key,
            MetaValue::Present(metadata.clone()),
            self.metadata.positive_expiry(),
            self.unchanged(key.kh, generation),
        );
    }

    /// Caches a payload read or written at `generation`. `owned` marks buffers this module
    /// assembled itself (exactly sized), which therefore need no defensive copy.
    fn cache_payload(
        &self,
        key: KeyRef<'_>,
        data: Bytes,
        metadata: ArtifactMetadata,
        generation: u64,
        owned: bool,
    ) {
        let opts = InsertOptions {
            generation: Some(generation),
            copy_small: !owned,
            ..InsertOptions::new(self.payload.expiry())
        };
        self.payload.insert(
            key,
            data,
            metadata,
            opts,
            self.unchanged(key.kh, generation),
        );
    }

    /// Records that the backend reported `key` as missing at `generation`.
    ///
    /// With negative caching a negative entry replaces whatever was cached. Without it, cached
    /// entries are still dropped: the backend lost the artifact behind the cache's back (corrupt
    /// local file removed, S3 lifecycle expiry, another replica's DELETE), and `HEAD` and batch
    /// queries must not report it as present after this server answered `404`. The removal is
    /// skipped when a write started since `generation` (its end step invalidates, and it may
    /// already have cached the new value).
    fn cache_absent(&self, key: KeyRef<'_>, generation: u64) {
        let Some(expires_at) = self.metadata.negative_expiry() else {
            self.metadata
                .remove_key_if(key, self.unchanged(key.kh, generation));
            self.payload
                .remove_key_if(key, self.unchanged(key.kh, generation));
            return;
        };
        if self.metadata.insert(
            key,
            MetaValue::Absent,
            Some(expires_at),
            self.unchanged(key.kh, generation),
        ) {
            self.payload.remove_key(key);
        }
    }

    /// Shared metadata probe for `head_hot` and `head` (metadata cache, then payload cache).
    fn probe_metadata(&self, key: KeyRef<'_>, count: Count) -> MetaLookup {
        match self.metadata.lookup(key) {
            MetaLookup::Hit(meta) => {
                if count.hits() {
                    self.metadata.record_hit(key.kh);
                }
                return MetaLookup::Hit(meta);
            }
            MetaLookup::Negative => {
                if count == Count::All {
                    self.metadata.record_hit(key.kh);
                }
                return MetaLookup::Negative;
            }
            MetaLookup::Miss => {}
        }
        // Captured before reading the payload entry so a concurrent write prevents the promotion.
        let generation = self.generations.current(key.kh);
        if let Some((meta, expires_at, cached_at)) = self.payload.peek_metadata(key) {
            // Only promote an entry cached at the current generation. A writer's end step bumps
            // the generation *before* it invalidates, so an entry cached during the write can
            // still be resident while the new generation is visible; promoting it would outlive
            // the writer's invalidation.
            if cached_at.is_none_or(|cached_at| cached_at == generation) {
                // Keep the payload entry's expiry: promotion must not extend its lifetime.
                self.metadata.insert(
                    key,
                    MetaValue::Present(meta.clone()),
                    expires_at,
                    self.unchanged(key.kh, generation),
                );
            }
            if count.hits() {
                self.metadata.record_hit(key.kh);
            }
            return MetaLookup::Hit(meta);
        }
        if count == Count::All {
            self.metadata.record_miss(key.kh);
        }
        MetaLookup::Miss
    }

    /// Body of a shared `GET` flight.
    async fn fill_get(&self, key: KeyRef<'_>, generation: u64) -> FlightResult<GetFill> {
        // A previous flight may have filled the cache after this request probed it.
        if let Some((data, metadata)) = self.payload.lookup(key, Count::Off) {
            return Ok(GetFill::Ready(data, metadata));
        }
        let fetched = self
            .inner
            .get(key.namespace, key.hash)
            .await
            .map_err(SharedError::new)?;
        let Some(stored) = fetched else {
            self.cache_absent(key, generation);
            return Ok(GetFill::Missing);
        };
        let StoredArtifact {
            mut metadata,
            payload,
            content_length,
        } = stored;
        let max_item = self.payload.max_item_bytes();

        match payload {
            ArtifactPayload::Bytes(data) => {
                self.cache_metadata(key, &metadata, generation);
                if data.len() <= max_item {
                    self.cache_payload(key, data.clone(), metadata.clone(), generation, false);
                }
                Ok(GetFill::Ready(data, metadata))
            }
            ArtifactPayload::Stream(stream) if metadata.size > max_item as u64 => {
                // Only cache metadata whose size is exact: without a known payload length the
                // backend's size may be a guess (an S3 GET without `Content-Length` whose
                // `x-amz-meta-size` was stripped reports 0), and a cached guess would make later
                // `HEAD`/batch answers report it. The next `HEAD` asks the backend instead.
                if content_length.is_some() {
                    self.cache_metadata(key, &metadata, generation);
                }
                Ok(GetFill::handoff(
                    StoredArtifact::from_stream(metadata, stream)
                        .with_content_length(content_length),
                ))
            }
            ArtifactPayload::Stream(mut stream) => {
                let mut buf = Vec::with_capacity(metadata.size as usize);
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk.map_err(|e| {
                        SharedError::new(AppError::Storage(format!(
                            "Failed to read artifact stream: {e}"
                        )))
                    })?;
                    if buf.len() + chunk.len() > max_item {
                        // Larger than announced: hand the already read prefix plus the rest on.
                        // The announced size is wrong or unknown, so the metadata is not cached
                        // (see above); the next `HEAD` asks the backend.
                        let prefix = Bytes::from(buf);
                        let combined = stream::iter([Ok(prefix), Ok(chunk)]).chain(stream);
                        return Ok(GetFill::handoff(
                            StoredArtifact::from_stream(metadata, Box::pin(combined))
                                .with_content_length(content_length),
                        ));
                    }
                    buf.extend_from_slice(&chunk);
                }
                // Exactly sized (a no-op unless the stream was shorter than announced), so the
                // cache can keep this buffer without copying it again.
                buf.shrink_to_fit();
                let data = Bytes::from(buf);
                metadata.size = data.len() as u64;
                self.cache_metadata(key, &metadata, generation);
                self.cache_payload(key, data.clone(), metadata.clone(), generation, true);
                Ok(GetFill::Ready(data, metadata))
            }
        }
    }

    /// Body of a shared `HEAD` flight.
    async fn fill_head(
        &self,
        key: KeyRef<'_>,
        generation: u64,
    ) -> FlightResult<Option<ArtifactMetadata>> {
        match self.probe_metadata(key, Count::Off) {
            MetaLookup::Hit(meta) => return Ok(Some(meta)),
            MetaLookup::Negative => return Ok(None),
            MetaLookup::Miss => {}
        }
        let fetched = self
            .inner
            .head(key.namespace, key.hash)
            .await
            .map_err(SharedError::new)?;
        match &fetched {
            Some(meta) => self.cache_metadata(key, meta, generation),
            None => self.cache_absent(key, generation),
        }
        Ok(fetched)
    }
}

/// Runs a backend write on its own task and waits for it.
///
/// Cancelling the request (client disconnect, timeout) therefore never cancels the write half
/// way: the backend call and the [`WriteGuard`] end step always run to completion, in that order,
/// so a timed-out or abandoned upload may still be stored. Otherwise a backend that keeps working
/// after its future is dropped (`spawn_blocking`, an already sent S3 request) could complete
/// *after* the end step, and a read in between would cache the old value until it expires. The
/// body is already fully received, and the backend bounds how long the write can run (e.g. the
/// S3 client request timeout).
///
/// Streamed uploads ([`Storage::put_stream`]) keep reading the request body on that task; once
/// the client is gone the body fails and the backend discards the upload.
pub(crate) async fn run_to_completion<T: Send + 'static>(
    write: impl Future<Output = AppResult<T>> + Send + 'static,
) -> AppResult<T> {
    match tokio::spawn(write).await {
        Ok(result) => result,
        Err(err) if err.is_panic() => std::panic::resume_unwind(err.into_panic()),
        Err(_) => Err(AppError::Internal(
            "storage write task was cancelled".to_string(),
        )),
    }
}

/// Storage decorator that puts strictly bounded in-memory payload and metadata LRU caches in
/// front of a slower backend. See the module docs for the consistency guarantees.
pub struct TieredStorage {
    core: Arc<TieredCore>,
}

impl TieredStorage {
    /// Creates a tiered cache without TTL or negative caching.
    pub fn new(
        inner: Arc<dyn Storage>,
        max_hot_bytes: usize,
        max_hot_entries: usize,
        max_hot_item_bytes: usize,
        max_metadata_entries: usize,
    ) -> Self {
        Self::with_options(
            inner,
            CacheOptions {
                max_bytes: max_hot_bytes,
                max_entries: max_hot_entries,
                max_item_bytes: max_hot_item_bytes,
                metadata_max_entries: max_metadata_entries,
                ttl: None,
                negative_ttl: None,
            },
        )
    }

    /// Builds the tiered cache from [`CacheOptions`], including entry TTL (`CACHE_TTL_SECS`) and
    /// negative caching (`NEGATIVE_CACHE_TTL_SECS`).
    pub fn with_options(inner: Arc<dyn Storage>, opts: CacheOptions) -> Self {
        Self {
            core: Arc::new(TieredCore {
                inner,
                payload: BoundedBytesCache::with_ttl(
                    opts.max_bytes,
                    opts.max_entries,
                    opts.max_item_bytes,
                    opts.ttl,
                ),
                metadata: BoundedMetadataCache::with_ttl(
                    opts.metadata_max_entries,
                    opts.ttl,
                    opts.negative_ttl,
                ),
                generations: Generations::new(),
                get_flights: FlightMap::new(),
                head_flights: FlightMap::new(),
            }),
        }
    }

    pub fn payload_cache(&self) -> &BoundedBytesCache {
        &self.core.payload
    }

    pub fn metadata_cache(&self) -> &BoundedMetadataCache {
        &self.core.metadata
    }
}

#[async_trait]
impl Storage for TieredStorage {
    fn backend_name(&self) -> &'static str {
        self.core.inner.backend_name()
    }

    #[inline]
    fn get_hot(&self, namespace: &str, hash: &str) -> Option<(Bytes, ArtifactMetadata)> {
        let ns = sanitize_namespace(namespace).ok()?;
        let hash = validate_hash(hash).ok()?;
        self.core.payload.lookup(KeyRef::new(ns, hash), Count::Hits)
    }

    #[inline]
    fn head_hot(&self, namespace: &str, hash: &str) -> Option<ArtifactMetadata> {
        let ns = sanitize_namespace(namespace).ok()?;
        let hash = validate_hash(hash).ok()?;
        match self.core.probe_metadata(KeyRef::new(ns, hash), Count::Hits) {
            MetaLookup::Hit(meta) => Some(meta),
            MetaLookup::Negative | MetaLookup::Miss => None,
        }
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
        let core = Arc::clone(&self.core);

        run_to_completion(async move {
            let key = KeyRef::new(&ns, &hash);
            let core = &*core;
            let cacheable = body
                .as_bytes()
                .filter(|b| b.len() <= core.payload.max_item_bytes())
                .cloned();

            let write = WriteGuard::begin(core, key, mode);
            let result = core
                .inner
                .put(&ns, &hash, body, metadata.clone(), mode)
                .await;
            if mode == PutMode::IfAbsent && matches!(result, Ok(PutOutcome::AlreadyExists)) {
                write.finish_unchanged();
                return Ok(PutOutcome::AlreadyExists);
            }
            let exclusive = write.finish();
            let outcome = result?;

            if outcome == PutOutcome::Stored
                && let Some(generation) = exclusive
            {
                core.cache_metadata(key, &metadata, generation);
                if let Some(data) = cacheable {
                    core.cache_payload(key, data, metadata, generation, false);
                }
            }
            Ok(outcome)
        })
        .await
    }

    fn streams_uploads(&self) -> bool {
        self.core.inner.streams_uploads()
    }

    /// Like [`TieredStorage::put`] in overwrite mode; the payload is never cached (streamed
    /// bodies are larger than `UPLOAD_MEMORY_THRESHOLD`), the metadata only when the length was
    /// announced.
    async fn put_stream(
        &self,
        namespace: &str,
        hash: &str,
        body: UploadStream,
        mut metadata: ArtifactMetadata,
    ) -> AppResult<PutOutcome> {
        let ns = sanitize_namespace(namespace)?.to_owned();
        let hash = validate_hash(hash)?.to_owned();
        let size = body.content_length();
        metadata.size = size.unwrap_or(0);
        let core = Arc::clone(&self.core);

        run_to_completion(async move {
            let key = KeyRef::new(&ns, &hash);
            let core = &*core;
            let write = WriteGuard::begin(core, key, PutMode::Overwrite);
            let result = core
                .inner
                .put_stream(&ns, &hash, body, metadata.clone())
                .await;
            let exclusive = write.finish();
            let outcome = result?;
            if outcome == PutOutcome::Stored
                && size.is_some()
                && let Some(generation) = exclusive
            {
                core.cache_metadata(key, &metadata, generation);
            }
            Ok(outcome)
        })
        .await
    }

    async fn get(&self, namespace: &str, hash: &str) -> AppResult<Option<StoredArtifact>> {
        let ns = sanitize_namespace(namespace)?;
        let hash = validate_hash(hash)?;
        let key = KeyRef::new(ns, hash);
        let core = &*self.core;

        if let Some((data, metadata)) = core.payload.lookup(key, Count::All) {
            return Ok(Some(StoredArtifact::from_bytes(metadata, data)));
        }
        if matches!(core.metadata.lookup(key), MetaLookup::Negative) {
            return Ok(None);
        }

        let generation = core.generations.current(key.kh);
        let flight = core.get_flights.join_or_start(key, generation, |guard| {
            let core = Arc::clone(&self.core);
            async move { core.fill_get(guard.key.as_key_ref(), generation).await }.boxed()
        });

        match flight.await {
            Ok(GetFill::Ready(data, metadata)) => {
                Ok(Some(StoredArtifact::from_bytes(metadata, data)))
            }
            Ok(GetFill::Missing) => Ok(None),
            Ok(GetFill::Handoff(slot)) => {
                let taken = lock(&slot).take();
                match taken {
                    Some(artifact) => Ok(Some(artifact)),
                    None => core.inner.get(ns, hash).await,
                }
            }
            Err(err) => Err(err.to_app_error()),
        }
    }

    async fn head(&self, namespace: &str, hash: &str) -> AppResult<Option<ArtifactMetadata>> {
        let ns = sanitize_namespace(namespace)?;
        let hash = validate_hash(hash)?;
        let key = KeyRef::new(ns, hash);
        let core = &*self.core;

        match core.probe_metadata(key, Count::All) {
            MetaLookup::Hit(meta) => return Ok(Some(meta)),
            MetaLookup::Negative => return Ok(None),
            MetaLookup::Miss => {}
        }

        let generation = core.generations.current(key.kh);
        let flight = core.head_flights.join_or_start(key, generation, |guard| {
            let core = Arc::clone(&self.core);
            async move { core.fill_head(guard.key.as_key_ref(), generation).await }.boxed()
        });
        flight.await.map_err(|err| err.to_app_error())
    }

    async fn delete(&self, namespace: &str, hash: &str) -> AppResult<bool> {
        let ns = sanitize_namespace(namespace)?.to_owned();
        let hash = validate_hash(hash)?.to_owned();
        let core = Arc::clone(&self.core);

        run_to_completion(async move {
            let key = KeyRef::new(&ns, &hash);
            let write = WriteGuard::begin(&core, key, PutMode::Overwrite);
            let result = core.inner.delete(&ns, &hash).await;
            write.finish();
            result
        })
        .await
    }

    async fn health_check(&self) -> AppResult<()> {
        self.core.inner.health_check().await
    }

    fn cache_stats(&self) -> Option<CacheStatsSnapshot> {
        Some(CacheStatsSnapshot {
            payload: self.core.payload.stats(),
            metadata: self.core.metadata.stats(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;

    use super::{
        BoundedBytesCache, BoundedMetadataCache, InsertOptions, InsertOutcome, KeyHash, KeyRef,
        StoredKey, TieredStorage,
    };
    use crate::{
        models::ArtifactMetadata,
        storage::{PutMode, Storage, local::LocalStorage},
    };

    fn meta(size: u64) -> ArtifactMetadata {
        ArtifactMetadata {
            duration: 10,
            size,
            ..Default::default()
        }
    }

    #[test]
    fn bytes_cache_evicts_lru_and_rejects_oversized_items() {
        let cache = BoundedBytesCache::new(100, 3, 60);
        let meta = meta(40);

        assert!(cache.put("ns", "h1", Bytes::from(vec![1u8; 40]), meta.clone()));
        assert!(cache.put("ns", "h2", Bytes::from(vec![2u8; 40]), meta.clone()));
        assert_eq!(cache.current_bytes(), 80);
        assert_eq!(cache.current_entries(), 2);

        // Touch h1 so h2 is least recently used; h3 does not fit the 100-byte budget.
        assert!(cache.get("ns", "h1").is_some());
        assert!(cache.put("ns", "h3", Bytes::from(vec![3u8; 40]), meta.clone()));
        assert_eq!(cache.current_bytes(), 80);
        assert_eq!(cache.current_entries(), 2);
        assert!(cache.get("ns", "h1").is_some());
        assert!(cache.get("ns", "h2").is_none());
        assert!(cache.get("ns", "h3").is_some());
        assert_eq!(cache.evictions(), 1);

        // Above the item limit: rejected, and an overwrite must not leave the old value behind.
        assert!(!cache.put("ns", "huge", Bytes::from(vec![9u8; 61]), meta.clone()));
        assert_eq!(cache.current_bytes(), 80);
        assert!(!cache.put("ns", "h1", Bytes::from(vec![9u8; 61]), meta));
        assert!(cache.get("ns", "h1").is_none());
        assert_eq!(cache.current_entries(), 1);
        assert_eq!(cache.current_bytes(), 40);
    }

    /// When concurrent reservations hold the whole budget and nothing is resident to evict, the
    /// insert gives up with `Busy` instead of spinning forever and leaves the cache unchanged.
    #[test]
    fn busy_budget_gives_up_and_recovers() {
        let cache = BoundedBytesCache::new(1 << 20, 100, 1 << 16);
        assert!(cache.reserve_for_test(cache.max_bytes()));

        let insert = || {
            cache.insert(
                KeyRef::new("team", "hash"),
                Bytes::from_static(b"payload"),
                meta(7),
                InsertOptions::new(None),
                || true,
            )
        };
        assert_eq!(insert(), InsertOutcome::Busy);
        assert!(cache.get("team", "hash").is_none());
        assert_eq!(cache.current_entries(), 0);

        cache.release_for_test(cache.max_bytes());
        assert_eq!(insert(), InsertOutcome::Inserted);
        assert_eq!(cache.current_bytes(), 7);
        assert!(cache.get("team", "hash").is_some());
    }

    /// The item limit is clamped whenever it leaves no room for metadata in the budget, and an
    /// item of exactly the effective limit is cached although it exceeds any per-shard share.
    #[test]
    fn item_limit_leaves_room_for_metadata() {
        use super::effective_item_limit;
        let mib = 1 << 20;
        let slack = super::METADATA_SLACK_BYTES;
        assert_eq!(effective_item_limit(mib, mib), mib - slack);
        assert_eq!(effective_item_limit(mib, 2 * mib), mib - slack);
        assert_eq!(effective_item_limit(mib, mib - slack + 1), mib - slack);
        assert_eq!(effective_item_limit(mib, mib - slack), mib - slack);
        assert_eq!(effective_item_limit(128 * mib, 4 * mib), 4 * mib);
        // Tiny budgets reserve 1/16 instead of 4 KiB.
        assert_eq!(effective_item_limit(16 * 1024, 16 * 1024), 15 * 1024);

        let cache = BoundedBytesCache::new(mib, 1000, mib);
        assert!(cache.num_shards() > 1);
        let limit = cache.max_item_bytes();
        assert_eq!(limit, mib - slack);
        let mut big = meta(limit as u64);
        big.tag = Some("t".repeat(600).into());
        assert!(cache.put("ns", "big", Bytes::from(vec![1u8; limit]), big));
        assert_eq!(cache.get("ns", "big").unwrap().0.len(), limit);
        assert!(cache.current_bytes() <= cache.max_bytes());
        assert!(!cache.put("ns", "h2", Bytes::from(vec![0u8; limit + 1]), meta(0)));
    }

    #[test]
    fn eviction_is_global_lru_across_shards() {
        let cache = BoundedBytesCache::new(64 * 128, 64, 128);
        assert_eq!(cache.num_shards(), 16);
        let payload = Bytes::from(vec![0u8; 128]);
        for i in 0..64 {
            assert!(cache.put("ns", &format!("k{i}"), payload.clone(), meta(128)));
        }
        // Touch the first half, so the second half is least recently used.
        for i in 0..32 {
            assert!(cache.get("ns", &format!("k{i}")).is_some());
        }
        for i in 64..96 {
            assert!(cache.put("ns", &format!("k{i}"), payload.clone(), meta(128)));
        }
        assert_eq!(cache.evictions(), 32);
        for i in 0..32 {
            assert!(cache.get("ns", &format!("k{i}")).is_some(), "k{i} evicted");
        }
        for i in 32..64 {
            assert!(cache.get("ns", &format!("k{i}")).is_none(), "k{i} kept");
        }
        assert_eq!(cache.current_entries(), 64);
        assert_eq!(cache.current_bytes(), 64 * 128);
    }

    /// The global budget is never exceeded under concurrent inserts, overwrites and removals
    /// into different shards, and the accounting returns to the resident size.
    #[test]
    fn global_budget_is_strict_under_concurrency() {
        let max_bytes = 64 << 10;
        let cache = Arc::new(BoundedBytesCache::new(max_bytes, 48, 16 << 10));
        assert!(cache.num_shards() > 1);
        let threads: Vec<_> = (0..8u64)
            .map(|t| {
                let cache = Arc::clone(&cache);
                std::thread::spawn(move || {
                    let mut x = 0x9E37_79B9_7F4A_7C15u64 ^ t;
                    for _ in 0..5000 {
                        x ^= x << 13;
                        x ^= x >> 7;
                        x ^= x << 17;
                        let key = format!("k{}", x % 200);
                        let len = (x >> 16) as usize % (16 << 10) + 1;
                        if x.is_multiple_of(5) {
                            cache.remove("ns", &key);
                        } else {
                            cache.put("ns", &key, Bytes::from(vec![1u8; len]), meta(len as u64));
                        }
                        assert!(cache.current_bytes() <= max_bytes);
                        assert!(cache.current_entries() <= 48);
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        let resident: usize = cache
            .slots
            .iter()
            .map(|slot| {
                let shard = super::lock(&slot.shard);
                shard.lru.iter().map(|(_, e)| e.size_bytes).sum::<usize>()
            })
            .sum();
        let entries: usize = cache
            .slots
            .iter()
            .map(|s| super::lock(&s.shard).lru.len())
            .sum();
        assert_eq!(cache.current_bytes(), resident);
        assert_eq!(cache.current_entries(), entries);
        // Published tail clocks match the shards once all work stopped.
        for slot in cache.slots.iter() {
            let clock = super::lock(&slot.shard).tail_clock();
            assert_eq!(
                slot.tail.0.load(std::sync::atomic::Ordering::Relaxed),
                clock
            );
        }
    }

    /// Overwriting a recently used key with a larger value only evicts what the size difference
    /// needs, not every older entry while the old copy is still resident.
    #[test]
    fn overwrite_evicts_only_the_size_difference() {
        let cache = BoundedBytesCache::new(10_000, 100, 10_000);
        let m = meta(0);
        let item = |len: usize| len + m.allocated_bytes();
        for i in 0..10 {
            assert!(cache.put(
                "ns",
                &format!("s{i}"),
                Bytes::from(vec![0u8; 500]),
                m.clone()
            ));
        }
        assert!(cache.put("ns", "big", Bytes::from(vec![1u8; 4_000]), m.clone()));
        assert!(cache.get("ns", "big").is_some());
        let used = cache.current_bytes();
        assert_eq!(used, 10 * item(500) + item(4_000));

        // Needs `used + 600 - 10_000` more bytes: exactly the 2 oldest small entries.
        let grow = 10_000 - used + 600;
        assert!(cache.put("ns", "big", Bytes::from(vec![2u8; 4_000 + grow]), m.clone()));
        assert_eq!(cache.evictions(), 2);
        assert!(cache.get("ns", "s0").is_none());
        assert!(cache.get("ns", "s1").is_none());
        for i in 2..10 {
            assert!(cache.get("ns", &format!("s{i}")).is_some(), "s{i} evicted");
        }
        assert_eq!(cache.get("ns", "big").unwrap().0.len(), 4_000 + grow);
        assert_eq!(cache.current_bytes(), 8 * item(500) + item(4_000 + grow));
    }

    /// Small payloads are copied, so they never pin the buffer they were sliced from.
    #[test]
    fn small_payloads_are_copied_into_owned_buffers() {
        let cache = BoundedBytesCache::new(1 << 20, 16, 64 << 10);
        let big = Bytes::from(vec![7u8; 256 << 10]);
        let slice = big.slice(1000..1100);
        assert!(cache.put("ns", "h", slice.clone(), meta(100)));
        let (cached, _) = cache.get("ns", "h").unwrap();
        assert_eq!(cached, slice);
        let range = big.as_ptr_range();
        assert!(
            !range.contains(&cached.as_ptr()),
            "cached payload must not alias the large source buffer"
        );
    }

    /// The namespace/hash boundary is part of the key ("ab"+"c" != "a"+"bc"), even when the
    /// prehash collides.
    #[test]
    fn stored_keys_keep_the_namespace_boundary() {
        let mut lru = super::new_lru::<u8>();
        lru.push(StoredKey::new(KeyRef::new("ab", "c")), 1);
        assert!(lru.peek(super::dk(&KeyRef::new("a", "bc"))).is_none());
        assert_eq!(lru.peek(super::dk(&KeyRef::new("ab", "c"))), Some(&1));
        let forged = KeyRef {
            kh: KeyHash::of("ab", "c"),
            namespace: "a",
            hash: "bc",
        };
        assert!(lru.peek(super::dk(&forged)).is_none());
        assert!(lru.put(StoredKey::new(forged), 2).is_none());
        assert_eq!(lru.len(), 2);
        assert_eq!(lru.peek(super::dk(&KeyRef::new("ab", "c"))), Some(&1));
        assert_eq!(lru.peek(super::dk(&forged)), Some(&2));
        let stored = StoredKey::new(KeyRef::new("ab", "c"));
        let back = stored.as_key_ref();
        assert_eq!((back.namespace, back.hash), ("ab", "c"));
        assert_eq!(back.kh, KeyHash::of("ab", "c"));
        // Longer than the inline key buffer.
        let long_ns = "n".repeat(200);
        let long_h = "h".repeat(128);
        let stored = StoredKey::new(KeyRef::new(&long_ns, &long_h));
        assert_eq!(stored.as_key_ref().hash, long_h);
    }

    #[test]
    fn metadata_cache_evicts_least_recently_used_at_its_bound() {
        // 3 entries give a single shard, so the bound and the LRU order are exact.
        let metas = BoundedMetadataCache::new(3);
        assert_eq!(metas.slots.len(), 1);
        for hash in ["a", "b", "c"] {
            metas.put("ns", hash, meta(1));
        }
        assert!(metas.get("ns", "a").is_some(), "promotes a over b and c");
        metas.put("ns", "d", meta(1));
        assert_eq!(metas.current_entries(), 3);
        assert_eq!(metas.evictions(), 1);
        assert!(metas.get("ns", "b").is_none(), "b was least recently used");
        for hash in ["a", "c", "d"] {
            assert!(metas.get("ns", hash).is_some(), "{hash} was evicted");
        }
        // Overwriting a resident key neither grows the shard nor evicts.
        metas.put("ns", "a", meta(2));
        assert_eq!(metas.current_entries(), 3);
        assert_eq!(metas.evictions(), 1);
        assert_eq!(metas.get("ns", "a").map(|m| m.size), Some(2));
    }

    /// A writer's end step bumps the generation before it invalidates. A payload entry cached
    /// during the write is still resident in that window; a metadata probe must not promote it,
    /// or it would survive the invalidation.
    #[tokio::test]
    async fn stale_payload_entry_is_not_promoted_after_end_bump() {
        let dir = tempfile::tempdir().unwrap();
        let inner = Arc::new(LocalStorage::new(dir.path()).await.unwrap());
        let tiered = TieredStorage::new(inner, 1 << 20, 16, 1024, 16);
        let core = &*tiered.core;
        let key = KeyRef::new("ns", "h");

        let start = core.generations.bump(key.kh); // write begins
        core.invalidate(key);
        // A concurrent GET read the pre-delete backend state and cached it at `start`.
        core.cache_payload(key, Bytes::from_static(b"old"), meta(3), start, false);
        assert!(core.payload.peek_metadata(key).is_some());
        core.generations.bump(key.kh); // write ends: bump ...

        // ... a HEAD probes before the invalidation runs.
        assert!(
            tiered.head_hot("ns", "h").is_some(),
            "stale value visible mid-write"
        );
        assert!(
            matches!(core.metadata.lookup(key), super::MetaLookup::Miss),
            "stale payload metadata promoted"
        );

        core.invalidate(key); // ... then invalidate.
        assert!(tiered.head_hot("ns", "h").is_none());
        assert!(tiered.get_hot("ns", "h").is_none());
        assert!(tiered.head("ns", "h").await.unwrap().is_none());

        // Entries cached at the current generation are still promoted.
        let generation = core.generations.current(key.kh);
        core.cache_payload(key, Bytes::from_static(b"new"), meta(3), generation, false);
        assert!(tiered.head_hot("ns", "h").is_some());
        assert!(matches!(
            core.metadata.lookup(key),
            super::MetaLookup::Hit(_)
        ));
    }

    /// Flight entries are removed once the flight completes.
    #[tokio::test]
    async fn flight_entries_are_cleaned_up() {
        let dir = tempfile::tempdir().unwrap();
        let inner = Arc::new(LocalStorage::new(dir.path()).await.unwrap());
        inner
            .put(
                "ns",
                "h",
                Bytes::from_static(b"abc").into(),
                meta(3),
                PutMode::Overwrite,
            )
            .await
            .unwrap();
        let tiered = TieredStorage::new(inner, 1 << 20, 16, 1024, 16);
        assert!(tiered.get("ns", "h").await.unwrap().is_some());
        assert!(tiered.get("ns", "missing").await.unwrap().is_none());
        assert!(tiered.head("ns", "missing").await.unwrap().is_none());
        assert_eq!(tiered.core.get_flights.len(), 0);
        assert_eq!(tiered.core.head_flights.len(), 0);
    }
}
