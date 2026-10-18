use std::{
    collections::{HashMap, VecDeque},
    fmt::Write as _,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use crate::{
    models::{AnalyticsEvent, CacheEvent, CacheSource},
    storage::{CacheStatsSnapshot, CacheTierStats},
};

/// Maximum analytics events retained per namespace (oldest are dropped first).
pub const MAX_STORED_EVENTS: usize = 1000;
/// Maximum namespaces with retained analytics events; when exceeded, the namespace that was
/// written least recently is forgotten. Together with [`MAX_STORED_EVENTS`] and
/// [`MAX_EVENT_FIELD_LEN`] this bounds the retained analytics memory (≈ 32 × 1000 × 0.5 KiB).
pub const MAX_EVENT_NAMESPACES: usize = 32;
/// Maximum byte length of the `hash` and `sessionId` fields of an analytics event. Longer
/// values are rejected, not truncated: truncating keeps the oversized allocation alive and can
/// split a UTF-8 character.
pub const MAX_EVENT_FIELD_LEN: usize = 256;

/// Upper bounds (seconds) of the storage latency histogram buckets.
const LATENCY_BUCKETS: [f64; 15] = [
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
];

/// A counter on its own cache line so that counters bumped by different cores on every
/// request do not false-share. Aligned to 128 bytes, like the hot cache's shard slots: Apple
/// Silicon has 128-byte cache lines, and x86 prefetches adjacent 64-byte lines in pairs.
#[derive(Debug, Default)]
#[repr(align(128))]
pub struct Counter(AtomicU64);

impl Counter {
    #[inline]
    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn add(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    fn sub(&self, n: u64) {
        self.0.fetch_sub(n, Ordering::Relaxed);
    }

    /// Adds `n` without wrapping around on overflow (for client-supplied values).
    #[inline]
    pub fn saturating_add(&self, n: u64) {
        let _ = self
            .0
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_add(n))
            });
    }

    #[inline]
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Storage operations instrumented by the routes (only calls that missed the synchronous
/// in-memory hot path, i.e. that actually reached the async storage stack).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageOp {
    Get,
    Head,
    Put,
    Delete,
}

impl StorageOp {
    const ALL: [StorageOp; 4] = [
        StorageOp::Get,
        StorageOp::Head,
        StorageOp::Put,
        StorageOp::Delete,
    ];

    fn label(self) -> &'static str {
        match self {
            StorageOp::Get => "get",
            StorageOp::Head => "head",
            StorageOp::Put => "put",
            StorageOp::Delete => "delete",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

/// Reasons an upload was rejected before it reached storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadRejection {
    /// Body idle/total timeout (408).
    Timeout,
    /// No upload slot became free within `UPLOAD_SLOT_WAIT_SECS` (429).
    SlotUnavailable,
    /// Body larger than `MAX_ARTIFACT_SIZE` (413).
    TooLarge,
}

/// Aligned like [`Counter`] (128 bytes) so histograms observed by different cores do not
/// false-share with each other or with neighbouring counters.
#[derive(Debug, Default)]
#[repr(align(128))]
struct Histogram {
    buckets: [AtomicU64; LATENCY_BUCKETS.len()],
    count: AtomicU64,
    sum_micros: AtomicU64,
}

impl Histogram {
    fn observe(&self, elapsed: Duration) {
        let secs = elapsed.as_secs_f64();
        if let Some(i) = LATENCY_BUCKETS.iter().position(|&bound| secs <= bound) {
            self.buckets[i].fetch_add(1, Ordering::Relaxed);
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        self.sum_micros.fetch_add(micros, Ordering::Relaxed);
    }
}

#[derive(Debug, Default)]
struct NamespaceEvents {
    events: VecDeque<AnalyticsEvent>,
    /// Monotonic write sequence number of the last append (for namespace eviction).
    last_write: u64,
}

#[derive(Debug, Default)]
struct EventStore {
    namespaces: HashMap<Box<str>, NamespaceEvents>,
    seq: u64,
}

/// Thread-safe metrics and bounded, per-namespace analytics event buffers.
#[derive(Debug, Default)]
pub struct CacheMetrics {
    pub get_hits: Counter,
    pub get_misses: Counter,
    pub head_hits: Counter,
    pub head_misses: Counter,
    pub puts: Counter,
    pub bytes_uploaded: Counter,
    pub bytes_downloaded: Counter,
    pub analytics_local_hits: Counter,
    pub analytics_local_misses: Counter,
    pub analytics_remote_hits: Counter,
    pub analytics_remote_misses: Counter,
    pub time_saved_ms: Counter,
    pub uploads_in_flight: Counter,
    upload_timeouts: Counter,
    upload_slot_unavailable: Counter,
    upload_too_large: Counter,
    storage_latency: [Histogram; StorageOp::ALL.len()],
    storage_errors: [Counter; StorageOp::ALL.len()],
    /// Never held across an `.await`, so a std mutex is cheaper than an async lock.
    events: Mutex<EventStore>,
}

/// Decrements the in-flight uploads gauge when dropped.
pub struct InFlightUpload(Arc<CacheMetrics>);

impl Drop for InFlightUpload {
    fn drop(&mut self) {
        self.0.uploads_in_flight.sub(1);
    }
}

/// Validates an analytics batch: every `hash`/`sessionId` must be at most
/// [`MAX_EVENT_FIELD_LEN`] bytes. Returns a client-facing error message on failure.
pub fn validate_analytics_events(events: &[AnalyticsEvent]) -> Result<(), String> {
    for (i, ev) in events.iter().enumerate() {
        if ev.hash.len() > MAX_EVENT_FIELD_LEN {
            return Err(format!(
                "Analytics event {i}: hash exceeds maximum length of {MAX_EVENT_FIELD_LEN} bytes"
            ));
        }
        if ev
            .session_id
            .as_ref()
            .is_some_and(|sid| sid.len() > MAX_EVENT_FIELD_LEN)
        {
            return Err(format!(
                "Analytics event {i}: sessionId exceeds maximum length of {MAX_EVENT_FIELD_LEN} bytes"
            ));
        }
    }
    Ok(())
}

impl CacheMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_get_hit(&self, bytes: u64) {
        self.get_hits.inc();
        self.bytes_downloaded.add(bytes);
    }

    pub fn record_get_miss(&self) {
        self.get_misses.inc();
    }

    pub fn record_head_hit(&self) {
        self.head_hits.inc();
    }

    pub fn record_head_miss(&self) {
        self.head_misses.inc();
    }

    pub fn record_put(&self, bytes: u64) {
        self.puts.inc();
        self.bytes_uploaded.add(bytes);
    }

    /// Records the latency and outcome of a storage call.
    pub fn observe_storage(&self, op: StorageOp, elapsed: Duration, ok: bool) {
        self.storage_latency[op.index()].observe(elapsed);
        if !ok {
            self.storage_errors[op.index()].inc();
        }
    }

    /// Marks an upload as in flight until the returned guard is dropped.
    pub fn upload_started(self: &Arc<Self>) -> InFlightUpload {
        self.uploads_in_flight.inc();
        InFlightUpload(Arc::clone(self))
    }

    pub fn record_upload_rejected(&self, reason: UploadRejection) {
        match reason {
            UploadRejection::Timeout => self.upload_timeouts.inc(),
            UploadRejection::SlotUnavailable => self.upload_slot_unavailable.inc(),
            UploadRejection::TooLarge => self.upload_too_large.inc(),
        }
    }

    /// Records analytics events reported for `namespace`. Events with oversized fields (see
    /// [`validate_analytics_events`]) are skipped; callers should reject such batches first.
    pub fn record_analytics(&self, namespace: &str, incoming: Vec<AnalyticsEvent>) {
        let incoming: Vec<AnalyticsEvent> = incoming
            .into_iter()
            .filter(|ev| validate_analytics_events(std::slice::from_ref(ev)).is_ok())
            .collect();
        if incoming.is_empty() {
            return;
        }

        for ev in &incoming {
            match (ev.source, ev.event) {
                (CacheSource::Local, CacheEvent::Hit) => {
                    self.analytics_local_hits.inc();
                    self.time_saved_ms.saturating_add(ev.duration);
                }
                (CacheSource::Local, CacheEvent::Miss) => self.analytics_local_misses.inc(),
                (CacheSource::Remote, CacheEvent::Hit) => {
                    self.analytics_remote_hits.inc();
                    self.time_saved_ms.saturating_add(ev.duration);
                }
                (CacheSource::Remote, CacheEvent::Miss) => self.analytics_remote_misses.inc(),
            }
        }

        let mut store = self.events.lock().unwrap_or_else(|e| e.into_inner());
        store.seq += 1;
        let seq = store.seq;
        if !store.namespaces.contains_key(namespace)
            && store.namespaces.len() >= MAX_EVENT_NAMESPACES
            && let Some(oldest) = store
                .namespaces
                .iter()
                .min_by_key(|(_, ns)| ns.last_write)
                .map(|(name, _)| name.clone())
        {
            store.namespaces.remove(&oldest);
        }
        let entry = store.namespaces.entry(namespace.into()).or_default();
        entry.last_write = seq;
        let skip = incoming.len().saturating_sub(MAX_STORED_EVENTS);
        for ev in incoming.into_iter().skip(skip) {
            if entry.events.len() >= MAX_STORED_EVENTS {
                entry.events.pop_front();
            }
            entry.events.push_back(ev);
        }
    }

    /// Returns the retained analytics events of `namespace` (at most [`MAX_STORED_EVENTS`]).
    pub fn recent_events(&self, namespace: &str) -> Vec<AnalyticsEvent> {
        let store = self.events.lock().unwrap_or_else(|e| e.into_inner());
        store
            .namespaces
            .get(namespace)
            .map(|ns| ns.events.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Renders all metrics in the Prometheus text exposition format.
    pub fn render_prometheus(
        &self,
        backend_name: &str,
        cache_stats: Option<CacheStatsSnapshot>,
    ) -> String {
        let b = backend_name;
        let mut out = String::with_capacity(8 * 1024);
        let mut counter = |name: &str, help: &str, value: u64| {
            let _ = write!(
                out,
                "# HELP {name} {help}\n# TYPE {name} counter\n{name}{{backend=\"{b}\"}} {value}\n"
            );
        };
        counter(
            "turbo_cache_get_hits_total",
            "Total number of GET artifact cache hits",
            self.get_hits.get(),
        );
        counter(
            "turbo_cache_get_misses_total",
            "Total number of GET artifact cache misses",
            self.get_misses.get(),
        );
        counter(
            "turbo_cache_head_hits_total",
            "Total number of HEAD artifact existence hits",
            self.head_hits.get(),
        );
        counter(
            "turbo_cache_head_misses_total",
            "Total number of HEAD artifact existence misses",
            self.head_misses.get(),
        );
        counter(
            "turbo_cache_puts_total",
            "Total number of PUT artifact uploads",
            self.puts.get(),
        );
        counter(
            "turbo_cache_bytes_uploaded_total",
            "Total bytes uploaded to remote cache",
            self.bytes_uploaded.get(),
        );
        counter(
            "turbo_cache_bytes_downloaded_total",
            "Total bytes downloaded from remote cache",
            self.bytes_downloaded.get(),
        );

        let _ = write!(
            out,
            "# HELP turbo_cache_analytics_events_total Total analytics events reported by Turborepo clients\n\
             # TYPE turbo_cache_analytics_events_total counter\n\
             turbo_cache_analytics_events_total{{backend=\"{b}\",source=\"local\",event=\"hit\"}} {}\n\
             turbo_cache_analytics_events_total{{backend=\"{b}\",source=\"local\",event=\"miss\"}} {}\n\
             turbo_cache_analytics_events_total{{backend=\"{b}\",source=\"remote\",event=\"hit\"}} {}\n\
             turbo_cache_analytics_events_total{{backend=\"{b}\",source=\"remote\",event=\"miss\"}} {}\n",
            self.analytics_local_hits.get(),
            self.analytics_local_misses.get(),
            self.analytics_remote_hits.get(),
            self.analytics_remote_misses.get(),
        );

        let _ = write!(
            out,
            "# HELP turbo_cache_time_saved_seconds_total Total task execution time saved (reported by clients) in seconds\n\
             # TYPE turbo_cache_time_saved_seconds_total counter\n\
             turbo_cache_time_saved_seconds_total{{backend=\"{b}\"}} {}\n",
            prom_f64(self.time_saved_ms.get() as f64 / 1000.0),
        );

        let _ = write!(
            out,
            "# HELP turbo_cache_uploads_in_flight Artifact uploads currently being received or stored\n\
             # TYPE turbo_cache_uploads_in_flight gauge\n\
             turbo_cache_uploads_in_flight{{backend=\"{b}\"}} {}\n\
             # HELP turbo_cache_upload_rejections_total Uploads rejected for their body (timeout, size) or for lack of an upload slot\n\
             # TYPE turbo_cache_upload_rejections_total counter\n\
             turbo_cache_upload_rejections_total{{backend=\"{b}\",reason=\"timeout\"}} {}\n\
             turbo_cache_upload_rejections_total{{backend=\"{b}\",reason=\"slot_unavailable\"}} {}\n\
             turbo_cache_upload_rejections_total{{backend=\"{b}\",reason=\"too_large\"}} {}\n",
            self.uploads_in_flight.get(),
            self.upload_timeouts.get(),
            self.upload_slot_unavailable.get(),
            self.upload_too_large.get(),
        );

        out.push_str(
            "# HELP turbo_cache_storage_operation_duration_seconds Latency of storage calls that missed the synchronous hot-cache path (GET: until the body starts streaming; PUT of an upload streamed to S3: including receiving the body)\n\
             # TYPE turbo_cache_storage_operation_duration_seconds histogram\n",
        );
        for op in StorageOp::ALL {
            let hist = &self.storage_latency[op.index()];
            let op_label = op.label();
            let mut cumulative = 0u64;
            for (i, bound) in LATENCY_BUCKETS.iter().enumerate() {
                cumulative += hist.buckets[i].load(Ordering::Relaxed);
                let _ = writeln!(
                    out,
                    "turbo_cache_storage_operation_duration_seconds_bucket{{backend=\"{b}\",operation=\"{op_label}\",le=\"{}\"}} {cumulative}",
                    prom_f64(*bound)
                );
            }
            let count = hist.count.load(Ordering::Relaxed);
            let sum = hist.sum_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0;
            let _ = write!(
                out,
                "turbo_cache_storage_operation_duration_seconds_bucket{{backend=\"{b}\",operation=\"{op_label}\",le=\"+Inf\"}} {count}\n\
                 turbo_cache_storage_operation_duration_seconds_sum{{backend=\"{b}\",operation=\"{op_label}\"}} {}\n\
                 turbo_cache_storage_operation_duration_seconds_count{{backend=\"{b}\",operation=\"{op_label}\"}} {count}\n",
                prom_f64(sum)
            );
        }

        out.push_str(
            "# HELP turbo_cache_storage_errors_total Storage calls that failed\n\
             # TYPE turbo_cache_storage_errors_total counter\n",
        );
        for op in StorageOp::ALL {
            let _ = writeln!(
                out,
                "turbo_cache_storage_errors_total{{backend=\"{b}\",operation=\"{}\"}} {}",
                op.label(),
                self.storage_errors[op.index()].get()
            );
        }

        if let Some(stats) = cache_stats {
            render_cache_stats(&mut out, b, &stats);
        }
        out
    }
}

/// (metric name, Prometheus type, help text, value accessor)
type CacheSeries = (
    &'static str,
    &'static str,
    &'static str,
    fn(&CacheTierStats) -> u64,
);

fn render_cache_stats(out: &mut String, backend: &str, stats: &CacheStatsSnapshot) {
    let tiers: [(&str, &CacheTierStats); 2] =
        [("payload", &stats.payload), ("metadata", &stats.metadata)];
    let series: [CacheSeries; 8] = [
        (
            "turbo_cache_hot_cache_hits_total",
            "counter",
            "In-memory cache hits",
            |s| s.hits,
        ),
        (
            "turbo_cache_hot_cache_misses_total",
            "counter",
            "In-memory cache misses",
            |s| s.misses,
        ),
        (
            "turbo_cache_hot_cache_evictions_total",
            "counter",
            "In-memory cache entries evicted to make room (capacity evictions)",
            |s| s.evictions,
        ),
        (
            "turbo_cache_hot_cache_expirations_total",
            "counter",
            "In-memory cache entries dropped because their TTL elapsed",
            |s| s.expirations,
        ),
        (
            "turbo_cache_hot_cache_bytes",
            "gauge",
            "Bytes currently held by the in-memory cache",
            |s| s.bytes,
        ),
        (
            "turbo_cache_hot_cache_entries",
            "gauge",
            "Entries currently held by the in-memory cache",
            |s| s.entries,
        ),
        (
            "turbo_cache_hot_cache_max_bytes",
            "gauge",
            "Configured byte budget of the in-memory cache",
            |s| s.max_bytes,
        ),
        (
            "turbo_cache_hot_cache_max_entries",
            "gauge",
            "Configured entry budget of the in-memory cache",
            |s| s.max_entries,
        ),
    ];
    for (name, kind, help, value) in series {
        let _ = write!(out, "# HELP {name} {help}\n# TYPE {name} {kind}\n");
        for (tier, tier_stats) in tiers {
            let _ = writeln!(
                out,
                "{name}{{backend=\"{backend}\",tier=\"{tier}\"}} {}",
                value(tier_stats)
            );
        }
    }
}

/// Formats a float for Prometheus (always with a decimal point, no exponent for our ranges).
fn prom_f64(v: f64) -> String {
    let s = format!("{v}");
    if s.contains('.') || s.contains('e') || s.contains("inf") || s.contains("NaN") {
        s
    } else {
        format!("{s}.0")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(hash: &str, session: Option<&str>) -> AnalyticsEvent {
        AnalyticsEvent {
            session_id: session.map(str::to_string),
            source: CacheSource::Remote,
            event: CacheEvent::Hit,
            hash: hash.to_string(),
            duration: 10,
        }
    }

    #[test]
    fn event_buffers_are_bounded() {
        let m = CacheMetrics::new();
        let batch: Vec<_> = (0..MAX_STORED_EVENTS + 5)
            .map(|i| event(&format!("h{i}"), None))
            .collect();
        m.record_analytics("a", batch);
        let events = m.recent_events("a");
        assert_eq!(events.len(), MAX_STORED_EVENTS);
        assert_eq!(events[0].hash, "h5");

        for i in 0..MAX_EVENT_NAMESPACES + 3 {
            m.record_analytics(&format!("ns{i}"), vec![event("x", None)]);
        }
        let store = m.events.lock().unwrap();
        assert_eq!(store.namespaces.len(), MAX_EVENT_NAMESPACES);
        // The least recently written namespaces were evicted first.
        assert!(!store.namespaces.contains_key("a"));
        assert!(store.namespaces.contains_key("ns34"));
    }

    #[test]
    fn oversized_or_non_ascii_boundary_fields_are_rejected_without_panic() {
        // 255 ASCII bytes + a 2-byte char: byte 256 is not a char boundary.
        let tricky = format!("{}é", "a".repeat(255));
        assert!(validate_analytics_events(&[event(&tricky, None)]).is_err());
        assert!(validate_analytics_events(&[event("h", Some(&tricky))]).is_err());
        let exact = "a".repeat(MAX_EVENT_FIELD_LEN);
        assert!(validate_analytics_events(&[event(&exact, Some(&exact))]).is_ok());

        let m = CacheMetrics::new();
        m.record_analytics("a", vec![event(&tricky, None), event("ok", None)]);
        assert_eq!(m.recent_events("a"), vec![event("ok", None)]);
    }

    #[test]
    fn time_saved_does_not_wrap() {
        let m = CacheMetrics::new();
        let mut ev = event("h", None);
        ev.duration = u64::MAX;
        m.record_analytics("a", vec![ev.clone(), ev]);
        assert_eq!(m.time_saved_ms.get(), u64::MAX);
    }

    #[test]
    fn prometheus_output_renders_all_series() {
        let m = Arc::new(CacheMetrics::new());
        m.observe_storage(StorageOp::Get, Duration::from_millis(3), true);
        m.observe_storage(StorageOp::Put, Duration::from_millis(30), false);
        let _guard = m.upload_started();
        m.record_analytics("a", vec![event("h", None)]);
        let stats = CacheStatsSnapshot {
            payload: CacheTierStats {
                hits: 5,
                expirations: 3,
                bytes: 42,
                ..Default::default()
            },
            metadata: CacheTierStats {
                entries: 9,
                ..Default::default()
            },
        };
        let text = m.render_prometheus("s3", Some(stats));
        assert!(text.contains("turbo_cache_uploads_in_flight{backend=\"s3\"} 1"));
        assert!(text.contains(
            "turbo_cache_storage_operation_duration_seconds_bucket{backend=\"s3\",operation=\"get\",le=\"0.005\"} 1"
        ));
        assert!(text.contains(
            "turbo_cache_storage_operation_duration_seconds_count{backend=\"s3\",operation=\"put\"} 1"
        ));
        assert!(
            text.contains("turbo_cache_storage_errors_total{backend=\"s3\",operation=\"put\"} 1")
        );
        assert!(text.contains("turbo_cache_time_saved_seconds_total{backend=\"s3\"} 0.01"));
        assert!(text.contains(
            "turbo_cache_analytics_events_total{backend=\"s3\",source=\"remote\",event=\"hit\"} 1"
        ));
        assert!(
            text.contains("turbo_cache_hot_cache_hits_total{backend=\"s3\",tier=\"payload\"} 5")
        );
        assert!(text.contains("turbo_cache_hot_cache_bytes{backend=\"s3\",tier=\"payload\"} 42"));
        assert!(text.contains(
            "turbo_cache_hot_cache_expirations_total{backend=\"s3\",tier=\"payload\"} 3"
        ));
        assert!(text.contains("turbo_cache_hot_cache_entries{backend=\"s3\",tier=\"metadata\"} 9"));
        drop(_guard);
        assert_eq!(m.uploads_in_flight.get(), 0);
        assert!(
            !m.render_prometheus("s3", None)
                .contains("turbo_cache_hot_cache_hits_total")
        );
    }
}
