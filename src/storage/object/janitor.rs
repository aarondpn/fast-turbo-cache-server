//! Retention for the object stores (`STORAGE_MAX_AGE_SECS`, `STORAGE_MAX_BYTES`).
//!
//! A run lists the prefix twice. The first listing sums the artifact bytes per minute of
//! `last_modified`, which yields the cutoff: artifacts older than the max age, then the oldest
//! minutes until the rest fits the budget. The second listing deletes everything before the
//! cutoff, and in the cutoff minute the oldest artifacts as far as needed. Memory is bounded by the
//! number of distinct minutes plus the artifacts of one minute, not by the bucket size.
//!
//! Object stores have no access time, so the age counts from the upload. Only artifact keys
//! (`[prefix/]<namespace>/<hash>` with a valid namespace and hash) are counted and deleted, never
//! the health check key.

use std::{
    collections::{BTreeMap, HashMap},
    time::{Duration, Instant},
};

use chrono::{DateTime, TimeDelta, Utc};
use futures_util::{Stream, StreamExt, TryStreamExt};
use object_store::{ObjectMeta, path::Path as ObjectPath};

use super::{Failure, ObjectStorage, parse_key};
use crate::{
    error::{AppError, AppResult},
    storage::RetentionOptions,
};

/// Keys per delete call (`DeleteObjects` accepts up to 1000).
const DELETE_BATCH: usize = 1000;

/// Result of one janitor run (see [`ObjectStorage::run_cleanup`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CleanupReport {
    /// Artifacts found by the first listing.
    pub listed: u64,
    /// Bytes of the listed artifacts.
    pub listed_bytes: u64,
    /// Artifacts deleted (expired or evicted).
    pub removed: u64,
    pub removed_bytes: u64,
    /// Deletes that failed (retried by the next run).
    pub failed: u64,
}

impl ObjectStorage {
    /// Spawns the janitor if `options` set a limit. It runs at once and then every interval, for
    /// as long as the runtime runs.
    pub fn spawn_janitor(&self, options: RetentionOptions) {
        if options.max_bytes.is_none() && options.max_age.is_none() {
            return;
        }
        let storage = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(options.interval.max(Duration::from_secs(1)));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                let started = Instant::now();
                match storage.run_cleanup(&options).await {
                    Ok(report) => tracing::info!(
                        backend = storage.options.name,
                        listed = report.listed,
                        listed_bytes = report.listed_bytes,
                        removed = report.removed,
                        removed_bytes = report.removed_bytes,
                        failed = report.failed,
                        duration_ms = started.elapsed().as_millis() as u64,
                        "Storage cleanup finished"
                    ),
                    Err(e) => tracing::warn!(
                        backend = storage.options.name,
                        error = %e,
                        "Storage cleanup failed; retrying at the next interval"
                    ),
                }
            }
        });
    }

    /// Runs one janitor pass now (the background task calls this periodically).
    pub async fn run_cleanup(&self, options: &RetentionOptions) -> AppResult<CleanupReport> {
        let mut report = CleanupReport::default();
        let mut survey = Survey::new(Utc::now(), options.max_age);
        let mut artifacts = self.artifacts()?;
        while let Some(meta) = artifacts.try_next().await? {
            report.listed += 1;
            report.listed_bytes += meta.size;
            survey.add(meta.last_modified, meta.size);
        }
        let Some(cutoff) = survey.cutoff(options.max_bytes) else {
            return Ok(report);
        };

        let mut batch = Vec::with_capacity(DELETE_BATCH);
        let mut boundary = Vec::new();
        let mut artifacts = self.artifacts()?;
        while let Some(meta) = artifacts.try_next().await? {
            match cutoff.verdict(meta.last_modified) {
                Verdict::Delete => batch.push(meta),
                Verdict::Boundary => boundary.push(meta),
                Verdict::Keep => continue,
            }
            if batch.len() == DELETE_BATCH {
                self.delete_batch(std::mem::take(&mut batch), &mut report)
                    .await;
            }
        }
        boundary.sort_by_key(|meta| meta.last_modified);
        let mut to_free = cutoff.boundary_bytes();
        for meta in boundary {
            if to_free == 0 {
                break;
            }
            to_free = to_free.saturating_sub(meta.size);
            batch.push(meta);
            if batch.len() == DELETE_BATCH {
                self.delete_batch(std::mem::take(&mut batch), &mut report)
                    .await;
            }
        }
        self.delete_batch(batch, &mut report).await;
        Ok(report)
    }

    /// Lists the artifacts under the prefix.
    fn artifacts(&self) -> AppResult<impl Stream<Item = AppResult<ObjectMeta>> + '_> {
        let prefix = if self.options.prefix.is_empty() {
            ObjectPath::default()
        } else {
            parse_key(&self.options.prefix).map_err(AppError::Internal)?
        };
        let listing = self.store.list(Some(&prefix));
        Ok(listing
            .map(move |result| {
                self.observe(result)
                    .map_err(|failure| self.failure_error("LIST", &prefix, &failure))
            })
            .try_filter(|meta| std::future::ready(self.is_artifact(&meta.location))))
    }

    /// Whether `path` is an artifact key (`[prefix/]<namespace>/<hash>`) other than the health
    /// check key.
    fn is_artifact(&self, path: &ObjectPath) -> bool {
        let key = path.as_ref();
        if self.options.health_check_key.as_deref() == Some(key) {
            return false;
        }
        let mut parts = key.rsplitn(3, '/');
        let (Some(hash), Some(namespace)) = (parts.next(), parts.next()) else {
            return false;
        };
        self.object_key(namespace, hash)
            .is_ok_and(|expected| expected == key)
    }

    /// Deletes `batch`; objects that are already gone count as deleted.
    async fn delete_batch(&self, batch: Vec<ObjectMeta>, report: &mut CleanupReport) {
        if batch.is_empty() {
            return;
        }
        let mut pending: HashMap<String, u64> = batch
            .iter()
            .map(|meta| (meta.location.to_string(), meta.size))
            .collect();
        let paths = futures_util::stream::iter(batch.into_iter().map(|meta| Ok(meta.location)));
        let mut results = self.store.delete_stream(paths.boxed());
        let mut error = None;
        while let Some(result) = results.next().await {
            let deleted = match result {
                Ok(path) => path.to_string(),
                Err(object_store::Error::NotFound { path, .. }) => path,
                Err(err) => {
                    error.get_or_insert(err);
                    continue;
                }
            };
            if let Some(size) = pending.remove(&deleted) {
                report.removed += 1;
                report.removed_bytes += size;
            }
        }
        if !pending.is_empty() {
            report.failed += pending.len() as u64;
            tracing::warn!(
                backend = self.options.name,
                failed = pending.len(),
                error = error.map(|e| Failure::new(e).to_string()),
                "Failed to delete artifacts during storage cleanup"
            );
        }
    }
}

/// Bytes per minute of `last_modified` of the artifacts that are not expired.
struct Survey {
    expire_before: Option<DateTime<Utc>>,
    expired: u64,
    minutes: BTreeMap<i64, u64>,
    total: u64,
}

impl Survey {
    fn new(now: DateTime<Utc>, max_age: Option<Duration>) -> Self {
        Self {
            expire_before: max_age
                .and_then(|age| TimeDelta::from_std(age).ok())
                .and_then(|age| now.checked_sub_signed(age)),
            expired: 0,
            minutes: BTreeMap::new(),
            total: 0,
        }
    }

    fn add(&mut self, last_modified: DateTime<Utc>, size: u64) {
        if self
            .expire_before
            .is_some_and(|before| last_modified < before)
        {
            self.expired += 1;
            return;
        }
        *self.minutes.entry(minute_of(last_modified)).or_default() += size;
        self.total += size;
    }

    /// What to delete; `None` when nothing is expired and the rest fits `max_bytes`.
    fn cutoff(&self, max_bytes: Option<u64>) -> Option<Cutoff> {
        let mut evict = None;
        if let Some(max_bytes) = max_bytes
            && self.total > max_bytes
        {
            let mut to_free = self.total - max_bytes;
            for (&minute, &bytes) in &self.minutes {
                if bytes >= to_free {
                    evict = Some((minute, to_free));
                    break;
                }
                to_free -= bytes;
            }
        }
        (self.expired > 0 || evict.is_some()).then_some(Cutoff {
            expire_before: self.expire_before,
            evict,
        })
    }
}

/// Artifacts modified before `expire_before` or in a minute before `evict.0` are deleted; in minute
/// `evict.0` the oldest ones until `evict.1` bytes are freed.
#[derive(Debug, PartialEq, Eq)]
struct Cutoff {
    expire_before: Option<DateTime<Utc>>,
    evict: Option<(i64, u64)>,
}

#[derive(Debug, PartialEq, Eq)]
enum Verdict {
    Delete,
    /// In the cutoff minute: deleted oldest-first as far as needed.
    Boundary,
    Keep,
}

impl Cutoff {
    fn verdict(&self, last_modified: DateTime<Utc>) -> Verdict {
        if self
            .expire_before
            .is_some_and(|before| last_modified < before)
        {
            return Verdict::Delete;
        }
        match self.evict {
            Some((minute, _)) if minute_of(last_modified) < minute => Verdict::Delete,
            Some((minute, _)) if minute_of(last_modified) == minute => Verdict::Boundary,
            _ => Verdict::Keep,
        }
    }

    fn boundary_bytes(&self) -> u64 {
        self.evict.map_or(0, |(_, bytes)| bytes)
    }
}

fn minute_of(time: DateTime<Utc>) -> i64 {
    time.timestamp().div_euclid(60)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cutoff_from_age_and_size_rules() {
        let now = DateTime::parse_from_rfc3339("2026-10-18T12:00:30Z")
            .unwrap()
            .to_utc();
        let ago = |minutes: i64| now - TimeDelta::minutes(minutes);
        let survey = |max_age_mins: Option<u64>, entries: &[(i64, u64)]| {
            let max_age = max_age_mins.map(|m| Duration::from_secs(m * 60));
            let mut survey = Survey::new(now, max_age);
            for &(minutes, size) in entries {
                survey.add(ago(minutes), size);
            }
            survey
        };
        let entries = [
            (120, 100),
            (50, 100),
            (40, 100),
            (30, 60),
            (30, 40),
            (20, 100),
        ];

        // Nothing to delete: nothing expired, the total fits.
        assert_eq!(survey(None, &entries).cutoff(None), None);
        assert_eq!(survey(None, &entries).cutoff(Some(500)), None);
        assert_eq!(survey(Some(180), &entries).cutoff(Some(500)), None);

        // Age rule only.
        let cutoff = survey(Some(60), &entries).cutoff(None).unwrap();
        assert_eq!(cutoff.verdict(ago(120)), Verdict::Delete);
        assert_eq!(cutoff.verdict(ago(59)), Verdict::Keep);
        assert_eq!(cutoff.boundary_bytes(), 0);

        // Size rule only: 500 bytes, budget 250: the 120 and 50 minute old minutes go entirely,
        // in the 40 minute old one 50 bytes are needed.
        let cutoff = survey(None, &entries).cutoff(Some(250)).unwrap();
        assert_eq!(cutoff.verdict(ago(120)), Verdict::Delete);
        assert_eq!(cutoff.verdict(ago(50)), Verdict::Delete);
        assert_eq!(cutoff.verdict(ago(40)), Verdict::Boundary);
        assert_eq!(cutoff.boundary_bytes(), 50);
        assert_eq!(cutoff.verdict(ago(30)), Verdict::Keep);

        // Both: expired bytes do not count against the budget (400 left, budget 300).
        let cutoff = survey(Some(60), &entries).cutoff(Some(300)).unwrap();
        assert_eq!(cutoff.verdict(ago(120)), Verdict::Delete);
        assert_eq!(cutoff.verdict(ago(50)), Verdict::Boundary);
        assert_eq!(cutoff.boundary_bytes(), 100);
        assert_eq!(cutoff.verdict(ago(40)), Verdict::Keep);
        // A budget the remainder exactly fits needs only the age rule.
        let cutoff = survey(Some(60), &entries).cutoff(Some(400)).unwrap();
        assert_eq!(cutoff.evict, None);
    }
}
