use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// Caching status values recognized by Turborepo (`turborepo-vercel-api`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum CachingStatus {
    Disabled,
    #[default]
    Enabled,
    OverLimit,
    Paused,
}

impl std::str::FromStr for CachingStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "enabled" => Ok(Self::Enabled),
            "disabled" => Ok(Self::Disabled),
            "over_limit" | "overlimit" => Ok(Self::OverLimit),
            "paused" => Ok(Self::Paused),
            other => Err(format!(
                "Invalid caching status '{other}', expected one of: enabled, disabled, over_limit, paused"
            )),
        }
    }
}

/// Response for `GET /v8/artifacts/status`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachingStatusResponse {
    pub status: CachingStatus,
    /// The token may read but not upload or delete artifacts.
    #[serde(rename = "readOnly", default)]
    pub read_only: bool,
}

/// Metadata associated with a stored Turborepo cache artifact.
///
/// String fields use `Arc<str>` so cloning metadata on cache hits requires zero heap allocations.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactMetadata {
    /// Task duration in milliseconds (`x-artifact-duration`).
    pub duration: u64,
    /// Size of the artifact payload in bytes (`Content-Length`).
    pub size: u64,
    /// Optional HMAC-SHA256 artifact signature tag (`x-artifact-tag`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tag: Option<Arc<str>>,
    /// Optional git commit SHA (`x-artifact-sha`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha: Option<Arc<str>>,
    /// Optional git dirty working tree hash (`x-artifact-dirty-hash`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dirty_hash: Option<Arc<str>>,
    /// Optional CI client identifier (`x-artifact-client-ci`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_ci: Option<Arc<str>>,
}

impl ArtifactMetadata {
    /// Returns the total byte length of optional string fields stored in this metadata entry.
    #[inline]
    pub fn allocated_bytes(&self) -> usize {
        self.tag.as_ref().map_or(0, |s| s.len())
            + self.sha.as_ref().map_or(0, |s| s.len())
            + self.dirty_hash.as_ref().map_or(0, |s| s.len())
            + self.client_ci.as_ref().map_or(0, |s| s.len())
    }
}

/// Request payload for `POST /v8/artifacts` batch artifact query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactQueryRequest {
    pub hashes: Vec<String>,
}

/// Metadata entry returned for a cache hit in `POST /v8/artifacts`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactQueryHit {
    pub size: u64,
    pub task_duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tag: Option<Arc<str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha: Option<Arc<str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dirty_hash: Option<Arc<str>>,
}

impl From<ArtifactMetadata> for ArtifactQueryHit {
    fn from(meta: ArtifactMetadata) -> Self {
        Self {
            size: meta.size,
            task_duration_ms: meta.duration,
            tag: meta.tag,
            sha: meta.sha,
            dirty_hash: meta.dirty_hash,
        }
    }
}

/// Response returned by `PUT /v8/artifacts/{hash}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PutArtifactResponse {
    pub urls: Vec<String>,
}

/// Source of a cache event reported to `POST /v8/artifacts/events`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum CacheSource {
    Local,
    Remote,
}

/// Type of cache event reported to `POST /v8/artifacts/events`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum CacheEvent {
    Hit,
    Miss,
}

/// Analytics event sent by Turborepo to `POST /v8/artifacts/events`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnalyticsEvent {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub source: CacheSource,
    pub event: CacheEvent,
    pub hash: String,
    #[serde(default)]
    pub duration: u64,
}
