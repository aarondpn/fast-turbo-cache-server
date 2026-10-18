use std::{
    collections::HashSet,
    fmt,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::Context as _;
use clap::{
    ArgAction, CommandFactory, FromArgMatches, Parser, Subcommand, ValueEnum,
    builder::BoolishValueParser,
};

use crate::{
    models::CachingStatus,
    oidc::{self, CacheTokenKey, ClaimRules, OidcConfig},
    storage::{CacheOptions, LocalOptions, RetentionOptions, sanitize_namespace},
};

/// Largest multipart upload of the object store backends: 10000 parts of 8 MiB.
pub const S3_MAX_MULTIPART_BYTES: u64 = crate::storage::object::MAX_MULTIPART_UPLOAD_BYTES;
pub const DEFAULT_BATCH_HEAD_CONCURRENCY: usize = crate::routes::DEFAULT_BATCH_HEAD_CONCURRENCY;

/// Supported storage backend types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
pub enum StorageBackendKind {
    /// Local filesystem storage.
    #[default]
    Local,
    /// Generic S3-compatible storage (AWS S3, Cloudflare R2, Ceph, etc.).
    S3,
    /// RustFS S3-compatible distributed object storage preset.
    Rustfs,
    /// Garage S3-compatible geo-distributed object storage preset.
    Garage,
    /// Google Cloud Storage.
    Gcs,
    /// Azure Blob Storage.
    Azure,
}

impl StorageBackendKind {
    /// Largest single upload request of an object store backend and its display text (`None`
    /// for `local`). Applies with `IMMUTABLE_ARTIFACTS`, whose conditional writes are single
    /// requests.
    pub fn single_put_limit(self) -> Option<(u64, &'static str)> {
        match self {
            StorageBackendKind::Local => None,
            StorageBackendKind::S3 | StorageBackendKind::Rustfs | StorageBackendKind::Garage => {
                Some((crate::storage::s3::MAX_SINGLE_PUT_BYTES, "5 GiB"))
            }
            StorageBackendKind::Gcs => Some((crate::storage::gcs::MAX_SINGLE_PUT_BYTES, "5 TiB")),
            StorageBackendKind::Azure => {
                Some((crate::storage::azure::MAX_SINGLE_PUT_BYTES, "5000 MiB"))
            }
        }
    }
}

/// Log output format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
pub enum LogFormat {
    /// Human-readable text lines.
    #[default]
    Text,
    /// One JSON object per line (for log aggregation).
    Json,
}

/// Optional sub-commands. Without a sub-command the server is started.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Subcommand)]
pub enum Command {
    /// Probe `GET /healthz` of the server running on this host (uses HOST/PORT; an unspecified
    /// HOST such as 0.0.0.0 or :: is probed via loopback). Exits 0 when healthy, 1 otherwise.
    /// Intended for container HEALTHCHECKs (no curl needed).
    Healthcheck,
}

/// A secret CLI/env value: `Debug` prints `"[REDACTED]"` instead of the value.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(pub String);

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt("[REDACTED]", f)
    }
}

impl std::str::FromStr for Secret {
    type Err = std::convert::Infallible;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_string()))
    }
}

impl std::ops::Deref for Secret {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

/// An endpoint URL from the CLI/env: `Debug` prints it with any `user:pass@` redacted.
#[derive(Clone, PartialEq, Eq)]
pub struct RedactedUrl(pub String);

impl fmt::Debug for RedactedUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&redact_url_userinfo(&self.0), f)
    }
}

impl std::str::FromStr for RedactedUrl {
    type Err = std::convert::Infallible;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_string()))
    }
}

impl std::ops::Deref for RedactedUrl {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

/// Every `bool`/`Option<bool>` option accepts `--flag`, `--flag=<bool>` and boolish env values.
fn boolish_flag(arg: clap::Arg) -> clap::Arg {
    if arg.get_value_parser().type_id() != clap::builder::ValueParser::bool().type_id() {
        return arg;
    }
    arg.action(ArgAction::Set)
        .num_args(0..=1)
        .require_equals(true)
        .default_missing_value("true")
        .value_parser(BoolishValueParser::new())
}

/// CLI and environment configuration for `fast-turbo-cache-server`.
#[derive(Clone, Debug, Parser)]
#[command(
    name = "fast-turbo-cache-server",
    version,
    about = "Self-hosted Turborepo remote cache server in Rust for RustFS, Garage, S3, Google Cloud Storage, Azure Blob Storage and local storage",
    long_about = None,
    after_help = "Boolean options accept `--flag`, `--flag=true` and `--flag=false` (also 1/0, yes/no, on/off); the matching environment variables accept the same values.",
    mut_args = boolish_flag
)]
pub struct CliConfig {
    #[command(subcommand)]
    pub command: Option<Command>,

    /// Host address to bind the HTTP server to (IPv4 or IPv6, e.g. `0.0.0.0`, `::`, `[::1]`,
    /// `localhost`).
    #[arg(long, env = "HOST", default_value = "0.0.0.0", global = true)]
    pub host: String,

    /// Port to listen on.
    #[arg(short, long, env = "PORT", default_value_t = 3000, global = true)]
    pub port: u16,

    /// Comma-separated list of valid Bearer authentication tokens.
    /// Required unless `--allow-unauthenticated` is enabled. Prefer `TURBO_TOKEN_FILE` so tokens
    /// do not show up in the process list.
    #[arg(
        long,
        env = "TURBO_TOKEN",
        hide_env_values = true,
        value_delimiter = ','
    )]
    pub tokens: Vec<Secret>,

    /// File containing Bearer tokens (separated by commas or newlines; `#` starts a comment line).
    /// Tokens from the file are added to `TURBO_TOKEN`.
    #[arg(long, env = "TURBO_TOKEN_FILE")]
    pub token_file: Option<PathBuf>,

    /// Comma-separated list of read-only Bearer tokens (all namespaces): they may read and query
    /// artifacts but not upload or delete them.
    #[arg(
        long,
        env = "TURBO_READ_ONLY_TOKENS",
        hide_env_values = true,
        value_delimiter = ','
    )]
    pub read_only_tokens: Vec<Secret>,

    /// File containing read-only tokens (same format as `TURBO_TOKEN_FILE`). Tokens from the file
    /// are added to `TURBO_READ_ONLY_TOKENS`.
    #[arg(long, env = "TURBO_READ_ONLY_TOKEN_FILE")]
    pub read_only_token_file: Option<PathBuf>,

    /// Allow requests without Bearer authentication (for trusted local networks/testing).
    /// Cannot be combined with any token.
    #[arg(long, env = "ALLOW_UNAUTHENTICATED", default_value_t = false)]
    pub allow_unauthenticated: bool,

    /// Storage backend type (`local`, `s3`, `rustfs`, `garage`, `gcs`, `azure`).
    #[arg(long, env = "STORAGE_TYPE", value_enum, default_value_t = StorageBackendKind::Local)]
    pub storage_type: StorageBackendKind,

    /// Base directory for local filesystem storage (`STORAGE_TYPE=local`).
    #[arg(long, env = "LOCAL_STORAGE_PATH", default_value = "./.turbo-cache")]
    pub local_storage_path: PathBuf,

    /// Storage budget in bytes; the oldest artifacts are evicted beyond it (0 = unlimited).
    #[arg(long, env = "STORAGE_MAX_BYTES", default_value_t = 0)]
    pub storage_max_bytes: u64,

    /// Delete artifacts older than this many seconds (0 = never). Local: since the last write or
    /// read; object stores: since the upload.
    #[arg(long, env = "STORAGE_MAX_AGE_SECS", default_value_t = 0)]
    pub storage_max_age_secs: u64,

    /// Interval in seconds between storage cleanup runs (also the interval of the orphaned spool
    /// file sweep in `SPOOL_DIR`, for every backend).
    #[arg(long, env = "STORAGE_CLEANUP_INTERVAL_SECS", default_value_t = 600)]
    pub storage_cleanup_interval_secs: u64,

    /// Orphaned local temp files older than this many seconds are deleted. Orphaned upload spool
    /// files in `SPOOL_DIR` are deleted once older than this or 2 x `BODY_TOTAL_TIMEOUT_SECS`,
    /// whichever is larger.
    #[arg(long, env = "LOCAL_TEMP_MAX_AGE_SECS", default_value_t = 3600)]
    pub local_temp_max_age_secs: u64,

    /// Optional key prefix inside the bucket or container of the object-store backends (`s3`,
    /// `rustfs`, `garage`, `gcs`, `azure`; ignored by `local`).
    #[arg(long, env = "STORAGE_PREFIX", default_value = "")]
    pub storage_prefix: String,

    /// Maximum number of retries for failed object-store requests (5xx, 408, 429, connection
    /// errors).
    #[arg(long, env = "STORAGE_MAX_RETRIES", default_value_t = 3)]
    pub storage_max_retries: u32,

    /// Multipart upload parts (8 MiB each) sent to the object store concurrently per upload.
    /// Each upload holds up to this + 1 parts in memory (the extra one is being received).
    /// `1`-`16`.
    #[arg(long, env = "STORAGE_PARTS_IN_FLIGHT", default_value_t = 4)]
    pub storage_parts_in_flight: usize,

    /// Object-store TCP/TLS connect timeout in seconds.
    #[arg(long, env = "STORAGE_CONNECT_TIMEOUT_SECS", default_value_t = 5)]
    pub storage_connect_timeout_secs: u64,

    /// Object-store read (idle) timeout in seconds: maximum time without receiving data from the
    /// object store.
    #[arg(long, env = "STORAGE_READ_TIMEOUT_SECS", default_value_t = 30)]
    pub storage_read_timeout_secs: u64,

    /// S3 bucket name (`STORAGE_TYPE=s3|rustfs|garage`). Also falls back to `AWS_BUCKET`.
    #[arg(long, env = "S3_BUCKET")]
    pub s3_bucket: Option<String>,

    /// S3 region. Falls back to `AWS_REGION`/`AWS_DEFAULT_REGION`, then `"garage"` when
    /// `STORAGE_TYPE=garage`, or `"us-east-1"` otherwise.
    #[arg(long, env = "S3_REGION")]
    pub s3_region: Option<String>,

    /// Custom S3 endpoint URL (e.g. `http://localhost:9000` for RustFS or `http://localhost:3900`
    /// for Garage). Also falls back to `AWS_ENDPOINT_URL`. Must not contain credentials
    /// (`user:pass@`).
    #[arg(long, env = "S3_ENDPOINT", hide_env_values = true)]
    pub s3_endpoint: Option<RedactedUrl>,

    /// S3 Access Key ID. Also falls back to `AWS_ACCESS_KEY_ID`.
    #[arg(long, env = "S3_ACCESS_KEY_ID", hide_env_values = true)]
    pub s3_access_key_id: Option<Secret>,

    /// S3 Secret Access Key. Also falls back to `AWS_SECRET_ACCESS_KEY`. Prefer
    /// `S3_SECRET_ACCESS_KEY_FILE` so the secret does not show up in the process list.
    #[arg(long, env = "S3_SECRET_ACCESS_KEY", hide_env_values = true)]
    pub s3_secret_access_key: Option<Secret>,

    /// File containing the S3 Secret Access Key (surrounding whitespace is trimmed).
    /// Mutually exclusive with `S3_SECRET_ACCESS_KEY`.
    #[arg(long, env = "S3_SECRET_ACCESS_KEY_FILE")]
    pub s3_secret_access_key_file: Option<PathBuf>,

    /// Optional S3 session token. Also falls back to `AWS_SESSION_TOKEN`.
    #[arg(long, env = "S3_SESSION_TOKEN", hide_env_values = true)]
    pub s3_session_token: Option<Secret>,

    /// Force path-style S3 addressing (`http://endpoint/bucket/key`).
    /// Enabled automatically for `rustfs`, `garage`, when a custom endpoint (`S3_ENDPOINT` or
    /// `AWS_ENDPOINT_URL`) is set and for bucket names containing `.`, unless overridden.
    #[arg(long, env = "S3_FORCE_PATH_STYLE")]
    pub s3_force_path_style: Option<bool>,

    /// Compute full SHA-256 payload digest for S3 uploads (`PutObject`, `UploadPart`) instead of
    /// `UNSIGNED-PAYLOAD`. Useful for S3 backends or policies that reject `UNSIGNED-PAYLOAD`.
    #[arg(long, env = "S3_SIGN_PAYLOAD", default_value_t = false)]
    pub s3_sign_payload: bool,

    /// Treat HTTP 403 on object GET/HEAD as a cache miss (for IAM policies without `s3:ListBucket`,
    /// where S3 answers 403 instead of 404 for missing objects).
    #[arg(long, env = "S3_TREAT_403_AS_MISS", default_value_t = false)]
    pub s3_treat_403_as_miss: bool,

    /// Object key probed by `/readyz` with a 1-byte `GET` instead of listing `STORAGE_PREFIX`
    /// (which needs `s3:ListBucket`). The key is relative to the bucket root (`STORAGE_PREFIX` is
    /// not applied); a missing object counts as healthy.
    #[arg(long, env = "S3_HEALTH_CHECK_KEY")]
    pub s3_health_check_key: Option<String>,

    /// GCS bucket name (`STORAGE_TYPE=gcs`). Also falls back to `GOOGLE_BUCKET`.
    #[arg(long, env = "GCS_BUCKET")]
    pub gcs_bucket: Option<String>,

    /// GCS service account key file (JSON). Also falls back to `GOOGLE_SERVICE_ACCOUNT`. Without
    /// it, Application Default Credentials are used: `GOOGLE_APPLICATION_CREDENTIALS`, the gcloud
    /// CLI login, then the metadata server (GCE, GKE Workload Identity).
    #[arg(long, env = "GCS_SERVICE_ACCOUNT_KEY_FILE")]
    pub gcs_service_account_key_file: Option<PathBuf>,

    /// Azure storage account name (`STORAGE_TYPE=azure`). Also falls back to
    /// `AZURE_STORAGE_ACCOUNT_NAME`.
    #[arg(long, env = "AZURE_STORAGE_ACCOUNT")]
    pub azure_storage_account: Option<String>,

    /// Azure Blob Storage container name. Also falls back to `AZURE_CONTAINER_NAME`.
    #[arg(long, env = "AZURE_CONTAINER")]
    pub azure_container: Option<String>,

    /// Azure storage account access key. Also falls back to `AZURE_STORAGE_ACCOUNT_KEY`. Prefer
    /// `AZURE_STORAGE_ACCESS_KEY_FILE` so the key does not show up in the process list. Without
    /// a key or SAS token, workload identity or managed identity is used.
    #[arg(long, env = "AZURE_STORAGE_ACCESS_KEY", hide_env_values = true)]
    pub azure_storage_access_key: Option<Secret>,

    /// File containing the Azure storage account access key (surrounding whitespace is trimmed).
    /// Mutually exclusive with `AZURE_STORAGE_ACCESS_KEY`.
    #[arg(long, env = "AZURE_STORAGE_ACCESS_KEY_FILE")]
    pub azure_storage_access_key_file: Option<PathBuf>,

    /// Azure shared access signature (SAS) token. Also falls back to `AZURE_STORAGE_SAS_KEY`.
    #[arg(long, env = "AZURE_STORAGE_SAS_TOKEN", hide_env_values = true)]
    pub azure_storage_sas_token: Option<Secret>,

    /// Azure Blob Storage endpoint URL (default: `https://<account>.blob.core.windows.net`), e.g.
    /// for sovereign clouds or Azurite (`http://127.0.0.1:10000/devstoreaccount1`).
    #[arg(long, env = "AZURE_STORAGE_ENDPOINT", hide_env_values = true)]
    pub azure_storage_endpoint: Option<RedactedUrl>,

    /// Maximum artifact upload size in bytes (default: 512 MiB). Must be > 0. For object store
    /// backends at most 10000 x 8 MiB (multipart upload); with `IMMUTABLE_ARTIFACTS=true` (single
    /// conditional `PUT`) at most 5 GiB for S3 and 5000 MiB for Azure.
    #[arg(long, env = "MAX_ARTIFACT_SIZE", default_value_t = 512 * 1024 * 1024)]
    pub max_artifact_size: usize,

    /// Maximum total bytes for the bounded in-memory LRU cache (default: 128 MiB).
    /// Set to 0 to disable the hot payload cache in front of Local/S3 backends.
    #[arg(long, env = "MEMORY_CACHE_MAX_BYTES", default_value_t = 128 * 1024 * 1024)]
    pub memory_cache_max_bytes: usize,

    /// Maximum number of artifact payloads kept in the in-memory LRU cache (default: 10000).
    #[arg(long, env = "MEMORY_CACHE_MAX_ENTRIES", default_value_t = 10_000)]
    pub memory_cache_max_entries: usize,

    /// Maximum size of a single artifact eligible for the hot in-memory payload cache
    /// (default: 4 MiB).
    #[arg(long, env = "HOT_CACHE_MAX_ITEM_BYTES", default_value_t = 4 * 1024 * 1024)]
    pub hot_cache_max_item_bytes: usize,

    /// Maximum number of artifact metadata entries kept in the in-memory metadata LRU cache.
    #[arg(long, env = "METADATA_CACHE_MAX_ENTRIES", default_value_t = 50_000)]
    pub metadata_cache_max_entries: usize,

    /// Lifetime in seconds of hot payload/metadata cache entries in front of Local/S3 backends
    /// (0 = no expiry). Bounds staleness across replicas sharing one bucket.
    #[arg(long, env = "CACHE_TTL_SECS", default_value_t = 300)]
    pub cache_ttl_secs: u64,

    /// Lifetime in seconds of cached misses (0 = misses are not cached).
    #[arg(long, env = "NEGATIVE_CACHE_TTL_SECS", default_value_t = 0)]
    pub negative_cache_ttl_secs: u64,

    /// Reported caching status on `GET /v8/artifacts/status` (`enabled`, `disabled`,
    /// `over_limit`, `paused`).
    #[arg(long, env = "CACHING_STATUS", default_value = "enabled")]
    pub caching_status: CachingStatus,

    /// Isolate artifacts by `teamId`/`slug` query parameters. Must stay enabled when scoped tokens
    /// are used.
    #[arg(long, env = "ISOLATE_TEAMS", default_value_t = true)]
    pub isolate_teams: bool,

    /// Optional comma-separated list of namespace-scoped tokens (`namespace=token` or
    /// `namespace:token`; split at the first `=` or `:`). Namespaces must be valid team/slug
    /// identifiers. A token listed for several namespaces is valid for each of them.
    #[arg(
        long,
        env = "TURBO_SCOPED_TOKENS",
        hide_env_values = true,
        value_delimiter = ','
    )]
    pub scoped_tokens: Vec<Secret>,

    /// File containing scoped tokens (same format as `TURBO_SCOPED_TOKENS`, separated by commas or
    /// newlines). Entries are added to `TURBO_SCOPED_TOKENS`.
    #[arg(long, env = "TURBO_SCOPED_TOKENS_FILE")]
    pub scoped_tokens_file: Option<PathBuf>,

    /// Optional comma-separated list of read-only namespace-scoped tokens (same format as
    /// `TURBO_SCOPED_TOKENS`).
    #[arg(
        long,
        env = "TURBO_SCOPED_READ_ONLY_TOKENS",
        hide_env_values = true,
        value_delimiter = ','
    )]
    pub scoped_read_only_tokens: Vec<Secret>,

    /// File containing read-only scoped tokens (same format as `TURBO_SCOPED_TOKENS_FILE`).
    /// Entries are added to `TURBO_SCOPED_READ_ONLY_TOKENS`.
    #[arg(long, env = "TURBO_SCOPED_READ_ONLY_TOKENS_FILE")]
    pub scoped_read_only_tokens_file: Option<PathBuf>,

    /// OIDC issuer URL (e.g. `https://token.actions.githubusercontent.com`). Enables the token
    /// exchange `POST /oidc/token`, which trades an ID token of this issuer for a cache token.
    /// Must use https (http only for loopback hosts).
    #[arg(long, env = "OIDC_ISSUER")]
    pub oidc_issuer: Option<String>,

    /// Audience the ID tokens must be issued for (`aud`). Required with `OIDC_ISSUER`.
    #[arg(long, env = "OIDC_AUDIENCE")]
    pub oidc_audience: Option<String>,

    /// Comma-separated `claim=value` rules every exchanged ID token must match, e.g.
    /// `repository_owner_id=123456`. Rules for the same claim are alternatives, rules for
    /// different claims must all match. Required with `OIDC_ISSUER`.
    #[arg(long, env = "OIDC_REQUIRED_CLAIMS", value_delimiter = ',')]
    pub oidc_required_claims: Vec<String>,

    /// `claim=value` rules (same syntax) for read-write cache tokens, e.g.
    /// `repository_id=123,ref=refs/heads/main,event_name=push`; other ID tokens get read-only
    /// cache tokens. Unset: every cache token is read-write.
    #[arg(long, env = "OIDC_WRITE_CLAIMS", value_delimiter = ',')]
    pub oidc_write_claims: Vec<String>,

    /// ID token claim whose value is the only namespace the cache token is valid for (e.g.
    /// `repository_id`). Unset: cache tokens are valid for every namespace.
    #[arg(long, env = "OIDC_NAMESPACE_CLAIM")]
    pub oidc_namespace_claim: Option<String>,

    /// Lifetime of minted cache tokens in seconds (1-86400).
    #[arg(long, env = "OIDC_TOKEN_TTL_SECS", default_value_t = 3600)]
    pub oidc_token_ttl_secs: u64,

    /// Base64-encoded key of at least 32 bytes (`openssl rand -base64 32`) for signing minted
    /// cache tokens (HMAC-SHA256); all replicas need the same key, other deployments their own.
    /// Required with `OIDC_ISSUER`. Prefer `OIDC_SIGNING_KEY_FILE`.
    #[arg(long, env = "OIDC_SIGNING_KEY", hide_env_values = true)]
    pub oidc_signing_key: Option<Secret>,

    /// File containing the OIDC signing key (surrounding whitespace is trimmed). Mutually
    /// exclusive with `OIDC_SIGNING_KEY`.
    #[arg(long, env = "OIDC_SIGNING_KEY_FILE")]
    pub oidc_signing_key_file: Option<PathBuf>,

    /// Maximum number of concurrent artifact uploads (`PUT /v8/artifacts/{hash}`). Must be > 0.
    #[arg(long, env = "MAX_CONCURRENT_UPLOADS", default_value_t = 32)]
    pub max_concurrent_uploads: usize,

    /// Maximum number of concurrent uploads of one namespace (team), so a single tenant cannot
    /// occupy every upload slot (e.g. with uploads that trickle data until
    /// `BODY_TOTAL_TIMEOUT_SECS`). `0` (default) = automatic: half of `MAX_CONCURRENT_UPLOADS`
    /// (at least 1) when `TURBO_SCOPED_TOKENS` or `OIDC_NAMESPACE_CLAIM` are configured, otherwise
    /// no per-namespace limit. Values >= `MAX_CONCURRENT_UPLOADS` disable the limit. Namespaces
    /// sharing a scoped token count as one namespace.
    #[arg(
        long,
        env = "MAX_CONCURRENT_UPLOADS_PER_NAMESPACE",
        default_value_t = 0
    )]
    pub max_concurrent_uploads_per_namespace: usize,

    /// Maximum number of backend `HEAD` lookups in flight for batch artifact queries
    /// (`POST /v8/artifacts`), shared by all concurrent batch requests (server-wide). Must be > 0.
    #[arg(long, env = "BATCH_HEAD_CONCURRENCY", default_value_t = DEFAULT_BATCH_HEAD_CONCURRENCY)]
    pub batch_head_concurrency: usize,

    /// Seconds an upload waits for a free upload slot before failing with 429 + `Retry-After`.
    /// The wait counts towards turbo's upload timeout (`remoteCache.uploadTimeout`, default 60 s),
    /// so keep it well below that.
    #[arg(long, env = "UPLOAD_SLOT_WAIT_SECS", default_value_t = 10)]
    pub upload_slot_wait_secs: u64,

    /// Uploads up to this many bytes are buffered in RAM; larger uploads are spooled to `SPOOL_DIR`,
    /// except uploads to S3 backends with a `Content-Length` (and `IMMUTABLE_ARTIFACTS=false`),
    /// which are streamed to S3 in 8 MiB parts (multipart upload).
    #[arg(long, env = "UPLOAD_MEMORY_THRESHOLD", default_value_t = 4 * 1024 * 1024)]
    pub upload_memory_threshold: usize,

    /// Directory for spooled upload bodies and S3 credential token copies. Default:
    /// `<LOCAL_STORAGE_PATH>/.spool` for the local backend (same filesystem, so stored by rename
    /// instead of copy), otherwise the OS temp directory.
    #[arg(long, env = "SPOOL_DIR")]
    pub spool_dir: Option<PathBuf>,

    /// Reject overwriting existing artifacts (`PUT` on an existing hash returns 403
    /// `artifact_immutable`). Requires `ALLOW_DELETE=false`.
    #[arg(long, env = "IMMUTABLE_ARTIFACTS", default_value_t = false)]
    pub immutable_artifacts: bool,

    /// Allow `DELETE /v8/artifacts/{hash}` requests (disabled by default; incompatible with
    /// `IMMUTABLE_ARTIFACTS=true`).
    #[arg(long, env = "ALLOW_DELETE", default_value_t = false)]
    pub allow_delete: bool,

    /// Require authentication for `/metrics` and `/readyz` observability endpoints.
    #[arg(long, env = "PROTECT_OBSERVABILITY", default_value_t = false)]
    pub protect_observability: bool,

    /// Allowed origin for CORS responses (`Access-Control-Allow-Origin`).
    #[arg(long, env = "CORS_ALLOW_ORIGIN", default_value = "*")]
    pub cors_allow_origin: String,

    /// Abort an upload with 408 when no body data arrives for this many seconds.
    #[arg(long, env = "BODY_IDLE_TIMEOUT_SECS", default_value_t = 30)]
    pub body_idle_timeout_secs: u64,

    /// Abort an upload with 408 when waiting for its body took this many seconds in total. Time
    /// spent storing received data (e.g. S3 accepting a multipart part) does not count.
    #[arg(long, env = "BODY_TOTAL_TIMEOUT_SECS", default_value_t = 900)]
    pub body_total_timeout_secs: u64,

    /// Seconds to wait for in-flight requests on shutdown (SIGTERM/SIGINT) before closing
    /// remaining connections.
    #[arg(long, env = "SHUTDOWN_GRACE_SECS", default_value_t = 30)]
    pub shutdown_grace_secs: u64,

    /// Log output format (`text` or `json`). Log level is controlled by `RUST_LOG`.
    #[arg(long, env = "LOG_FORMAT", value_enum, default_value_t = LogFormat::Text)]
    pub log_format: LogFormat,
}

/// Resolved S3 configuration ready for the S3 client.
#[derive(Clone)]
pub struct S3StorageConfig {
    pub provider_name: &'static str,
    pub bucket: String,
    pub region: String,
    /// Endpoint base URL without trailing slash. Guaranteed (by `resolve_s3_config`) to be an
    /// http(s) URL without userinfo.
    pub endpoint: String,
    /// Static access key ID. Empty (together with `secret_access_key`) when no static
    /// credentials are configured and the default credential provider chain should be used.
    pub access_key_id: String,
    /// Static secret access key (empty when `access_key_id` is empty).
    pub secret_access_key: String,
    pub session_token: Option<String>,
    pub prefix: String,
    /// Path-style addressing. Defaults to `true` for the presets, custom endpoints and bucket
    /// names containing `.` (wildcard TLS certificates match a single label).
    pub force_path_style: bool,
    pub sign_payload: bool,
    /// Retries after the first attempt for retryable failures.
    pub max_retries: u32,
    pub parts_in_flight: usize,
    pub connect_timeout: Duration,
    /// Idle read timeout (time without receiving bytes).
    pub read_timeout: Duration,
    pub treat_403_as_miss: bool,
    pub health_check_key: Option<String>,
    /// Directory for the private copies of credential tokens (web identity, container
    /// credentials): `SPOOL_DIR`, which must be writable anyway, so a read-only `TMPDIR` only
    /// needs `SPOOL_DIR` to point elsewhere.
    pub token_dir: PathBuf,
}

impl Default for S3StorageConfig {
    fn default() -> Self {
        Self {
            provider_name: "s3",
            bucket: String::new(),
            region: "us-east-1".to_string(),
            endpoint: "https://s3.us-east-1.amazonaws.com".to_string(),
            access_key_id: String::new(),
            secret_access_key: String::new(),
            session_token: None,
            prefix: String::new(),
            force_path_style: false,
            sign_payload: false,
            max_retries: 3,
            parts_in_flight: 4,
            connect_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(30),
            treat_403_as_miss: false,
            health_check_key: None,
            token_dir: std::env::temp_dir(),
        }
    }
}

impl S3StorageConfig {
    /// Whether static credentials are configured (otherwise a credential provider chain is needed).
    pub fn has_static_credentials(&self) -> bool {
        !self.access_key_id.is_empty() && !self.secret_access_key.is_empty()
    }
}

impl fmt::Debug for S3StorageConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3StorageConfig")
            .field("provider_name", &self.provider_name)
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("endpoint", &redact_url_userinfo(&self.endpoint))
            .field("access_key_id", &"[REDACTED]")
            .field("secret_access_key", &"[REDACTED]")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field("prefix", &self.prefix)
            .field("force_path_style", &self.force_path_style)
            .field("sign_payload", &self.sign_payload)
            .field("max_retries", &self.max_retries)
            .field("parts_in_flight", &self.parts_in_flight)
            .field("connect_timeout", &self.connect_timeout)
            .field("read_timeout", &self.read_timeout)
            .field("treat_403_as_miss", &self.treat_403_as_miss)
            .field("health_check_key", &self.health_check_key)
            .field("token_dir", &self.token_dir)
            .finish()
    }
}

/// Resolved Google Cloud Storage configuration.
#[derive(Clone, Debug)]
pub struct GcsStorageConfig {
    pub bucket: String,
    pub prefix: String,
    /// Service account key file; without it, Application Default Credentials are used.
    pub service_account_key_file: Option<PathBuf>,
    /// `GOOGLE_APPLICATION_CREDENTIALS` (used without `service_account_key_file`).
    pub application_credentials: Option<PathBuf>,
    pub max_retries: u32,
    pub parts_in_flight: usize,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
}

impl Default for GcsStorageConfig {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            prefix: String::new(),
            service_account_key_file: None,
            application_credentials: None,
            max_retries: 3,
            parts_in_flight: 4,
            connect_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(30),
        }
    }
}

/// Resolved Azure Blob Storage configuration. Without an access key or SAS token, workload or
/// managed identity is used.
#[derive(Clone, Debug)]
pub struct AzureStorageConfig {
    pub account: String,
    pub container: String,
    pub prefix: String,
    /// Blob service endpoint (`None`: `https://<account>.blob.core.windows.net`).
    pub endpoint: Option<RedactedUrl>,
    pub access_key: Option<Secret>,
    pub sas_token: Option<Secret>,
    pub max_retries: u32,
    pub parts_in_flight: usize,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
}

impl Default for AzureStorageConfig {
    fn default() -> Self {
        Self {
            account: String::new(),
            container: String::new(),
            prefix: String::new(),
            endpoint: None,
            access_key: None,
            sas_token: None,
            max_retries: 3,
            parts_in_flight: 4,
            connect_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(30),
        }
    }
}

/// HTTP server / upload pipeline options (consumed by `server::serve` and `AppState`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerOptions {
    /// Upload aborted (408) when no body bytes arrive for this long.
    pub body_idle_timeout: Duration,
    /// Upload aborted (408) when the full body is not received within this time.
    pub body_total_timeout: Duration,
    /// Maximum wait for an upload slot before 429.
    pub upload_slot_wait: Duration,
    /// Graceful shutdown deadline.
    pub shutdown_grace: Duration,
    /// Directory for spooled upload bodies.
    pub spool_dir: PathBuf,
    /// Bodies up to this size stay in RAM; larger ones are spooled to `spool_dir`.
    pub upload_memory_threshold: usize,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            body_idle_timeout: Duration::from_secs(30),
            body_total_timeout: Duration::from_secs(900),
            upload_slot_wait: Duration::from_secs(10),
            shutdown_grace: Duration::from_secs(30),
            spool_dir: std::env::temp_dir(),
            upload_memory_threshold: 4 * 1024 * 1024,
        }
    }
}

/// Replaces `user:pass@` in a URL-like string with `[REDACTED]@` for safe logging.
pub fn redact_url_userinfo(raw: &str) -> String {
    let (scheme, rest) = match raw.find("://") {
        Some(idx) => (&raw[..idx + 3], &raw[idx + 3..]),
        None => ("", raw),
    };
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    match rest[..authority_end].rfind('@') {
        Some(at) => format!("{scheme}[REDACTED]@{}", &rest[at + 1..]),
        None => raw.to_string(),
    }
}

fn secs_or_none(secs: u64) -> Option<Duration> {
    (secs > 0).then(|| Duration::from_secs(secs))
}

fn non_empty(value: Option<String>) -> Option<String> {
    value.filter(|s| !s.trim().is_empty())
}

/// Splits a secret list (tokens) on commas and newlines, ignoring blank and `#` comment lines.
fn split_secret_list(content: &str) -> impl Iterator<Item = String> + '_ {
    content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .flat_map(|line| line.split(','))
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(str::to_string)
}

fn read_secret_file(path: &Path, what: &str) -> anyhow::Result<String> {
    std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("Failed to read {what} '{}': {e}", path.display()))
}

/// Reads the secret file `path` (option `<name>_FILE`) into `secret` (option `name`).
fn load_secret_file(
    secret: &mut Option<Secret>,
    path: Option<&Path>,
    name: &str,
) -> anyhow::Result<()> {
    let Some(path) = path else {
        return Ok(());
    };
    if secret.as_deref().is_some_and(|s| !s.trim().is_empty()) {
        anyhow::bail!("Set only one of {name} and {name}_FILE");
    }
    let content = read_secret_file(path, &format!("{name}_FILE"))?;
    let value = content.trim();
    if value.is_empty() {
        anyhow::bail!("{name}_FILE '{}' is empty", path.display());
    }
    *secret = Some(Secret(value.to_string()));
    Ok(())
}

/// Adds the entries of the token file `path` (option `what`) to `tokens`.
fn extend_from_token_file(
    tokens: &mut Vec<Secret>,
    path: Option<&Path>,
    what: &str,
) -> anyhow::Result<()> {
    let Some(path) = path else {
        return Ok(());
    };
    let content = read_secret_file(path, what)?;
    let before = tokens.len();
    tokens.extend(split_secret_list(&content).map(Secret));
    if tokens.len() == before {
        anyhow::bail!("{what} '{}' contains no tokens", path.display());
    }
    Ok(())
}

fn trimmed_tokens(tokens: &[Secret]) -> Vec<String> {
    tokens
        .iter()
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty())
        .collect()
}

/// Parses scoped tokens. Each entry is `namespace=token` or `namespace:token`, split at the FIRST
/// `=` or `:` (so tokens may contain either character). Namespaces are validated.
fn parse_scoped_tokens(entries: &[Secret]) -> anyhow::Result<Vec<(String, String)>> {
    let mut out = Vec::new();
    for entry in entries {
        let trimmed = entry.trim();
        if trimmed.is_empty() {
            continue;
        }
        let idx = trimmed.find([':', '=']).ok_or_else(|| {
            anyhow::anyhow!(
                "Invalid scoped token format (expected 'namespace=token' or 'namespace:token')"
            )
        })?;
        let ns = trimmed[..idx].trim();
        let tok = trimmed[idx + 1..].trim();
        if ns.is_empty() || tok.is_empty() {
            anyhow::bail!("Scoped token namespace and secret must both be non-empty");
        }
        let ns = sanitize_namespace(ns).map_err(|_| {
            anyhow::anyhow!(
                "Invalid scoped token namespace '{ns}': use 1-128 characters [A-Za-z0-9._-], not starting with '.'"
            )
        })?;
        out.push((ns.to_string(), tok.to_string()));
    }
    Ok(out)
}

/// Validates an S3 endpoint URL: http(s), has a host, and carries no credentials.
/// Error messages never echo the endpoint (it may contain secrets).
fn validate_s3_endpoint(endpoint: &str) -> anyhow::Result<()> {
    let url = url::Url::parse(endpoint).map_err(|e| {
        anyhow::anyhow!("Invalid S3 endpoint URL (S3_ENDPOINT/AWS_ENDPOINT_URL): {e}")
    })?;
    if !url.username().is_empty() || url.password().is_some() {
        anyhow::bail!(
            "S3 endpoint URL must not contain credentials (user:pass@host); configure S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY(_FILE) instead"
        );
    }
    if !matches!(url.scheme(), "http" | "https") {
        anyhow::bail!("S3 endpoint URL must use the http:// or https:// scheme");
    }
    if url.host_str().is_none_or(str::is_empty) {
        anyhow::bail!("S3 endpoint URL must contain a host");
    }
    Ok(())
}

impl CliConfig {
    /// Parses command-line arguments while ignoring all environment variables (no `env = ...`
    /// fallbacks). Intended for tests and tooling that must not depend on the caller's environment.
    pub fn try_parse_from_ignoring_env<I, T>(args: I) -> Result<Self, clap::Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        let cmd = Self::command().mut_args(|arg| arg.env(None::<&str>));
        let mut matches = cmd.try_get_matches_from(args)?;
        Self::from_arg_matches_mut(&mut matches)
    }

    /// Socket address to bind. `HOST` may be an IPv4 address, a bare (`::`) or bracketed
    /// (`[::1]`) IPv6 address, or `localhost`.
    pub fn socket_addr(&self) -> anyhow::Result<SocketAddr> {
        let raw = self.host.trim();
        let host = raw
            .strip_prefix('[')
            .and_then(|h| h.strip_suffix(']'))
            .unwrap_or(raw);
        let ip: IpAddr = if host.eq_ignore_ascii_case("localhost") {
            IpAddr::V4(Ipv4Addr::LOCALHOST)
        } else {
            host.parse().map_err(|_| {
                anyhow::anyhow!(
                    "Invalid HOST '{}': expected an IPv4 or IPv6 address (e.g. 0.0.0.0, ::, [::1]) or 'localhost'",
                    self.host
                )
            })?
        };
        Ok(SocketAddr::new(ip, self.port))
    }

    /// Address probed by the `healthcheck` sub-command: the bind address, with unspecified
    /// addresses (`0.0.0.0`, `::`) replaced by the loopback address of the same family.
    pub fn healthcheck_addr(&self) -> anyhow::Result<SocketAddr> {
        let mut addr = self.socket_addr()?;
        match addr.ip() {
            IpAddr::V4(ip) if ip.is_unspecified() => addr.set_ip(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            IpAddr::V6(ip) if ip.is_unspecified() => addr.set_ip(IpAddr::V6(Ipv6Addr::LOCALHOST)),
            _ => {}
        }
        Ok(addr)
    }

    /// Reads the token files (`TURBO_TOKEN_FILE`, `TURBO_SCOPED_TOKENS_FILE` and their read-only
    /// variants), `S3_SECRET_ACCESS_KEY_FILE`, `AZURE_STORAGE_ACCESS_KEY_FILE` and
    /// `OIDC_SIGNING_KEY_FILE` and merges their contents into the corresponding options. Call once
    /// at startup.
    pub fn load_secret_files(&mut self) -> anyhow::Result<()> {
        extend_from_token_file(
            &mut self.tokens,
            self.token_file.as_deref(),
            "TURBO_TOKEN_FILE",
        )?;
        extend_from_token_file(
            &mut self.scoped_tokens,
            self.scoped_tokens_file.as_deref(),
            "TURBO_SCOPED_TOKENS_FILE",
        )?;
        extend_from_token_file(
            &mut self.read_only_tokens,
            self.read_only_token_file.as_deref(),
            "TURBO_READ_ONLY_TOKEN_FILE",
        )?;
        extend_from_token_file(
            &mut self.scoped_read_only_tokens,
            self.scoped_read_only_tokens_file.as_deref(),
            "TURBO_SCOPED_READ_ONLY_TOKENS_FILE",
        )?;
        load_secret_file(
            &mut self.s3_secret_access_key,
            self.s3_secret_access_key_file.as_deref(),
            "S3_SECRET_ACCESS_KEY",
        )?;
        load_secret_file(
            &mut self.oidc_signing_key,
            self.oidc_signing_key_file.as_deref(),
            "OIDC_SIGNING_KEY",
        )?;
        load_secret_file(
            &mut self.azure_storage_access_key,
            self.azure_storage_access_key_file.as_deref(),
            "AZURE_STORAGE_ACCESS_KEY",
        )
    }

    /// Validates option combinations that would otherwise silently weaken security or break
    /// clients. Call after [`CliConfig::load_secret_files`].
    pub fn validate(&self) -> anyhow::Result<()> {
        let tokens = self.effective_tokens();
        let scoped = self.effective_scoped_tokens()?;
        let read_only = self.effective_read_only_tokens();
        let scoped_read_only = self.effective_scoped_read_only_tokens()?;
        let unscoped: HashSet<&str> = tokens
            .iter()
            .chain(&read_only)
            .map(String::as_str)
            .collect();
        let all_scoped: Vec<&(String, String)> = scoped.iter().chain(&scoped_read_only).collect();
        let has_oidc = self.oidc_config()?.is_some();
        let has_any_tokens = !unscoped.is_empty() || !all_scoped.is_empty() || has_oidc;

        if self.allow_unauthenticated && has_any_tokens {
            anyhow::bail!(
                "ALLOW_UNAUTHENTICATED=true cannot be combined with tokens (TURBO_TOKEN, TURBO_SCOPED_TOKENS, TURBO_READ_ONLY_TOKENS, TURBO_SCOPED_READ_ONLY_TOKENS) or OIDC_ISSUER: anonymous requests would bypass team scoping. Remove the tokens or disable ALLOW_UNAUTHENTICATED."
            );
        }
        if !self.allow_unauthenticated && !has_any_tokens {
            anyhow::bail!(
                "No authentication tokens configured. Set TURBO_TOKEN / --tokens, TURBO_TOKEN_FILE, TURBO_SCOPED_TOKENS / --scoped-tokens, OIDC_ISSUER, or pass --allow-unauthenticated."
            );
        }
        let read_write: HashSet<&str> = tokens
            .iter()
            .chain(scoped.iter().map(|(_, tok)| tok))
            .map(String::as_str)
            .collect();
        if read_only
            .iter()
            .chain(scoped_read_only.iter().map(|(_, tok)| tok))
            .any(|tok| read_write.contains(tok.as_str()))
        {
            anyhow::bail!(
                "A token is configured both as read-only (TURBO_READ_ONLY_TOKENS / TURBO_SCOPED_READ_ONLY_TOKENS) and as read-write (TURBO_TOKEN / TURBO_SCOPED_TOKENS). Use a distinct secret for each, or remove one of the entries."
            );
        }
        let mut conflicts: Vec<&str> = all_scoped
            .iter()
            .filter(|(_, tok)| unscoped.contains(tok.as_str()))
            .map(|(ns, _)| ns.as_str())
            .collect();
        if !conflicts.is_empty() {
            conflicts.sort_unstable();
            conflicts.dedup();
            // Name only the namespaces, never the secret.
            anyhow::bail!(
                "A token configured as unscoped (TURBO_TOKEN / TURBO_READ_ONLY_TOKENS) is also configured as a scoped token (TURBO_SCOPED_TOKENS / TURBO_SCOPED_READ_ONLY_TOKENS) for namespace(s) {}: it would be valid for every namespace, not only the scoped ones. Use a distinct secret for each, or remove one of the entries.",
                conflicts
                    .iter()
                    .map(|ns| format!("'{ns}'"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        if !self.isolate_teams && !all_scoped.is_empty() {
            anyhow::bail!(
                "ISOLATE_TEAMS=false cannot be combined with scoped tokens (TURBO_SCOPED_TOKENS, TURBO_SCOPED_READ_ONLY_TOKENS): every request would use the 'default' namespace and all scoped tokens would be rejected."
            );
        }
        if self.immutable_artifacts && self.allow_delete {
            anyhow::bail!(
                "IMMUTABLE_ARTIFACTS=true requires ALLOW_DELETE=false (deleting and re-uploading would defeat immutability)."
            );
        }
        if self.max_artifact_size == 0 {
            anyhow::bail!("MAX_ARTIFACT_SIZE must be greater than 0");
        }
        let max_artifact_size = self.max_artifact_size as u64;
        if let Some((limit, limit_text)) = self.storage_type.single_put_limit() {
            if self.immutable_artifacts && max_artifact_size > limit {
                anyhow::bail!(
                    "MAX_ARTIFACT_SIZE ({max_artifact_size} bytes) exceeds the single-request upload limit of {limit_text} ({limit} bytes), which applies with IMMUTABLE_ARTIFACTS=true (conditional writes are single requests)"
                );
            }
            if max_artifact_size > S3_MAX_MULTIPART_BYTES {
                anyhow::bail!(
                    "MAX_ARTIFACT_SIZE ({max_artifact_size} bytes) exceeds the multipart upload limit of 10000 parts of 8 MiB ({S3_MAX_MULTIPART_BYTES} bytes)"
                );
            }
        }
        if self.max_concurrent_uploads == 0 {
            anyhow::bail!("MAX_CONCURRENT_UPLOADS must be at least 1");
        }
        if !(1..=16).contains(&self.storage_parts_in_flight) {
            anyhow::bail!("STORAGE_PARTS_IN_FLIGHT must be between 1 and 16");
        }
        if self.batch_head_concurrency == 0 {
            anyhow::bail!("BATCH_HEAD_CONCURRENCY must be at least 1");
        }
        // Browsers compare the serialized origin byte for byte, so e.g. a trailing slash would
        // silently fail every CORS request.
        let origin = self.cors_allow_origin.trim();
        if !matches!(origin, "" | "*")
            && !url::Url::parse(origin)
                .is_ok_and(|url| url.origin().ascii_serialization() == origin)
        {
            anyhow::bail!(
                "CORS_ALLOW_ORIGIN must be * or one origin such as https://app.example.com (scheme, host and non-default port only: no path or trailing slash, internationalized domains in punycode)"
            );
        }
        // The janitor treats every `<namespace>/<hash>`-shaped key below the prefix as an
        // artifact; without a prefix that includes unrelated objects in the bucket.
        if self.storage_type != StorageBackendKind::Local
            && (self.storage_max_bytes > 0 || self.storage_max_age_secs > 0)
            && self.storage_prefix.trim_matches('/').is_empty()
        {
            anyhow::bail!(
                "STORAGE_MAX_BYTES and STORAGE_MAX_AGE_SECS require STORAGE_PREFIX with object store backends: retention deletes every key of the form <namespace>/<hash> below the prefix, which without one includes unrelated objects in the bucket. Set STORAGE_PREFIX (e.g. turbo-cache)."
            );
        }
        for (name, value) in [
            ("BODY_IDLE_TIMEOUT_SECS", self.body_idle_timeout_secs),
            ("BODY_TOTAL_TIMEOUT_SECS", self.body_total_timeout_secs),
            (
                "STORAGE_CONNECT_TIMEOUT_SECS",
                self.storage_connect_timeout_secs,
            ),
            ("STORAGE_READ_TIMEOUT_SECS", self.storage_read_timeout_secs),
            (
                "STORAGE_CLEANUP_INTERVAL_SECS",
                self.storage_cleanup_interval_secs,
            ),
            ("LOCAL_TEMP_MAX_AGE_SECS", self.local_temp_max_age_secs),
        ] {
            if value == 0 {
                anyhow::bail!("{name} must be greater than 0");
            }
        }
        self.socket_addr()?;
        Ok(())
    }

    /// OIDC token exchange options; `None` without `OIDC_ISSUER`. Call after
    /// [`CliConfig::load_secret_files`].
    pub fn oidc_config(&self) -> anyhow::Result<Option<OidcConfig>> {
        let Some(issuer) = non_empty(self.oidc_issuer.clone()) else {
            return Ok(None);
        };
        // Without a trailing slash, as the discovery URL is built by appending a path (ID tokens
        // are accepted with either form of `iss`).
        let issuer = issuer.trim().trim_end_matches('/').to_string();
        oidc::validate_url(&issuer).context("Invalid OIDC_ISSUER")?;
        let audience = non_empty(self.oidc_audience.clone())
            .ok_or_else(|| anyhow::anyhow!("OIDC_AUDIENCE is required with OIDC_ISSUER"))?
            .trim()
            .to_string();
        let required_claims = ClaimRules::parse(&self.oidc_required_claims)
            .context("Invalid OIDC_REQUIRED_CLAIMS")?;
        if required_claims.is_empty() {
            anyhow::bail!(
                "OIDC_REQUIRED_CLAIMS is required with OIDC_ISSUER (e.g. repository_owner_id=123456): any GitHub workflow can request an ID token for any audience, so issuer and audience alone would admit everyone"
            );
        }
        let write_claims =
            ClaimRules::parse(&self.oidc_write_claims).context("Invalid OIDC_WRITE_CLAIMS")?;
        let namespace_claim = non_empty(self.oidc_namespace_claim.clone()).map(|c| c.trim().into());
        if namespace_claim.is_some() && !self.isolate_teams {
            anyhow::bail!(
                "ISOLATE_TEAMS=false cannot be combined with OIDC_NAMESPACE_CLAIM: every request would use the 'default' namespace and all scoped cache tokens would be rejected."
            );
        }
        if !(1..=oidc::MAX_TOKEN_TTL_SECS).contains(&self.oidc_token_ttl_secs) {
            anyhow::bail!(
                "OIDC_TOKEN_TTL_SECS must be between 1 and {}",
                oidc::MAX_TOKEN_TTL_SECS
            );
        }
        let signing_key = self
            .oidc_signing_key
            .as_deref()
            .map(str::trim)
            .filter(|key| !key.is_empty())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "OIDC_SIGNING_KEY or OIDC_SIGNING_KEY_FILE is required with OIDC_ISSUER"
                )
            })?;
        let signing_key = oidc::decode_signing_key(signing_key)?;
        Ok(Some(OidcConfig {
            signing_key: CacheTokenKey::new(&signing_key, &issuer, &audience),
            issuer,
            audience,
            required_claims,
            write_claims: (!write_claims.is_empty()).then_some(write_claims),
            namespace_claim,
            token_ttl: Duration::from_secs(self.oidc_token_ttl_secs),
        }))
    }

    pub fn effective_tokens(&self) -> Vec<String> {
        trimmed_tokens(&self.tokens)
    }

    pub fn effective_scoped_tokens(&self) -> anyhow::Result<Vec<(String, String)>> {
        parse_scoped_tokens(&self.scoped_tokens)
    }

    pub fn effective_read_only_tokens(&self) -> Vec<String> {
        trimmed_tokens(&self.read_only_tokens)
    }

    pub fn effective_scoped_read_only_tokens(&self) -> anyhow::Result<Vec<(String, String)>> {
        parse_scoped_tokens(&self.scoped_read_only_tokens)
    }

    /// Options for the hot payload/metadata cache in front of Local/S3 backends.
    pub fn cache_options(&self) -> CacheOptions {
        CacheOptions {
            max_bytes: self.memory_cache_max_bytes,
            max_entries: self.memory_cache_max_entries,
            max_item_bytes: self.hot_cache_max_item_bytes,
            metadata_max_entries: self.metadata_cache_max_entries,
            ttl: secs_or_none(self.cache_ttl_secs),
            negative_ttl: secs_or_none(self.negative_cache_ttl_secs),
        }
    }

    /// Options for the local filesystem backend.
    pub fn local_options(&self) -> LocalOptions {
        LocalOptions {
            max_bytes: (self.storage_max_bytes > 0).then_some(self.storage_max_bytes),
            max_age: secs_or_none(self.storage_max_age_secs),
            cleanup_interval: Duration::from_secs(self.storage_cleanup_interval_secs),
            temp_max_age: Duration::from_secs(self.local_temp_max_age_secs),
        }
    }

    /// Retention options for the object store backends.
    pub fn retention_options(&self) -> RetentionOptions {
        RetentionOptions {
            max_bytes: (self.storage_max_bytes > 0).then_some(self.storage_max_bytes),
            max_age: secs_or_none(self.storage_max_age_secs),
            interval: Duration::from_secs(self.storage_cleanup_interval_secs),
        }
    }

    /// Minimum age (by mtime) after which a `.turbo-spool-*.tmp` file in `SPOOL_DIR` is treated
    /// as orphaned: `max(LOCAL_TEMP_MAX_AGE_SECS, 2 * BODY_TOTAL_TIMEOUT_SECS)`, so a spool file
    /// still being received or handed to a slow backend is never removed.
    pub fn spool_max_age(&self) -> Duration {
        Duration::from_secs(
            self.local_temp_max_age_secs
                .max(self.body_total_timeout_secs.saturating_mul(2)),
        )
    }

    /// Interval between orphaned spool file sweeps (`STORAGE_CLEANUP_INTERVAL_SECS`).
    pub fn spool_cleanup_interval(&self) -> Duration {
        Duration::from_secs(self.storage_cleanup_interval_secs)
    }

    /// Effective `MAX_CONCURRENT_UPLOADS_PER_NAMESPACE` (`0` = no per-namespace limit) with the
    /// automatic default resolved. Scoped tokens imply tenants that do not trust each other.
    pub fn uploads_per_namespace(&self, has_scoped_tokens: bool) -> usize {
        let total = self.max_concurrent_uploads.max(1);
        match self.max_concurrent_uploads_per_namespace {
            0 if has_scoped_tokens && total > 1 => (total / 2).max(1),
            0 => 0,
            n if n >= total => 0,
            n => n,
        }
    }

    /// HTTP server and upload pipeline options.
    pub fn server_options(&self) -> ServerOptions {
        let spool_dir = self.spool_dir.clone().unwrap_or_else(|| {
            if self.storage_type == StorageBackendKind::Local {
                self.local_storage_path.join(".spool")
            } else {
                std::env::temp_dir()
            }
        });
        ServerOptions {
            body_idle_timeout: Duration::from_secs(self.body_idle_timeout_secs),
            body_total_timeout: Duration::from_secs(self.body_total_timeout_secs),
            upload_slot_wait: Duration::from_secs(self.upload_slot_wait_secs),
            shutdown_grace: Duration::from_secs(self.shutdown_grace_secs),
            spool_dir,
            upload_memory_threshold: self.upload_memory_threshold,
        }
    }

    /// Resolves S3 configuration with provider-specific defaults for RustFS, Garage, and AWS S3,
    /// using the process environment for `AWS_*` fallbacks.
    pub fn resolve_s3_config(&self) -> anyhow::Result<S3StorageConfig> {
        self.resolve_s3_config_with_env(&|name| std::env::var(name).ok())
    }

    /// Warning for an `S3_ENDPOINT` that is set but blank (e.g. `S3_ENDPOINT=` in an env file).
    /// A blank value counts as unset, so the default endpoint is used; under docker-compose the
    /// blank `.env` line also overrides the bundled store's endpoint from
    /// `compose/<STORAGE_TYPE>.env`, and the loopback default then points at the server itself.
    fn blank_s3_endpoint_warning(
        &self,
        has_custom_endpoint: bool,
        endpoint: &str,
    ) -> Option<String> {
        let blank = self
            .s3_endpoint
            .as_deref()
            .is_some_and(|value| value.trim().is_empty());
        (blank && !has_custom_endpoint).then(|| {
            format!(
                "S3_ENDPOINT is set but empty; using the default endpoint {endpoint}. Remove the \
                 setting (or comment it out) to keep an endpoint configured elsewhere, e.g. the \
                 bundled store's in compose/<STORAGE_TYPE>.env"
            )
        })
    }

    /// Like [`CliConfig::resolve_s3_config`] but reads `AWS_*` fallbacks through `env`
    /// (pass `&|_| None` to ignore the environment entirely).
    pub fn resolve_s3_config_with_env(
        &self,
        env: &dyn Fn(&str) -> Option<String>,
    ) -> anyhow::Result<S3StorageConfig> {
        let provider_name = match self.storage_type {
            StorageBackendKind::Rustfs => "rustfs",
            StorageBackendKind::Garage => "garage",
            _ => "s3",
        };

        let bucket = non_empty(self.s3_bucket.clone())
            .or_else(|| non_empty(env("AWS_BUCKET")))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "S3 bucket is required for '{}' storage (set --s3-bucket or S3_BUCKET)",
                    provider_name
                )
            })?;

        let region = non_empty(self.s3_region.clone())
            .or_else(|| non_empty(env("AWS_REGION")))
            .or_else(|| non_empty(env("AWS_DEFAULT_REGION")))
            .unwrap_or_else(|| match self.storage_type {
                // Garage defaults its S3 region to "garage" in garage.toml
                StorageBackendKind::Garage => "garage".to_string(),
                _ => "us-east-1".to_string(),
            });

        let custom_endpoint = non_empty(self.s3_endpoint.as_deref().map(str::to_string))
            .or_else(|| non_empty(env("AWS_ENDPOINT_URL")))
            .map(|e| e.trim().to_string());
        let has_custom_endpoint = custom_endpoint.is_some();
        let endpoint = custom_endpoint.unwrap_or_else(|| match self.storage_type {
            StorageBackendKind::Rustfs => "http://127.0.0.1:9000".to_string(),
            StorageBackendKind::Garage => "http://127.0.0.1:3900".to_string(),
            _ => format!("https://s3.{region}.amazonaws.com"),
        });
        validate_s3_endpoint(&endpoint)?;
        if let Some(warning) = self.blank_s3_endpoint_warning(has_custom_endpoint, &endpoint) {
            tracing::warn!("{warning}");
        }

        let access_key_id = non_empty(self.s3_access_key_id.as_deref().map(str::to_string))
            .or_else(|| non_empty(env("AWS_ACCESS_KEY_ID")));
        let secret_access_key = non_empty(self.s3_secret_access_key.as_deref().map(str::to_string))
            .or_else(|| non_empty(env("AWS_SECRET_ACCESS_KEY")));
        let (access_key_id, secret_access_key) = match (access_key_id, secret_access_key) {
            (Some(id), Some(secret)) => (id, secret),
            // No static credentials: the S3 backend uses its credential provider chain.
            (None, None) => (String::new(), String::new()),
            (Some(_), None) => anyhow::bail!(
                "S3 secret access key is required for '{}' storage when an access key ID is set (set --s3-secret-access-key, S3_SECRET_ACCESS_KEY(_FILE), or AWS_SECRET_ACCESS_KEY)",
                provider_name
            ),
            (None, Some(_)) => anyhow::bail!(
                "S3 access key ID is required for '{}' storage when a secret access key is set (set --s3-access-key-id, S3_ACCESS_KEY_ID, or AWS_ACCESS_KEY_ID)",
                provider_name
            ),
        };

        let session_token = non_empty(self.s3_session_token.as_deref().map(str::to_string))
            .or_else(|| non_empty(env("AWS_SESSION_TOKEN")));

        let force_path_style = self.s3_force_path_style.unwrap_or(
            matches!(
                self.storage_type,
                StorageBackendKind::Rustfs | StorageBackendKind::Garage
            ) || has_custom_endpoint
                || bucket.contains('.'),
        );

        Ok(S3StorageConfig {
            provider_name,
            bucket,
            region,
            endpoint: endpoint.trim_end_matches('/').to_string(),
            access_key_id,
            secret_access_key,
            session_token,
            prefix: self.storage_prefix.trim_matches('/').to_string(),
            force_path_style,
            sign_payload: self.s3_sign_payload,
            max_retries: self.storage_max_retries,
            parts_in_flight: self.storage_parts_in_flight,
            connect_timeout: Duration::from_secs(self.storage_connect_timeout_secs),
            read_timeout: Duration::from_secs(self.storage_read_timeout_secs),
            treat_403_as_miss: self.s3_treat_403_as_miss,
            health_check_key: non_empty(self.s3_health_check_key.clone())
                .map(|k| k.trim().trim_start_matches('/').to_string()),
            token_dir: self.server_options().spool_dir,
        })
    }

    /// Resolves the GCS configuration, using the process environment for `GOOGLE_*` fallbacks.
    pub fn resolve_gcs_config(&self) -> anyhow::Result<GcsStorageConfig> {
        self.resolve_gcs_config_with_env(&|name| std::env::var(name).ok())
    }

    /// Like [`CliConfig::resolve_gcs_config`] but reads `GOOGLE_*` fallbacks through `env`.
    pub fn resolve_gcs_config_with_env(
        &self,
        env: &dyn Fn(&str) -> Option<String>,
    ) -> anyhow::Result<GcsStorageConfig> {
        let bucket = non_empty(self.gcs_bucket.clone())
            .or_else(|| non_empty(env("GOOGLE_BUCKET")))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "GCS bucket is required for 'gcs' storage (set --gcs-bucket or GCS_BUCKET)"
                )
            })?;
        let service_account_key_file = self
            .gcs_service_account_key_file
            .clone()
            .or_else(|| non_empty(env("GOOGLE_SERVICE_ACCOUNT")).map(PathBuf::from));
        Ok(GcsStorageConfig {
            bucket,
            prefix: self.storage_prefix.trim_matches('/').to_string(),
            service_account_key_file,
            application_credentials: non_empty(env("GOOGLE_APPLICATION_CREDENTIALS"))
                .map(PathBuf::from),
            max_retries: self.storage_max_retries,
            parts_in_flight: self.storage_parts_in_flight,
            connect_timeout: Duration::from_secs(self.storage_connect_timeout_secs),
            read_timeout: Duration::from_secs(self.storage_read_timeout_secs),
        })
    }

    /// Resolves the Azure configuration, using the process environment for `AZURE_*` fallbacks.
    pub fn resolve_azure_config(&self) -> anyhow::Result<AzureStorageConfig> {
        self.resolve_azure_config_with_env(&|name| std::env::var(name).ok())
    }

    /// Like [`CliConfig::resolve_azure_config`] but reads `AZURE_*` fallbacks through `env`.
    pub fn resolve_azure_config_with_env(
        &self,
        env: &dyn Fn(&str) -> Option<String>,
    ) -> anyhow::Result<AzureStorageConfig> {
        let account = non_empty(self.azure_storage_account.clone())
            .or_else(|| non_empty(env("AZURE_STORAGE_ACCOUNT_NAME")))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Azure storage account is required for 'azure' storage (set --azure-storage-account or AZURE_STORAGE_ACCOUNT)"
                )
            })?;
        let container = non_empty(self.azure_container.clone())
            .or_else(|| non_empty(env("AZURE_CONTAINER_NAME")))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Azure container is required for 'azure' storage (set --azure-container or AZURE_CONTAINER)"
                )
            })?;
        let secret = |value: &Option<Secret>, fallback: &str| {
            non_empty(value.as_deref().map(str::to_string))
                .or_else(|| non_empty(env(fallback)))
                .map(|s| Secret(s.trim().to_string()))
        };
        let access_key = secret(&self.azure_storage_access_key, "AZURE_STORAGE_ACCOUNT_KEY");
        let sas_token = secret(&self.azure_storage_sas_token, "AZURE_STORAGE_SAS_KEY");
        if access_key.is_some() && sas_token.is_some() {
            anyhow::bail!(
                "Set only one of AZURE_STORAGE_ACCESS_KEY(_FILE) and AZURE_STORAGE_SAS_TOKEN"
            );
        }
        let endpoint = non_empty(self.azure_storage_endpoint.as_deref().map(str::to_string))
            .map(|e| RedactedUrl(e.trim().trim_end_matches('/').to_string()));
        Ok(AzureStorageConfig {
            account,
            container,
            prefix: self.storage_prefix.trim_matches('/').to_string(),
            endpoint,
            access_key,
            sas_token,
            max_retries: self.storage_max_retries,
            parts_in_flight: self.storage_parts_in_flight,
            connect_timeout: Duration::from_secs(self.storage_connect_timeout_secs),
            read_timeout: Duration::from_secs(self.storage_read_timeout_secs),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, io::Write, path::PathBuf, time::Duration};

    use clap::CommandFactory;

    use super::{CliConfig, Command, S3_MAX_MULTIPART_BYTES, redact_url_userinfo};

    /// Parses explicit args only; the developer's environment is never consulted.
    fn parse(args: &[&str]) -> CliConfig {
        try_parse(args).unwrap_or_else(|e| panic!("failed to parse {args:?}: {e}"))
    }

    fn try_parse(args: &[&str]) -> Result<CliConfig, clap::Error> {
        CliConfig::try_parse_from_ignoring_env(
            std::iter::once("fast-turbo-cache-server").chain(args.iter().copied()),
        )
    }

    fn no_env(_: &str) -> Option<String> {
        None
    }

    fn validate_err(args: &[&str]) -> String {
        let err = parse(args)
            .validate()
            .expect_err("expected validation error");
        format!("{err:#}")
    }

    fn s3_args<'a>(kind: &'a str, extra: &[&'a str]) -> Vec<&'a str> {
        let mut args = vec![
            "--storage-type",
            kind,
            "--s3-bucket",
            "bucket",
            "--s3-access-key-id",
            "AKID",
            "--s3-secret-access-key",
            "SECRET",
        ];
        args.extend_from_slice(extra);
        args
    }

    fn secret_file(content: &str) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file
    }

    #[test]
    fn clap_definition_is_consistent() {
        CliConfig::command().debug_assert();
    }

    #[test]
    fn bool_flags_accept_bare_and_explicit_values() {
        type Getter = fn(&CliConfig) -> bool;
        let flags: &[(&str, bool, Getter)] = &[
            ("allow-unauthenticated", false, |c| c.allow_unauthenticated),
            ("s3-sign-payload", false, |c| c.s3_sign_payload),
            ("s3-treat-403-as-miss", false, |c| c.s3_treat_403_as_miss),
            ("isolate-teams", true, |c| c.isolate_teams),
            ("immutable-artifacts", false, |c| c.immutable_artifacts),
            ("allow-delete", false, |c| c.allow_delete),
            ("protect-observability", false, |c| c.protect_observability),
        ];
        for &(flag, default, get) in flags {
            assert_eq!(get(&parse(&[])), default, "default of --{flag}");
            assert!(get(&parse(&[&format!("--{flag}")])), "--{flag}");
            for (value, expected) in [
                ("true", true),
                ("false", false),
                ("1", true),
                ("0", false),
                ("yes", true),
                ("no", false),
                ("on", true),
                ("off", false),
            ] {
                let arg = format!("--{flag}={value}");
                assert_eq!(get(&parse(&[&arg])), expected, "{arg}");
            }
            assert!(try_parse(&[&format!("--{flag}=maybe")]).is_err());
        }
    }

    #[test]
    fn healthcheck_subcommand_accepts_options_before_and_after_it() {
        // A bare bool flag must not consume the sub-command as its value.
        let cfg = parse(&["--allow-unauthenticated", "healthcheck"]);
        assert!(cfg.allow_unauthenticated);
        assert_eq!(cfg.command, Some(Command::Healthcheck));
        let cfg = parse(&["healthcheck", "--port", "4123", "--host", "::"]);
        assert_eq!(cfg.command, Some(Command::Healthcheck));
        assert_eq!((cfg.port, cfg.host.as_str()), (4123, "::"));
        assert_eq!(parse(&[]).command, None);
    }

    #[test]
    fn validate_accepts_sane_configurations() {
        parse(&["--tokens", "t"]).validate().unwrap();
        parse(&["--allow-unauthenticated"]).validate().unwrap();
        parse(&["--tokens", "t", "--immutable-artifacts"])
            .validate()
            .unwrap();
        parse(&["--scoped-tokens", "acme=tok"]).validate().unwrap();
    }

    #[test]
    fn validate_requires_some_authentication() {
        assert!(validate_err(&[]).contains("No authentication tokens"));
    }

    #[test]
    fn validate_rejects_unauthenticated_with_tokens() {
        assert!(
            validate_err(&["--allow-unauthenticated", "--tokens", "t"])
                .contains("ALLOW_UNAUTHENTICATED")
        );
        assert!(
            validate_err(&["--allow-unauthenticated", "--scoped-tokens", "acme=tok"])
                .contains("ALLOW_UNAUTHENTICATED")
        );
    }

    #[test]
    fn validate_rejects_scoped_tokens_without_team_isolation() {
        let err = validate_err(&[
            "--isolate-teams=false",
            "--tokens",
            "t",
            "--scoped-tokens",
            "acme=tok",
        ]);
        assert!(err.contains("ISOLATE_TEAMS"), "{err}");
        parse(&["--isolate-teams=false", "--tokens", "t"])
            .validate()
            .unwrap();
    }

    #[test]
    fn validate_requires_prefix_for_object_store_retention() {
        for limit in [
            ["--storage-max-bytes", "1000"],
            ["--storage-max-age-secs", "60"],
        ] {
            let err = validate_err(&s3_args("s3", &[&["--tokens", "t"][..], &limit].concat()));
            assert!(err.contains("require STORAGE_PREFIX"), "{err}");
            let err = validate_err(&s3_args(
                "s3",
                &[&["--tokens", "t", "--storage-prefix", "/"][..], &limit].concat(),
            ));
            assert!(err.contains("require STORAGE_PREFIX"), "{err}");
            parse(&s3_args(
                "s3",
                &[&["--tokens", "t", "--storage-prefix", "ci"][..], &limit].concat(),
            ))
            .validate()
            .unwrap();
            parse(&[&["--tokens", "t"][..], &limit].concat())
                .validate()
                .unwrap();
        }
    }

    #[test]
    fn validate_rejects_invalid_cors_origin() {
        for origin in [
            "https://café.example",
            "https://app.example.com/",
            "https://app.example.com:443",
            "https://a.example, https://b.example",
            "app.example.com",
        ] {
            let err = validate_err(&["--tokens", "t", "--cors-allow-origin", origin]);
            assert!(err.contains("CORS_ALLOW_ORIGIN"), "{origin}: {err}");
        }
        for origin in [
            "*",
            "",
            "https://xn--caf-dma.example",
            "http://localhost:3000",
        ] {
            parse(&["--tokens", "t", "--cors-allow-origin", origin])
                .validate()
                .unwrap();
        }
    }

    #[test]
    fn validate_rejects_immutable_with_delete() {
        let err = validate_err(&["--tokens", "t", "--immutable-artifacts", "--allow-delete"]);
        assert!(err.contains("IMMUTABLE_ARTIFACTS"), "{err}");
        parse(&["--tokens", "t", "--allow-delete"])
            .validate()
            .unwrap();
    }

    #[test]
    fn validate_artifact_size_limits() {
        assert!(
            validate_err(&["--tokens", "t", "--max-artifact-size", "0"])
                .contains("MAX_ARTIFACT_SIZE")
        );

        let validate = |kind: &str, size: &str, immutable: bool| {
            let mut args = vec!["--tokens", "t", "--storage-type", kind];
            args.extend(["--max-artifact-size", size]);
            if immutable {
                args.push("--immutable-artifacts");
            }
            parse(&args).validate().map_err(|e| format!("{e:#}"))
        };
        let multipart_max = S3_MAX_MULTIPART_BYTES.to_string();
        let over_multipart = (S3_MAX_MULTIPART_BYTES + 1).to_string();
        let gib: u64 = 1024 * 1024 * 1024;
        for (kind, single_put, text) in [
            ("s3", 5 * gib, "5 GiB"),
            ("rustfs", 5 * gib, "5 GiB"),
            ("garage", 5 * gib, "5 GiB"),
            ("azure", 5000 * 1024 * 1024, "5000 MiB"),
        ] {
            let (at, over) = (single_put.to_string(), (single_put + 1).to_string());
            let err = validate(kind, &over, true).unwrap_err();
            assert!(err.contains(text), "{kind}: {err}");
            validate(kind, &at, true).unwrap();
            validate(kind, &over, false).unwrap();
            validate(kind, &multipart_max, false).unwrap();
            let err = validate(kind, &over_multipart, false).unwrap_err();
            assert!(err.contains("multipart upload limit"), "{kind}: {err}");
        }
        // A single GCS `PUT` may be larger than any multipart upload.
        validate("gcs", &multipart_max, true).unwrap();
        let err = validate("gcs", &over_multipart, false).unwrap_err();
        assert!(err.contains("multipart upload limit"), "{err}");
        parse(&[
            "--tokens",
            "t",
            "--storage-type",
            "local",
            "--max-artifact-size",
            &over_multipart,
        ])
        .validate()
        .unwrap();
    }

    #[test]
    fn validate_rejects_token_both_unscoped_and_scoped() {
        let secret = "s3cr3t-shared-value";
        let err = validate_err(&[
            "--tokens",
            &format!("other,{secret}"),
            "--scoped-tokens",
            &format!("team-b={secret},team-a={secret},team-c=distinct"),
        ]);
        assert!(err.contains("'team-a', 'team-b'"), "{err}");
        assert!(!err.contains("team-c"), "{err}");
        assert!(!err.contains(secret), "secret leaked: {err}");
        // Whitespace around the secret does not hide the conflict.
        let err = validate_err(&[
            "--tokens",
            &format!(" {secret} "),
            "--scoped-tokens",
            &format!("team-a= {secret}"),
        ]);
        assert!(err.contains("'team-a'"), "{err}");
        parse(&["--tokens", "global", "--scoped-tokens", "team-a=scoped"])
            .validate()
            .unwrap();
    }

    #[test]
    fn validate_rejects_token_both_read_only_and_read_write() {
        let secret = "s3cr3t-shared-value";
        let scoped = format!("team-a={secret}");
        for args in [
            ["--tokens", secret, "--read-only-tokens", secret],
            ["--tokens", secret, "--scoped-read-only-tokens", &scoped],
            ["--scoped-tokens", &scoped, "--read-only-tokens", secret],
        ] {
            let err = validate_err(&args);
            assert!(err.contains("both as read-only"), "{err}");
            assert!(!err.contains(secret), "secret leaked: {err}");
        }
        parse(&["--tokens", "rw", "--read-only-tokens", "ro"])
            .validate()
            .unwrap();
    }

    #[test]
    fn validate_rejects_zero_and_out_of_range_limits() {
        for (flag, value) in [
            ("--max-concurrent-uploads", "0"),
            ("--batch-head-concurrency", "0"),
            ("--storage-parts-in-flight", "0"),
            ("--storage-parts-in-flight", "17"),
            ("--body-idle-timeout-secs", "0"),
            ("--body-total-timeout-secs", "0"),
            ("--storage-connect-timeout-secs", "0"),
            ("--storage-read-timeout-secs", "0"),
            ("--storage-cleanup-interval-secs", "0"),
            ("--local-temp-max-age-secs", "0"),
            ("--host", "not-an-ip"),
        ] {
            assert!(
                parse(&["--tokens", "t", flag, value]).validate().is_err(),
                "{flag}={value}"
            );
        }
    }

    /// Validates the OIDC options with `overrides` (`(flag, value)`; an empty value drops the
    /// flag) and returns the configuration.
    fn validate_oidc(overrides: &[(&str, &str)]) -> Result<CliConfig, String> {
        let mut options = vec![
            (
                "--oidc-issuer",
                "https://token.actions.githubusercontent.com",
            ),
            ("--oidc-audience", "https://cache.example.com"),
            ("--oidc-required-claims", "repository_owner=acme"),
            // 32 bytes.
            (
                "--oidc-signing-key",
                "a2V5LWZvci10aGUtb2lkYy10ZXN0cy0zMi1ieXRlcyE=",
            ),
        ];
        for &(flag, value) in overrides {
            options.retain(|(f, _)| *f != flag);
            if !value.is_empty() {
                options.push((flag, value));
            }
        }
        let args: Vec<String> = options.iter().map(|(f, v)| format!("{f}={v}")).collect();
        let cfg = parse(&args.iter().map(String::as_str).collect::<Vec<_>>());
        cfg.validate().map(|()| cfg).map_err(|e| format!("{e:#}"))
    }

    #[test]
    fn oidc_requires_audience_claim_rules_and_a_long_signing_key() {
        // OIDC alone is enough authentication.
        let oidc = validate_oidc(&[]).unwrap().oidc_config().unwrap().unwrap();
        assert_eq!(oidc.token_ttl, Duration::from_secs(3600));
        assert!(oidc.write_claims.is_none() && oidc.namespace_claim.is_none());
        assert!(parse(&[]).oidc_config().unwrap().is_none());

        for (flag, expected) in [
            ("--oidc-audience", "OIDC_AUDIENCE is required"),
            ("--oidc-required-claims", "OIDC_REQUIRED_CLAIMS is required"),
            (
                "--oidc-signing-key",
                "OIDC_SIGNING_KEY or OIDC_SIGNING_KEY_FILE",
            ),
        ] {
            let err = validate_oidc(&[(flag, "")]).unwrap_err();
            assert!(err.contains(expected), "{err}");
        }
        // Raw strings are rejected even when 32 characters long.
        let raw_key = "raw-secret-key-of-32-characters!";
        let err = validate_oidc(&[("--oidc-signing-key", raw_key)]).unwrap_err();
        assert!(err.contains("must be base64"), "{err}");
        assert!(!err.contains("raw-secret"), "secret leaked: {err}");

        let slash = [(
            "--oidc-issuer",
            "https://token.actions.githubusercontent.com/",
        )];
        let oidc = validate_oidc(&slash).unwrap().oidc_config().unwrap();
        let issuer = oidc.unwrap().issuer;
        assert_eq!(issuer, "https://token.actions.githubusercontent.com");

        for invalid in [
            (
                "--oidc-issuer",
                "http://token.actions.githubusercontent.com",
            ),
            ("--oidc-required-claims", "repository_owner"),
            ("--oidc-write-claims", "ref"),
            ("--oidc-token-ttl-secs", "0"),
            ("--oidc-token-ttl-secs", "86401"),
            ("--allow-unauthenticated", "true"),
        ] {
            assert!(validate_oidc(&[invalid]).is_err(), "{invalid:?}");
        }
        let unisolated = [
            ("--oidc-namespace-claim", "repository_owner"),
            ("--isolate-teams", "false"),
        ];
        let err = validate_oidc(&unisolated).unwrap_err();
        assert!(err.contains("ISOLATE_TEAMS"), "{err}");
    }

    #[test]
    fn uploads_per_namespace_defaults_to_half_with_scoped_tokens() {
        let cfg = parse(&["--tokens", "t"]);
        // Single trust domain: only the server-wide limit.
        assert_eq!(cfg.uploads_per_namespace(false), 0);
        // Namespace-scoped tenants: half of MAX_CONCURRENT_UPLOADS (32).
        assert_eq!(cfg.uploads_per_namespace(true), 16);
        let cfg = parse(&["--tokens", "t", "--max-concurrent-uploads", "3"]);
        assert_eq!(cfg.uploads_per_namespace(true), 1);
        let cfg = parse(&["--tokens", "t", "--max-concurrent-uploads", "1"]);
        assert_eq!(cfg.uploads_per_namespace(true), 0);
        // Explicit values win; values >= the server-wide limit disable the share.
        let cfg = parse(&[
            "--tokens",
            "t",
            "--max-concurrent-uploads-per-namespace",
            "8",
        ]);
        assert_eq!(cfg.uploads_per_namespace(false), 8);
        assert_eq!(cfg.uploads_per_namespace(true), 8);
        let cfg = parse(&[
            "--tokens",
            "t",
            "--max-concurrent-uploads-per-namespace",
            "32",
        ]);
        assert_eq!(cfg.uploads_per_namespace(true), 0);
    }

    #[test]
    fn scoped_tokens_split_at_first_separator() {
        let cfg = parse(&["--scoped-tokens", "team=abc:def,other:x=y, spaced = tok "]);
        let scoped = cfg.effective_scoped_tokens().unwrap();
        assert_eq!(
            scoped,
            vec![
                ("team".to_string(), "abc:def".to_string()),
                ("other".to_string(), "x=y".to_string()),
                ("spaced".to_string(), "tok".to_string()),
            ]
        );
    }

    #[test]
    fn scoped_tokens_reject_invalid_entries() {
        let long_ns = format!("{}=tok", "a".repeat(129));
        for bad in [
            "a/b=tok",
            ".hidden=tok",
            "..=tok",
            "=tok",
            "ns=",
            "notoken",
            "bad ns=tok",
            &long_ns,
        ] {
            let cfg = parse(&["--scoped-tokens", bad]);
            assert!(cfg.validate().is_err(), "{bad} should be rejected");
        }
    }

    #[test]
    fn host_supports_ipv4_ipv6_and_localhost() {
        let addr = |host: &str| {
            parse(&["--host", host, "--port", "3001"])
                .socket_addr()
                .unwrap()
                .to_string()
        };
        assert_eq!(addr("0.0.0.0"), "0.0.0.0:3001");
        assert_eq!(addr("::"), "[::]:3001");
        assert_eq!(addr("[::]"), "[::]:3001");
        assert_eq!(addr("::1"), "[::1]:3001");
        assert_eq!(addr("[::1]"), "[::1]:3001");
        assert_eq!(addr("fe80::1"), "[fe80::1]:3001");
        assert_eq!(addr("localhost"), "127.0.0.1:3001");
        assert!(parse(&["--host", "example.com"]).socket_addr().is_err());
        assert!(parse(&["--host", "[::1"]).socket_addr().is_err());
    }

    #[test]
    fn healthcheck_addr_maps_unspecified_to_loopback() {
        let hc = |host: &str| {
            parse(&["--host", host, "--port", "4000"])
                .healthcheck_addr()
                .unwrap()
                .to_string()
        };
        assert_eq!(hc("0.0.0.0"), "127.0.0.1:4000");
        assert_eq!(hc("::"), "[::1]:4000");
        assert_eq!(hc("10.1.2.3"), "10.1.2.3:4000");
        assert_eq!(
            parse(&[]).healthcheck_addr().unwrap().to_string(),
            "127.0.0.1:3000"
        );
    }

    #[test]
    fn s3_presets_and_endpoint_defaults() {
        let default_endpoint = "https://s3.us-east-1.amazonaws.com";
        let custom = ["--s3-endpoint", "http://store:9000/"];
        for (kind, extra, provider, region, endpoint) in [
            ("s3", &[][..], "s3", "us-east-1", default_endpoint),
            (
                "rustfs",
                &[],
                "rustfs",
                "us-east-1",
                "http://127.0.0.1:9000",
            ),
            ("garage", &[], "garage", "garage", "http://127.0.0.1:3900"),
            ("s3", &custom, "s3", "us-east-1", "http://store:9000"),
        ] {
            let cfg = parse(&s3_args(kind, extra))
                .resolve_s3_config_with_env(&no_env)
                .unwrap();
            assert_eq!(
                (
                    cfg.provider_name,
                    cfg.region.as_str(),
                    cfg.endpoint.as_str()
                ),
                (provider, region, endpoint),
                "{kind} {extra:?}"
            );
        }
    }

    #[test]
    fn force_path_style_defaults_and_explicit_override() {
        type Env<'a> = &'a dyn Fn(&str) -> Option<String>;
        let aws_endpoint = |name: &str| {
            (name == "AWS_ENDPOINT_URL").then(|| "http://store.local:9000/".to_string())
        };
        let resolve = |kind: &str, bucket: &str, extra: &[&str], env: Env| {
            let mut args = vec!["--storage-type", kind, "--s3-bucket", bucket];
            args.extend_from_slice(extra);
            parse(&args).resolve_s3_config_with_env(env).unwrap()
        };
        let endpoint = ["--s3-endpoint", "https://r2.example.com"];
        // Unset: on for the presets, custom endpoints and dotted bucket names.
        let cases: [(&str, &str, &[&str], Env, bool); 6] = [
            ("s3", "bucket", &[], &no_env, false),
            ("rustfs", "bucket", &[], &no_env, true),
            ("garage", "bucket", &[], &no_env, true),
            ("s3", "my.dotted.bucket", &[], &no_env, true),
            ("s3", "bucket", &endpoint, &no_env, true),
            ("s3", "bucket", &[], &aws_endpoint, true),
        ];
        for (kind, bucket, extra, env, expected) in cases {
            let cfg = resolve(kind, bucket, extra, env);
            assert_eq!(cfg.force_path_style, expected, "{kind} {bucket} {extra:?}");
            let forced_off = [extra, &["--s3-force-path-style=false"]].concat();
            let cfg = resolve(kind, bucket, &forced_off, env);
            assert!(!cfg.force_path_style, "explicit false: {kind} {bucket}");
        }
        let cfg = resolve("s3", "bucket", &["--s3-force-path-style"], &no_env);
        assert!(cfg.force_path_style);
        let cfg = resolve("s3", "bucket", &[], &aws_endpoint);
        assert_eq!(cfg.endpoint, "http://store.local:9000");
    }

    #[test]
    fn aws_env_fallbacks_are_read_through_env_function() {
        let vars: HashMap<&str, &str> = HashMap::from([
            ("AWS_BUCKET", "env-bucket"),
            ("AWS_REGION", "eu-central-1"),
            ("AWS_ACCESS_KEY_ID", "ENVKEY"),
            ("AWS_SECRET_ACCESS_KEY", "ENVSECRET"),
            ("AWS_SESSION_TOKEN", "ENVSESSION"),
        ]);
        let env = |name: &str| vars.get(name).map(|v| v.to_string());
        let cfg = parse(&["--storage-type", "s3"])
            .resolve_s3_config_with_env(&env)
            .unwrap();
        assert_eq!(cfg.bucket, "env-bucket");
        assert_eq!(cfg.region, "eu-central-1");
        assert_eq!(cfg.endpoint, "https://s3.eu-central-1.amazonaws.com");
        assert_eq!(cfg.access_key_id, "ENVKEY");
        assert_eq!(cfg.secret_access_key, "ENVSECRET");
        assert_eq!(cfg.session_token.as_deref(), Some("ENVSESSION"));
    }

    #[test]
    fn blank_s3_endpoint_uses_the_default_and_warns() {
        let cfg = parse(&s3_args("rustfs", &["--s3-endpoint", ""]));
        let resolved = cfg.resolve_s3_config_with_env(&no_env).unwrap();
        assert_eq!(resolved.endpoint, "http://127.0.0.1:9000");
        let warning = cfg.blank_s3_endpoint_warning(false, &resolved.endpoint);
        assert!(warning.is_some());
        // AWS_ENDPOINT_URL supplied the endpoint: nothing to warn about.
        assert!(cfg.blank_s3_endpoint_warning(true, "http://x").is_none());
        let unset = parse(&s3_args("rustfs", &[]));
        assert!(unset.blank_s3_endpoint_warning(false, "http://x").is_none());
    }

    /// `CliConfig` derives `Debug`, so secrets stay out of it only through their field types
    /// (`Secret`, `RedactedUrl`). Every option whose env value `--help` hides is a secret: give
    /// each one a unique value and check that none of them appears in the `Debug` output. A new
    /// secret option declared as a plain `String` fails here.
    #[test]
    fn debug_output_never_contains_a_hidden_option_value() {
        let cmd = CliConfig::command();
        let hidden: Vec<String> = cmd
            .get_arguments()
            .filter(|arg| arg.is_hide_env_values_set())
            .map(|arg| {
                arg.get_long()
                    .expect("hidden option has a long name")
                    .to_string()
            })
            .collect();
        assert!(hidden.len() >= 6, "{hidden:?}");
        let values: Vec<String> = hidden
            .iter()
            .enumerate()
            // URL-shaped so that `RedactedUrl` fields hide it as userinfo too.
            .map(|(i, long)| format!("--{long}=http://u:hidden-value-{i}@host/"))
            .collect();
        let mut args = vec!["--storage-type", "s3", "--s3-bucket", "bucket"];
        args.extend(values.iter().map(String::as_str));
        let debug = format!("{:?}", parse(&args));
        for (i, long) in hidden.iter().enumerate() {
            assert!(
                !debug.contains(&format!("hidden-value-{i}")),
                "--{long} leaked: {debug}"
            );
        }
    }

    #[test]
    fn s3_endpoint_must_be_http_without_credentials() {
        let cfg = parse(&s3_args(
            "s3",
            &["--s3-endpoint", "https://user:hunter2@s3.example.com"],
        ));
        let err = format!("{:#}", cfg.resolve_s3_config_with_env(&no_env).unwrap_err());
        assert!(err.contains("must not contain credentials"), "{err}");
        assert!(!err.contains("hunter2"), "{err}");

        let env = |name: &str| {
            (name == "AWS_ENDPOINT_URL").then(|| "http://u:p4ss@store:9000".to_string())
        };
        let err = format!(
            "{:#}",
            parse(&s3_args("s3", &[]))
                .resolve_s3_config_with_env(&env)
                .unwrap_err()
        );
        assert!(!err.contains("p4ss"), "{err}");

        for bad in ["ftp://s3.example.com", "not a url", "http://"] {
            let err =
                parse(&s3_args("s3", &["--s3-endpoint", bad])).resolve_s3_config_with_env(&no_env);
            assert!(err.is_err(), "{bad} should be rejected");
        }
    }

    #[test]
    fn redact_url_userinfo_hides_credentials() {
        assert_eq!(
            redact_url_userinfo("https://u:p@host:9000/x"),
            "https://[REDACTED]@host:9000/x"
        );
        assert_eq!(redact_url_userinfo("https://host/a@b"), "https://host/a@b");
        assert_eq!(redact_url_userinfo("http://host:9000"), "http://host:9000");
    }

    #[test]
    fn s3_static_credentials_are_all_or_nothing() {
        let resolve = |extra: &[&str]| {
            let args = [&["--storage-type", "s3", "--s3-bucket", "b"], extra].concat();
            parse(&args).resolve_s3_config_with_env(&no_env)
        };
        // No static credentials: the credential provider chain is used.
        assert!(!resolve(&[]).unwrap().has_static_credentials());
        assert!(resolve(&["--s3-access-key-id", "A"]).is_err());
        assert!(resolve(&["--s3-secret-access-key", "S"]).is_err());
        let no_bucket = parse(&["--storage-type", "s3"]).resolve_s3_config_with_env(&no_env);
        assert!(no_bucket.is_err(), "bucket is required");
    }

    #[test]
    fn token_files_are_merged() {
        let file = secret_file("file-a,file-b\n# comment line\n\n  file-c  \n");
        let path = file.path().to_str().unwrap();
        let mut cfg = parse(&["--tokens", "cli-token", "--token-file", path]);
        cfg.load_secret_files().unwrap();
        assert_eq!(
            cfg.effective_tokens(),
            vec!["cli-token", "file-a", "file-b", "file-c"]
        );

        let file = secret_file("acme=tok1\nbeta:tok2\n");
        let mut cfg = parse(&["--scoped-tokens-file", file.path().to_str().unwrap()]);
        cfg.load_secret_files().unwrap();
        assert_eq!(
            cfg.effective_scoped_tokens().unwrap(),
            vec![
                ("acme".into(), "tok1".into()),
                ("beta".into(), "tok2".into())
            ]
        );
        cfg.validate().unwrap();

        let (file, scoped_file) = (secret_file("ro\n"), secret_file("acme=ro-scoped\n"));
        let mut cfg = parse(&[
            "--read-only-token-file",
            file.path().to_str().unwrap(),
            "--scoped-read-only-tokens-file",
            scoped_file.path().to_str().unwrap(),
        ]);
        cfg.load_secret_files().unwrap();
        assert_eq!(cfg.effective_read_only_tokens(), vec!["ro"]);
        assert_eq!(
            cfg.effective_scoped_read_only_tokens().unwrap(),
            vec![("acme".into(), "ro-scoped".into())]
        );
    }

    #[test]
    fn secret_files_report_errors() {
        let empty = secret_file(" \n");
        let comments_only = secret_file("\n# nothing\n");
        for (flag, file) in [
            ("--token-file", &comments_only),
            ("--scoped-tokens-file", &comments_only),
            ("--s3-secret-access-key-file", &empty),
            ("--azure-storage-access-key-file", &empty),
        ] {
            let path = file.path().to_str().unwrap();
            assert!(parse(&[flag, path]).load_secret_files().is_err(), "{flag}");
        }

        let mut missing = parse(&["--token-file", "/nonexistent/turbo-token-file"]);
        let err = format!("{:#}", missing.load_secret_files().unwrap_err());
        assert!(err.contains("TURBO_TOKEN_FILE"), "{err}");
    }

    #[test]
    fn s3_secret_access_key_file_is_read_and_exclusive() {
        let file = secret_file("  s3-secret-from-file\n");
        let path = file.path().to_str().unwrap();
        let mut cfg = parse(&[
            "--storage-type",
            "s3",
            "--s3-bucket",
            "bucket",
            "--s3-access-key-id",
            "AKID",
            "--s3-secret-access-key-file",
            path,
        ]);
        cfg.load_secret_files().unwrap();
        let resolved = cfg.resolve_s3_config_with_env(&no_env).unwrap();
        assert_eq!(resolved.secret_access_key, "s3-secret-from-file");

        let mut both = parse(&s3_args("s3", &["--s3-secret-access-key-file", path]));
        let err = format!("{:#}", both.load_secret_files().unwrap_err());
        assert!(err.contains("only one"), "{err}");
        assert!(!err.contains("SECRET\""), "{err}");
    }

    #[test]
    fn gcs_options_and_google_env_fallbacks() {
        let resolve = |args: &[&str], env: &dyn Fn(&str) -> Option<String>| {
            parse(&[&["--storage-type", "gcs"], args].concat()).resolve_gcs_config_with_env(env)
        };
        assert!(resolve(&[], &no_env).is_err(), "bucket is required");
        let cfg = resolve(
            &[
                "--gcs-bucket",
                "b",
                "--storage-prefix",
                "/ci/",
                "--gcs-service-account-key-file",
                "/k.json",
                "--storage-max-retries",
                "5",
            ],
            &no_env,
        )
        .unwrap();
        assert_eq!(
            (cfg.bucket.as_str(), cfg.prefix.as_str(), cfg.max_retries),
            ("b", "ci", 5)
        );
        assert_eq!(cfg.service_account_key_file, Some(PathBuf::from("/k.json")));

        let vars = HashMap::from([
            ("GOOGLE_BUCKET", "env-bucket"),
            ("GOOGLE_SERVICE_ACCOUNT", "/sa.json"),
            ("GOOGLE_APPLICATION_CREDENTIALS", "/adc.json"),
        ]);
        let cfg = resolve(&[], &|name| vars.get(name).map(|v| v.to_string())).unwrap();
        assert_eq!(cfg.bucket, "env-bucket");
        assert_eq!(
            cfg.service_account_key_file,
            Some(PathBuf::from("/sa.json"))
        );
        assert_eq!(
            cfg.application_credentials,
            Some(PathBuf::from("/adc.json"))
        );
    }

    #[test]
    fn azure_options_env_fallbacks_and_exclusive_credentials() {
        let resolve = |args: &[&str], env: &dyn Fn(&str) -> Option<String>| {
            parse(&[&["--storage-type", "azure"], args].concat()).resolve_azure_config_with_env(env)
        };
        assert!(resolve(&["--azure-container", "c"], &no_env).is_err());
        assert!(resolve(&["--azure-storage-account", "a"], &no_env).is_err());

        let vars = HashMap::from([
            ("AZURE_STORAGE_ACCOUNT_NAME", "acct"),
            ("AZURE_CONTAINER_NAME", "cont"),
            ("AZURE_STORAGE_ACCOUNT_KEY", "a2V5"),
        ]);
        let env = |name: &str| vars.get(name).map(|v| v.to_string());
        let endpoint = "http://127.0.0.1:10000/devstoreaccount1/";
        let args = [
            "--storage-prefix",
            "ci/",
            "--azure-storage-endpoint",
            endpoint,
        ];
        let cfg = resolve(&args, &env).unwrap();
        assert_eq!(
            (
                cfg.account.as_str(),
                cfg.container.as_str(),
                cfg.prefix.as_str()
            ),
            ("acct", "cont", "ci")
        );
        assert_eq!(
            cfg.endpoint.as_deref(),
            Some("http://127.0.0.1:10000/devstoreaccount1")
        );
        assert_eq!(cfg.access_key.as_deref(), Some("a2V5"));
        let err = format!(
            "{:#}",
            resolve(&["--azure-storage-sas-token", "sv=1&sig=2"], &env).unwrap_err()
        );
        assert!(err.contains("only one"), "{err}");

        let file = secret_file("  file-key\n");
        let path = file.path().to_str().unwrap();
        let account = ["--storage-type", "azure", "--azure-storage-account", "a"];
        let args = [&account[..], &["--azure-container", "c"]].concat();
        let mut cfg = parse(&[&args[..], &["--azure-storage-access-key-file", path]].concat());
        cfg.load_secret_files().unwrap();
        let resolved = cfg.resolve_azure_config_with_env(&no_env).unwrap();
        assert_eq!(resolved.access_key.as_deref(), Some("file-key"));
        let inline = [
            "--azure-storage-access-key",
            "k",
            "--azure-storage-access-key-file",
        ];
        let mut both = parse(&[&args[..], &inline, &[path]].concat());
        let err = format!("{:#}", both.load_secret_files().unwrap_err());
        assert!(err.contains("only one"), "{err}");
    }

    #[test]
    fn zero_disables_optional_limits() {
        let cache = parse(&["--cache-ttl-secs", "0"]).cache_options();
        assert_eq!((cache.ttl, cache.negative_ttl), (None, None));
        let cache = parse(&["--negative-cache-ttl-secs", "15"]).cache_options();
        assert_eq!(cache.negative_ttl, Some(Duration::from_secs(15)));

        let local = parse(&[]).local_options();
        assert_eq!((local.max_bytes, local.max_age), (None, None));
        let retention = parse(&[]).retention_options();
        assert_eq!((retention.max_bytes, retention.max_age), (None, None));
        let limits = parse(&[
            "--storage-max-bytes",
            "1000",
            "--storage-max-age-secs",
            "60",
        ]);
        let local = limits.local_options();
        assert_eq!(local.max_bytes, Some(1000));
        assert_eq!(local.max_age, Some(Duration::from_secs(60)));
        let retention = limits.retention_options();
        assert_eq!(retention.max_bytes, Some(1000));
        assert_eq!(retention.max_age, Some(Duration::from_secs(60)));
    }

    #[test]
    fn spool_max_age_covers_slow_uploads() {
        let cfg = parse(&[
            "--local-temp-max-age-secs",
            "60",
            "--body-total-timeout-secs",
            "900",
        ]);
        assert_eq!(cfg.spool_max_age(), Duration::from_secs(1800));
        let cfg = parse(&["--local-temp-max-age-secs", "7200"]);
        assert_eq!(cfg.spool_max_age(), Duration::from_secs(7200));
    }

    #[test]
    fn spool_dir_defaults_and_holds_s3_token_copies() {
        let spool_dir = |args: &[&str]| parse(args).server_options().spool_dir;
        assert_eq!(
            spool_dir(&["--local-storage-path", "/data/cache"]),
            PathBuf::from("/data/cache/.spool")
        );
        assert_eq!(spool_dir(&["--storage-type", "s3"]), std::env::temp_dir());
        assert_eq!(
            spool_dir(&["--spool-dir", "/tmp/spool-x"]),
            PathBuf::from("/tmp/spool-x")
        );

        let token_dir = |extra: &[&str]| {
            parse(&s3_args("s3", extra))
                .resolve_s3_config_with_env(&no_env)
                .unwrap()
                .token_dir
        };
        assert_eq!(token_dir(&[]), std::env::temp_dir());
        assert_eq!(
            token_dir(&["--spool-dir", "/var/spool-x"]),
            PathBuf::from("/var/spool-x")
        );
    }
}
