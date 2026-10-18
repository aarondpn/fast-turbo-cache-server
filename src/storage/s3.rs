//! S3-compatible storage backend (AWS S3, RustFS, Garage): the S3 configuration of the shared
//! [`ObjectStorage`] backend (see `storage::object` for the upload strategy, error classification
//! and HTTP client policy).
//!
//! - Metadata is stored in `x-amz-meta-{duration,size,tag,sha,dirty-hash,client-ci}` headers.
//! - `PutMode::IfAbsent` is one `PUT` with `If-None-Match: *`. Some S3-compatible stores ignore
//!   `If-None-Match` on `PUT`; with those, immutable mode is best effort. An abort that cannot be
//!   sent leaves multipart parts behind: configure an `AbortIncompleteMultipartUpload` lifecycle
//!   rule.
//! - Missing objects are misses (404 `NoSuchKey`, or 403 when `S3_TREAT_403_AS_MISS` is set for
//!   policies without `s3:ListBucket`); `NoSuchBucket` and credential problems are storage errors.
//! - Credentials: static keys, or the chain web identity (STS), container credentials (ECS, EKS
//!   Pod Identity) and EC2 instance metadata (IMDSv2), loaded and refreshed by `object_store`
//!   (see [`with_credential_chain`] and [`TokenCopy`]).

use std::{
    fmt, io,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey, S3ConditionalPut};
use url::{Host, Url};

use crate::{
    config::S3StorageConfig,
    error::AppResult,
    models::ArtifactMetadata,
    storage::{
        PutMode, PutOutcome, StoredArtifact, UploadBody,
        object::{
            Connector, ErrorCodes, Failure, METADATA_NAMES, ObjectStorage, StoreOptions,
            is_loopback_host, parse_endpoint, parse_key, retry_config,
        },
    },
    upload::UploadStream,
};

use super::Storage;

/// Largest object a single `PutObject` request may upload.
pub const MAX_SINGLE_PUT_BYTES: u64 = 5 * 1024 * 1024 * 1024;

const ERROR_CODES: ErrorCodes = ErrorCodes {
    missing_object: "NoSuchKey",
    missing_bucket: "NoSuchBucket",
};

#[derive(Clone)]
pub struct S3Storage {
    /// Effective configuration (payload signing may be forced on, see `with_env`).
    config: S3StorageConfig,
    inner: ObjectStorage,
    credential_source: &'static str,
    /// The private token copy `object_store` reads for web identity and container credentials
    /// (also owned by the credential provider; removed when the last user is dropped).
    _token_copy: Option<Arc<TokenCopy>>,
}

impl fmt::Debug for S3Storage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Storage")
            .field("provider", &self.config.provider_name)
            .field("bucket", &self.config.bucket)
            .field("region", &self.config.region)
            .field("endpoint", &self.config.endpoint)
            .field("prefix", &self.config.prefix)
            .field("force_path_style", &self.config.force_path_style)
            .field("sign_payload", &self.config.sign_payload)
            .field("credentials", &self.credential_source)
            .finish_non_exhaustive()
    }
}

impl S3Storage {
    /// Creates the backend, resolving credentials from the configuration or (when no static keys
    /// are configured) from the AWS provider chain using the process environment.
    pub fn new(config: S3StorageConfig) -> anyhow::Result<Self> {
        Self::with_env(config, &|name| std::env::var(name).ok())
    }

    /// Like [`S3Storage::new`] but reads credential-chain environment variables through `env`.
    pub fn with_env(
        mut config: S3StorageConfig,
        env: &dyn Fn(&str) -> Option<String>,
    ) -> anyhow::Result<Self> {
        let endpoint = parse_endpoint(&config.endpoint, "S3 endpoint")?;
        validate_bucket(&config.bucket)?;
        if !config.prefix.is_empty() {
            parse_key(&config.prefix)
                .map_err(|reason| anyhow::anyhow!("Invalid STORAGE_PREFIX: {reason}"))?;
        }
        if let Some(key) = &config.health_check_key {
            parse_key(key)
                .map_err(|reason| anyhow::anyhow!("Invalid S3 health check key: {reason}"))?;
        }

        // Plain HTTP to a remote host: UNSIGNED-PAYLOAD would give the payload no integrity
        // protection at all, so sign it.
        if endpoint.scheme() == "http" && !is_loopback_host(&endpoint) {
            tracing::warn!(
                provider = config.provider_name,
                "S3 endpoint uses plain HTTP to a non-loopback host: artifacts and metadata travel unencrypted. Use HTTPS; payload signing is enabled automatically for integrity"
            );
            config.sign_payload = true;
        }

        let builder = AmazonS3Builder::new()
            .with_bucket_name(&config.bucket)
            .with_region(&config.region)
            .with_endpoint(store_endpoint(&endpoint, &config)?)
            .with_virtual_hosted_style_request(!config.force_path_style)
            .with_unsigned_payload(!config.sign_payload)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .with_http_connector(Connector {
                connect_timeout: config.connect_timeout,
                read_timeout: config.read_timeout,
            });
        let (builder, credential_source, token_copy) = if config.has_static_credentials() {
            let mut builder = builder
                .with_access_key_id(&config.access_key_id)
                .with_secret_access_key(&config.secret_access_key);
            if let Some(token) = &config.session_token {
                builder = builder.with_token(token);
            }
            (builder, "static", None)
        } else {
            let aws_endpoint = is_aws_endpoint(&endpoint);
            let (builder, source, token) =
                with_credential_chain(builder, env, aws_endpoint, &config.token_dir)?;
            tracing::info!(
                provider = config.provider_name,
                source,
                "No static S3 credentials configured; using the credential provider chain"
            );
            (builder, source, token)
        };

        let mut builder = builder.with_retry(retry_config(config.max_retries));
        if let Some(copy) = &token_copy {
            // The provider `object_store` builds for the chain configuration, wrapped so the token
            // copy it reads is synced from the source first.
            let chain = builder
                .clone()
                .build()
                .map_err(|e| anyhow::anyhow!("Invalid S3 configuration: {e}"))?;
            builder = builder.with_credentials(Arc::new(SyncedTokenCredentials {
                copy: Arc::clone(copy),
                inner: Arc::clone(chain.credentials()),
            }));
        }
        let store = builder
            .clone()
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid S3 configuration: {e}"))?;
        let probe_store = builder
            .with_credentials(Arc::clone(store.credentials()))
            .with_retry(retry_config(0))
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid S3 configuration: {e}"))?;

        let forbidden_miss: fn(&Failure) -> bool = |failure| !is_credential_problem(failure);
        let options = StoreOptions {
            name: config.provider_name,
            prefix: config.prefix.clone(),
            health_check_key: config.health_check_key.clone(),
            parts_in_flight: config.parts_in_flight,
            connect_timeout: config.connect_timeout,
            read_timeout: config.read_timeout,
            max_single_put: MAX_SINGLE_PUT_BYTES,
            metadata: &METADATA_NAMES,
            codes: ERROR_CODES,
            forbidden_miss: config.treat_403_as_miss.then_some(forbidden_miss),
            lost_commit: None,
        };
        Ok(Self {
            config,
            inner: ObjectStorage::new(store, probe_store, options),
            credential_source,
            _token_copy: token_copy,
        })
    }

    /// Whether request payloads are signed (`S3_SIGN_PAYLOAD`, or forced for remote plain HTTP).
    pub fn signs_payload(&self) -> bool {
        self.config.sign_payload
    }

    /// Whether path-style addressing is used.
    pub fn uses_path_style(&self) -> bool {
        self.config.force_path_style
    }

    /// Where the credentials come from: `static`, `web-identity`, `ecs-container` or `ec2-imds`.
    pub fn credential_source(&self) -> &'static str {
        self.credential_source
    }

    /// Builds the full object key inside the bucket: `[prefix/]<namespace>/<hash>`.
    pub fn object_key(&self, namespace: &str, hash: &str) -> AppResult<String> {
        self.inner.object_key(namespace, hash)
    }

    /// The shared object store backend (retention: [`ObjectStorage::run_cleanup`]).
    pub fn object_storage(&self) -> &ObjectStorage {
        &self.inner
    }
}

/// 403s caused by credentials/signing/permission problems are never treated as misses.
///
/// Garage reports all of them (and nothing else) as `AccessDenied` with a `Forbidden: <reason>`
/// message, e.g. `Forbidden: Invalid signature`, `Forbidden: No such key: GK...` or
/// `Forbidden: Operation is not allowed for this key.`; AWS uses `Access Denied` or `User: ...
/// is not authorized ...` for `AccessDenied`, which includes a missing object without
/// `s3:ListBucket`.
fn is_credential_problem(failure: &Failure) -> bool {
    let garage_forbidden = failure.code.as_deref() == Some("AccessDenied")
        && failure
            .message()
            .is_some_and(|message| message.starts_with("Forbidden: "));
    garage_forbidden
        || failure.code.as_deref().is_some_and(|code| {
            matches!(
                code,
                "SignatureDoesNotMatch"
                    | "InvalidAccessKeyId"
                    | "ExpiredToken"
                    | "TokenRefreshRequired"
                    | "InvalidToken"
                    | "RequestTimeTooSkewed"
                    | "AuthorizationHeaderMalformed"
                    | "AccountProblem"
                    | "AllAccessDisabled"
            )
        })
}

#[async_trait]
impl Storage for S3Storage {
    fn backend_name(&self) -> &'static str {
        self.inner.backend_name()
    }

    fn streams_uploads(&self) -> bool {
        self.inner.streams_uploads()
    }

    async fn put(
        &self,
        namespace: &str,
        hash: &str,
        body: UploadBody,
        metadata: ArtifactMetadata,
        mode: PutMode,
    ) -> AppResult<PutOutcome> {
        self.inner.put(namespace, hash, body, metadata, mode).await
    }

    async fn put_stream(
        &self,
        namespace: &str,
        hash: &str,
        body: UploadStream,
        metadata: ArtifactMetadata,
    ) -> AppResult<PutOutcome> {
        self.inner.put_stream(namespace, hash, body, metadata).await
    }

    async fn get(&self, namespace: &str, hash: &str) -> AppResult<Option<StoredArtifact>> {
        self.inner.get(namespace, hash).await
    }

    async fn head(&self, namespace: &str, hash: &str) -> AppResult<Option<ArtifactMetadata>> {
        self.inner.head(namespace, hash).await
    }

    async fn delete(&self, namespace: &str, hash: &str) -> AppResult<bool> {
        self.inner.delete(namespace, hash).await
    }

    async fn health_check(&self) -> AppResult<()> {
        self.inner.health_check().await
    }
}

// ---------------------------------------------------------------------------------------------
// Configuration helpers
// ---------------------------------------------------------------------------------------------

/// Configures the credential chain (static credentials are handled by the caller): web identity,
/// container credentials, then EC2 instance metadata. IMDS is only used implicitly for AWS
/// endpoints, so a keyless Garage/RustFS deployment fails at startup instead of every request
/// waiting for an unreachable metadata service. Returns the builder, the source name and the
/// token copy `object_store` reads (see [`TokenCopy`]), which is kept in `token_dir`.
fn with_credential_chain(
    builder: AmazonS3Builder,
    env: &dyn Fn(&str) -> Option<String>,
    aws_endpoint: bool,
    token_dir: &Path,
) -> anyhow::Result<(AmazonS3Builder, &'static str, Option<Arc<TokenCopy>>)> {
    let get = |name: &str| {
        env(name)
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
    };

    if let (Some(token_file), Some(role_arn)) =
        (get("AWS_WEB_IDENTITY_TOKEN_FILE"), get("AWS_ROLE_ARN"))
    {
        let session_name = get("AWS_ROLE_SESSION_NAME").unwrap_or_else(|| {
            format!("fast-turbo-cache-server-{}", chrono::Utc::now().timestamp())
        });
        let copy = TokenCopy::of_file(
            "AWS_WEB_IDENTITY_TOKEN_FILE",
            PathBuf::from(token_file),
            token_dir,
        )?;
        let mut builder = builder
            .with_config(
                AmazonS3ConfigKey::WebIdentityTokenFile,
                copy.path().to_string_lossy(),
            )
            .with_config(AmazonS3ConfigKey::RoleArn, role_arn)
            .with_config(AmazonS3ConfigKey::RoleSessionName, session_name);
        if let Some(sts) = get("AWS_ENDPOINT_URL_STS") {
            validate_endpoint_url(&sts, "AWS_ENDPOINT_URL_STS", false)?;
            builder = builder.with_config(AmazonS3ConfigKey::StsEndpoint, sts);
        }
        return Ok((builder, "web-identity", Some(Arc::new(copy))));
    }

    if let Some(full_uri) = get("AWS_CONTAINER_CREDENTIALS_FULL_URI") {
        validate_endpoint_url(&full_uri, "AWS_CONTAINER_CREDENTIALS_FULL_URI", true)?;
        let copy = match get("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE") {
            Some(file) => TokenCopy::of_file(
                "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
                PathBuf::from(file),
                token_dir,
            )?,
            None => TokenCopy::inline(
                "AWS_CONTAINER_AUTHORIZATION_TOKEN",
                &get("AWS_CONTAINER_AUTHORIZATION_TOKEN").unwrap_or_default(),
                token_dir,
            )?,
        };
        let builder = builder
            .with_config(AmazonS3ConfigKey::ContainerCredentialsFullUri, full_uri)
            .with_config(
                AmazonS3ConfigKey::ContainerAuthorizationTokenFile,
                copy.path().to_string_lossy(),
            );
        return Ok((builder, "ecs-container", Some(Arc::new(copy))));
    }
    if let Some(relative) = get("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI") {
        let relative = format!("/{}", relative.trim_start_matches('/'));
        let builder =
            builder.with_config(AmazonS3ConfigKey::ContainerCredentialsRelativeUri, relative);
        return Ok((builder, "ecs-container", None));
    }

    let imds_setting = get("AWS_EC2_METADATA_DISABLED").map(|v| v.to_ascii_lowercase());
    let imds_disabled = matches!(imds_setting.as_deref(), Some("true" | "1"));
    let imds_endpoint = get("AWS_EC2_METADATA_SERVICE_ENDPOINT");
    let imds_requested =
        matches!(imds_setting.as_deref(), Some("false" | "0")) || imds_endpoint.is_some();
    if !imds_disabled && (aws_endpoint || imds_requested) {
        let mut builder = builder;
        if let Some(endpoint) = imds_endpoint {
            validate_endpoint_url(&endpoint, "AWS_EC2_METADATA_SERVICE_ENDPOINT", false)?;
            builder = builder.with_config(
                AmazonS3ConfigKey::MetadataEndpoint,
                endpoint.trim_end_matches('/'),
            );
        }
        return Ok((builder, "ec2-imds", None));
    }

    anyhow::bail!(
        "No S3 credentials found: set S3_ACCESS_KEY_ID/S3_SECRET_ACCESS_KEY (or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY), use a web identity (AWS_WEB_IDENTITY_TOKEN_FILE + AWS_ROLE_ARN), container credentials (AWS_CONTAINER_CREDENTIALS_*), or EC2 instance metadata (used automatically for AWS endpoints; for other endpoints set AWS_EC2_METADATA_DISABLED=false)"
    )
}

/// A token source is re-read at most this often (only when credentials are requested).
const TOKEN_SYNC_INTERVAL: Duration = Duration::from_secs(1);
const TOKEN_COPY_PREFIX: &str = ".turbo-credential-token-";

/// The private copy of a credential token that `object_store` reads: the content of
/// `AWS_WEB_IDENTITY_TOKEN_FILE` / `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`, or an inline
/// `AWS_CONTAINER_AUTHORIZATION_TOKEN` (`object_store` reads tokens from files only).
///
/// `object_store` sends the raw file content: a trailing newline (`echo token > file`, Kubernetes
/// secrets created from files) would reach STS, and in the container `Authorization` header it
/// makes the request invalid, which panics inside `object_store` on every S3 request. The copy
/// holds the trimmed token and only ever a valid one: an unreadable or invalid source fails at
/// startup, and later (a rotated file) keeps the previous token with a warning. Token files are
/// re-read at most every [`TOKEN_SYNC_INTERVAL`] when credentials are requested (see
/// [`SyncedTokenCredentials`]); the copy is replaced atomically and recreated if it was removed
/// (e.g. by a temp directory cleaner).
///
/// The copy lives in `SPOOL_DIR`, which must be writable anyway for spooled uploads, so a
/// read-only root filesystem only needs a writable `SPOOL_DIR`.
struct TokenCopy {
    /// The variable naming the source file and its path (`None`: inline token, never re-read).
    source: Option<(&'static str, PathBuf)>,
    copy: tempfile::TempPath,
    /// When the source was last read, and the token in the copy.
    synced: std::sync::Mutex<(Instant, String)>,
    /// Held while the source is read, so only one request does it.
    sync: tokio::sync::Mutex<()>,
}

impl fmt::Debug for TokenCopy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenCopy")
            .field("source", &self.source)
            .field("copy", &self.copy)
            .finish_non_exhaustive()
    }
}

impl TokenCopy {
    fn of_file(name: &'static str, path: PathBuf, dir: &Path) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(&path)
            .map_err(|e| anyhow::anyhow!("{name} '{}' cannot be read: {e}", path.display()))?;
        let token = clean_token(&raw)
            .map_err(|reason| anyhow::anyhow!("{name} '{}' {reason}", path.display()))?;
        Self::create(name, token, Some((name, path)), dir)
    }

    fn inline(name: &'static str, token: &str, dir: &Path) -> anyhow::Result<Self> {
        let token = clean_token(token).map_err(|reason| anyhow::anyhow!("{name} {reason}"))?;
        Self::create(name, token, None, dir)
    }

    fn create(
        name: &'static str,
        token: &str,
        source: Option<(&'static str, PathBuf)>,
        dir: &Path,
    ) -> anyhow::Result<Self> {
        let copy = write_private_temp_file(dir, token).map_err(|e| {
            anyhow::anyhow!(
                "Cannot keep a private copy of {name} in SPOOL_DIR '{}' ({e}); set SPOOL_DIR to a writable directory (default: the OS temp directory, TMPDIR)",
                dir.display()
            )
        })?;
        Ok(Self {
            source,
            copy,
            synced: std::sync::Mutex::new((Instant::now(), token.to_string())),
            sync: tokio::sync::Mutex::new(()),
        })
    }

    fn path(&self) -> &std::path::Path {
        &self.copy
    }

    fn synced(&self) -> std::sync::MutexGuard<'_, (Instant, String)> {
        self.synced
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn due(&self) -> bool {
        self.synced().0.elapsed() >= TOKEN_SYNC_INTERVAL
    }

    /// Re-reads a token file when due and updates the copy; a removed copy is recreated (also for
    /// an inline token).
    async fn sync(&self) {
        if !self.due() {
            return;
        }
        let _guard = self.sync.lock().await;
        if !self.due() {
            return;
        }
        let (source, copy) = (self.source.clone(), self.copy.to_path_buf());
        let current = self.synced().1.clone();
        let read = tokio::task::spawn_blocking(move || -> Result<Option<String>, String> {
            let token = match &source {
                Some((_, path)) => {
                    let raw = std::fs::read_to_string(path)
                        .map_err(|e| format!("cannot be read: {e}"))?;
                    clean_token(&raw)?.to_string()
                }
                None => current.clone(),
            };
            if token == current && copy.exists() {
                return Ok(None);
            }
            replace_private_file(&copy, &token).map_err(|e| {
                format!(
                    "cannot be copied to '{}' (SPOOL_DIR): {e}",
                    copy.parent().unwrap_or(&copy).display()
                )
            })?;
            Ok(Some(token))
        })
        .await
        .unwrap_or_else(|e| Err(format!("cannot be read: {e}")));
        let mut synced = self.synced();
        synced.0 = Instant::now();
        match read {
            Ok(Some(token)) => synced.1 = token,
            Ok(None) => {}
            Err(reason) => {
                let (name, path) = match &self.source {
                    Some((name, path)) => (*name, path.display().to_string()),
                    None => ("AWS_CONTAINER_AUTHORIZATION_TOKEN", String::new()),
                };
                tracing::warn!(path = %path, "{name} {reason}; the previous token stays in use");
            }
        }
    }
}

/// The token without surrounding whitespace; an error unless it is valid in an HTTP header.
fn clean_token(raw: &str) -> Result<&str, String> {
    let token = raw.trim();
    if token
        .bytes()
        .all(|b| b == b'\t' || (0x20..0x7f).contains(&b))
    {
        Ok(token)
    } else {
        Err("contains characters that are not allowed in a token (only visible ASCII and spaces; surrounding whitespace is removed)".into())
    }
}

/// The `object_store` credential provider for web identity or container credentials, which syncs
/// the token copy it reads before every credential request (the provider caches credentials and
/// reads the token only when it refreshes them).
#[derive(Debug)]
struct SyncedTokenCredentials {
    copy: Arc<TokenCopy>,
    inner: object_store::aws::AwsCredentialProvider,
}

#[async_trait]
impl object_store::CredentialProvider for SyncedTokenCredentials {
    type Credential = object_store::aws::AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        self.copy.sync().await;
        self.inner.get_credential().await
    }
}

/// Writes `content` to a new temp file in `dir` (created if missing, mode 0700 on unix) that is
/// readable only by this user (mode 0600 on unix).
fn write_private_temp_file(dir: &Path, content: &str) -> io::Result<tempfile::TempPath> {
    use std::io::Write as _;
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
    builder.create(dir)?;
    let mut file = tempfile::Builder::new()
        .prefix(TOKEN_COPY_PREFIX)
        .tempfile_in(dir)?;
    file.write_all(content.as_bytes())?;
    file.flush()?;
    Ok(file.into_temp_path())
}

/// Atomically replaces `path` with a private file holding `content`, so a concurrent reader sees
/// the old or the new token, never a partial one.
fn replace_private_file(path: &std::path::Path, content: &str) -> io::Result<()> {
    use std::io::Write as _;
    let dir = path.parent().unwrap_or_else(|| std::path::Path::new("."));
    let mut file = tempfile::Builder::new()
        .prefix(TOKEN_COPY_PREFIX)
        .tempfile_in(dir)?;
    file.write_all(content.as_bytes())?;
    file.flush()?;
    file.persist(path).map(drop).map_err(|e| e.error)
}

/// Validates a credential endpoint URL. For container credentials, plain HTTP is only allowed to
/// loopback or the documented ECS/EKS link-local addresses (as the AWS SDKs enforce), because the
/// response contains secrets.
fn validate_endpoint_url(value: &str, name: &str, container: bool) -> anyhow::Result<()> {
    let url = Url::parse(value).map_err(|e| anyhow::anyhow!("Invalid {name}: {e}"))?;
    if !url.username().is_empty() || url.password().is_some() {
        anyhow::bail!("Invalid {name}: credentials in the URL are not allowed");
    }
    match url.scheme() {
        "https" => Ok(()),
        "http" if !container => Ok(()),
        "http" => {
            let allowed = match url.host() {
                Some(Host::Domain(d)) => d.eq_ignore_ascii_case("localhost"),
                Some(Host::Ipv4(ip)) => {
                    ip.is_loopback()
                        || ip == std::net::Ipv4Addr::new(169, 254, 170, 2)
                        || ip == std::net::Ipv4Addr::new(169, 254, 170, 23)
                }
                Some(Host::Ipv6(ip)) => {
                    ip.is_loopback()
                        || ip == "fd00:ec2::23".parse::<std::net::Ipv6Addr>().expect("valid")
                }
                None => false,
            };
            if !allowed {
                anyhow::bail!(
                    "Invalid {name}: plain HTTP is only allowed for loopback or the ECS/EKS link-local credential endpoints"
                );
            }
            Ok(())
        }
        _ => anyhow::bail!("Invalid {name}: only http(s) URLs are supported"),
    }
}

/// The endpoint passed to `object_store`: the normalised endpoint URL for path-style requests,
/// with the bucket prepended to the host for virtual-hosted-style requests.
fn store_endpoint(endpoint: &Url, config: &S3StorageConfig) -> anyhow::Result<String> {
    let mut url = endpoint.clone();
    if !config.force_path_style {
        let host = endpoint
            .host_str()
            .ok_or_else(|| anyhow::anyhow!("S3 endpoint URL has no host"))?;
        url.set_host(Some(&format!("{}.{host}", config.bucket)))
            .map_err(|e| anyhow::anyhow!("Invalid virtual-hosted S3 bucket host: {e}"))?;
    }
    Ok(url.as_str().trim_end_matches('/').to_string())
}

/// Bucket names become part of the request URL (host or path) without escaping.
fn validate_bucket(bucket: &str) -> anyhow::Result<()> {
    if bucket.is_empty() {
        anyhow::bail!("S3 bucket name must not be empty");
    }
    if !bucket
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_'))
    {
        anyhow::bail!(
            "Invalid S3 bucket name '{bucket}': only ASCII letters, digits, '.', '-' and '_' are allowed"
        );
    }
    Ok(())
}

/// Whether the endpoint is AWS S3 (`*.amazonaws.com` / `*.amazonaws.com.cn`), where EC2 instance
/// metadata is a sensible implicit credential source.
fn is_aws_endpoint(url: &Url) -> bool {
    match url.host() {
        Some(Host::Domain(domain)) => {
            let domain = domain.trim_end_matches('.').to_ascii_lowercase();
            domain.ends_with(".amazonaws.com") || domain.ends_with(".amazonaws.com.cn")
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::Bytes;
    use futures_util::StreamExt;
    use http::StatusCode;

    use crate::config::S3StorageConfig;

    use super::*;

    fn sample_garage_config() -> S3StorageConfig {
        S3StorageConfig {
            provider_name: "garage",
            bucket: "turbo-cache".to_string(),
            region: "garage".to_string(),
            endpoint: "http://127.0.0.1:3900".to_string(),
            access_key_id: "GK0123456789abcdef01234567".to_string(),
            secret_access_key: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                .to_string(),
            session_token: None,
            prefix: "ci".to_string(),
            force_path_style: true,
            sign_payload: true,
            ..S3StorageConfig::default()
        }
    }

    fn env_of(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> + use<> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name| map.get(name).cloned()
    }

    fn keyless(endpoint: &str) -> S3StorageConfig {
        S3StorageConfig {
            access_key_id: String::new(),
            secret_access_key: String::new(),
            endpoint: endpoint.to_string(),
            ..sample_garage_config()
        }
    }

    #[test]
    fn object_keys_and_endpoints_for_path_and_virtual_hosted_style() {
        let garage = S3Storage::new(sample_garage_config()).unwrap();
        assert_eq!(
            garage.object_key("my-team", "abc123def456").unwrap(),
            "ci/my-team/abc123def456"
        );
        let endpoint = |cfg: &S3StorageConfig| {
            store_endpoint(&parse_endpoint(&cfg.endpoint, "S3 endpoint").unwrap(), cfg).unwrap()
        };
        assert_eq!(endpoint(&sample_garage_config()), "http://127.0.0.1:3900");

        let mut vhost = sample_garage_config();
        vhost.endpoint = "https://s3.us-east-1.amazonaws.com".to_string();
        vhost.force_path_style = false;
        assert_eq!(
            endpoint(&vhost),
            "https://turbo-cache.s3.us-east-1.amazonaws.com"
        );
        assert!(!S3Storage::new(vhost).unwrap().uses_path_style());

        // The endpoint base path is kept (and encoded once).
        let mut proxied = sample_garage_config();
        proxied.endpoint = "http://127.0.0.1:3900/s3 proxy/".to_string();
        assert_eq!(endpoint(&proxied), "http://127.0.0.1:3900/s3%20proxy");
    }

    #[test]
    fn prefix_bucket_and_health_key_are_validated() {
        let with = |configure: &dyn Fn(&mut S3StorageConfig)| {
            let mut cfg = sample_garage_config();
            configure(&mut cfg);
            S3Storage::new(cfg)
        };
        let s3 = with(&|cfg| cfg.prefix = "my prefix/ünï+code".to_string()).unwrap();
        assert_eq!(
            s3.object_key("team", "hash1").unwrap(),
            "my prefix/ünï+code/team/hash1"
        );
        // Keys are not re-encoded: the path is the key.
        assert_eq!(
            s3.inner.object_path("team", "hash1").unwrap().as_ref(),
            "my prefix/ünï+code/team/hash1"
        );
        for bad in ["a//b", "a/./b", "../x", "a/..", "a\tb", "a\nb"] {
            let err = with(&|cfg| cfg.prefix = bad.to_string()).unwrap_err();
            assert!(err.to_string().contains("STORAGE_PREFIX"), "{bad:?}: {err}");
            let err = with(&|cfg| cfg.health_check_key = Some(bad.to_string())).unwrap_err();
            assert!(
                err.to_string().contains("health check key"),
                "{bad:?}: {err}"
            );
        }
        for bad in ["", "turbo cache", "bucket/x", "b?x"] {
            assert!(
                with(&|cfg| cfg.bucket = bad.to_string()).is_err(),
                "{bad:?}"
            );
        }
        with(&|cfg| cfg.bucket = "My_Bucket.v2-x".to_string()).unwrap();
    }

    #[test]
    fn debug_output_never_contains_secrets() {
        let mut cfg = sample_garage_config();
        cfg.session_token = Some("session-token-value".to_string());
        let secrets = [
            cfg.secret_access_key.clone(),
            cfg.access_key_id.clone(),
            "session-token-value".to_string(),
        ];
        let s3 = S3Storage::new(cfg).unwrap();
        for out in [format!("{s3:?}"), format!("{s3:#?}")] {
            for secret in &secrets {
                assert!(!out.contains(secret.as_str()), "{out}");
            }
            assert!(out.contains("static"), "{out}");
        }
    }

    // ---- construction rules ----

    #[test]
    fn plain_http_to_remote_host_forces_payload_signing() {
        let mut cfg = sample_garage_config();
        cfg.sign_payload = false;
        cfg.endpoint = "http://10.0.0.5:3900".to_string();
        assert!(S3Storage::new(cfg.clone()).unwrap().signs_payload());

        cfg.endpoint = "http://garage:3900".to_string();
        assert!(S3Storage::new(cfg.clone()).unwrap().signs_payload());

        for local in [
            "http://127.0.0.1:3900",
            "http://localhost:3900",
            "http://[::1]:3900",
            "https://garage.example.com",
        ] {
            cfg.endpoint = local.to_string();
            assert!(
                !S3Storage::new(cfg.clone()).unwrap().signs_payload(),
                "{local}"
            );
        }
    }

    #[test]
    fn endpoint_with_userinfo_is_rejected_without_echoing_it() {
        let mut cfg = sample_garage_config();
        cfg.endpoint = "https://admin:hunter2secret@s3.example.com".to_string();
        let err = S3Storage::new(cfg).unwrap_err().to_string();
        assert!(!err.contains("hunter2secret"), "{err}");
        assert!(!err.contains("admin"), "{err}");

        let mut cfg = sample_garage_config();
        cfg.endpoint = "ftp://s3.example.com".to_string();
        assert!(S3Storage::new(cfg).is_err());
    }

    #[test]
    fn missing_static_credentials_use_provider_chain() {
        let cfg = keyless("http://127.0.0.1:3900");
        // Non-AWS endpoint: no implicit IMDS, so a keyless deployment fails at startup.
        let err = S3Storage::with_env(cfg.clone(), &|_| None).unwrap_err();
        assert!(
            err.to_string().contains("AWS_EC2_METADATA_DISABLED=false"),
            "{err}"
        );
        for env in [
            env_of(&[("AWS_EC2_METADATA_DISABLED", "false")]),
            env_of(&[("AWS_EC2_METADATA_SERVICE_ENDPOINT", "http://127.0.0.1:1338")]),
        ] {
            let s3 = S3Storage::with_env(cfg.clone(), &env).unwrap();
            assert_eq!(s3.credential_source(), "ec2-imds");
        }

        // AWS endpoint: IMDS is the implicit last link of the chain.
        let aws = keyless("https://s3.eu-central-1.amazonaws.com");
        let s3 = S3Storage::with_env(aws.clone(), &|_| None).unwrap();
        assert_eq!(s3.credential_source(), "ec2-imds");
        let disabled = env_of(&[("AWS_EC2_METADATA_DISABLED", "true")]);
        assert!(S3Storage::with_env(aws, &disabled).is_err());
    }

    #[test]
    fn credential_chain_prefers_web_identity_then_container_then_imds() {
        let cfg = keyless("https://s3.eu-west-1.amazonaws.com");
        let source = |pairs: &[(&str, &str)]| {
            S3Storage::with_env(cfg.clone(), &env_of(pairs)).map(|s3| s3.credential_source())
        };
        let token = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(token.path(), "jwt").unwrap();
        let token = token.path().to_str().unwrap();
        let web = [
            ("AWS_WEB_IDENTITY_TOKEN_FILE", token),
            ("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/test"),
            ("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/v2/creds"),
        ];
        assert_eq!(source(&web).unwrap(), "web-identity");
        assert_eq!(source(&web[2..]).unwrap(), "ecs-container");
        assert_eq!(source(&[]).unwrap(), "ec2-imds");
        // Explicit sources are used for non-AWS endpoints too.
        let garage = keyless("http://127.0.0.1:3900");
        let s3 = S3Storage::with_env(garage, &env_of(&web[2..])).unwrap();
        assert_eq!(s3.credential_source(), "ecs-container");

        assert!(source(&[("AWS_ENDPOINT_URL_STS", "ftp://sts")]).is_ok());
        let err = source(&[
            ("AWS_WEB_IDENTITY_TOKEN_FILE", token),
            ("AWS_ROLE_ARN", "arn"),
            ("AWS_ENDPOINT_URL_STS", "https://u:p@sts.example.com"),
        ])
        .unwrap_err();
        assert!(err.to_string().contains("AWS_ENDPOINT_URL_STS"), "{err}");
    }

    #[test]
    fn container_full_uri_must_be_https_or_local() {
        let cfg = keyless("https://s3.us-east-1.amazonaws.com");
        for ok in [
            "https://creds.example.com/x",
            "http://127.0.0.1:8080/creds",
            "http://169.254.170.23/v1/credentials",
            "http://[fd00:ec2::23]/v1/credentials",
            "http://localhost/creds",
        ] {
            let env = env_of(&[("AWS_CONTAINER_CREDENTIALS_FULL_URI", ok)]);
            assert!(S3Storage::with_env(cfg.clone(), &env).is_ok(), "{ok}");
        }
        for bad in ["http://evil.example.com/creds", "ftp://127.0.0.1/x"] {
            let env = env_of(&[("AWS_CONTAINER_CREDENTIALS_FULL_URI", bad)]);
            assert!(S3Storage::with_env(cfg.clone(), &env).is_err(), "{bad}");
        }
    }

    /// Token files are copied without surrounding whitespace; a token that cannot be sent (or a
    /// missing file) is a startup error that names the variable but never shows the token.
    #[test]
    fn credential_tokens_are_trimmed_and_validated_at_startup() {
        let cfg = keyless("http://127.0.0.1:3900");
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("token");
        let file_str = file.to_str().unwrap();
        let full_uri = (
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            "http://127.0.0.1:1/creds",
        );
        let cases = [
            ("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", true),
            ("AWS_WEB_IDENTITY_TOKEN_FILE", true),
            ("AWS_CONTAINER_AUTHORIZATION_TOKEN", false),
        ];
        for (name, is_file) in cases {
            let build = |content: &str| {
                std::fs::write(&file, content).unwrap();
                let value = if is_file { file_str } else { content };
                S3Storage::with_env(
                    cfg.clone(),
                    &env_of(&[
                        full_uri,
                        (name, value),
                        ("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/t"),
                    ]),
                )
            };
            let s3 = build(" pod-token\r\n").unwrap();
            let copy = s3._token_copy.as_ref().unwrap().path();
            assert_eq!(
                std::fs::read_to_string(copy).unwrap(),
                "pod-token",
                "{name}"
            );
            for bad in ["pod\ntoken", "pod-t\u{f6}ken", "pod\u{7f}"] {
                let err = build(bad).unwrap_err().to_string();
                assert!(err.contains(name) && err.contains("not allowed"), "{err}");
                assert!(!err.contains(bad), "{err}");
            }
        }
        std::fs::remove_file(&file).unwrap();
        let env = env_of(&[
            full_uri,
            ("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", file_str),
        ]);
        let err = S3Storage::with_env(cfg, &env).unwrap_err().to_string();
        assert!(err.contains("cannot be read"), "{err}");
    }

    /// An inline container token is written to a private file in `SPOOL_DIR` (created if
    /// missing) that lives as long as the backend; an unusable `SPOOL_DIR` is a startup error
    /// that names it (but never the token).
    #[test]
    fn token_copy_is_private_and_lives_in_the_spool_dir() {
        let spool = tempfile::tempdir().unwrap();
        let dir = spool.path().join("nested").join("spool");
        let cfg = S3StorageConfig {
            token_dir: dir.clone(),
            ..keyless("http://127.0.0.1:3900")
        };
        let env = env_of(&[
            (
                "AWS_CONTAINER_CREDENTIALS_FULL_URI",
                "http://127.0.0.1:1/creds",
            ),
            ("AWS_CONTAINER_AUTHORIZATION_TOKEN", "inline-secret-token"),
        ]);
        let s3 = S3Storage::with_env(cfg.clone(), &env).unwrap();
        let copy = s3._token_copy.as_ref().unwrap().path().to_path_buf();
        assert_eq!(copy.parent(), Some(dir.as_path()));
        assert_eq!(
            std::fs::read_to_string(&copy).unwrap(),
            "inline-secret-token"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for path in [&dir, &copy] {
                let mode = std::fs::metadata(path).unwrap().permissions().mode();
                assert_eq!(mode & 0o077, 0, "{path:?} must be private");
            }
        }
        let clone = s3.clone();
        drop(s3);
        assert!(copy.exists());
        drop(clone);
        assert!(!copy.exists());

        // A SPOOL_DIR that cannot be created (below a regular file) fails at startup.
        let blocker = spool.path().join("file");
        std::fs::write(&blocker, "x").unwrap();
        let cfg = S3StorageConfig {
            token_dir: blocker.join("spool"),
            ..cfg
        };
        let err = S3Storage::with_env(cfg, &env).unwrap_err().to_string();
        assert!(
            err.contains("SPOOL_DIR") && err.contains("AWS_CONTAINER_AUTHORIZATION_TOKEN"),
            "{err}"
        );
        assert!(!err.contains("inline-secret-token"), "{err}");
    }

    /// A token copy removed behind our back (e.g. by a temp directory cleaner) is recreated on the
    /// next sync, even if the source token did not change (and for inline tokens).
    #[tokio::test]
    async fn removed_token_copy_is_recreated() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("token");
        std::fs::write(&file, "pod-token\n").unwrap();
        let file_str = file.to_str().unwrap();
        let cfg = S3StorageConfig {
            token_dir: dir.path().join("spool"),
            ..keyless("http://127.0.0.1:3900")
        };
        let full_uri = (
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            "http://127.0.0.1:1/creds",
        );
        let role = ("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/t");
        for source in [
            ("AWS_WEB_IDENTITY_TOKEN_FILE", file_str),
            ("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", file_str),
            ("AWS_CONTAINER_AUTHORIZATION_TOKEN", "pod-token"),
        ] {
            let s3 = S3Storage::with_env(cfg.clone(), &env_of(&[full_uri, role, source])).unwrap();
            let copy = Arc::clone(s3._token_copy.as_ref().unwrap());
            std::fs::remove_file(copy.path()).unwrap();
            // Not due yet: nothing is read or written.
            copy.synced().0 = Instant::now();
            copy.sync().await;
            assert!(!copy.path().exists(), "{source:?}");
            copy.synced().0 = Instant::now().checked_sub(TOKEN_SYNC_INTERVAL).unwrap();
            copy.sync().await;
            assert_eq!(
                std::fs::read_to_string(copy.path()).unwrap(),
                "pod-token",
                "{source:?}"
            );
        }
    }

    #[test]
    fn aws_endpoint_detection() {
        for (endpoint, aws) in [
            ("https://s3.amazonaws.com", true),
            ("https://s3.eu-central-1.amazonaws.com", true),
            (
                "https://bucket.vpce-1a2b.s3.us-east-1.vpce.amazonaws.com",
                true,
            ),
            ("https://s3.cn-north-1.amazonaws.com.cn", true),
            ("https://amazonaws.com.evil.example", false),
            ("https://notamazonaws.com", false),
            ("http://127.0.0.1:9000", false),
            ("https://r2.cloudflarestorage.com", false),
        ] {
            assert_eq!(
                is_aws_endpoint(&parse_endpoint(endpoint, "S3 endpoint").unwrap()),
                aws,
                "{endpoint}"
            );
        }
    }

    // ---- error classification ----

    /// The text `object_store` reports for a non-2xx answer.
    fn answer(status: &str, code: Option<&str>, message: &str) -> String {
        let body = code.map_or(String::new(), |code| {
            format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{code}</Code><Message>{message}</Message></Error>"
            )
        });
        format!(
            "Error performing GET http://127.0.0.1:3900/turbo-cache/ci/t/h in 2ms - Server returned non-2xx status code: {status}: {body}"
        )
    }

    fn failure(status: u16, code: Option<&str>, message: &str) -> Failure {
        let source = answer(
            &StatusCode::from_u16(status).unwrap().to_string(),
            code,
            message,
        )
        .into();
        let path = "ci/t/h".to_string();
        let err = match status {
            404 => object_store::Error::NotFound { path, source },
            403 => object_store::Error::PermissionDenied { path, source },
            _ => object_store::Error::Generic {
                store: "S3",
                source,
            },
        };
        Failure::new(err)
    }

    #[test]
    fn failure_classification_from_error_text() {
        let f = failure(
            404,
            Some("NoSuchBucket"),
            "The specified bucket does not exist",
        );
        assert_eq!(f.status, Some(StatusCode::NOT_FOUND));
        assert_eq!(f.code.as_deref(), Some("NoSuchBucket"));
        assert_eq!(
            f.message().as_deref(),
            Some("The specified bucket does not exist")
        );
        let shown = f.to_string();
        assert!(
            shown.starts_with("HTTP 404 NoSuchBucket: The specified bucket does not exist ("),
            "{shown}"
        );
        assert!(shown.contains("GET http://127.0.0.1:3900/"), "{shown}");
        // Statuses of generic errors come from the text; HEAD answers carry no code.
        let range = failure(416, Some("InvalidRange"), "x");
        assert_eq!(range.status, Some(StatusCode::RANGE_NOT_SATISFIABLE));
        assert_eq!(
            failure(503, None, "").status,
            Some(StatusCode::SERVICE_UNAVAILABLE)
        );
        assert_eq!(failure(404, None, "").code, None);
        let transport = Failure::new(object_store::Error::Generic {
            store: "S3",
            source: "HTTP error: connection refused".into(),
        });
        assert_eq!(transport.status, None);
        assert_eq!(
            transport.to_string(),
            "Generic S3 error: HTTP error: connection refused"
        );

        let mut cfg = sample_garage_config();
        let strict = S3Storage::new(cfg.clone()).unwrap();
        cfg.treat_403_as_miss = true;
        let lenient = S3Storage::new(cfg).unwrap();
        assert!(strict.inner.is_miss(&failure(404, Some("NoSuchKey"), "")));
        assert!(strict.inner.is_miss(&failure(404, None, "")));
        assert!(
            !strict
                .inner
                .is_miss(&failure(404, Some("NoSuchBucket"), ""))
        );
        assert!(
            !strict
                .inner
                .is_miss(&failure(403, Some("AccessDenied"), ""))
        );
        assert!(!strict.inner.is_miss(&failure(500, None, "")));
        assert!(
            lenient
                .inner
                .is_miss(&failure(403, Some("AccessDenied"), "Access Denied"))
        );
        assert!(lenient.inner.is_miss(&failure(403, None, "")));
        assert!(
            !lenient
                .inner
                .is_miss(&failure(403, Some("SignatureDoesNotMatch"), ""))
        );
        assert!(
            !lenient
                .inner
                .is_miss(&failure(403, Some("InvalidAccessKeyId"), ""))
        );

        // AWS `AccessDenied` messages stay misses (Garage's `Forbidden: ...` ones never are, see
        // `garage_forbidden_is_never_a_miss`).
        for message in [
            "Access Denied",
            "User: arn:aws:iam::123456789012:user/ci is not authorized to perform: s3:ListBucket",
        ] {
            assert!(
                lenient
                    .inner
                    .is_miss(&failure(403, Some("AccessDenied"), message)),
                "{message}"
            );
        }
        // Only `AccessDenied` (Garage's code for `Forbidden`) is classified by its message.
        assert!(
            lenient
                .inner
                .is_miss(&failure(403, Some("Forbidden"), "Forbidden: x"))
        );
    }

    // ---- request sequences ----

    /// Minimal S3 endpoint for request-sequence tests: answers with `respond(method, target)` and
    /// logs every request. Signatures are not checked (see `tests/it/support/mock_s3.rs`).
    struct MiniS3 {
        endpoint: String,
        log: Arc<std::sync::Mutex<Vec<String>>>,
    }

    type Responder = dyn Fn(&http::Method, &str) -> axum::response::Response + Send + Sync;

    impl MiniS3 {
        async fn spawn(
            respond: impl Fn(&http::Method, &str) -> axum::response::Response + Send + Sync + 'static,
        ) -> Self {
            let log: Arc<std::sync::Mutex<Vec<String>>> = Arc::default();
            let respond: Arc<Responder> = Arc::new(respond);
            let app = axum::Router::new().fallback({
                let log = log.clone();
                move |method: http::Method, uri: http::Uri| {
                    let (log, respond) = (log.clone(), respond.clone());
                    async move {
                        let target = match uri.query() {
                            Some(query) => format!("{}?{query}", uri.path()),
                            None => uri.path().to_string(),
                        };
                        log.lock().unwrap().push(format!("{method} {target}"));
                        respond(&method, &target)
                    }
                }
            });
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            Self { endpoint, log }
        }

        fn storage(&self, configure: impl FnOnce(&mut S3StorageConfig)) -> S3Storage {
            let mut cfg = sample_garage_config();
            cfg.endpoint = self.endpoint.clone();
            cfg.max_retries = 0;
            configure(&mut cfg);
            S3Storage::new(cfg).unwrap()
        }

        /// Requests since the last call, as `"METHOD path[?query]"`.
        fn take_log(&self) -> Vec<String> {
            std::mem::take(&mut *self.log.lock().unwrap())
        }
    }

    fn status(code: u16) -> axum::response::Response {
        axum::response::IntoResponse::into_response(StatusCode::from_u16(code).unwrap())
    }

    fn xml_error(code: u16, s3_code: &str, message: &str) -> axum::response::Response {
        axum::response::IntoResponse::into_response((
            StatusCode::from_u16(code).unwrap(),
            format!("<Error><Code>{s3_code}</Code><Message>{message}</Message></Error>"),
        ))
    }

    fn deleted(key: &str) -> axum::response::Response {
        axum::response::IntoResponse::into_response(format!(
            "<DeleteResult><Deleted><Key>{key}</Key></Deleted></DeleteResult>"
        ))
    }

    /// S3 answers a delete of a missing key with success; `delete` asks with `HEAD` first and
    /// reports a missing artifact as not found. Only a 404 short-cuts the delete (`DeleteObjects`,
    /// `POST ?delete`).
    #[tokio::test]
    async fn delete_reports_missing_objects_as_not_found() {
        const HEAD: &str = "HEAD /turbo-cache/ci/t/h";
        const DELETE: &str = "POST /turbo-cache?delete";
        let head = Arc::new(std::sync::Mutex::new(404u16));
        let delete = Arc::new(std::sync::Mutex::new(200u16));
        let mock = MiniS3::spawn({
            let (head, delete) = (head.clone(), delete.clone());
            move |method, _target| {
                if *method == http::Method::HEAD {
                    status(*head.lock().unwrap())
                } else {
                    match *delete.lock().unwrap() {
                        200 => deleted("ci/t/h"),
                        404 => {
                            xml_error(404, "NoSuchBucket", "The specified bucket does not exist")
                        }
                        code => xml_error(code, "AccessDenied", "Access Denied"),
                    }
                }
            }
        })
        .await;
        let set = |h: u16, d: u16| {
            *head.lock().unwrap() = h;
            *delete.lock().unwrap() = d;
        };

        // Existing object: HEAD, then DELETE.
        let s3 = mock.storage(|_| {});
        set(200, 200);
        assert!(s3.delete("t", "h").await.unwrap());
        assert_eq!(mock.take_log(), [HEAD, DELETE]);

        // Missing object, bucket seen just now: no DELETE.
        set(404, 200);
        assert!(!s3.delete("t", "h").await.unwrap());
        assert_eq!(mock.take_log(), [HEAD]);

        // A bare 404 before the bucket was seen could be `NoSuchBucket`: the DELETE tells.
        assert!(!mock.storage(|_| {}).delete("t", "h").await.unwrap());
        assert_eq!(mock.take_log(), [HEAD, DELETE]);
        set(404, 404);
        let err = mock.storage(|_| {}).delete("t", "h").await.unwrap_err();
        assert!(err.to_string().contains("NoSuchBucket"), "{err}");
        mock.take_log();

        // Any other HEAD answer sends the DELETE and reports it as without the check: a 403
        // that S3_TREAT_403_AS_MISS takes as a miss, a non-retryable 400, a 5xx.
        let lenient = mock.storage(|cfg| cfg.treat_403_as_miss = true);
        for head_status in [403, 400, 503] {
            set(head_status, 200);
            assert!(lenient.delete("t", "h").await.unwrap(), "{head_status}");
            assert_eq!(mock.take_log(), [HEAD, DELETE], "{head_status}");
            set(head_status, 403);
            assert!(lenient.delete("t", "h").await.is_err(), "{head_status}");
            mock.take_log();
        }
    }

    /// Garage answers a wrong secret, an unknown access key and a key without bucket permission
    /// with `403 AccessDenied` "Forbidden: ...": with S3_TREAT_403_AS_MISS these are errors for
    /// lookups and the readiness probe, while AWS-style 403s (missing object without
    /// `s3:ListBucket`) stay misses.
    #[tokio::test]
    async fn garage_forbidden_is_never_a_miss() {
        let message = Arc::new(std::sync::Mutex::new(String::new()));
        let mock = MiniS3::spawn({
            let message = message.clone();
            move |method, _target| {
                if *method == http::Method::HEAD {
                    status(403)
                } else {
                    xml_error(403, "AccessDenied", &message.lock().unwrap())
                }
            }
        })
        .await;
        let configure = |cfg: &mut S3StorageConfig| {
            cfg.treat_403_as_miss = true;
            cfg.health_check_key = Some("healthz/missing".to_string());
        };
        for garage in [
            "Forbidden: Invalid signature",
            "Forbidden: No such key: GK00000000000000000000unknown",
            "Forbidden: Operation is not allowed for this key.",
        ] {
            *message.lock().unwrap() = garage.to_string();
            let s3 = mock.storage(configure);
            let err = s3.health_check().await.unwrap_err().to_string();
            assert!(err.contains(garage), "{err}");
            assert_eq!(mock.take_log(), ["GET /turbo-cache/healthz/missing"]);
            assert!(s3.get("t", "h").await.is_err(), "{garage}");
            // HEAD (no body) is re-checked with a ranged GET.
            let err = s3.head("t", "h").await.unwrap_err().to_string();
            assert!(err.contains(garage), "{err}");
            assert_eq!(
                mock.take_log(),
                [
                    "GET /turbo-cache/ci/t/h",
                    "HEAD /turbo-cache/ci/t/h",
                    "GET /turbo-cache/ci/t/h"
                ]
            );
        }

        *message.lock().unwrap() = "Access Denied".to_string();
        let s3 = mock.storage(configure);
        assert!(s3.health_check().await.is_ok());
        assert_eq!(mock.take_log(), ["GET /turbo-cache/healthz/missing"]);
        assert!(s3.get("t", "h").await.unwrap().is_none());
        assert!(s3.head("t", "h").await.unwrap().is_none());
    }

    /// TRACE logging of a request (by `object_store`, reqwest, hyper) never contains the
    /// credentials.
    #[tokio::test]
    async fn trace_logging_never_contains_credentials() {
        #[derive(Clone, Default)]
        struct Capture(Arc<std::sync::Mutex<Vec<u8>>>);
        impl std::io::Write for Capture {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let mock = MiniS3::spawn(|_, _| status(404)).await;
        let capture = Capture::default();
        let writer = capture.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_writer(move || writer.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        tracing::trace!("capture works");
        let s3 = mock.storage(|cfg| cfg.session_token = Some("session-token-value".into()));
        let secrets = [
            s3.config.access_key_id.clone(),
            s3.config.secret_access_key.clone(),
            "session-token-value".to_string(),
        ];
        assert!(s3.get("t", "h").await.unwrap().is_none());
        let out = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
        assert!(out.contains("capture works"), "{out}");
        for secret in &secrets {
            assert!(!out.contains(secret.as_str()), "{out}");
        }
    }

    /// A stalled S3 response body fails after the read timeout, also when `object_store` would
    /// wait for it (here: the body of an error answer).
    #[tokio::test]
    async fn stalled_response_bodies_time_out() {
        let mock = MiniS3::spawn(|_, _| {
            let body = futures_util::stream::once(async {
                Ok::<_, io::Error>(Bytes::from_static(b"<Error><Code>"))
            })
            .chain(futures_util::stream::pending());
            let mut response = axum::response::IntoResponse::into_response(StatusCode::NOT_FOUND);
            *response.body_mut() = axum::body::Body::from_stream(body);
            response
        })
        .await;
        let s3 = mock.storage(|cfg| cfg.read_timeout = Duration::from_millis(200));
        let started = Instant::now();
        let err = s3.get("t", "h").await.err().unwrap().to_string();
        assert!(err.contains("stalled"), "{err}");
        assert!(started.elapsed() < Duration::from_secs(5));
    }
}
