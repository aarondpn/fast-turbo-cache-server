//! Google Cloud Storage backend: the GCS configuration of the shared [`ObjectStorage`] backend
//! (see `storage::object`). `object_store` talks to the GCS XML API, whose error bodies carry
//! S3-style codes (`NoSuchKey`, `NoSuchBucket`).
//!
//! - Metadata is stored in `x-goog-meta-*` headers.
//! - `PutMode::IfAbsent` is one `PUT` with `x-goog-if-generation-match: 0`.
//! - Large uploads are XML API multipart uploads; an abort that cannot be sent leaves parts
//!   behind (configure an `AbortIncompleteMultipartUpload` lifecycle rule).
//! - Credentials: a service account key file, else Application Default Credentials
//!   (`GOOGLE_APPLICATION_CREDENTIALS`, the gcloud CLI login, then the metadata server).

use std::sync::Arc;

use object_store::{ClientOptions, gcp::GoogleCloudStorageBuilder};

use crate::{
    config::GcsStorageConfig,
    storage::object::{
        Connector, ErrorCodes, METADATA_NAMES, ObjectStorage, StoreOptions, parse_key, retry_config,
    },
};

/// Largest object a single `PUT` may upload.
pub const MAX_SINGLE_PUT_BYTES: u64 = 5 * 1024 * 1024 * 1024 * 1024;

/// Creates the backend.
pub fn new(config: GcsStorageConfig) -> anyhow::Result<ObjectStorage> {
    validate_bucket(&config.bucket)?;
    if !config.prefix.is_empty() {
        parse_key(&config.prefix)
            .map_err(|reason| anyhow::anyhow!("Invalid STORAGE_PREFIX: {reason}"))?;
    }
    let mut builder = GoogleCloudStorageBuilder::new()
        .with_bucket_name(&config.bucket)
        // The builder's default options allow plain HTTP, which `Connector` takes as the mark of
        // a metadata server client.
        .with_client_options(ClientOptions::new())
        .with_http_connector(Connector {
            connect_timeout: config.connect_timeout,
            read_timeout: config.read_timeout,
        })
        .with_retry(retry_config(config.max_retries));
    if let Some(path) = &config.service_account_key_file {
        builder = builder.with_service_account_path(path.to_string_lossy());
    } else {
        if let Some(path) = &config.application_credentials {
            builder = builder.with_application_credentials(path.to_string_lossy());
        }
        tracing::info!(
            "No GCS service account key file configured; using Application Default Credentials"
        );
    }
    let store = builder
        .clone()
        .build()
        .map_err(|e| anyhow::anyhow!("Invalid GCS configuration: {e}"))?;
    let probe_store = builder
        .with_credentials(Arc::clone(store.credentials()))
        .with_retry(retry_config(0))
        .build()
        .map_err(|e| anyhow::anyhow!("Invalid GCS configuration: {e}"))?;
    let options = StoreOptions {
        name: "gcs",
        prefix: config.prefix,
        health_check_key: None,
        parts_in_flight: config.parts_in_flight,
        connect_timeout: config.connect_timeout,
        read_timeout: config.read_timeout,
        max_single_put: MAX_SINGLE_PUT_BYTES,
        metadata: &METADATA_NAMES,
        codes: ErrorCodes {
            missing_object: "NoSuchKey",
            missing_bucket: "NoSuchBucket",
        },
        forbidden_miss: None,
        lost_commit: None,
    };
    Ok(ObjectStorage::new(store, probe_store, options))
}

fn validate_bucket(bucket: &str) -> anyhow::Result<()> {
    if bucket.is_empty()
        || !bucket.bytes().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'-' | b'_')
        })
    {
        anyhow::bail!(
            "Invalid GCS bucket name '{bucket}': only lowercase ASCII letters, digits, '.', '-' and '_' are allowed"
        );
    }
    Ok(())
}
