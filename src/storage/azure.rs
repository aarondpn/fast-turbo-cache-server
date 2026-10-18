//! Azure Blob Storage backend: the Azure configuration of the shared [`ObjectStorage`] backend
//! (see `storage::object`).
//!
//! - Metadata is stored in `x-ms-meta-*` headers. Azure metadata names must be C# identifiers,
//!   so the hyphenated names are written with `_` (`dirty_hash`, `client_ci`).
//! - `PutMode::IfAbsent` is one `Put Blob` with `If-None-Match: *`.
//! - Large uploads are block uploads committed by `Put Block List`. Uncommitted blocks cannot be
//!   deleted: Azure discards those of a failed upload after 7 days. A commit discards the
//!   uncommitted blocks of concurrent uploads of the same blob, whose commits then fail with
//!   `InvalidBlockList`; they count as stored when the blob has the expected size.
//! - Missing blobs are misses (404 `BlobNotFound`); `ContainerNotFound` and authorization
//!   failures are storage errors.
//! - Credentials: an access key or SAS token, else workload identity (`AZURE_CLIENT_ID`,
//!   `AZURE_TENANT_ID`, `AZURE_FEDERATED_TOKEN_FILE`, optional `AZURE_AUTHORITY_HOST`), else
//!   managed identity (instance metadata, or `IDENTITY_ENDPOINT`; `AZURE_CLIENT_ID` selects a
//!   user-assigned identity).

use std::sync::Arc;

use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};

use crate::{
    config::AzureStorageConfig,
    storage::object::{
        Connector, ErrorCodes, METADATA_NAMES, MetadataNames, ObjectStorage, StoreOptions,
        is_loopback_host, parse_endpoint, parse_key, retry_config,
    },
};

/// Largest blob a single `Put Blob` may upload.
pub const MAX_SINGLE_PUT_BYTES: u64 = 5000 * 1024 * 1024;

const AZURE_METADATA_NAMES: MetadataNames = MetadataNames {
    dirty_hash: "dirty_hash",
    client_ci: "client_ci",
    ..METADATA_NAMES
};

/// Creates the backend, using the process environment for workload and managed identity.
pub fn new(config: AzureStorageConfig) -> anyhow::Result<ObjectStorage> {
    with_env(config, &|name| std::env::var(name).ok())
}

/// Like [`new`] but reads the identity environment variables through `env`.
pub fn with_env(
    config: AzureStorageConfig,
    env: &dyn Fn(&str) -> Option<String>,
) -> anyhow::Result<ObjectStorage> {
    validate_name("storage account", &config.account, |b| {
        b.is_ascii_lowercase() || b.is_ascii_digit()
    })?;
    validate_name("container", &config.container, |b| {
        b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'
    })?;
    if !config.prefix.is_empty() {
        parse_key(&config.prefix)
            .map_err(|reason| anyhow::anyhow!("Invalid STORAGE_PREFIX: {reason}"))?;
    }
    let mut builder = MicrosoftAzureBuilder::new()
        .with_account(&config.account)
        .with_container_name(&config.container)
        .with_http_connector(Connector {
            connect_timeout: config.connect_timeout,
            read_timeout: config.read_timeout,
        })
        .with_retry(retry_config(config.max_retries));
    if let Some(endpoint) = &config.endpoint {
        let url = parse_endpoint(endpoint, "Azure storage endpoint")?;
        if url.scheme() == "http" && !is_loopback_host(&url) {
            tracing::warn!(
                "Azure storage endpoint uses plain HTTP to a non-loopback host: artifacts, metadata and SAS tokens travel unencrypted. Use HTTPS"
            );
        }
        builder = builder.with_endpoint(url.as_str().trim_end_matches('/').to_string());
    }
    let builder = match (&config.access_key, &config.sas_token) {
        (Some(key), _) => builder.with_access_key(&**key),
        (None, Some(sas)) => builder.with_config(AzureConfigKey::SasKey, &**sas),
        (None, None) => {
            let (builder, source) = with_identity(builder, env)?;
            tracing::info!(
                source,
                "No Azure access key or SAS token configured; using {source}"
            );
            builder
        }
    };
    let store = builder
        .clone()
        .build()
        .map_err(|e| anyhow::anyhow!("Invalid Azure configuration: {e}"))?;
    let probe_store = builder
        .with_credentials(Arc::clone(store.credentials()))
        .with_retry(retry_config(0))
        .build()
        .map_err(|e| anyhow::anyhow!("Invalid Azure configuration: {e}"))?;
    let options = StoreOptions {
        name: "azure",
        prefix: config.prefix,
        health_check_key: None,
        parts_in_flight: config.parts_in_flight,
        connect_timeout: config.connect_timeout,
        read_timeout: config.read_timeout,
        max_single_put: MAX_SINGLE_PUT_BYTES,
        metadata: &AZURE_METADATA_NAMES,
        codes: ErrorCodes {
            missing_object: "BlobNotFound",
            missing_bucket: "ContainerNotFound",
        },
        forbidden_miss: None,
        lost_commit: Some("InvalidBlockList"),
    };
    Ok(ObjectStorage::new(store, probe_store, options))
}

/// Configures workload identity (the variables the AKS workload identity webhook injects) or,
/// without a federated token file, managed identity. Returns the builder and the source name.
fn with_identity(
    mut builder: MicrosoftAzureBuilder,
    env: &dyn Fn(&str) -> Option<String>,
) -> anyhow::Result<(MicrosoftAzureBuilder, &'static str)> {
    let get = |name: &str| {
        env(name)
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
    };
    if let Some(client_id) = get("AZURE_CLIENT_ID") {
        builder = builder.with_client_id(client_id);
    }
    let Some(token_file) = get("AZURE_FEDERATED_TOKEN_FILE") else {
        if let Some(endpoint) = get("IDENTITY_ENDPOINT") {
            builder = builder.with_msi_endpoint(endpoint);
        }
        return Ok((builder, "managed-identity"));
    };
    let (Some(_), Some(tenant_id)) = (get("AZURE_CLIENT_ID"), get("AZURE_TENANT_ID")) else {
        anyhow::bail!(
            "AZURE_FEDERATED_TOKEN_FILE (workload identity) also needs AZURE_CLIENT_ID and AZURE_TENANT_ID"
        );
    };
    builder = builder
        .with_tenant_id(tenant_id)
        .with_federated_token_file(token_file);
    if let Some(host) = get("AZURE_AUTHORITY_HOST") {
        builder = builder.with_authority_host(host);
    }
    Ok((builder, "workload-identity"))
}

/// Account and container names become part of the request URL (host or path).
fn validate_name(what: &str, name: &str, allowed: fn(u8) -> bool) -> anyhow::Result<()> {
    if name.is_empty() || !name.bytes().all(allowed) {
        anyhow::bail!(
            "Invalid Azure {what} name '{name}': only lowercase ASCII letters and digits{} are allowed",
            if allowed(b'-') { " and '-'" } else { "" }
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Without a key or SAS token: workload identity when a federated token file is set (which
    /// then needs the client and tenant), managed identity otherwise.
    #[test]
    fn identity_selection() {
        let source = |pairs: &[(&str, &str)]| {
            let env = |name: &str| {
                let found = pairs.iter().find(|(key, _)| *key == name);
                found.map(|(_, value)| value.to_string())
            };
            with_identity(MicrosoftAzureBuilder::new(), &env).map(|(_, source)| source)
        };
        assert_eq!(source(&[]).unwrap(), "managed-identity");
        assert_eq!(
            source(&[("AZURE_CLIENT_ID", "id"), ("AZURE_TENANT_ID", "t")]).unwrap(),
            "managed-identity"
        );
        let workload = [
            ("AZURE_FEDERATED_TOKEN_FILE", "/var/run/token"),
            ("AZURE_CLIENT_ID", "id"),
            ("AZURE_TENANT_ID", "t"),
        ];
        assert_eq!(source(&workload).unwrap(), "workload-identity");
        let err = source(&workload[..2]).unwrap_err().to_string();
        assert!(err.contains("AZURE_TENANT_ID"), "{err}");
    }

    #[test]
    fn names_endpoint_and_prefix_are_validated() {
        let build = |configure: &dyn Fn(&mut AzureStorageConfig)| {
            let mut config = AzureStorageConfig {
                account: "devstoreaccount1".into(),
                container: "turbo-cache".into(),
                access_key: Some(crate::config::Secret("a2V5".into())),
                ..AzureStorageConfig::default()
            };
            configure(&mut config);
            with_env(config, &|_| None)
        };
        build(&|_| {}).unwrap();
        build(&|c| {
            c.endpoint = Some(crate::config::RedactedUrl(
                "http://127.0.0.1:10000/devstoreaccount1".into(),
            ))
        })
        .unwrap();
        for bad in ["evil.example.com/x", "Account", ""] {
            assert!(build(&|c| c.account = bad.into()).is_err(), "{bad:?}");
        }
        for bad in ["a/b", "Turbo", "a_b"] {
            assert!(build(&|c| c.container = bad.into()).is_err(), "{bad:?}");
        }
        assert!(build(&|c| c.prefix = "a//b".into()).is_err());
        for bad in [
            "https://u:secret-pw@acct.blob.core.windows.net",
            "https://acct.blob.core.windows.net/?sv=2022&sig=secret-pw",
            "ftp://acct.blob.core.windows.net",
        ] {
            let err = build(&|c| c.endpoint = Some(crate::config::RedactedUrl(bad.into())))
                .unwrap_err()
                .to_string();
            assert!(!err.contains("secret-pw"), "{err}");
        }
    }
}
