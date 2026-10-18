use std::{error::Error as StdError, fmt::Write as _, io};

use axum::{
    Json,
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Application error. `Clone` so that one backend failure can be handed unchanged to every
/// request sharing it (e.g. single-flight cache fills); all variants carry plain data only.
#[derive(Debug, Clone, Error)]
pub enum AppError {
    #[error("Unauthorized: {0}")]
    Unauthorized(String),

    #[error("Forbidden ({code}): {message}")]
    Forbidden { code: String, message: String },

    #[error("Bad request: {0}")]
    BadRequest(String),

    #[error("Artifact not found: {0}")]
    NotFound(String),

    /// The client did not deliver the request body in time (idle or total body timeout); 408.
    #[error("Request timeout: {0}")]
    RequestTimeout(String),

    #[error("Payload too large: {0}")]
    PayloadTooLarge(String),

    /// The server is temporarily busy (no upload slot became free in time); 429 with
    /// `Retry-After: 1`. Not 503: turbo counts every 5xx towards its outage breaker, which turns
    /// the remote cache off for 30 s (reads included) after three failures in a row, while it
    /// applies a 429's `Retry-After` as a cooldown to all artifact requests of the token and team.
    #[error("Too many requests: {0}")]
    TooManyRequests(String),

    /// An upstream storage service (S3-compatible API) failed; mapped to HTTP 502.
    #[error("Storage error: {0}")]
    Storage(String),

    /// The storage backend ran out of space (e.g. `ENOSPC`); mapped to HTTP 507.
    #[error("Insufficient storage: {0}")]
    InsufficientStorage(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorEnvelope {
    pub error: ApiErrorDetail,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiErrorDetail {
    pub code: String,
    pub message: String,
}

/// Formats an error together with its whole `source()` chain (`outer: inner: root`), so the
/// root cause is preserved when an error is converted into a string-carrying [`AppError`].
pub fn error_chain(err: &(dyn StdError + 'static)) -> String {
    let mut out = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        let text = cause.to_string();
        // Many wrappers already embed their source's message; avoid printing it twice.
        if !out.contains(&text) {
            let _ = write!(out, ": {text}");
        }
        source = cause.source();
    }
    out
}

/// Returns true when `err` means "the disk / quota is full" (`ENOSPC`, `EDQUOT`) or the file
/// would exceed the filesystem's / process's file size limit (`EFBIG`).
pub fn is_out_of_space(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::StorageFull | io::ErrorKind::QuotaExceeded | io::ErrorKind::FileTooLarge
    )
}

impl AppError {
    pub fn forbidden(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Forbidden {
            code: code.into(),
            message: message.into(),
        }
    }

    /// Converts a local I/O error (local disk backend, upload spooling): `ENOSPC`/`EDQUOT`/`EFBIG`
    /// become [`AppError::InsufficientStorage`] (507), everything else [`AppError::Internal`]
    /// (500, not 502: a local disk is not an upstream). The message keeps the source chain.
    pub fn io(context: impl AsRef<str>, err: io::Error) -> Self {
        let message = format!("{}: {}", context.as_ref(), error_chain(&err));
        if is_out_of_space(&err) {
            Self::InsufficientStorage(message)
        } else {
            Self::Internal(message)
        }
    }

    /// [`AppError::Internal`] whose message preserves the full source chain of `err`.
    pub fn internal_with_source(context: impl AsRef<str>, err: &(dyn StdError + 'static)) -> Self {
        Self::Internal(format!("{}: {}", context.as_ref(), error_chain(err)))
    }

    /// Client-facing message: internal details of server-side failures are not exposed.
    fn into_public_message(self) -> String {
        match self {
            AppError::Storage(_) => "Upstream storage backend error".to_string(),
            AppError::InsufficientStorage(_) => "Insufficient storage".to_string(),
            AppError::Internal(_) => "Internal server error".to_string(),
            AppError::Unauthorized(m)
            | AppError::BadRequest(m)
            | AppError::NotFound(m)
            | AppError::RequestTimeout(m)
            | AppError::PayloadTooLarge(m)
            | AppError::TooManyRequests(m)
            | AppError::Forbidden { message: m, .. } => m,
        }
    }

    /// HTTP status code this error maps to.
    pub fn status(&self) -> StatusCode {
        match self {
            AppError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            AppError::Forbidden { .. } => StatusCode::FORBIDDEN,
            AppError::BadRequest(_) => StatusCode::BAD_REQUEST,
            AppError::NotFound(_) => StatusCode::NOT_FOUND,
            AppError::RequestTimeout(_) => StatusCode::REQUEST_TIMEOUT,
            AppError::PayloadTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
            AppError::TooManyRequests(_) => StatusCode::TOO_MANY_REQUESTS,
            AppError::Storage(_) => StatusCode::BAD_GATEWAY,
            AppError::InsufficientStorage(_) => StatusCode::INSUFFICIENT_STORAGE,
            AppError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Machine-readable error code of the JSON envelope.
    fn code(&self) -> &str {
        match self {
            AppError::Unauthorized(_) => "unauthorized",
            AppError::Forbidden { code, .. } => code,
            AppError::BadRequest(_) => "bad_request",
            AppError::NotFound(_) => "not_found",
            AppError::RequestTimeout(_) => "request_timeout",
            AppError::PayloadTooLarge(_) => "payload_too_large",
            AppError::TooManyRequests(_) => "too_many_requests",
            AppError::Storage(_) => "storage_error",
            AppError::InsufficientStorage(_) => "insufficient_storage",
            AppError::Internal(_) => "internal_server_error",
        }
    }
}

impl From<io::Error> for AppError {
    fn from(err: io::Error) -> Self {
        AppError::io("I/O error", err)
    }
}

impl IntoResponse for AppError {
    /// Builds the standard `{"error":{"code","message"}}` envelope.
    fn into_response(self) -> Response {
        match &self {
            AppError::RequestTimeout(msg) => tracing::warn!(error = %msg, "Request body timed out"),
            AppError::TooManyRequests(msg) => {
                tracing::warn!(error = %msg, "Request rejected: busy")
            }
            AppError::Storage(msg) => {
                tracing::error!(error = %msg, "Upstream storage backend error")
            }
            AppError::InsufficientStorage(msg) => {
                tracing::error!(error = %msg, "Storage backend is out of space");
            }
            AppError::Internal(msg) => tracing::error!(error = %msg, "Internal server error"),
            _ => {}
        }
        let status = self.status();
        let body = ErrorEnvelope {
            error: ApiErrorDetail {
                code: self.code().to_string(),
                message: self.into_public_message(),
            },
        };
        let mut resp = (status, Json(body)).into_response();
        if status == StatusCode::TOO_MANY_REQUESTS {
            resp.headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        }
        resp
    }
}

pub type AppResult<T> = Result<T, AppError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Error)]
    #[error("outer failure")]
    struct Outer(#[source] io::Error);

    #[test]
    fn io_errors_map_to_500_and_enospc_to_507() {
        let generic = AppError::io("write", io::Error::other("boom"));
        assert_eq!(generic.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let full = AppError::io("write", io::Error::from(io::ErrorKind::StorageFull));
        assert_eq!(full.status(), StatusCode::INSUFFICIENT_STORAGE);

        #[cfg(unix)]
        {
            // ENOSPC is 28 on Linux and macOS.
            let raw = AppError::io("write", io::Error::from_raw_os_error(28));
            assert_eq!(raw.status(), StatusCode::INSUFFICIENT_STORAGE);
        }
    }

    #[test]
    fn error_chain_keeps_sources() {
        let err = Outer(io::Error::other("disk on fire"));
        let app = AppError::internal_with_source("saving", &err);
        let AppError::Internal(msg) = app else {
            panic!("expected Internal")
        };
        assert_eq!(msg, "saving: outer failure: disk on fire");
    }

    /// The JSON `code` and the status of every variant (clients match on both).
    #[tokio::test]
    async fn every_variant_has_its_status_and_json_code() {
        for (err, status, code) in [
            (
                AppError::Unauthorized("x".into()),
                StatusCode::UNAUTHORIZED,
                "unauthorized",
            ),
            (
                AppError::forbidden("artifact_immutable", "x"),
                StatusCode::FORBIDDEN,
                "artifact_immutable",
            ),
            (
                AppError::BadRequest("x".into()),
                StatusCode::BAD_REQUEST,
                "bad_request",
            ),
            (
                AppError::NotFound("x".into()),
                StatusCode::NOT_FOUND,
                "not_found",
            ),
            (
                AppError::RequestTimeout("x".into()),
                StatusCode::REQUEST_TIMEOUT,
                "request_timeout",
            ),
            (
                AppError::PayloadTooLarge("x".into()),
                StatusCode::PAYLOAD_TOO_LARGE,
                "payload_too_large",
            ),
            (
                AppError::TooManyRequests("x".into()),
                StatusCode::TOO_MANY_REQUESTS,
                "too_many_requests",
            ),
            (
                AppError::Storage("x".into()),
                StatusCode::BAD_GATEWAY,
                "storage_error",
            ),
            (
                AppError::InsufficientStorage("x".into()),
                StatusCode::INSUFFICIENT_STORAGE,
                "insufficient_storage",
            ),
            (
                AppError::Internal("x".into()),
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_server_error",
            ),
        ] {
            let resp = err.into_response();
            assert_eq!(resp.status(), status, "{code}");
            let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            let envelope: ErrorEnvelope = serde_json::from_slice(&body).unwrap();
            assert_eq!(envelope.error.code, code);
        }
    }
}
