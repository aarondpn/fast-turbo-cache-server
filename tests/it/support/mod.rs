//! Helpers shared by the integration-test modules.

pub mod fake_storage;
pub mod mock_s3;
pub mod process;

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime},
};

use axum::Router;
use fast_turbo_cache_server::{
    AppState, AuthConfig, Storage, build_router,
    storage::{ArtifactPayload, StoredArtifact},
};
use futures_util::StreamExt;
use reqwest::{Method, RequestBuilder, Response};
use tokio::net::TcpListener;

/// Serves `app` on an ephemeral loopback port; returns its base URL and address.
pub async fn spawn_server(app: Router) -> (String, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), addr)
}

/// The complete payload of `artifact`.
pub async fn body_bytes(artifact: StoredArtifact) -> Vec<u8> {
    match artifact.payload {
        ArtifactPayload::Bytes(b) => b.to_vec(),
        ArtifactPayload::Stream(mut stream) => {
            let mut out = Vec::new();
            while let Some(chunk) = stream.next().await {
                out.extend_from_slice(&chunk.unwrap());
            }
            out
        }
    }
}

/// Every file below `dir` (recursively).
pub fn all_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let e = e.unwrap();
            let t = e.file_type().unwrap();
            if t.is_dir() {
                stack.push(e.path());
            } else {
                out.push(e.path());
            }
        }
    }
    out
}

/// Sets the modification time of `path` to `ago` before now.
pub fn set_mtime(path: &Path, ago: Duration) {
    let f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    f.set_modified(SystemTime::now() - ago).unwrap();
}

/// [`AppState`] over `storage` that accepts the bearer token `tok`.
pub fn tok_state(storage: Arc<dyn Storage>) -> AppState {
    AppState::new(storage, AuthConfig::new(vec!["tok".into()], false))
}

/// HTTP client for one server: request paths are relative to `base`, and the bearer token (if
/// any) is sent with every request. Clones share one connection pool.
#[derive(Clone)]
pub struct Api {
    pub base: String,
    client: reqwest::Client,
    token: Option<String>,
}

impl Api {
    /// A client for the server at `base` (`http://host:port`), without a token.
    pub fn new(base: impl Into<String>) -> Self {
        Self {
            base: base.into(),
            client: reqwest::Client::new(),
            token: None,
        }
    }

    /// Serves `build_router(state)` on an ephemeral loopback port; the client has no token.
    pub async fn serve(state: AppState) -> Self {
        Self::new(spawn_server(build_router(state)).await.0)
    }

    /// The server's socket address (for raw TCP requests).
    pub fn addr(&self) -> SocketAddr {
        self.base.strip_prefix("http://").unwrap().parse().unwrap()
    }

    /// The same server and connection pool, sending `token`.
    pub fn with_token(&self, token: &str) -> Self {
        Self {
            token: Some(token.to_string()),
            ..self.clone()
        }
    }

    /// The same server and connection pool, without a token.
    pub fn anonymous(&self) -> Self {
        Self {
            token: None,
            ..self.clone()
        }
    }

    pub fn request(&self, method: Method, path: impl AsRef<str>) -> RequestBuilder {
        let request = self
            .client
            .request(method, format!("{}{}", self.base, path.as_ref()));
        match &self.token {
            Some(token) => request.bearer_auth(token),
            None => request,
        }
    }

    pub fn get(&self, path: impl AsRef<str>) -> RequestBuilder {
        self.request(Method::GET, path)
    }

    pub fn head(&self, path: impl AsRef<str>) -> RequestBuilder {
        self.request(Method::HEAD, path)
    }

    pub fn put(&self, path: impl AsRef<str>) -> RequestBuilder {
        self.request(Method::PUT, path)
    }

    pub fn post(&self, path: impl AsRef<str>) -> RequestBuilder {
        self.request(Method::POST, path)
    }

    pub fn delete(&self, path: impl AsRef<str>) -> RequestBuilder {
        self.request(Method::DELETE, path)
    }

    pub fn options(&self, path: impl AsRef<str>) -> RequestBuilder {
        self.request(Method::OPTIONS, path)
    }
}

pub trait Fetch {
    /// Sends the request; panics on transport errors.
    async fn fetch(self) -> Response;
}

impl Fetch for RequestBuilder {
    async fn fetch(self) -> Response {
        self.send().await.unwrap()
    }
}
