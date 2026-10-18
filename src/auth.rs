use std::{borrow::Cow, collections::HashMap, fmt, sync::Arc};

use axum::http::{HeaderMap, header::AUTHORIZATION};
use sha2::{Digest, Sha256};
use subtle::{ConditionallySelectable, ConstantTimeEq};

use crate::{
    error::{AppError, AppResult},
    oidc::CacheTokenKey,
};

/// What a token may access.
#[derive(Clone)]
pub struct Grant {
    /// Valid for every namespace (e.g. `TURBO_TOKEN`).
    pub(crate) unscoped: bool,
    /// Namespaces this token is bound to (e.g. `TURBO_SCOPED_TOKENS`).
    pub(crate) namespaces: Box<[Box<str>]>,
    /// May not upload or delete artifacts (e.g. `TURBO_READ_ONLY_TOKENS`).
    pub(crate) read_only: bool,
}

impl Grant {
    /// Access of a minted cache token: one namespace, or every namespace (`None`).
    pub(crate) fn minted(namespace: Option<String>, read_only: bool) -> Self {
        Self {
            unscoped: namespace.is_none(),
            namespaces: namespace.map(Box::from).into_iter().collect(),
            read_only,
        }
    }
}

/// One distinct configured token (by SHA-256 digest) and what it may access. A token configured
/// several times (unscoped and/or for several namespaces) is merged into one entry. A token that
/// is both unscoped and scoped is valid for every namespace, and one that is both read-only and
/// read-write is read-only (`CliConfig::validate` rejects both combinations at startup).
#[derive(Clone)]
struct TokenEntry {
    digest: [u8; 32],
    grant: Grant,
}

#[derive(Clone)]
pub struct AuthConfig {
    tokens: Arc<[TokenEntry]>,
    /// Verifies cache tokens minted by the OIDC token exchange (`None`: OIDC not configured).
    cache_token_key: Option<CacheTokenKey>,
    allow_unauthenticated: bool,
    /// Upload share groups: namespaces that are linked by a scoped token valid for several of
    /// them, mapped to the group's key (its smallest namespace). Namespaces not listed form a
    /// group of their own. See [`AuthConfig::upload_share_key`].
    upload_groups: Arc<HashMap<Box<str>, Box<str>>>,
}

impl fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthConfig")
            .field(
                "tokens",
                &format_args!("[REDACTED; {} token(s)]", self.tokens.len()),
            )
            .field("cache_token_key", &self.cache_token_key)
            .field("allow_unauthenticated", &self.allow_unauthenticated)
            .finish()
    }
}

/// Result of authenticating a request (opaque).
#[derive(Clone)]
pub struct Principal<'a> {
    /// `None`: anonymous access (no tokens configured and `ALLOW_UNAUTHENTICATED=true`).
    /// Borrowed for configured tokens, owned for minted cache tokens.
    grant: Option<Cow<'a, Grant>>,
}

impl Principal<'_> {
    /// True for anonymous access (open server) and unscoped tokens, i.e. callers that may read
    /// global (cross-namespace) data such as `/metrics`.
    pub fn is_unscoped(&self) -> bool {
        self.grant.as_deref().is_none_or(|grant| grant.unscoped)
    }

    /// Whether this principal may access `namespace`. `None` means the request does not target
    /// any namespace (e.g. `/v8/artifacts/status` without query), which every token may call.
    pub fn allows_namespace(&self, namespace: Option<&str>) -> bool {
        match (self.grant.as_deref(), namespace) {
            (None, _) | (_, None) => true,
            (Some(grant), Some(ns)) => {
                grant.unscoped || grant.namespaces.iter().any(|allowed| &**allowed == ns)
            }
        }
    }

    /// True for read-only tokens (anonymous access may write).
    pub fn is_read_only(&self) -> bool {
        self.grant.as_deref().is_some_and(|grant| grant.read_only)
    }

    /// Rejects uploads and deletes with a read-only token.
    pub fn ensure_writable(&self) -> AppResult<()> {
        if self.is_read_only() {
            // Not `forbidden` or `remote_caching_*`: turbo clients answer those by disabling the
            // remote cache for the rest of the run, reads included. Any other code only fails the
            // one upload (with a warning).
            Err(AppError::forbidden(
                "forbidden_read_only",
                "Token is read-only (run turbo with --cache=local:rw,remote:r)",
            ))
        } else {
            Ok(())
        }
    }
}

impl AuthConfig {
    pub fn new(tokens: Vec<String>, allow_unauthenticated: bool) -> Self {
        Self {
            tokens: Arc::default(),
            cache_token_key: None,
            allow_unauthenticated,
            upload_groups: Arc::default(),
        }
        .add_tokens(tokens.into_iter().map(|tok| (None::<&str>, tok)), false)
    }

    /// Adds namespace-scoped tokens (`(namespace, token)` pairs) that are only valid
    /// for requests targeting their bound team/slug namespace.
    pub fn with_scoped_tokens<I, N, T>(self, scoped: I) -> Self
    where
        I: IntoIterator<Item = (N, T)>,
        N: AsRef<str>,
        T: AsRef<str>,
    {
        self.add_tokens(scoped.into_iter().map(|(ns, tok)| (Some(ns), tok)), false)
    }

    /// Adds read-only tokens that are valid for every namespace.
    pub fn with_read_only_tokens<I, T>(self, tokens: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        self.add_tokens(tokens.into_iter().map(|tok| (None::<&str>, tok)), true)
    }

    /// Adds read-only namespace-scoped tokens (`(namespace, token)` pairs).
    pub fn with_scoped_read_only_tokens<I, N, T>(self, scoped: I) -> Self
    where
        I: IntoIterator<Item = (N, T)>,
        N: AsRef<str>,
        T: AsRef<str>,
    {
        self.add_tokens(scoped.into_iter().map(|(ns, tok)| (Some(ns), tok)), true)
    }

    /// Also accepts the cache tokens minted by the OIDC token exchange with `key`.
    pub fn with_cache_token_key(mut self, key: CacheTokenKey) -> Self {
        self.cache_token_key = Some(key);
        self
    }

    /// Adds `(namespace, token)` pairs (`None` = unscoped), ignoring blank entries.
    fn add_tokens<I, N, T>(mut self, tokens: I, read_only: bool) -> Self
    where
        I: IntoIterator<Item = (Option<N>, T)>,
        N: AsRef<str>,
        T: AsRef<str>,
    {
        let mut entries: Vec<TokenEntry> = (*self.tokens).to_vec();
        for (ns, tok) in tokens {
            let ns = ns.as_ref().map(|ns| ns.as_ref().trim());
            let tok = tok.as_ref().trim();
            if !tok.is_empty() && ns.is_none_or(|ns| !ns.is_empty()) {
                upsert_token(&mut entries, tok, ns, read_only);
            }
        }
        self.upload_groups = Arc::new(upload_groups(&entries));
        self.tokens = Arc::from(entries);
        self
    }

    /// Key under which uploads to `namespace` are counted against
    /// `MAX_CONCURRENT_UPLOADS_PER_NAMESPACE`.
    ///
    /// Usually the namespace itself. When one scoped token is configured for several namespaces
    /// (`team-a=tok,team-c=tok`), those namespaces share one allowance: otherwise the token would
    /// get the per-namespace share once per namespace and could hold every server-wide slot.
    /// Unscoped tokens do not link namespaces (they may access every namespace by design).
    pub fn upload_share_key<'a>(&'a self, namespace: &'a str) -> &'a str {
        self.upload_groups
            .get(namespace)
            .map_or(namespace, |key| &**key)
    }

    /// Authenticates the `Authorization: Bearer <token>` header: a configured token, or a cache
    /// token minted by the OIDC token exchange.
    ///
    /// Anonymous access is granted only when `allow_unauthenticated` is set AND no token (and no
    /// OIDC token exchange) is configured at all. As soon as any token exists, every request must
    /// present a valid token: otherwise omitting the header would bypass team scoping (config
    /// validation also rejects that combination at startup).
    pub fn authenticate(&self, headers: &HeaderMap) -> AppResult<Principal<'_>> {
        if self.allow_unauthenticated && self.tokens.is_empty() && self.cache_token_key.is_none() {
            return Ok(Principal { grant: None });
        }
        let token = bearer_token(headers)?.ok_or_else(missing_header)?;
        if let Some(entry) = self.find_token(token) {
            return Ok(Principal {
                grant: Some(Cow::Borrowed(&entry.grant)),
            });
        }
        match self
            .cache_token_key
            .as_ref()
            .and_then(|key| key.verify(token))
        {
            Some(grant) => Ok(Principal {
                grant: Some(Cow::Owned(grant)),
            }),
            None => Err(AppError::Unauthorized("Invalid Bearer token".to_string())),
        }
    }

    /// The configured token equal to `token`.
    fn find_token(&self, token: &str) -> Option<&TokenEntry> {
        let candidate: [u8; 32] = Sha256::digest(token.as_bytes()).into();

        // Constant-time scan over all entries (entries have distinct digests, so at most one
        // matches); the index of the match is selected without data-dependent branches.
        let mut found = subtle::Choice::from(0u8);
        let mut index = 0u64;
        for (i, entry) in self.tokens.iter().enumerate() {
            let eq = candidate.ct_eq(&entry.digest);
            index = u64::conditional_select(&index, &(i as u64), eq);
            found |= eq;
        }
        bool::from(found).then(|| &self.tokens[index as usize])
    }

    /// Validates the `Authorization` header (any configured token is accepted).
    pub fn verify_headers(&self, headers: &HeaderMap) -> AppResult<()> {
        self.authenticate(headers).map(|_| ())
    }

    /// Validates the `Authorization` header and requires a token that is not bound to a single
    /// namespace (needed for global data such as `/metrics`).
    pub fn verify_unscoped(&self, headers: &HeaderMap) -> AppResult<()> {
        if self.authenticate(headers)?.is_unscoped() {
            Ok(())
        } else {
            Err(AppError::forbidden(
                "forbidden_scope",
                "This endpoint requires an unscoped token",
            ))
        }
    }

    /// Validates the `Authorization` header and enforces optional namespace scoping.
    pub fn verify_headers_for_namespace(
        &self,
        headers: &HeaderMap,
        namespace: Option<&str>,
    ) -> AppResult<Principal<'_>> {
        let principal = self.authenticate(headers)?;
        if principal.allows_namespace(namespace) {
            Ok(principal)
        } else {
            Err(AppError::forbidden(
                "forbidden_namespace",
                format!(
                    "Token is not authorized for namespace '{}'",
                    namespace.unwrap_or("default")
                ),
            ))
        }
    }
}

/// Groups namespaces that share a read-write scoped token (connected components); maps every
/// namespace of a group with more than one member to the group's smallest namespace.
fn upload_groups(entries: &[TokenEntry]) -> HashMap<Box<str>, Box<str>> {
    let mut groups: HashMap<Box<str>, Box<str>> = HashMap::new();
    for entry in entries {
        let grant = &entry.grant;
        if grant.unscoped || grant.read_only || grant.namespaces.len() < 2 {
            continue;
        }
        let keys: Vec<Box<str>> = grant
            .namespaces
            .iter()
            .map(|ns| groups.get(ns).cloned().unwrap_or_else(|| ns.clone()))
            .collect();
        let merged = keys.iter().min().expect("at least two namespaces").clone();
        for key in groups.values_mut() {
            if keys.contains(key) {
                *key = merged.clone();
            }
        }
        for ns in grant.namespaces.iter() {
            groups.insert(ns.clone(), merged.clone());
        }
    }
    groups
}

/// Merges `token` into `entries` (deduplicated by digest).
fn upsert_token(
    entries: &mut Vec<TokenEntry>,
    token: &str,
    namespace: Option<&str>,
    read_only: bool,
) {
    let digest: [u8; 32] = Sha256::digest(token.as_bytes()).into();
    let entry = match entries.iter_mut().position(|e| e.digest == digest) {
        Some(pos) => &mut entries[pos],
        None => {
            entries.push(TokenEntry {
                digest,
                grant: Grant {
                    unscoped: false,
                    namespaces: Box::new([]),
                    read_only: false,
                },
            });
            entries.last_mut().expect("just pushed")
        }
    };
    let grant = &mut entry.grant;
    grant.read_only |= read_only;
    match namespace {
        None => grant.unscoped = true,
        Some(ns) => {
            if !grant.namespaces.iter().any(|n| &**n == ns) {
                let mut list = std::mem::take(&mut grant.namespaces).into_vec();
                list.push(ns.into());
                grant.namespaces = list.into_boxed_slice();
            }
        }
    }
}

pub(crate) fn missing_header() -> AppError {
    AppError::Unauthorized("Missing Authorization header".to_string())
}

/// Extracts the bearer token from the `Authorization` header (`None` if the header is absent).
pub(crate) fn bearer_token(headers: &HeaderMap) -> AppResult<Option<&str>> {
    let Some(value) = headers.get(AUTHORIZATION) else {
        return Ok(None);
    };
    let value = value.to_str().map_err(|_| {
        AppError::Unauthorized("Invalid characters in Authorization header".to_string())
    })?;
    let token = value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))
        .map(str::trim)
        .ok_or_else(|| {
            AppError::Unauthorized("Expected Bearer token in Authorization header".to_string())
        })?;
    if token.is_empty() {
        return Err(AppError::Unauthorized("Empty Bearer token".to_string()));
    }
    Ok(Some(token))
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;

    fn bearer(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        headers
    }

    #[test]
    fn anonymous_access_only_without_any_token() {
        let open = AuthConfig::new(vec![], true);
        assert!(open.verify_headers(&HeaderMap::new()).is_ok());
        assert!(
            open.verify_headers_for_namespace(&HeaderMap::new(), Some("x"))
                .is_ok()
        );

        // Any token configured: the header is required even with allow_unauthenticated.
        let mixed = AuthConfig::new(vec![], true).with_scoped_tokens([("team-a", "tok-a")]);
        assert!(matches!(
            mixed.verify_headers_for_namespace(&HeaderMap::new(), Some("team-b")),
            Err(AppError::Unauthorized(_))
        ));
        let mixed = AuthConfig::new(vec!["global".into()], true);
        assert!(matches!(
            mixed.verify_headers(&HeaderMap::new()),
            Err(AppError::Unauthorized(_))
        ));

        let closed = AuthConfig::new(vec![], false);
        assert!(closed.verify_headers(&HeaderMap::new()).is_err());
        assert!(closed.verify_headers(&bearer("anything")).is_err());
    }

    #[test]
    fn namespaces_sharing_a_scoped_token_share_one_upload_allowance() {
        let auth = AuthConfig::new(vec!["global".into()], false).with_scoped_tokens([
            ("team-c", "tok-ac"),
            ("team-a", "tok-ac"),
            ("team-b", "tok-b"),
            // Transitive: team-d shares a token with team-c, which shares one with team-a.
            ("team-d", "tok-cd"),
            ("team-c", "tok-cd"),
            ("team-e", "tok-ef"),
            ("team-f", "tok-ef"),
        ]);
        for ns in ["team-a", "team-c", "team-d"] {
            assert_eq!(auth.upload_share_key(ns), "team-a", "{ns}");
        }
        assert_eq!(auth.upload_share_key("team-b"), "team-b");
        assert_eq!(auth.upload_share_key("team-e"), "team-e");
        assert_eq!(auth.upload_share_key("team-f"), "team-e");
        assert_eq!(auth.upload_share_key("unlisted"), "unlisted");

        // Unscoped tokens (even when also listed as scoped) do not link namespaces.
        let auth = AuthConfig::new(vec!["both".into()], false)
            .with_scoped_tokens([("team-a", "both"), ("team-b", "both")]);
        assert_eq!(auth.upload_share_key("team-a"), "team-a");
        assert_eq!(auth.upload_share_key("team-b"), "team-b");
    }

    #[test]
    fn scoping_and_unscoped_checks() {
        let auth = AuthConfig::new(vec!["global".into()], false).with_scoped_tokens([
            ("team-a", "tok-a"),
            ("team-b", "tok-a"),
            ("team-c", "tok-c"),
            // Also unscoped: stays valid for every namespace and for unscoped endpoints.
            ("team-c", "global"),
        ]);
        // One token bound to two namespaces is valid for both.
        assert!(
            auth.verify_headers_for_namespace(&bearer("tok-a"), Some("team-a"))
                .is_ok()
        );
        assert!(
            auth.verify_headers_for_namespace(&bearer("tok-a"), Some("team-b"))
                .is_ok()
        );
        assert!(matches!(
            auth.verify_headers_for_namespace(&bearer("tok-a"), Some("team-c")),
            Err(AppError::Forbidden { .. })
        ));
        assert!(
            auth.verify_headers_for_namespace(&bearer("tok-c"), None)
                .is_ok()
        );
        assert!(
            auth.verify_headers_for_namespace(&bearer("global"), Some("any"))
                .is_ok()
        );
        assert!(matches!(
            auth.verify_headers_for_namespace(&bearer("nope"), Some("team-a")),
            Err(AppError::Unauthorized(_))
        ));

        assert!(auth.verify_unscoped(&bearer("global")).is_ok());
        assert!(matches!(
            auth.verify_unscoped(&bearer("tok-a")),
            Err(AppError::Forbidden { .. })
        ));
    }

    #[test]
    fn read_only_tokens_keep_their_scope_but_may_not_write() {
        let auth = AuthConfig::new(vec!["rw".into()], false)
            .with_read_only_tokens(["ro"])
            .with_scoped_read_only_tokens([("team-a", "ro-a"), ("team-b", "ro-a")]);
        let principal = |token| auth.authenticate(&bearer(token)).unwrap();

        assert!(principal("ro").allows_namespace(Some("any")));
        assert!(principal("ro-a").allows_namespace(Some("team-b")));
        assert!(!principal("ro-a").allows_namespace(Some("team-c")));
        for token in ["ro", "ro-a"] {
            assert!(principal(token).is_read_only());
            assert!(matches!(
                principal(token).ensure_writable(),
                Err(AppError::Forbidden { .. })
            ));
        }
        assert!(!principal("rw").is_read_only());
        assert!(principal("rw").ensure_writable().is_ok());

        let open = AuthConfig::new(vec![], true);
        assert!(
            open.authenticate(&HeaderMap::new())
                .unwrap()
                .ensure_writable()
                .is_ok()
        );
        // Read-only tokens cannot upload, so they do not link namespaces.
        assert_eq!(auth.upload_share_key("team-b"), "team-b");
    }

    #[test]
    fn malformed_headers_are_rejected() {
        let auth = AuthConfig::new(vec!["tok".into()], false);
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Basic abc"));
        assert!(auth.verify_headers(&headers).is_err());
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer "));
        assert!(auth.verify_headers(&headers).is_err());
        headers.insert(AUTHORIZATION, HeaderValue::from_static("bearer tok"));
        assert!(auth.verify_headers(&headers).is_ok());
    }
}
