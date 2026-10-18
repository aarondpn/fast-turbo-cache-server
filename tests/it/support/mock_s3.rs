//! In-process mock S3 server with an independent SigV4 verifier (canonical URI computed from the
//! raw request path the way S3 does: decode each segment once, re-encode once), multipart uploads,
//! `ListObjectsV2`, `DeleteObjects` and fault injection. Shared by every test that talks to an S3
//! backend.

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use axum::{
    Router,
    body::Body,
    extract::{Request, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use chrono::{DateTime, SecondsFormat, Utc};
use hmac::{Hmac, KeyInit, Mac};
use percent_encoding::percent_decode_str;
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

pub const ACCESS_KEY: &str = "AKIAMOCKACCESSKEY";
pub const SECRET_KEY: &str = "mock/secret+key/0123456789";
pub const BUCKET: &str = "turbo-bucket";
pub const REGION: &str = "us-east-1";

#[derive(Clone)]
pub struct StoredObject {
    pub body: Bytes,
    pub meta: Vec<(String, String)>,
    pub etag: String,
    pub last_modified: SystemTime,
}

impl StoredObject {
    /// An object as stored by `PutObject` with these `x-amz-meta-*` headers.
    pub fn new(body: &[u8], meta: &[(&str, &str)]) -> Self {
        Self {
            body: Bytes::copy_from_slice(body),
            meta: meta
                .iter()
                .map(|(n, v)| (n.to_string(), v.to_string()))
                .collect(),
            etag: "\"seeded\"".to_string(),
            last_modified: SystemTime::now(),
        }
    }

    /// The value of the `x-amz-meta-*` (or `content-type`) header `name`.
    pub fn meta(&self, name: &str) -> Option<&str> {
        self.meta
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }
}

#[derive(Clone, Debug)]
pub struct LoggedRequest {
    pub method: Method,
    pub raw_path: String,
    pub query: String,
    pub headers: HeaderMap,
    pub body_len: usize,
}

impl LoggedRequest {
    /// Whether the query string has the parameter `name`.
    pub fn has_param(&self, name: &str) -> bool {
        self.query
            .split('&')
            .any(|pair| pair.split('=').next() == Some(name))
    }

    /// `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload` or
    /// `AbortMultipartUpload`, by method and query parameters.
    pub fn multipart_op(&self) -> Option<&'static str> {
        match self.method {
            Method::POST if self.has_param("uploads") => Some("create"),
            Method::PUT if self.has_param("partNumber") => Some("part"),
            Method::POST if self.has_param("uploadId") => Some("complete"),
            Method::DELETE if self.has_param("uploadId") => Some("abort"),
            _ => None,
        }
    }
}

#[allow(dead_code)]
pub enum Scripted {
    /// Process the request normally (lets a script target a later request).
    Pass,
    /// Respond with this status (and optional S3 error code) without touching the store.
    Status(u16, Option<&'static str>),
    /// Process the request normally, then pretend it failed with this status (lost response).
    ApplyThen(u16),
    /// Respond with a redirect to `location`.
    Redirect(u16, String),
    /// Run the orphan spool sweep on this directory with a zero age cutoff (as if the upload
    /// outlived the sweep's age limit), then respond with this status.
    SweepSpoolThen(std::path::PathBuf, u16),
}

/// An unfinished multipart upload.
#[derive(Clone)]
pub struct PendingUpload {
    pub key: String,
    pub meta: Vec<(String, String)>,
    pub parts: BTreeMap<u32, Bytes>,
}

#[derive(Default)]
pub struct MockInner {
    /// The only bucket that exists.
    pub bucket: String,
    /// The region every request must be signed for.
    pub region: String,
    pub secrets: HashMap<String, String>,
    pub required_token: Option<String>,
    pub objects: HashMap<String, StoredObject>,
    pub log: Vec<LoggedRequest>,
    pub signature_failures: usize,
    pub scripted: VecDeque<Scripted>,
    pub missing_as_403: bool,
    pub no_such_bucket: bool,
    /// HEAD answers carry no `Content-Length` (a proxy stripped it).
    pub strip_head_length: bool,
    /// Status of `ListObjectsV2` (the readiness probe) when not 200.
    pub list_status: Option<u16>,
    pub list_delay: Duration,
    /// Every `UploadPart` takes at least this long.
    pub part_delay: Duration,
    pub uploads: HashMap<String, PendingUpload>,
    pub aborted_uploads: usize,
    pub parts_in_flight: usize,
    pub max_parts_in_flight: usize,
    pub counter: u64,
}

#[derive(Clone, Default)]
pub struct Mock(Arc<Mutex<MockInner>>);

impl Mock {
    /// A mock serving [`BUCKET`] in [`REGION`] that accepts [`ACCESS_KEY`] / [`SECRET_KEY`].
    pub fn new() -> Self {
        Self::with_identity(BUCKET, REGION, ACCESS_KEY, SECRET_KEY)
    }

    /// A mock serving `bucket` in `region` that accepts one access key pair.
    pub fn with_identity(bucket: &str, region: &str, access_key: &str, secret: &str) -> Self {
        let mock = Mock::default();
        mock.with(|m| {
            m.bucket = bucket.to_string();
            m.region = region.to_string();
            m.secrets.insert(access_key.to_string(), secret.to_string());
        });
        mock
    }

    pub fn with<R>(&self, f: impl FnOnce(&mut MockInner) -> R) -> R {
        f(&mut self.0.lock().unwrap())
    }

    pub fn log(&self) -> Vec<LoggedRequest> {
        self.with(|m| m.log.clone())
    }

    pub fn clear_log(&self) {
        self.with(|m| m.log.clear());
    }

    pub fn script(&self, items: impl IntoIterator<Item = Scripted>) {
        self.with(|m| m.scripted.extend(items));
    }

    pub fn object(&self, key: &str) -> Option<StoredObject> {
        self.with(|m| m.objects.get(key).cloned())
    }

    pub fn signature_failures(&self) -> usize {
        self.with(|m| m.signature_failures)
    }

    /// The multipart operations of the logged requests, in order.
    pub fn multipart_ops(&self) -> Vec<&'static str> {
        self.log()
            .iter()
            .filter_map(LoggedRequest::multipart_op)
            .collect()
    }
}

/// Serves `mock` on an ephemeral loopback port; returns the endpoint URL.
pub async fn spawn_mock(mock: &Mock) -> String {
    super::spawn_server(Router::new().fallback(handle).with_state(mock.clone()))
        .await
        .0
}

fn xml_error(status: u16, code: &str, with_body: bool) -> Response {
    let status = StatusCode::from_u16(status).unwrap();
    if !with_body {
        return status.into_response();
    }
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{code}</Code><Message>mock {code}</Message><RequestId>REQ123</RequestId></Error>"
    );
    (
        status,
        [(header::CONTENT_TYPE, "application/xml")],
        [("x-amz-request-id", "REQ123")],
        body,
    )
        .into_response()
}

fn xml(body: String) -> Response {
    ([(header::CONTENT_TYPE, "application/xml")], body).into_response()
}

fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn unescape(text: &str) -> String {
    text.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

/// Query parameters, decoded like S3 does (`+` is a space).
fn query_params(uri: &Uri) -> HashMap<String, String> {
    let decode = |s: &str| {
        percent_decode_str(&s.replace('+', " "))
            .decode_utf8()
            .unwrap()
            .to_string()
    };
    uri.query()
        .unwrap_or("")
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            (decode(k), decode(v))
        })
        .collect()
}

/// The texts of all `<name>` elements of `xml`.
fn elements(xml: &str, name: &str) -> Vec<String> {
    let (open, close) = (format!("<{name}>"), format!("</{name}>"));
    xml.split(&open)
        .skip(1)
        .filter_map(|rest| rest.split_once(&close).map(|(text, _)| unescape(text)))
        .collect()
}

/// Decrements the in-flight part counter when the part request is done (also when cancelled).
struct PartInFlight(Mock);

impl Drop for PartInFlight {
    fn drop(&mut self) {
        self.0.with(|m| m.parts_in_flight -= 1);
    }
}

async fn handle(State(mock): State<Mock>, req: Request) -> Response {
    let (parts, body) = req.into_parts();
    let method = parts.method.clone();
    let has_body = method != Method::HEAD;
    let _part = (method == Method::PUT && query_params(&parts.uri).contains_key("partNumber"))
        .then(|| {
            mock.with(|m| {
                m.parts_in_flight += 1;
                m.max_parts_in_flight = m.max_parts_in_flight.max(m.parts_in_flight);
            });
            PartInFlight(mock.clone())
        });
    let body = axum::body::to_bytes(body, usize::MAX).await.unwrap();

    let verdict = mock.with(|m| verify_sigv4(m, &method, &parts.uri, &parts.headers, &body));
    mock.with(|m| {
        m.log.push(LoggedRequest {
            method: method.clone(),
            raw_path: parts.uri.path().to_string(),
            query: parts.uri.query().unwrap_or("").to_string(),
            headers: parts.headers.clone(),
            body_len: body.len(),
        })
    });
    if let Err(reason) = verdict {
        mock.with(|m| m.signature_failures += 1);
        eprintln!("mock S3: signature verification failed: {reason}");
        return xml_error(403, "SignatureDoesNotMatch", has_body);
    }

    let scripted = mock.with(|m| m.scripted.pop_front());
    let mut apply_then = None;
    match scripted {
        Some(Scripted::Status(status, code)) => {
            return xml_error(status, code.unwrap_or("InternalError"), has_body);
        }
        Some(Scripted::Redirect(status, location)) => {
            return (
                StatusCode::from_u16(status).unwrap(),
                [(header::LOCATION, location)],
            )
                .into_response();
        }
        Some(Scripted::ApplyThen(status)) => apply_then = Some(status),
        Some(Scripted::Pass) => {}
        Some(Scripted::SweepSpoolThen(dir, status)) => {
            let removed =
                fast_turbo_cache_server::storage::purge_stale_spool_files(&dir, Duration::ZERO)
                    .await
                    .unwrap();
            assert_eq!(removed, 1, "the in-use spool file was swept");
            return xml_error(status, "InternalError", has_body);
        }
        None => {}
    }

    let response = process(&mock, &method, &parts.uri, &parts.headers, body).await;
    match apply_then {
        Some(status) => xml_error(status, "InternalError", has_body),
        None => response,
    }
}

async fn process(
    mock: &Mock,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: Bytes,
) -> Response {
    let has_body = method != Method::HEAD;
    let raw = uri.path().trim_start_matches('/');
    let (bucket, raw_key) = raw.split_once('/').unwrap_or((raw, ""));
    let key = percent_decode_str(raw_key)
        .decode_utf8()
        .unwrap()
        .to_string();
    if mock.with(|m| bucket != m.bucket || m.no_such_bucket) {
        return xml_error(404, "NoSuchBucket", has_body);
    }
    let params = query_params(uri);
    if key.is_empty() {
        return bucket_operation(mock, method, &params, &body).await;
    }
    if params.contains_key("uploads") || params.contains_key("uploadId") {
        return multipart_operation(mock, method, &key, &params, headers, body).await;
    }

    let missing = |m: &MockInner| {
        if m.missing_as_403 {
            xml_error(403, "AccessDenied", has_body)
        } else {
            xml_error(404, "NoSuchKey", has_body)
        }
    };

    match *method {
        Method::PUT => mock.with(|m| {
            if headers.get(header::IF_NONE_MATCH).map(|v| v.as_bytes()) == Some(b"*")
                && m.objects.contains_key(&key)
            {
                return xml_error(412, "PreconditionFailed", true);
            }
            m.counter += 1;
            let etag = format!("\"etag-{}\"", m.counter);
            m.objects.insert(
                key.clone(),
                StoredObject {
                    body,
                    meta: stored_meta(headers),
                    etag: etag.clone(),
                    last_modified: SystemTime::now(),
                },
            );
            (StatusCode::OK, [(header::ETAG, etag)]).into_response()
        }),
        Method::GET | Method::HEAD => {
            let (object, strip_head_length) =
                mock.with(|m| (m.objects.get(&key).cloned(), m.strip_head_length));
            let Some(object) = object else {
                return mock.with(|m| missing(m));
            };
            let mut resp_headers = HeaderMap::new();
            resp_headers.insert(header::ETAG, HeaderValue::from_str(&object.etag).unwrap());
            for (n, v) in &object.meta {
                resp_headers.insert(
                    header::HeaderName::from_bytes(n.as_bytes()).unwrap(),
                    HeaderValue::from_str(v).unwrap(),
                );
            }
            if *method == Method::HEAD {
                if strip_head_length {
                    // A body of unknown length keeps hyper from adding `Content-Length: 0`.
                    let empty = futures_util::stream::empty::<Result<Bytes, std::io::Error>>();
                    let mut resp = (StatusCode::OK, resp_headers).into_response();
                    *resp.body_mut() = Body::from_stream(empty);
                    return resp;
                }
                resp_headers.insert(header::CONTENT_LENGTH, HeaderValue::from(object.body.len()));
                return (StatusCode::OK, resp_headers).into_response();
            }
            if headers.get(header::RANGE).is_some() {
                if object.body.is_empty() {
                    return xml_error(416, "InvalidRange", true);
                }
                let first = object.body.slice(..1);
                resp_headers.insert(
                    header::CONTENT_RANGE,
                    HeaderValue::from_str(&format!("bytes 0-0/{}", object.body.len())).unwrap(),
                );
                return (StatusCode::PARTIAL_CONTENT, resp_headers, first).into_response();
            }
            (StatusCode::OK, resp_headers, object.body).into_response()
        }
        _ => xml_error(405, "MethodNotAllowed", has_body),
    }
}

/// The `x-amz-meta-*` and `content-type` headers of an upload.
fn stored_meta(headers: &HeaderMap) -> Vec<(String, String)> {
    headers
        .iter()
        .filter(|(n, _)| n.as_str().starts_with("x-amz-meta-") || *n == header::CONTENT_TYPE)
        .map(|(n, v)| (n.to_string(), v.to_str().unwrap().to_string()))
        .collect()
}

/// `ListObjectsV2` and `DeleteObjects`.
async fn bucket_operation(
    mock: &Mock,
    method: &Method,
    params: &HashMap<String, String>,
    body: &[u8],
) -> Response {
    match *method {
        Method::GET if params.get("list-type").map(String::as_str) == Some("2") => {
            let delay = mock.with(|m| m.list_delay);
            tokio::time::sleep(delay).await;
            if let Some(status) = mock.with(|m| m.list_status) {
                return xml_error(status, "AccessDenied", true);
            }
            let prefix = params.get("prefix").cloned().unwrap_or_default();
            let max_keys = params
                .get("max-keys")
                .map_or(1000, |v| v.parse::<usize>().unwrap());
            let contents: String = mock.with(|m| {
                let mut keys: Vec<_> = m
                    .objects
                    .iter()
                    .filter(|(k, _)| k.starts_with(&prefix))
                    .collect();
                keys.sort_by(|a, b| a.0.cmp(b.0));
                keys.into_iter()
                    .take(max_keys)
                    .map(|(k, o)| {
                        format!(
                            "<Contents><Key>{}</Key><LastModified>{}</LastModified><ETag>{}</ETag><Size>{}</Size></Contents>",
                            escape(k),
                            DateTime::<Utc>::from(o.last_modified)
                                .to_rfc3339_opts(SecondsFormat::Millis, true),
                            escape(&o.etag),
                            o.body.len()
                        )
                    })
                    .collect()
            });
            xml(format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult><Name>{}</Name><Prefix>{}</Prefix>{contents}</ListBucketResult>",
                mock.with(|m| m.bucket.clone()),
                escape(&prefix)
            ))
        }
        Method::POST if params.contains_key("delete") => {
            let request = std::str::from_utf8(body).unwrap();
            let keys = elements(request, "Key");
            let deleted: String = mock.with(|m| {
                keys.iter()
                    .map(|k| {
                        m.objects.remove(k);
                        format!("<Deleted><Key>{}</Key></Deleted>", escape(k))
                    })
                    .collect()
            });
            xml(format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<DeleteResult>{deleted}</DeleteResult>"
            ))
        }
        _ => xml_error(405, "MethodNotAllowed", *method != Method::HEAD),
    }
}

/// `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload`.
async fn multipart_operation(
    mock: &Mock,
    method: &Method,
    key: &str,
    params: &HashMap<String, String>,
    headers: &HeaderMap,
    body: Bytes,
) -> Response {
    let no_such_upload = || xml_error(404, "NoSuchUpload", true);
    let upload_id = params.get("uploadId").cloned().unwrap_or_default();
    match *method {
        Method::POST if params.contains_key("uploads") => {
            let id = mock.with(|m| {
                m.counter += 1;
                let id = format!("upload-{}", m.counter);
                m.uploads.insert(
                    id.clone(),
                    PendingUpload {
                        key: key.to_string(),
                        meta: stored_meta(headers),
                        parts: BTreeMap::new(),
                    },
                );
                id
            });
            xml(format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<InitiateMultipartUploadResult><Bucket>{}</Bucket><Key>{}</Key><UploadId>{id}</UploadId></InitiateMultipartUploadResult>",
                mock.with(|m| m.bucket.clone()),
                escape(key)
            ))
        }
        Method::PUT => {
            let delay = mock.with(|m| m.part_delay);
            tokio::time::sleep(delay).await;
            let number: u32 = params["partNumber"].parse().unwrap();
            mock.with(|m| match m.uploads.get_mut(&upload_id) {
                Some(upload) if upload.key == key => {
                    upload.parts.insert(number, body);
                    (
                        StatusCode::OK,
                        [(header::ETAG, format!("\"part-{number}\""))],
                    )
                        .into_response()
                }
                _ => no_such_upload(),
            })
        }
        Method::POST => {
            let request = std::str::from_utf8(&body).unwrap().to_string();
            mock.with(|m| {
                let Some(upload) = m.uploads.get(&upload_id).filter(|u| u.key == key).cloned()
                else {
                    return no_such_upload();
                };
                let numbers: Vec<u32> = elements(&request, "PartNumber")
                    .iter()
                    .map(|n| n.parse().unwrap())
                    .collect();
                let mut data = Vec::new();
                for number in &numbers {
                    let Some(part) = upload.parts.get(number) else {
                        return xml_error(400, "InvalidPart", true);
                    };
                    data.extend_from_slice(part);
                }
                assert_eq!(
                    numbers.len(),
                    upload.parts.len(),
                    "every uploaded part is completed"
                );
                m.uploads.remove(&upload_id);
                m.counter += 1;
                let etag = format!("\"multipart-{}\"", m.counter);
                m.objects.insert(
                    key.to_string(),
                    StoredObject {
                        body: Bytes::from(data),
                        meta: upload.meta,
                        etag: etag.clone(),
                        last_modified: SystemTime::now(),
                    },
                );
                xml(format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CompleteMultipartUploadResult><Key>{}</Key><ETag>{}</ETag></CompleteMultipartUploadResult>",
                    escape(key),
                    escape(&etag)
                ))
            })
        }
        Method::DELETE => mock.with(|m| match m.uploads.remove(&upload_id) {
            Some(_) => {
                m.aborted_uploads += 1;
                StatusCode::NO_CONTENT.into_response()
            }
            None => no_such_upload(),
        }),
        _ => xml_error(405, "MethodNotAllowed", *method != Method::HEAD),
    }
}

// ---- Independent SigV4 verifier ----

fn aws_uri_encode(bytes: &[u8]) -> String {
    let mut out = String::new();
    for &b in bytes {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn canonical_uri_from_raw(raw_path: &str) -> String {
    raw_path
        .split('/')
        .map(|seg| aws_uri_encode(&percent_decode_str(seg).collect::<Vec<u8>>()))
        .collect::<Vec<_>>()
        .join("/")
}

fn canonical_query_from_raw(query: Option<&str>) -> String {
    let Some(query) = query.filter(|q| !q.is_empty()) else {
        return String::new();
    };
    let mut pairs: Vec<(String, String)> = query
        .split('&')
        .map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            // S3 decodes `+` in query strings as a space.
            let dec = |s: &str| percent_decode_str(&s.replace('+', " ")).collect::<Vec<u8>>();
            (aws_uri_encode(&dec(k)), aws_uri_encode(&dec(v)))
        })
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn collapse(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn verify_sigv4(
    m: &MockInner,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<(), String> {
    let auth = headers
        .get(header::AUTHORIZATION)
        .ok_or("missing Authorization")?
        .to_str()
        .map_err(|e| e.to_string())?;
    let rest = auth
        .strip_prefix("AWS4-HMAC-SHA256 ")
        .ok_or("wrong algorithm")?;
    let mut credential = None;
    let mut signed_headers = None;
    let mut signature = None;
    for part in rest.split(',') {
        let part = part.trim();
        if let Some(v) = part.strip_prefix("Credential=") {
            credential = Some(v);
        } else if let Some(v) = part.strip_prefix("SignedHeaders=") {
            signed_headers = Some(v);
        } else if let Some(v) = part.strip_prefix("Signature=") {
            signature = Some(v);
        }
    }
    let credential = credential.ok_or("no Credential")?;
    let signed_headers = signed_headers.ok_or("no SignedHeaders")?;
    let signature = signature.ok_or("no Signature")?;
    let scope: Vec<&str> = credential.split('/').collect();
    let [access_key, date, region, service, terminator] = scope[..] else {
        return Err("bad credential scope".into());
    };
    if region != m.region || service != "s3" || terminator != "aws4_request" {
        return Err(format!("bad scope {credential}"));
    }
    let secret = m.secrets.get(access_key).ok_or("unknown access key")?;
    let names: Vec<&str> = signed_headers.split(';').collect();
    for required in ["host", "x-amz-content-sha256", "x-amz-date"] {
        if !names.contains(&required) {
            return Err(format!("{required} not signed"));
        }
    }
    if headers.contains_key("x-amz-security-token") && !names.contains(&"x-amz-security-token") {
        return Err("security token not signed".into());
    }
    if let Some(required) = &m.required_token
        && headers
            .get("x-amz-security-token")
            .map(|v| v.to_str().unwrap())
            != Some(required.as_str())
    {
        return Err("missing/wrong security token".into());
    }
    let amz_date = headers
        .get("x-amz-date")
        .ok_or("no x-amz-date")?
        .to_str()
        .unwrap();
    let payload_hash = headers
        .get("x-amz-content-sha256")
        .ok_or("no x-amz-content-sha256")?
        .to_str()
        .unwrap();
    if payload_hash != "UNSIGNED-PAYLOAD" && payload_hash != hex::encode(Sha256::digest(body)) {
        return Err("payload hash mismatch".into());
    }

    let mut canonical_headers = String::new();
    for name in &names {
        let values: Vec<String> = headers
            .get_all(*name)
            .iter()
            .map(|v| collapse(v.to_str().unwrap()))
            .collect();
        if values.is_empty() {
            return Err(format!("signed header {name} missing"));
        }
        canonical_headers.push_str(&format!("{name}:{}\n", values.join(",")));
    }
    let canonical_request = format!(
        "{method}\n{}\n{}\n{canonical_headers}\n{signed_headers}\n{payload_hash}",
        canonical_uri_from_raw(uri.path()),
        canonical_query_from_raw(uri.query()),
    );
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{date}/{region}/s3/aws4_request\n{}",
        hex::encode(Sha256::digest(canonical_request.as_bytes()))
    );
    let k_date = hmac(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac(&k_date, region.as_bytes());
    let k_service = hmac(&k_region, b"s3");
    let k_signing = hmac(&k_service, b"aws4_request");
    let expected = hex::encode(hmac(&k_signing, string_to_sign.as_bytes()));
    if expected != signature {
        return Err(format!(
            "signature mismatch; canonical request was:\n{canonical_request}"
        ));
    }
    Ok(())
}
