use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use futures_util::{stream, StreamExt};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// What the mock update endpoint and binary endpoint return for the
/// adapter's one automatic check at startup -- configured before the
/// Tauri app is built, since there's no public hook to re-trigger a check.
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum MockResponse {
    /// 204 No Content.
    NoUpdate,
    /// 200 with a body that isn't valid JSON at all -- reqwest's own
    /// `.json()` wraps this as `Error::Reqwest`, not `Serialization`.
    MalformedJson,
    /// 200 with valid JSON missing a required `RemoteRelease` field --
    /// `Error::Serialization`.
    WrongShapeJson,
    /// Every non-2xx status with no other endpoint -- `ReleaseNotFound`.
    NotFound,
    /// The feed URL itself is unreachable (nothing listening) --
    /// `Error::Reqwest`. Handled specially by the caller, which points
    /// `update_feed_url` at a dead port instead of this mock server.
    ConnectionRefused,
    /// A feed URL that fails to parse as a URL at all -- the adapter's own
    /// updater-builder step fails before ever reaching `tauri_plugin_updater`,
    /// landing on the generic `unknown`/`unknown` fallback.
    ///
    /// Handled specially by the caller, the same way as `ConnectionRefused`.
    InvalidFeedUrl,
    /// Static-format manifest whose `platforms` map has no entry any
    /// target could match -- `TargetsNotFound`.
    StaticNoMatchingTarget,
    /// Dynamic-format manifest signed with this test-service's real
    /// keypair, binary served from `/binary` by this same mock server.
    Available {
        version: String,
        #[serde(default)]
        notes: Option<String>,
        /// Overrides the version written into the signature's own
        /// trusted comment -- mismatching `version` triggers
        /// `SignedVersionMismatch`.
        #[serde(default, rename = "signedVersion")]
        signed_version: Option<String>,
        /// Replaces a well-formed signature with corrupted bytes,
        /// bypassing real signing -- tests signature-decoding failures.
        #[serde(default, rename = "corruptSignature")]
        corrupt_signature: Option<CorruptSignature>,
        /// `/binary` fails with a non-2xx status -- `Error::Network`.
        #[serde(default, rename = "binaryFails")]
        binary_fails: bool,
    },
}

#[derive(Clone, Copy, Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CorruptSignature {
    /// Not valid base64 -- `Error::Base64`.
    NotBase64,
    /// Valid base64, but the decoded bytes aren't a minisign signature
    /// box -- `Error::Minisign`.
    NotAMinisignBox,
    /// Valid base64, but the decoded bytes aren't valid UTF-8 --
    /// `Error::SignatureUtf8`.
    NotUtf8,
}

/// What `/binary` serves once `/update` has pointed at it.
#[derive(Clone, Copy, Debug, Default)]
enum BinaryResponse {
    #[default]
    Ok,
    /// Non-2xx status -- `Error::Network`.
    Failing,
}

struct ServerState {
    response: MockResponse,
    binary_response: BinaryResponse,
    signing_key: Arc<minisign::SecretKey>,
    /// Filled in right after bind, since the manifest response needs to
    /// embed this same server's own `/binary` URL, port included.
    binary_url: String,
}

/// One ephemeral mock server per adapter instance, serving both the
/// update-manifest endpoint and the binary it may point at.
pub struct MockUpdateServer {
    pub port: u16,
    // Held for the server's lifetime only -- axum's router keeps its own
    // clone for the actual request handling.
    _state: Arc<Mutex<ServerState>>,
}

impl MockUpdateServer {
    pub async fn start(response: MockResponse, signing_key: Arc<minisign::SecretKey>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let binary_response = match &response {
            MockResponse::Available {
                binary_fails: true, ..
            } => BinaryResponse::Failing,
            _ => BinaryResponse::Ok,
        };

        let state = Arc::new(Mutex::new(ServerState {
            response,
            binary_response,
            signing_key,
            binary_url: format!("http://127.0.0.1:{port}/binary"),
        }));

        let router = Router::new()
            .route("/update", get(handle_update))
            .route("/binary", get(handle_binary))
            .with_state(state.clone());

        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        Self {
            port,
            _state: state,
        }
    }

    pub fn update_url(&self) -> String {
        format!("http://127.0.0.1:{}/update", self.port)
    }
}

async fn handle_update(State(state): State<Arc<Mutex<ServerState>>>) -> Response {
    let (response, signing_key, binary_url) = {
        let guard = state.lock().unwrap();
        (
            guard.response.clone(),
            guard.signing_key.clone(),
            guard.binary_url.clone(),
        )
    };

    match response {
        MockResponse::NoUpdate => StatusCode::NO_CONTENT.into_response(),
        MockResponse::MalformedJson => (StatusCode::OK, "{not json").into_response(),
        MockResponse::WrongShapeJson => (
            StatusCode::OK,
            axum::Json(serde_json::json!({ "no_version_field": true })),
        )
            .into_response(),
        MockResponse::NotFound => StatusCode::NOT_FOUND.into_response(),
        // Never actually reached -- the caller points `update_feed_url`
        // at a dead port/an unparseable URL instead of this server.
        MockResponse::ConnectionRefused | MockResponse::InvalidFeedUrl => {
            StatusCode::NOT_FOUND.into_response()
        }
        MockResponse::StaticNoMatchingTarget => {
            let body = serde_json::json!({
                "version": "2.0.0",
                "platforms": {
                    "no-such-os-arch": { "url": "http://127.0.0.1:1/unused", "signature": "" }
                }
            });
            (StatusCode::OK, axum::Json(body)).into_response()
        }
        MockResponse::Available {
            version,
            notes,
            signed_version,
            corrupt_signature,
            ..
        } => {
            let signature = build_signature(SignatureOptions {
                signing_key: &signing_key,
                announced_version: &version,
                signed_version_override: signed_version.as_deref(),
                corrupt: corrupt_signature,
            });
            let body = serde_json::json!({
                "version": version,
                "notes": notes,
                "pub_date": "2026-01-01T00:00:00Z",
                "url": binary_url,
                "signature": signature,
            });
            (StatusCode::OK, axum::Json(body)).into_response()
        }
    }
}

async fn handle_binary(State(state): State<Arc<Mutex<ServerState>>>) -> Response {
    let binary_response = state.lock().unwrap().binary_response;

    match binary_response {
        // Streamed as separate chunks with a real delay between them --
        // a single-write response arrives as one chunk, too fast for a
        // scenario to ever observe an intermediate `Downloading` state.
        BinaryResponse::Ok => {
            let chunks = [
                b"fake-".as_slice(),
                b"update-".as_slice(),
                b"bytes".as_slice(),
            ];
            let total_len: usize = chunks.iter().map(|chunk| chunk.len()).sum();
            let body = Body::from_stream(stream::iter(chunks).then(|chunk| async move {
                tokio::time::sleep(Duration::from_millis(150)).await;
                Ok::<_, std::io::Error>(Bytes::from_static(chunk))
            }));
            // A streamed body has no Content-Length by default -- without
            // one, the client can't report download progress at all.
            (
                StatusCode::OK,
                [(axum::http::header::CONTENT_LENGTH, total_len.to_string())],
                body,
            )
                .into_response()
        }
        BinaryResponse::Failing => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

struct SignatureOptions<'a> {
    signing_key: &'a minisign::SecretKey,
    announced_version: &'a str,
    signed_version_override: Option<&'a str>,
    corrupt: Option<CorruptSignature>,
}

/// The actual signed content never matters to these scenarios -- only
/// whether the signature and trusted comment validate against it.
fn build_signature(options: SignatureOptions<'_>) -> String {
    if let Some(corrupt) = options.corrupt {
        return match corrupt {
            CorruptSignature::NotBase64 => "not-valid-base64!!!".to_string(),
            CorruptSignature::NotAMinisignBox => BASE64.encode(b"definitely not a minisign box"),
            CorruptSignature::NotUtf8 => BASE64.encode([0xff, 0xfe, 0x80, 0x80]),
        };
    }

    let signed_version = options
        .signed_version_override
        .unwrap_or(options.announced_version);
    let trusted_comment = format!("timestamp:0\tfile:update.bin\tversion:{signed_version}");
    let signature_box = minisign::sign(
        None,
        options.signing_key,
        b"fake-update-bytes".as_slice(),
        Some(&trusted_comment),
        None,
    )
    .expect("signing a fixed test payload never fails");

    BASE64.encode(signature_box.into_string())
}
