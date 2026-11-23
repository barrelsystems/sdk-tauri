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

struct ServerState {
    available: bool,
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
    pub async fn start(signing_key: Arc<minisign::SecretKey>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let state = Arc::new(Mutex::new(ServerState {
            available: false,
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

    pub fn offer_update(&self) {
        self._state.lock().unwrap().available = true;
    }

    pub fn update_url(&self) -> String {
        format!("http://127.0.0.1:{}/update", self.port)
    }
}

async fn handle_update(State(state): State<Arc<Mutex<ServerState>>>) -> Response {
    let (available, signing_key, binary_url) = {
        let state = state.lock().unwrap();
        (
            state.available,
            state.signing_key.clone(),
            state.binary_url.clone(),
        )
    };
    if !available {
        return StatusCode::NO_CONTENT.into_response();
    }

    let trusted_comment = "timestamp:0\tfile:update.bin\tversion:2.0.0";
    let signature = minisign::sign(
        None,
        &signing_key,
        b"fake-update-bytes".as_slice(),
        Some(trusted_comment),
        None,
    )
    .expect("fixed fixture signing succeeds");
    axum::Json(serde_json::json!({
        "version": "2.0.0", "pub_date": "2026-01-01T00:00:00Z", "url": binary_url,
        "signature": BASE64.encode(signature.into_string()),
    }))
    .into_response()
}

async fn handle_binary(State(state): State<Arc<Mutex<ServerState>>>) -> Response {
    let _ = state;
    // Separate chunks expose genuine intermediate progress to the observer.
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
