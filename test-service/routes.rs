//! The harness's own normative wire-protocol contract, proxying adapter
//! commands to a dedicated child process per adapter.

use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use tokio::io::AsyncBufReadExt;

use crate::mock_update_server::MockResponse;
use crate::wire_types::{
    Capabilities, CreateAdapterRequest, CreateAdapterResponse, JitterFractionRequest,
};
use crate::WORKER_ENV_VAR;

struct ChildAdapter {
    child: tokio::process::Child,
    port: u16,
}

type Adapters = std::sync::Arc<tokio::sync::Mutex<std::collections::HashMap<String, ChildAdapter>>>;

#[derive(Clone)]
struct RoutesState {
    adapters: Adapters,
    http: reqwest::Client,
}

pub async fn run() {
    let state = RoutesState {
        adapters: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        http: reqwest::Client::new(),
    };

    let router = Router::new()
        .route("/", get(capabilities))
        .route("/adapters", post(create_adapter))
        .route("/adapters/{id}/commands", post(run_command))
        .route("/adapters/{id}", delete(destroy_adapter))
        .route("/pure/jitter-fraction", post(jitter_fraction))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    println!("tauri test-service listening on http://127.0.0.1:{port}");

    axum::serve(listener, router).await.unwrap();
}

async fn capabilities() -> Json<Capabilities> {
    Json(Capabilities {
        sdk_name: "tauri",
        sdk_version: env!("CARGO_PKG_VERSION"),
        supports_check_in: true,
    })
}

/// Spawns a worker process for this one adapter, configured via `variant`
/// -- JSON-encoding the `MockResponse` the worker's own mock update
/// server serves to its one automatic check.
async fn create_adapter(
    State(state): State<RoutesState>,
    Json(payload): Json<CreateAdapterRequest>,
) -> Response {
    let variant_json = payload
        .variant
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| r#"{"kind":"noUpdate"}"#.to_string());

    // Bounded well above any real MockResponse's encoded size -- rejects a
    // pathological payload before it ever reaches the deserializer.
    const MAX_VARIANT_JSON_LEN: usize = 4096;
    if variant_json.len() > MAX_VARIANT_JSON_LEN {
        return (
            StatusCode::BAD_REQUEST,
            "variant JSON exceeds the size limit",
        )
            .into_response();
    }

    // Deserialize straight into the strict, validated type -- never hold
    // onto the untyped `Value` this parses through, so nothing downstream
    // can see a field this type doesn't itself constrain.
    let mock_response: MockResponse = match serde_json::from_str(&variant_json) {
        Ok(mock_response) => mock_response,
        Err(error) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("invalid variant JSON: {error}"),
            )
                .into_response()
        }
    };

    // The check-in `baseUrl`/`publicKeyPem` every adapter now always
    // carries are passed through here unchanged, regardless of `variant`.
    let config_json = serde_json::json!({
        "mockResponse": mock_response,
        "checkInBaseUrl": payload.check_in_base_url,
        "publicKeyPem": payload.public_key_pem,
    })
    .to_string();

    let exe = std::env::current_exe().expect("current_exe resolves");
    let mut child = match tokio::process::Command::new(exe)
        .env(WORKER_ENV_VAR, &config_json)
        .stdout(std::process::Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(error) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("spawn failed: {error}"),
            )
                .into_response()
        }
    };

    let stdout = child.stdout.take().expect("piped stdout is present");
    let mut lines: tokio::io::Lines<_> = tokio::io::BufReader::new(stdout).lines();
    let port = match tokio::time::timeout(Duration::from_secs(10), lines.next_line()).await {
        Ok(Ok(Some(line))) => match line.trim().parse::<u16>() {
            Ok(port) => port,
            Err(_) => {
                let _ = child.kill().await;
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("worker printed a non-port line: {line:?}"),
                )
                    .into_response();
            }
        },
        _ => {
            let _ = child.kill().await;
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "worker never printed its port",
            )
                .into_response();
        }
    };

    let id = uuid::Uuid::new_v4().to_string();
    state
        .adapters
        .lock()
        .await
        .insert(id.clone(), ChildAdapter { child, port });

    Json(CreateAdapterResponse { id }).into_response()
}

async fn run_command(
    State(state): State<RoutesState>,
    Path(id): Path<String>,
    Json(command): Json<serde_json::Value>,
) -> Response {
    let port = {
        let adapters = state.adapters.lock().await;
        let Some(adapter) = adapters.get(&id) else {
            return (StatusCode::NOT_FOUND, "unknown adapter").into_response();
        };
        adapter.port
    };

    let response = state
        .http
        .post(format!("http://127.0.0.1:{port}/command"))
        .json(&command)
        .send()
        .await;

    match response {
        Ok(response) => {
            let status = StatusCode::from_u16(response.status().as_u16())
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            let body = response.text().await.unwrap_or_default();
            (
                status,
                [(axum::http::header::CONTENT_TYPE, "application/json")],
                body,
            )
                .into_response()
        }
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("worker unreachable: {error}"),
        )
            .into_response(),
    }
}

async fn destroy_adapter(State(state): State<RoutesState>, Path(id): Path<String>) -> Response {
    if let Some(mut adapter) = state.adapters.lock().await.remove(&id) {
        let _ = adapter.child.kill().await;
    }
    // The endpoint declares no success schema, so the client expects
    // 204, not 200 -- confirmed against a real HttpApiClient decode error.
    StatusCode::NO_CONTENT.into_response()
}

/// Not implemented against the real adapter -- `jitterFraction` is a pure
/// cross-SDK equivalence check, out of scope for this adapter-mapping work.
async fn jitter_fraction(Json(_payload): Json<JitterFractionRequest>) -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        "tauri test-service: jitterFraction equivalence not yet wired",
    )
        .into_response()
}
