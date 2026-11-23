//! The harness's own normative wire-protocol contract, proxying adapter
//! commands to a dedicated child process per adapter.

use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use tokio::io::AsyncBufReadExt;

use crate::wire_types::{
    Capabilities, CreateAdapterRequest, CreateAdapterResponse, JitterFractionRequest,
};
use crate::WORKER_ENV_VAR;

struct ChildAdapter {
    child: tokio::process::Child,
    port: u16,
    directory: std::path::PathBuf,
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

/// Each adapter owns an isolated native runtime and release-feed fixture.
async fn create_adapter(
    State(state): State<RoutesState>,
    Json(payload): Json<CreateAdapterRequest>,
) -> Response {
    let directory =
        std::env::temp_dir().join(format!("barrel-tauri-runtime-{}", uuid::Uuid::new_v4()));
    let config_json = serde_json::json!({
        "directory": directory,
        "checkInBaseUrl": payload.check_in_base_url,
        "publicKeyPem": payload.public_key_pem,
        "channel": payload.channel,
        "attributes": payload.attributes,
        "debug": payload.debug,
        "checkInIntervalMs": payload.check_in_interval_ms,
        "lifecycle": payload.lifecycle,
        "installationPersistence": payload.installation_persistence,
    })
    .to_string();

    let exe = std::env::current_exe().expect("current_exe resolves");
    let mut child = match tokio::process::Command::new(exe)
        .env(WORKER_ENV_VAR, &config_json)
        .stdout(std::process::Stdio::piped())
        .kill_on_drop(true)
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
                let _ = tokio::fs::remove_dir_all(&directory).await;
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("worker printed a non-port line: {line:?}"),
                )
                    .into_response();
            }
        },
        _ => {
            let _ = child.kill().await;
            let _ = tokio::fs::remove_dir_all(&directory).await;
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "worker never printed its port",
            )
                .into_response();
        }
    };

    let id = uuid::Uuid::new_v4().to_string();
    state.adapters.lock().await.insert(
        id.clone(),
        ChildAdapter {
            child,
            port,
            directory,
        },
    );

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
        let _ = tokio::fs::remove_dir_all(adapter.directory).await;
    }
    // The endpoint declares no success schema, so the client expects
    // 204, not 200 -- confirmed against a real HttpApiClient decode error.
    StatusCode::NO_CONTENT.into_response()
}

async fn jitter_fraction(Json(payload): Json<JitterFractionRequest>) -> Response {
    Json(serde_json::json!({
        "fraction": tauri_plugin_barrel_kit::test_service::runtime::jitter_fraction(&payload.installation_id)
    })).into_response()
}
