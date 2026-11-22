//! Owns exactly one `App<MockRuntime>` for its whole life, reached from
//! its own tiny HTTP server via a thread + channel bridge (the app itself
//! is neither `Send` nor `Sync`).

use std::io::Write;
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use serde::Deserialize;
use tauri::test::mock_builder;
use tauri_plugin_barrel_kit::protocol::check_in::{send_check_in, CheckInParams};
use tauri_plugin_barrel_kit::protocol::policy::verify_and_parse_policy;
use tauri_plugin_barrel_kit::protocol::pre_action_check::{
    check_pre_action, PreActionCheckParams, Stage,
};
use tauri_plugin_barrel_kit::{
    get_adapter_state, get_last_update_error, subscribe_adapter_state, AdapterState,
    BackendConnection, BarrelKitExt, InitConfig, UpdateFailure,
};

use crate::mock_update_server::{MockResponse, MockUpdateServer};
use crate::wire_types::{AdapterCommand, ResultWrapper};

/// Decoded from `WORKER_ENV_VAR`'s JSON -- the worker's own mock-update
/// variant plus the check-in `baseUrl`/`publicKeyPem` every adapter now
/// always carries, regardless of whether a `variant` was also given.
#[derive(Deserialize)]
struct WorkerConfig {
    #[serde(rename = "mockResponse")]
    mock_response: MockResponse,
    #[serde(rename = "checkInBaseUrl")]
    check_in_base_url: String,
    #[serde(rename = "publicKeyPem")]
    public_key_pem: String,
}

/// Shared axum state for the `/command` handler. `check_in` backs the two
/// commands (`checkNow`, `triggerPreActionCheck`) that talk to the
/// check-in backend directly, bypassing the mock-app adapter thread
/// entirely; `request_tx` is everything else, unchanged.
#[derive(Clone)]
struct DispatchState {
    request_tx: tokio::sync::mpsc::Sender<ThreadRequest>,
    check_in: CheckInState,
}

/// This test-service's own fixed stand-in credential/installation
/// identity for check-in/pre-action-check calls -- the harness never
/// supplies one of its own for these wire calls.
#[derive(Clone)]
struct CheckInState {
    http: reqwest::Client,
    check_in_base_url: String,
    public_key_pem: String,
    /// Fresh per worker process -- every adapter instance is its own
    /// process in this SDK, so this is this instance's own identity, the
    /// same way the other SDKs' test services generate one per instance.
    installation_id: String,
    /// Serializes `checkNow` the same way the real cadence loop's own
    /// `in_flight` flag does -- two concurrent `checkNow` calls must never
    /// reach the backend at the same time.
    check_in_lock: std::sync::Arc<tokio::sync::Mutex<()>>,
}

impl CheckInState {
    fn connection(&self) -> BackendConnection<'_> {
        BackendConnection {
            base_url: &self.check_in_base_url,
            credential: TEST_CREDENTIAL,
            installation_id: &self.installation_id,
        }
    }
}

const TEST_CREDENTIAL: &str = "test-service-credential";

fn noop_log(_message: &str) {}

const SIGNING_KEY_B64: &str = "dW50cnVzdGVkIGNvbW1lbnQ6IHJzaWduIGVuY3J5cHRlZCBzZWNyZXQga2V5ClJXUlRZMEl5U09xQ0dNSi9YeWhyeUFMb214WVlGZVg1bVM1Nk5CSHpycEhSOEFLZDFDRUFBQkFBQUFBQUFBQUFBQUlBQUFBQUlxeklvZ3hOdldra01GOGt6ZlNIbjg4OWl4djRJV3hkeWFnQkRlcHM4TGhrSXo2QVZ6U0F6MFpBcDk5YWFEMkt1c3JKSnFXb1lEeERVTE1yTklNRUxVWXNwa1k4UjE3SlZzNStROWg0TG9FN1lSTUg1YkorZ0RMRGIrQXpMOVh0TDV2NWhVRXIyWFk9Cg==";

fn fixed_signing_key() -> minisign::SecretKey {
    let key_text = String::from_utf8(
        BASE64
            .decode(SIGNING_KEY_B64)
            .expect("fixed test keypair decodes"),
    )
    .unwrap();
    // Tauri's own CLI (`signer generate`) always writes an encrypted-format
    // key, using an empty password when none is given -- not a true
    // unencrypted (KDF_NONE) key, despite the "without password" message.
    minisign::SecretKey::from_box(
        minisign::SecretKeyBox::from_string(&key_text).expect("fixed test keypair parses"),
        Some(String::new()),
    )
    .expect("fixed test keypair was generated with an empty password")
}

/// One message sent from the HTTP handler's own task to the dedicated
/// thread that owns the real adapter -- distinct from `AdapterCommand`,
/// which is the wire-level shape a request body decodes into.
enum ThreadRequest {
    GetState(tokio::sync::oneshot::Sender<Option<AdapterState>>),
    GetLastError(tokio::sync::oneshot::Sender<Option<UpdateFailure>>),
    TriggerDownload(tokio::sync::oneshot::Sender<Option<AdapterState>>),
    StartDownload(tokio::sync::oneshot::Sender<Option<AdapterState>>),
}

pub async fn run(config_json: String) {
    let worker_config: WorkerConfig =
        serde_json::from_str(&config_json).expect("parent already validated this JSON");
    let signing_key = std::sync::Arc::new(fixed_signing_key());

    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    // `tokio::sync::mpsc`, not `std::sync::mpsc` -- a sync `recv()` blocks
    // the adapter thread's whole runtime between commands, starving the
    // Tauri-spawned download task of any chance to progress while idle.
    let (request_tx, request_rx) = tokio::sync::mpsc::channel::<ThreadRequest>(8);

    std::thread::spawn(move || {
        run_adapter_thread(
            worker_config.mock_response,
            signing_key,
            request_rx,
            ready_tx,
        )
    });
    ready_rx.await.expect("adapter thread died before starting");

    let state = DispatchState {
        request_tx,
        check_in: CheckInState {
            http: reqwest::Client::new(),
            check_in_base_url: worker_config.check_in_base_url,
            public_key_pem: worker_config.public_key_pem,
            installation_id: uuid::Uuid::new_v4().to_string(),
            check_in_lock: std::sync::Arc::new(tokio::sync::Mutex::new(())),
        },
    };
    let router = Router::new()
        .route("/command", post(dispatch_command))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    println!("{port}");
    std::io::stdout().flush().ok();

    axum::serve(listener, router).await.unwrap();
}

/// Owns the `App<MockRuntime>` for this worker's whole lifetime, on its
/// own thread with its own single-threaded runtime -- never shared.
fn run_adapter_thread(
    mock_response: MockResponse,
    signing_key: std::sync::Arc<minisign::SecretKey>,
    mut request_rx: tokio::sync::mpsc::Receiver<ThreadRequest>,
    ready_tx: tokio::sync::oneshot::Sender<()>,
) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("local runtime builds");

    runtime.block_on(async move {
        let update_feed_url = match &mock_response {
            MockResponse::ConnectionRefused => Some("http://127.0.0.1:1/unreachable".to_string()),
            MockResponse::InvalidFeedUrl => Some("not a valid url at all".to_string()),
            _ => None,
        };
        let mock_server = MockUpdateServer::start(mock_response, signing_key).await;
        let update_feed_url = update_feed_url.unwrap_or_else(|| mock_server.update_url());

        let config = InitConfig {
            credential: "test-credential".to_string(),
            base_url: Some("http://127.0.0.1:1/unreachable".to_string()),
            update_feed_url: Some(update_feed_url),
            // Off, unlike the real default -- otherwise a found update
            // races straight past `Available` into downloading within
            // the same tick, before a scenario can observe it stable.
            auto_download: Some(false),
            ..Default::default()
        };

        let builder = mock_builder().barrel_kit(config);
        let app = builder
            .build(tauri::generate_context!("test-service/tauri.conf.json"))
            .expect("mock app builds");

        let mut state_rx = subscribe_until_available(app.handle()).await;
        // Marks the pre-trigger value "seen" so `wait_until_settled`'s own
        // `changed()` check can tell "already settled" apart from "hasn't
        // moved yet", even if the real transition circles back to this value.
        state_rx.borrow_and_update();
        wait_until_settled(&mut state_rx).await;
        let _ = ready_tx.send(());

        while let Some(request) = request_rx.recv().await {
            match request {
                ThreadRequest::GetState(reply) => {
                    let _ = reply.send(get_adapter_state(app.handle()));
                }
                ThreadRequest::GetLastError(reply) => {
                    let _ = reply.send(get_last_update_error(app.handle()));
                }
                ThreadRequest::TriggerDownload(reply) => {
                    state_rx.borrow_and_update();
                    tauri_plugin_barrel_kit::download_update(app.handle());
                    wait_until_settled(&mut state_rx).await;
                    let _ = reply.send(get_adapter_state(app.handle()));
                }
                // Replies as soon as the download starts, not once it
                // settles -- lets a scenario poll `getAdapterState`
                // afterward and observe `Downloading`/`progress` for real.
                ThreadRequest::StartDownload(reply) => {
                    state_rx.borrow_and_update();
                    tauri_plugin_barrel_kit::download_update(app.handle());
                    let _ = state_rx.changed().await;
                    let _ = reply.send(get_adapter_state(app.handle()));
                }
            }
        }

        // `_mock_server` keeps the mock HTTP server alive for this scope.
        drop(mock_server);
    });
}

/// The plugin's `setup` hook registers the adapter on a separately
/// spawned task, so a subscribe attempt right after `build()` can still
/// be too early -- retried until it succeeds.
async fn subscribe_until_available(
    handle: &tauri::AppHandle<tauri::test::MockRuntime>,
) -> tokio::sync::watch::Receiver<AdapterState> {
    loop {
        if let Some(receiver) = subscribe_adapter_state(handle) {
            return receiver;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Waits past any transitional state (`Checking`/`Downloading`) to the
/// next stable one. The caller must have already marked the pre-trigger
/// value "seen" via `borrow_and_update` -- see `run_adapter_thread`.
async fn wait_until_settled(state_rx: &mut tokio::sync::watch::Receiver<AdapterState>) {
    let in_transit =
        |s: &AdapterState| matches!(s, AdapterState::Checking | AdapterState::Downloading { .. });

    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if state_rx.changed().await.is_err() {
                return;
            }
            if !in_transit(&state_rx.borrow_and_update()) {
                return;
            }
        }
    })
    .await;
}

async fn dispatch_command(
    State(state): State<DispatchState>,
    Json(command): Json<AdapterCommand>,
) -> Response {
    match command {
        AdapterCommand::GetAdapterState => {
            send_adapter_request(&state.request_tx, ThreadRequest::GetState).await
        }
        AdapterCommand::GetLastUpdateError => {
            send_adapter_request(&state.request_tx, ThreadRequest::GetLastError).await
        }
        AdapterCommand::InjectEvent { event_name, .. } if event_name == "triggerDownload" => {
            send_adapter_request(&state.request_tx, ThreadRequest::TriggerDownload).await
        }
        AdapterCommand::InjectEvent { event_name, .. } if event_name == "startDownload" => {
            send_adapter_request(&state.request_tx, ThreadRequest::StartDownload).await
        }
        AdapterCommand::InjectEvent { event_name, .. } => (
            StatusCode::BAD_REQUEST,
            format!("tauri test-service: unknown injectEvent eventName {event_name:?}"),
        )
            .into_response(),
        // Exercises the check-in call itself, not update-lifecycle state --
        // `getAdapterState` keeps answering from the mock app's own real
        // state regardless of what this check-in resolves to.
        AdapterCommand::CheckNow => check_now(&state.check_in).await,
        AdapterCommand::TriggerPreActionCheck { stage } => {
            trigger_pre_action_check(&state.check_in, &stage).await
        }
    }
}

/// `checkNow`: sends one real check-in against the stored
/// `checkInBaseUrl` and verifies the response against the stored
/// `publicKeyPem`.
async fn check_now(check_in: &CheckInState) -> Response {
    // Awaited in full before replying -- a scenario calling
    // `getAdapterState` right after this call returns expects it already done.
    let _guard = check_in.check_in_lock.lock().await;
    let attributes = std::collections::HashMap::new();
    let response = send_check_in(
        &check_in.http,
        CheckInParams {
            connection: check_in.connection(),
            sdk_version: env!("CARGO_PKG_VERSION"),
            channel: "stable",
            observed_version: env!("CARGO_PKG_VERSION"),
            platform: "test",
            arch: "test",
            os_version: "test",
            attributes: &attributes,
            adapter_state: AdapterState::Idle,
            buffered_facts: Vec::new(),
        },
        &noop_log,
    )
    .await;

    // Fails open on a hung/failing/unverifiable check-in, same as the
    // real cadence loop -- there's nothing else for this command to do
    // with the resolved policy, since it doesn't drive adapter state here.
    let _policy = response.and_then(|r| verify_and_parse_policy(&r, &check_in.public_key_pem));

    Json(ResultWrapper {
        result: serde_json::Value::Null,
    })
    .into_response()
}

/// `triggerPreActionCheck`: sends one real pre-action-check for `stage`
/// and reports the resolved policy's own `type` tag as `result`, e.g.
/// `"held"` -- a failed/unverifiable response fails open, reporting `"current"`.
async fn trigger_pre_action_check(check_in: &CheckInState, stage: &str) -> Response {
    let stage = match stage {
        "pre-download" => Stage::PreDownload,
        "pre-apply" => Stage::PreApply,
        other => {
            return (
                StatusCode::BAD_REQUEST,
                format!("tauri test-service: unknown pre-action-check stage {other:?}"),
            )
                .into_response()
        }
    };

    let policy = check_pre_action(
        &check_in.http,
        PreActionCheckParams {
            connection: check_in.connection(),
            stage,
            trusted_key_pem: &check_in.public_key_pem,
        },
        &noop_log,
    )
    .await;

    // A failed/unverifiable check-in fails open, same as `host_control.rs`
    // elsewhere -- reported as the `current` Policy's own tag, not `null`.
    let result = policy
        .and_then(|policy| serde_json::to_value(&policy).ok())
        .and_then(|value| {
            value
                .get("type")
                .and_then(|t| t.as_str().map(str::to_string))
        })
        .or_else(|| Some("current".to_string()));

    Json(ResultWrapper { result }).into_response()
}

/// Shared shape behind every `AdapterCommand` that round-trips to the
/// adapter thread: build a reply channel, send the request it constructs,
/// await the reply, and wrap it as this protocol's own `{ result }` body.
async fn send_adapter_request<T: serde::Serialize>(
    request_tx: &tokio::sync::mpsc::Sender<ThreadRequest>,
    make_request: impl FnOnce(tokio::sync::oneshot::Sender<Option<T>>) -> ThreadRequest,
) -> Response {
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    if request_tx.send(make_request(reply_tx)).await.is_err() {
        return (StatusCode::INTERNAL_SERVER_ERROR, "adapter thread gone").into_response();
    }
    let result: Option<T> = reply_rx.await.unwrap_or(None);
    Json(ResultWrapper { result }).into_response()
}
