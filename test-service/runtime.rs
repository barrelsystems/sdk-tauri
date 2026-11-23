//! Owns exactly one `App<MockRuntime>` for its whole life, reached from
//! its own tiny HTTP server via a thread + channel bridge (the app itself
//! is neither `Send` nor `Sync`).

use std::io::Write;

use axum::routing::post;
use axum::Router;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use serde::Deserialize;
use tauri::test::mock_builder;
use tauri_plugin_barrel_kit::test_service;
use tauri_plugin_barrel_kit::updates::protocol::pre_action_check::Stage;
use tauri_plugin_barrel_kit::updates::{self, Failure, State};
use tauri_plugin_barrel_kit::{BarrelKitExt, InitConfig};

use crate::mock_update_server::MockUpdateServer;

/// Isolated worker settings supplied by the service HTTP endpoint.
#[derive(Deserialize)]
struct WorkerConfig {
    attributes: Option<std::collections::HashMap<String, String>>,
    debug: Option<bool>,
    #[serde(rename = "checkInIntervalMs")]
    check_in_interval_ms: Option<u64>,
    lifecycle: Option<bool>,
    #[serde(rename = "installationPersistence")]
    installation_persistence: Option<String>,

    channel: Option<String>,
    directory: std::path::PathBuf,
    #[serde(rename = "checkInBaseUrl")]
    check_in_base_url: String,
    #[serde(rename = "publicKeyPem")]
    public_key_pem: String,
}

#[derive(Clone)]
pub(crate) struct DispatchState {
    pub(crate) request_tx: tokio::sync::mpsc::Sender<ThreadRequest>,
    pub(crate) app: tauri::AppHandle<tauri::test::MockRuntime>,
}

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
pub(crate) enum ThreadRequest {
    GetState(tokio::sync::oneshot::Sender<Option<State>>),
    GetLastError(tokio::sync::oneshot::Sender<Option<Failure>>),
    PrepareAction {
        stage: Stage,
        reply: tokio::sync::oneshot::Sender<()>,
    },
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
        run_adapter_thread(worker_config, signing_key, request_rx, ready_tx)
    });
    let app = ready_rx.await.expect("adapter thread died before starting");
    let state = DispatchState { request_tx, app };
    let router = Router::new()
        .route("/command", post(crate::dispatch::dispatch_command))
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
    config: WorkerConfig,
    signing_key: std::sync::Arc<minisign::SecretKey>,
    mut request_rx: tokio::sync::mpsc::Receiver<ThreadRequest>,
    ready_tx: tokio::sync::oneshot::Sender<tauri::AppHandle<tauri::test::MockRuntime>>,
) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("local runtime builds");

    runtime.block_on(async move {
        test_service::resilience_commands::configure(config.lifecycle.unwrap_or(false));
        if config.installation_persistence.as_deref() == Some("failed") {
            std::fs::create_dir_all(config.directory.join("barrel-installation.json"))
                .expect("failed installation fixture");
        }
        test_service::runtime::configure(config.directory, &config.public_key_pem)
            .expect("isolated storage");
        let mock_server = MockUpdateServer::start(signing_key).await;
        let update_feed_url = mock_server.update_url();

        let config = InitConfig {
            credential: "test-service".to_string(),
            base_url: Some(config.check_in_base_url),
            debug: config.debug.unwrap_or(false),
            updates: tauri_plugin_barrel_kit::updates::Config {
                channel: config.channel,
                attributes: config.attributes.unwrap_or_default(),
                check_in_interval_ms: config.check_in_interval_ms,
                update_feed_url: Some(update_feed_url),
                auto_download: Some(false),
                ..Default::default()
            },
        };

        let builder = mock_builder().barrel_kit(config);
        let app = builder
            .build(crate::test_context())
            .expect("mock app builds");

        test_service::runtime::wait_until_ready(app.handle()).await;
        let _ = ready_tx.send(app.handle().clone());

        while let Some(request) = request_rx.recv().await {
            match request {
                ThreadRequest::PrepareAction { stage, reply } => {
                    mock_server.offer_update();
                    test_service::runtime::prepare_action(app.handle(), stage).await;
                    let _ = reply.send(());
                }
                ThreadRequest::GetState(reply) => {
                    let _ = reply.send(snapshot(app.handle()).map(|status| status.state));
                }
                ThreadRequest::GetLastError(reply) => {
                    let _ = reply.send(snapshot(app.handle()).and_then(|status| status.last_error));
                }
            }
        }

        // `_mock_server` keeps the mock HTTP server alive for this scope.
        drop(mock_server);
    });
}

/// Reads the initial public subscription value for conformance inspection.
pub(crate) fn snapshot<R: tauri::Runtime>(app: &tauri::AppHandle<R>) -> Option<updates::Status> {
    updates::subscribe(app).map(|receiver| receiver.borrow().clone())
}
