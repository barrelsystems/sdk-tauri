#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::sync::Mutex;
use tauri::{ipc::Channel, webview::PageLoadEvent, AppHandle, Manager, State, WindowEvent};
use tauri_plugin_barrel_kit::{updates, BarrelKitExt, InitConfig};

type StatusRelay = Mutex<Option<tauri::async_runtime::JoinHandle<()>>>;

#[tauri::command]
fn subscribe_updates(
    app: AppHandle,
    channel: Channel<updates::Status>,
    relay: State<'_, StatusRelay>,
) -> Result<(), String> {
    let mut receiver = updates::subscribe(&app).ok_or("SDK not registered")?;
    let mut task = relay.lock().expect("status relay lock poisoned");

    if let Some(previous) = task.take() {
        previous.abort();
    }

    *task = Some(tauri::async_runtime::spawn(async move {
        loop {
            let status = receiver.borrow_and_update().clone();
            println!(
                "[barrel-kit] lifecycleState: {}",
                serde_json::to_string(&status.state).unwrap_or_default()
            );
            if channel.send(status).is_err() || receiver.changed().await.is_err() {
                break;
            }
        }
    }));

    Ok(())
}

#[tauri::command]
async fn apply_update(app: AppHandle) -> Result<(), updates::ActionError> {
    updates::apply(&app).await
}

fn stop_relay(app: &AppHandle) {
    let task = app
        .state::<StatusRelay>()
        .lock()
        .expect("status relay lock poisoned")
        .take();

    if let Some(task) = task {
        task.abort();
    }
}

fn main() {
    let mut context = tauri::generate_context!();

    if let Ok(identifier) = std::env::var("BARREL_TEST_APPLICATION_ID") {
        context.config_mut().identifier = identifier;
    }

    tauri::Builder::default()
        .manage(StatusRelay::default())
        .invoke_handler(tauri::generate_handler![
            subscribe_updates,
            apply_update
        ])
        .barrel_kit(InitConfig {
            credential: std::env::var("BARREL_CREDENTIAL").unwrap_or_default(),
            base_url: std::env::var("BARREL_BASE_URL").ok(),
            debug: true,
            updates: updates::Config {
                update_feed_url: std::env::var("BARREL_UPDATE_FEED_URL").ok(),
                ..Default::default()
            },
        })
        .on_page_load(|webview, payload| {
            if webview.label() == "main" && matches!(payload.event(), PageLoadEvent::Started) {
                stop_relay(webview.app_handle());
            }
        })
        .on_window_event(|window, event| {
            if window.label() == "main" && matches!(event, WindowEvent::Destroyed) {
                stop_relay(window.app_handle());
            }
        })
        .run(context)
        .expect("error while running the Barrel Tauri example");
}
