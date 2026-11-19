#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use tauri_plugin_barrel_kit::BarrelKitExt;
use serde::Serialize;
use tauri::{AppHandle, Runtime};

#[derive(Serialize)]
struct Status {
    policy: Option<tauri_plugin_barrel_kit::Policy>,
    last_update_error: Option<tauri_plugin_barrel_kit::UpdateFailure>,
    adapter_state: Option<tauri_plugin_barrel_kit::AdapterState>,
}

#[tauri::command]
fn barrel_get_status<R: Runtime>(app: AppHandle<R>) -> Status {
    Status {
        policy: tauri_plugin_barrel_kit::get_policy(),
        last_update_error: tauri_plugin_barrel_kit::get_last_update_error(&app),
        adapter_state: tauri_plugin_barrel_kit::get_adapter_state(&app),
    }
}

#[tauri::command]
fn barrel_apply_update<R: Runtime>(app: AppHandle<R>) {
    tauri_plugin_barrel_kit::apply_update(&app);
}

#[tauri::command]
fn barrel_download_update<R: Runtime>(app: AppHandle<R>) {
    tauri_plugin_barrel_kit::download_update(&app);
}

// No Playwright-equivalent driver exists for Tauri's webview, so this
// auto-triggers the download itself and reports every transition via the
// same `[barrel-kit] adapterState: ` stdout-marker pattern this SDK's
// Swift sibling's own example app uses.
fn watch_and_auto_download<R: Runtime>(app: &AppHandle<R>) {
    let handle = app.clone();
    tauri::async_runtime::spawn(async move {
        let Some(mut receiver) = tauri_plugin_barrel_kit::subscribe_adapter_state(&handle) else {
            eprintln!("[barrel-kit] subscribe_adapter_state() returned None in setup");
            return;
        };
        loop {
            let state = receiver.borrow_and_update().clone();
            println!(
                "[barrel-kit] adapterState: {}",
                serde_json::to_string(&state).unwrap_or_default()
            );
            if matches!(state, tauri_plugin_barrel_kit::AdapterState::Available { .. }) {
                tauri_plugin_barrel_kit::download_update(&handle);
            }
            if receiver.changed().await.is_err() {
                break;
            }
        }
    });
}

fn main() {
    tauri::Builder::default()
        .barrel_kit(tauri_plugin_barrel_kit::InitConfig {
            credential: std::env::var("BARREL_CREDENTIAL").unwrap_or_default(),
            base_url: std::env::var("BARREL_BASE_URL").ok(),
            update_feed_url: std::env::var("BARREL_UPDATE_FEED_URL").ok(),
            debug: true,
            // Stops at `Available`: this unsigned dev build has no real
            // artifact to install. `watch_and_auto_download` below still
            // exercises the manual trigger, as a real `auto_download: false` host would.
            auto_download: Some(false),
            check_in_interval_ms: None,
            attributes: Default::default(),
            hold_cache_max_age_ms: None,
        })
        .invoke_handler(tauri::generate_handler![
            barrel_get_status,
            barrel_apply_update,
            barrel_download_update
        ])
        .setup(|app| {
            watch_and_auto_download(app.handle());
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running the Barrel Tauri example");
}
