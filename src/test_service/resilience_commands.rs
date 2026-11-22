//! Cadence observation and installation persistence controls for conformance.

use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Mutex,
};
use tauri::{AppHandle, Runtime};

static AUTOMATIC: AtomicBool = AtomicBool::new(false);
static INTERVAL: AtomicU64 = AtomicU64::new(0);
static LOGS: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// Selects the real cadence loop for lifecycle scenarios.
pub fn configure(automatic: bool) {
    AUTOMATIC.store(automatic, Ordering::Relaxed);
}

pub(crate) fn automatic_cadence() -> bool {
    AUTOMATIC.load(Ordering::Relaxed)
}

pub(crate) fn record_interval(interval: u64) {
    INTERVAL.store(interval, Ordering::Relaxed);
}

pub(crate) fn record_log(message: &str) {
    LOGS.lock().unwrap().push(message.to_string());
}

/// Observes the interval passed to the production timer and its debug output.
pub fn cadence_state<R: Runtime>(app: &AppHandle<R>) -> serde_json::Value {
    let in_flight =
        crate::shared_for(app).is_some_and(|shared| shared.in_flight.load(Ordering::SeqCst));
    serde_json::json!({
        "intervalMs": INTERVAL.load(Ordering::Relaxed),
        "inFlight": in_flight,
        "logs": LOGS.lock().unwrap().clone(),
        "lifecycleEvents": ["foreground"]
    })
}

/// Drives the production window-focus handler; the mock runtime cannot emit OS focus events.
pub fn trigger_lifecycle(event: &str) -> bool {
    if event != "foreground" {
        return false;
    }
    crate::handle_window_event(&tauri::WindowEvent::Focused(true));
    true
}

/// Restores writable storage without invoking any production persistence method.
pub fn restore_installation_persistence<R: Runtime>(app: &AppHandle<R>) -> serde_json::Value {
    let shared = crate::shared_for(app).expect("SDK initialized");
    std::fs::remove_dir_all(&shared.installation_id_path).expect("failed installation fixture");
    serde_json::Value::Null
}

/// Observes disk only after the production runtime's already-scheduled save completes.
pub async fn persisted_installation_id<R: Runtime>(app: &AppHandle<R>) -> serde_json::Value {
    let shared = crate::shared_for(app).expect("SDK initialized");
    shared.installation_ids.wait_for_pending_persistence().await;
    std::fs::read(&shared.installation_id_path)
        .ok()
        .and_then(|data| serde_json::from_slice::<serde_json::Value>(&data).ok())
        .and_then(|value| value.get("installationId").cloned())
        .unwrap_or(serde_json::Value::Null)
}
