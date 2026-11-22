//! Test-process environment and observation at the native updater boundary.
//! Enabled only for the standalone conformance service.

use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};
use tauri::{AppHandle, Runtime};

use crate::protocol::pre_action_check::Stage;
use crate::AdapterState;

static DIRECTORY: OnceLock<PathBuf> = OnceLock::new();
static ACTIVITY: Mutex<UpdaterActivity> = Mutex::new(UpdaterActivity {
    checks: 0,
    downloads: 0,
    applies: 0,
});

/// Calls made to the native updater, independent of Policy decisions.
#[derive(Clone, Debug, serde::Serialize)]
pub struct UpdaterActivity {
    /// Vendor feed checks.
    pub checks: u64,
    /// Download attempts.
    pub downloads: u64,
    /// Install attempts.
    pub applies: u64,
}

/// Installs isolated storage and an already-trusted verification key.
pub fn configure(directory: PathBuf, key: &str) -> std::io::Result<()> {
    std::fs::create_dir_all(&directory)?;
    std::fs::write(
        directory.join("barrel-trusted-key.json"),
        serde_json::json!({ "publicKeyPem": key }).to_string(),
    )?;
    DIRECTORY.set(directory).expect("one SDK per worker");
    Ok(())
}

pub(crate) fn directory() -> Option<PathBuf> {
    DIRECTORY.get().cloned()
}
pub(crate) fn record_check() {
    ACTIVITY.lock().unwrap().checks += 1;
}
pub(crate) fn record_download() {
    ACTIVITY.lock().unwrap().downloads += 1;
}
pub(crate) fn intercept_install() -> bool {
    if DIRECTORY.get().is_none() {
        return false;
    }
    ACTIVITY.lock().unwrap().applies += 1;
    true
}

/// Returns observations without deriving them from a Policy or adapter state.
pub fn activity() -> UpdaterActivity {
    ACTIVITY.lock().unwrap().clone()
}

/// Awaits the production cadence tick, including its response handler.
pub async fn check_now<R: Runtime>(app: &AppHandle<R>) {
    if let Some(shared) = crate::shared_for(app) {
        crate::cadence::tick(&shared).await;
    }
}

/// Establishes a vendor fixture before the action under test.
pub async fn prepare_action<R: Runtime>(app: &AppHandle<R>, stage: Stage) {
    let shared = crate::shared_for(app).expect("SDK initialized");
    shared.adapter.check_for_updates(app, &shared.facts).await;
    if matches!(stage, Stage::PreApply) {
        shared.adapter.download_update(&shared.facts).await;
    }
    assert!(matches!(
        shared.adapter.state(),
        AdapterState::Available { .. } | AdapterState::Downloaded
    ));
}

/// Awaits the same action implementation invoked by the public entry points.
pub async fn run_action<R: Runtime>(app: &AppHandle<R>, stage: Stage) {
    let shared = crate::shared_for(app).expect("SDK initialized");
    match stage {
        Stage::PreDownload => crate::perform_download(shared).await,
        Stage::PreApply => crate::perform_apply(shared).await,
    }
}
