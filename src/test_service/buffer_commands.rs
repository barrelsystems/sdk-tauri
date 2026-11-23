//! Production buffer observations, snapshots, and persistence inspection for conformance.

use tauri::{AppHandle, Runtime};

use super::runtime::directory;
use crate::updates::models::state::UpdateState;

/// An observation fed to the production fact store by conformance scenarios.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind")]
pub enum FactObservation {
    /// A local lifecycle-state transition.
    #[serde(rename = "lifecycleState")]
    LifecycleState {
        /// The observed lifecycle state.
        state: UpdateState,
    },
    /// A local update failure with a display-only title.
    #[serde(rename = "updateError")]
    UpdateError {
        /// The verbatim platform error identifier.
        #[serde(rename = "platformCode")]
        platform_code: String,
    },
}

/// Records through the same store used by the adapter and cadence.
pub async fn record_fact<R: Runtime>(app: &AppHandle<R>, fact: FactObservation) {
    let shared = crate::shared_for(app).expect("SDK initialized");
    match fact {
        FactObservation::LifecycleState { state } => {
            // Mirrors `Adapter::set_state`'s own guard, injected or not.
            if state.is_reportable() {
                shared.facts.record_lifecycle_state(state).await;
            }
        }
        FactObservation::UpdateError { platform_code } => {
            let failure = crate::updates::error_taxonomy::build_failure_state(
                "tauri-updater",
                crate::updates::models::failure::UpdateFailureMode::Unknown,
                crate::updates::models::failure::Phase::Download,
                &platform_code,
            )
            .0;
            shared.facts.record_update_error(failure).await;
        }
    }
}

/// Returns serialized facts without exposing private runtime types.
pub async fn buffered_facts<R: Runtime>(app: &AppHandle<R>) -> serde_json::Value {
    let shared = crate::shared_for(app).expect("SDK initialized");
    serde_json::to_value(shared.facts.snapshot().await).expect("facts serialize")
}

/// Clears the current snapshot to establish an empty fixture.
pub async fn clear_buffer<R: Runtime>(app: &AppHandle<R>) {
    let shared = crate::shared_for(app).expect("SDK initialized");
    shared.facts.clear(&shared.facts.snapshot().await).await;
}

/// Invalidates only the disk copy so exit persistence must restore the live buffer.
pub fn prepare_buffer_exit<R: Runtime>(app: &AppHandle<R>) {
    use tauri_plugin_store::StoreExt;
    let path = directory()
        .expect("test storage")
        .join("barrel-buffered-facts.json");
    app.store(&path)
        .expect("buffer disk store")
        .set("facts", serde_json::json!([]));
    let _ = std::fs::remove_file(path);
}

/// Waits for the production writer's current snapshot to finish its disk attempt.
pub async fn wait_for_buffer_writes<R: Runtime>(app: &AppHandle<R>) {
    if let Some(shared) = crate::shared_for(app) {
        shared.facts.wait_for_pending_writes().await;
    }
}

/// Records a batch through the production store with the requested concurrency.
pub async fn record_facts<R: Runtime>(
    app: &AppHandle<R>,
    facts: Vec<FactObservation>,
    concurrent: bool,
) {
    if concurrent {
        let mut tasks = tokio::task::JoinSet::new();
        for fact in facts {
            let app = app.clone();
            tasks.spawn(async move {
                record_fact(&app, fact).await;
            });
        }

        while let Some(result) = tasks.join_next().await {
            result.expect("fact task completed");
        }
    } else {
        for fact in facts {
            record_fact(app, fact).await;
        }
    }
}

/// Reads the isolated persisted snapshot after pending writes complete.
pub async fn persisted_buffer<R: Runtime>(app: &AppHandle<R>) -> serde_json::Value {
    wait_for_buffer_writes(app).await;
    let path = directory()
        .expect("test storage")
        .join("barrel-buffered-facts.json");
    let data = std::fs::read(path).expect("persisted facts");
    serde_json::from_slice(&data).expect("persisted JSON")
}
